defmodule Bedrock.JobQueue.Consumer.LeaseExtender do
  @moduledoc """
  Periodically extends a lease while a job is being processed.

  Per [QuiCK paper](https://www.foundationdb.org/files/QuiCK.pdf) Algorithm 3:
  Workers should extend leases in parallel with job execution to prevent
  long-running jobs from losing their lease.

  ## Behavior

  - Started via `start/5` which spawns a linked process
  - The process loops, extending the lease at regular intervals
  - Stopped via `stop/1` when the job completes (sends `:stop` message)
  - Uses `spawn_link` so the extender dies if the parent job process crashes
  - Transient extension failures are retried only until the current lease expires
  - Expiry, a missing lease, a lease mismatch, or a removed item is reported to
    the owner and stops the extender. The owner must cancel the running job
    handler.

  ## Timing

  By default, the extender runs every `lease_duration / 3` milliseconds and
  extends the lease by `lease_duration` milliseconds. This ensures the lease
  is extended well before expiration even if one extension attempt fails.
  """

  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store

  require Logger

  @doc """
  Starts a lease extender process.

  Options:
  - `:interval` - How often to extend the lease in ms (default: lease_duration / 3)
  - `:extension` - How much to extend by in ms (default: lease_duration)
  - `:notify` - Process to receive `{:lease_lost, lease_id, reason}` when
    ownership is conclusively lost (default: the calling process)
  - `:clock` - Zero-argument function returning the current time in ms
    (default: system time)

  Returns the pid of the extender process.
  """
  @spec start(module(), term(), Lease.t(), pos_integer(), keyword()) :: pid()
  def start(repo, root, lease, lease_duration, opts \\ []) do
    start_extender(repo, root, lease, lease_duration, opts, nil)
  end

  @doc false
  @spec start_ready(module(), term(), Lease.t(), pos_integer(), keyword()) ::
          {:ok, pid()} | {:error, :lease_expired}
  def start_ready(repo, root, lease, lease_duration, opts \\ []) do
    ready_ref = make_ref()
    pid = start_extender(repo, root, lease, lease_duration, opts, {self(), ready_ref})

    receive do
      {:lease_extender_ready, ^ready_ref, ^pid, :ok} -> {:ok, pid}
      {:lease_extender_ready, ^ready_ref, ^pid, {:error, reason}} -> {:error, reason}
    end
  end

  defp start_extender(repo, root, lease, lease_duration, opts, starter) do
    interval = Keyword.get(opts, :interval, div(lease_duration, 3))
    extension = Keyword.get(opts, :extension, lease_duration)
    notify = Keyword.get(opts, :notify, self())
    clock = Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end)
    owner = self()

    # Worker must not release a handler until its watchdog has actually
    # observed a live lease. Merely spawning it leaves a scheduler gap where a
    # lease can expire before the watchdog gets its first turn.
    spawn_link(fn ->
      Process.flag(:trap_exit, true)

      case remaining_ms(lease, clock) do
        0 ->
          reply_ready(starter, {:error, :lease_expired})
          report_loss(lease, notify, :lease_expired)

        _ ->
          reply_ready(starter, :ok)
          loop(repo, root, lease, interval, extension, notify, clock, owner)
      end
    end)
  end

  defp reply_ready(nil, _result), do: :ok

  defp reply_ready({starter, ready_ref}, result) do
    send(starter, {:lease_extender_ready, ready_ref, self(), result})
    :ok
  end

  @doc """
  Stops a lease extender process.
  """
  @spec stop(pid()) :: :ok
  def stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      send(pid, :stop)
    end

    :ok
  end

  # Main loop - waits for interval, extends lease, repeats
  defp loop(repo, root, lease, interval, extension, notify, clock, owner) do
    case remaining_ms(lease, clock) do
      0 ->
        report_loss(lease, notify, :lease_expired)

      remaining_ms ->
        receive do
          :stop ->
            :ok

          {:EXIT, ^owner, _reason} ->
            :ok

          {:EXIT, _pid, :normal} ->
            loop(repo, root, lease, interval, extension, notify, clock, owner)

          {:EXIT, _pid, _reason} ->
            loop(repo, root, lease, interval, extension, notify, clock, owner)
        after
          min(interval, remaining_ms) ->
            renew_or_expire(repo, root, lease, interval, extension, notify, clock, owner)
        end
    end
  end

  defp renew_or_expire(repo, root, lease, interval, extension, notify, clock, owner) do
    case remaining_ms(lease, clock) do
      0 -> report_loss(lease, notify, :lease_expired)

      _ ->
        handle_renewal(
          await_renewal(repo, root, lease, extension, clock, owner),
          {repo, root, lease, interval, extension, notify, clock, owner}
        )
    end
  end

  defp handle_renewal({:ok, updated_lease}, {repo, root, _lease, interval, extension, notify, clock, owner}),
    do: loop(repo, root, updated_lease, interval, extension, notify, clock, owner)

  defp handle_renewal({:retry, reason}, {repo, root, lease, interval, extension, notify, clock, owner}) do
    Logger.warning(
      "Failed to extend lease for item #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}; will retry while the lease remains valid"
    )

    loop(repo, root, lease, interval, extension, notify, clock, owner)
  end

  defp handle_renewal({:lost, reason}, {_repo, _root, lease, _interval, _extension, notify, _clock, _owner}),
    do: report_loss(lease, notify, reason)

  defp handle_renewal(:stopped, {_repo, _root, _lease, _interval, _extension, _notify, _clock, _owner}), do: :ok

  # A renewal must not hide the lease deadline. Run it in a linked, monitored
  # process so expiry (or stop) can kill a stalled transaction without leaving
  # work behind. The link preserves the Worker -> Extender -> renewal lifetime.
  defp await_renewal(repo, root, lease, extension, clock, owner) do
    result_ref = make_ref()
    parent = self()

    pid =
      spawn_link(fn ->
        send(parent, {:renewal_result, result_ref, extend_lease(repo, root, lease, extension, clock)})
      end)

    monitor = Process.monitor(pid)

    wait_for_renewal(pid, monitor, result_ref, lease, clock, owner)
  end

  defp wait_for_renewal(pid, monitor, result_ref, lease, clock, owner) do
    remaining_ms = remaining_ms(lease, clock)

    receive do
      :stop ->
        stop_renewal(pid, monitor)
        :stopped

      {:EXIT, ^owner, _reason} ->
        stop_renewal(pid, monitor)
        :stopped

      {:renewal_result, ^result_ref, result} ->
        Process.unlink(pid)
        Process.demonitor(monitor, [:flush])
        result

      {:EXIT, ^pid, :normal} ->
        wait_for_renewal(pid, monitor, result_ref, lease, clock, owner)

      {:EXIT, ^pid, reason} ->
        Process.demonitor(monitor, [:flush])
        {:retry, {:renewal_task_exit, reason}}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        Process.unlink(pid)
        {:retry, {:renewal_task_exit, reason}}
    after
      remaining_ms ->
        stop_renewal(pid, monitor)
        {:lost, :lease_expired}
    end
  end

  defp stop_renewal(pid, monitor) do
    Process.unlink(pid)

    if Process.alive?(pid) do
      Process.exit(pid, :kill)
      receive do
        {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
      end
    end

    Process.demonitor(monitor, [:flush])
    :ok
  end

  # Extends the lease. Missing/mismatched storage and lease expiry prove the
  # worker no longer has an exclusive right to execute; transaction failures and
  # exceptions do not and are retried until the expiry deadline.
  defp extend_lease(repo, root, lease, extension, clock) do
    result = transaction_result(repo, fn -> Store.extend_lease(repo, root, lease, extension, clock: clock) end)

    case result do
      {:ok, %Lease{} = updated_lease} ->
        Logger.debug("Extended lease for item #{Base.encode16(lease.item_id, case: :lower)}")
        {:ok, updated_lease}

      {:error, reason}
      when reason in [:lease_expired, :lease_not_found, :lease_mismatch, :item_not_found] ->
        {:lost, reason}

      {:error, reason} ->
        {:retry, reason}
    end
  end

  # An unavailable repository cannot establish that the lease was lost. Keep
  # the linked extender alive so it can retry until expiry, where ownership is
  # conclusively no longer safe to assume.
  defp transaction_result(repo, callback) do
    repo.transact(callback)
  rescue
    exception -> {:error, {:exception, exception}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp remaining_ms(lease, clock) do
    max(0, lease.expires_at - clock.())
  end

  defp report_loss(lease, notify, reason) do
    Logger.warning(
      "Failed to extend lease for item #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}; lease is lost"
    )

    send(notify, {:lease_lost, lease.id, reason})
  end
end
