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
    interval = Keyword.get(opts, :interval, div(lease_duration, 3))
    extension = Keyword.get(opts, :extension, lease_duration)
    notify = Keyword.get(opts, :notify, self())
    clock = Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end)

    spawn_link(fn ->
      loop(repo, root, lease, interval, extension, notify, clock)
    end)
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
  defp loop(repo, root, lease, interval, extension, notify, clock) do
    case remaining_ms(lease, clock) do
      0 ->
        report_loss(lease, notify, :lease_expired)

      remaining_ms ->
        receive do
          :stop ->
            :ok
        after
          min(interval, remaining_ms) ->
            case extend_lease(repo, root, lease, extension, clock) do
              {:ok, updated_lease} ->
                loop(repo, root, updated_lease, interval, extension, notify, clock)

              {:retry, reason} ->
                Logger.warning(
                  "Failed to extend lease for item #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}; will retry while the lease remains valid"
                )

                loop(repo, root, lease, interval, extension, notify, clock)

              {:lost, reason} ->
                report_loss(lease, notify, reason)
            end
        end
    end
  end

  # Extends the lease. Missing/mismatched storage and lease expiry prove the
  # worker no longer has an exclusive right to execute; transaction failures do
  # not and are retried until the expiry deadline.
  defp extend_lease(repo, root, lease, extension, clock) do
    result =
      repo.transact(fn ->
        Store.extend_lease(repo, root, lease, extension, now: clock.())
      end)

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
