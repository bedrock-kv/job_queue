defmodule Bedrock.JobQueue.Consumer.Worker do
  @moduledoc """
  Job execution logic.

  Provides the execute/2 function that runs job modules' perform/2 callbacks
  with timeout protection. Used directly by Manager via Task.Supervisor.

  Workers are configured via the JobQueue module's workers map:

      defmodule MyApp.JobQueue do
        use Bedrock.JobQueue,
          otp_app: :my_app,
          repo: MyApp.Repo,
          workers: %{
            "email:send" => MyApp.Jobs.SendEmail,
            "order:process" => MyApp.Jobs.ProcessOrder
          }
      end
  """

  alias Bedrock.JobQueue.Consumer.LeaseExtender
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Payload
  alias Bedrock.JobQueue.Store

  require Logger

  @doc """
  Executes a job and returns the result.

  Called from a Task spawned by Manager. Looks up the handler for the item's
  topic from the workers map and executes it with timeout protection.

  ## Return Values

  Returns the result from the job module's `perform/2` callback, or an error:

  - `:ok` - Job completed successfully, will be removed from queue
  - `{:ok, result}` - Job completed with result (logged but otherwise same as `:ok`)
  - `{:error, reason}` - Job failed, will be requeued with backoff
  - `{:discard, reason}` - Job failed permanently, removed without retry
  - `{:snooze, delay_ms}` - Reschedule for later and count it in retry accounting
  - `{:error, :timeout}` - Job exceeded timeout, will be requeued
  - `{:discard, :no_handler}` - No worker configured for this topic
  - `{:deferred, {:lease_check_unavailable, reason}}` - Ownership could not be
    checked, so the handler was not run and the lease is left unchanged

  ## Timeout

  Jobs are executed with a timeout (default 30 seconds). If the job module
  implements `timeout/0`, that value is used instead. On timeout, the job
  is killed and `{:error, :timeout}` is returned.

  ## Lease boundary

  Before invoking a handler, a worker confirms that it owns an unexpired lease.
  It also kills the handler when the extender reports lease loss or expiry.
  This bounds further handler execution, but cannot undo external side effects
  the handler performed before cancellation. Job handlers must remain
  idempotent when they interact with systems outside the queue transaction.
  """
  @spec execute(Item.t(), map(), keyword()) :: term()
  def execute(%Item{} = item, workers, opts \\ []) when is_map(workers) do
    case lease_context(opts) do
      :none ->
        execute_job(item, workers, nil)

      {:ok, context} ->
        execute_with_verified_lease(item, workers, context)
    end
  end

  defp execute_with_verified_lease(item, workers, %{repo: repo, root: root, lease: lease} = context) do
    case transaction_result(repo, fn -> Store.lease_owned?(repo, root, lease, context.lease_check_opts) end) do
      :ok -> execute_with_lease_guard(item, workers, context)
      {:error, reason} when reason in [:lease_not_found, :lease_mismatch, :lease_expired] ->
        {:cancelled, {:lease_lost, reason}}
      {:error, reason} -> {:deferred, {:lease_check_unavailable, reason}}
    end
  end

  # A failed preflight is not proof that ownership was lost. Returning a
  # deferred result prevents an unavailable repository from consuming retry
  # budget for a handler that never started.
  defp transaction_result(repo, callback) do
    repo.transact(callback)
  rescue
    exception -> {:error, {:exception, exception}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp execute_job(%Item{} = item, workers, lease) do
    case Map.get(workers, item.topic) do
      nil ->
        Logger.warning("No worker configured for topic: #{item.topic}")
        {:discard, :no_handler}

      job_module ->
        execute_with_timeout(job_module, item, lease)
    end
  end

  defp execute_with_lease_guard(item, workers, context) do
    if lease_still_active?(context.lease, context.lease_check_opts) do
      extender =
        LeaseExtender.start(
          context.repo,
          context.root,
          context.lease,
          context.lease_duration,
          Keyword.put(context.lease_extender_opts, :notify, self())
        )

      try do
        execute_job(item, workers, context.lease)
      after
        stop_extender(extender)
      end
    else
      {:cancelled, {:lease_lost, :lease_expired}}
    end
  end

  # The transaction confirms stored ownership at its decision point. Its return
  # can still be delayed, so sample the deadline again immediately before the
  # extender or handler is started.
  defp lease_still_active?(lease, opts), do: lease.expires_at > lease_clock(opts).()

  defp lease_clock(opts) do
    case Keyword.fetch(opts, :clock) do
      {:ok, clock} when is_function(clock, 0) -> clock
      :error -> fn -> Keyword.get(opts, :now) || System.system_time(:millisecond) end
    end
  end

  defp execute_with_timeout(job_module, item, lease) do
    timeout = get_timeout(job_module)
    payload = Payload.decode(item.payload)

    meta = %{
      topic: item.topic,
      queue_id: item.queue_id,
      item_id: item.id,
      attempt: item.error_count + 1
    }

    task =
      Task.async(fn ->
        job_module.perform(payload, meta)
      end)

    await_job(task, timeout, lease)
  rescue
    e ->
      Logger.error("Job execution failed: #{Exception.message(e)}")
      {:error, {:exception, e}}
  end

  defp await_job(task, timeout, nil) do
    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
      {:exit, reason} -> {:error, {:exit, reason}}
    end
  end

  defp await_job(task, timeout, %Lease{id: lease_id}) do
    receive do
      {:lease_lost, ^lease_id, reason} ->
        Task.shutdown(task, :brutal_kill)
        {:cancelled, {:lease_lost, reason}}

      {ref, result} when ref == task.ref ->
        Process.demonitor(ref, [:flush])
        result

      {:DOWN, ref, :process, _pid, reason} when ref == task.ref ->
        {:error, {:exit, reason}}
    after
      timeout ->
        case Task.shutdown(task, :brutal_kill) do
          {:ok, result} -> result
          nil -> {:error, :timeout}
          {:exit, reason} -> {:error, {:exit, reason}}
        end
    end
  end

  defp lease_context(opts) do
    with {:ok, repo} <- Keyword.fetch(opts, :repo),
         {:ok, root} <- Keyword.fetch(opts, :root),
         {:ok, %Lease{} = lease} <- Keyword.fetch(opts, :lease) do
      {:ok,
       %{
         repo: repo,
         root: root,
         lease: lease,
         lease_duration: Keyword.get(opts, :lease_duration, 30_000),
         lease_check_opts: Keyword.get(opts, :lease_check_opts, []),
         lease_extender_opts: Keyword.get(opts, :lease_extender_opts, [])
       }}
    else
      :error -> :none
    end
  end

  defp stop_extender(extender) do
    LeaseExtender.stop(extender)
    Process.unlink(extender)

    if Process.alive?(extender) do
      Process.exit(extender, :shutdown)
    end
  end

  defp get_timeout(job_module) do
    if function_exported?(job_module, :timeout, 0) do
      job_module.timeout()
    else
      30_000
    end
  end
end
