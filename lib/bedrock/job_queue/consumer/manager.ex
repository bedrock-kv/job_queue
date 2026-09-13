defmodule Bedrock.JobQueue.Consumer.Manager do
  @moduledoc """
  Manages job processing by dequeuing items and dispatching to workers.

  Per [QuiCK paper](https://www.foundationdb.org/files/QuiCK.pdf): The Manager
  receives queue notifications from the Scanner, batch dequeues items, obtains
  leases, and dispatches to the Worker pool.

  ## Configuration

  - `:repo` - Required. The Bedrock Repo module
  - `:workers` - Required. Map of topic strings to job modules
  - `:worker_pool` - Required. The Task.Supervisor for spawning job tasks
  - `:action_hook` - Optional hook invoked inside queue action transactions
  - `:name` - Process name (default: `Bedrock.JobQueue.Consumer.Manager`)
  - `:root` - Root keyspace (default: `Keyspace.new("job_queue/")`)
  - `:concurrency` - Max concurrent workers (default: `System.schedulers_online()`)
  - `:batch_size` - Items to dequeue per batch (default: 10)
  - `:lease_duration` - Item lease duration in ms (default: 30_000)
  - `:queue_lease_duration` - Queue lease duration in ms (default: 5_000)
  - `:holder_id` - Unique identifier for this consumer (default: random bytes)
  - `:backoff_fn` - Retry backoff function (default: `Bedrock.JobQueue.Config.default_backoff/1`)

  ## Message Protocol

  The Manager expects `{:queue_ready, queue_id}` messages from the Scanner
  to trigger processing of a queue.
  """

  use GenServer

  alias Bedrock.JobQueue.Config
  alias Bedrock.JobQueue.Consumer.Action
  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Store

  require Logger

  defstruct [
    :repo,
    :root,
    :workers,
    :action_hook,
    :worker_pool,
    :concurrency,
    :batch_size,
    :lease_duration,
    :queue_lease_duration,
    :holder_id,
    :backoff_fn,
    pending_queues: MapSet.new(),
    # Maps task ref -> {kind, lease, task pid} for active work.
    task_info: %{}
  ]

  @default_batch_size 10
  @default_lease_duration 30_000
  # Queue lease duration is short - just enough to dequeue items
  # Per QuiCK paper: prevents thundering herd on hot queues
  @default_queue_lease_duration 5_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      repo: Keyword.fetch!(opts, :repo),
      root: Keyword.fetch!(opts, :root),
      workers: Keyword.fetch!(opts, :workers),
      action_hook: Keyword.get(opts, :action_hook),
      worker_pool: Keyword.fetch!(opts, :worker_pool),
      concurrency: Keyword.get(opts, :concurrency, System.schedulers_online()),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      lease_duration: Keyword.get(opts, :lease_duration, @default_lease_duration),
      queue_lease_duration: Keyword.get(opts, :queue_lease_duration, @default_queue_lease_duration),
      holder_id: Keyword.get(opts, :holder_id, :crypto.strong_rand_bytes(16)),
      backoff_fn: Keyword.get(opts, :backoff_fn, &Config.default_backoff/1)
    }

    {:ok, state}
  end

  @impl true
  def handle_info({:queue_ready, queue_id}, state) do
    state = %{state | pending_queues: MapSet.put(state.pending_queues, queue_id)}
    {:noreply, process_pending(state)}
  end

  # Task completed successfully
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.task_info, ref) do
      {nil, _} ->
        # Unknown task, ignore
        {:noreply, state}

      {{:worker, lease, _task_pid}, task_info} ->
        state = %{state | task_info: task_info}
        {:noreply, start_job_action(state, lease, result)}

      {{:action, lease, _task_pid}, task_info} ->
        handle_action_result(lease, result)
        state = %{state | task_info: task_info}
        {:noreply, process_pending(state)}
    end
  end

  # Task crashed
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.task_info, ref) do
      {nil, _} ->
        # Unknown task, ignore
        {:noreply, state}

      {{:worker, lease, _task_pid}, task_info} ->
        Logger.error("Job task crashed: #{inspect(reason)}")
        state = %{state | task_info: task_info}
        {:noreply, start_job_action(state, lease, {:error, {:crash, reason}})}

      {{:action, lease, _task_pid}, task_info} ->
        handle_action_result(lease, {:error, {:action_task_crashed, reason}})
        state = %{state | task_info: task_info}
        {:noreply, process_pending(state)}
    end
  end

  # Worker and action tasks are linked to this Manager so they terminate with
  # it. Trapping their exit signals lets the corresponding :DOWN handler handle
  # failures without crashing the Manager.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.task_info, fn {_ref, {_kind, _lease, task_pid}} ->
      Process.exit(task_pid, :kill)
    end)

    :ok
  end

  defp process_pending(state) do
    if MapSet.size(state.pending_queues) == 0 do
      state
    else
      process_next_pending(state)
    end
  end

  defp process_next_pending(state) do
    case available_workers(state) do
      0 ->
        state

      available ->
        {queue_id, remaining} = pop_queue(state.pending_queues)

        if queue_id do
          state = %{state | pending_queues: remaining}
          process_queue(state, queue_id, available)
        else
          state
        end
    end
  end

  defp available_workers(state) do
    # Task.Supervisor uses :workers instead of :active
    children = Task.Supervisor.children(state.worker_pool)
    max(0, state.concurrency - length(children))
  end

  defp pop_queue(queues) do
    case Enum.take(queues, 1) do
      [queue_id] -> {queue_id, MapSet.delete(queues, queue_id)}
      [] -> {nil, queues}
    end
  end

  defp process_queue(state, queue_id, max_items) do
    limit = min(max_items, state.batch_size)

    # Per QuiCK Algorithm 2: First obtain queue lease to prevent thundering herd
    result = state.repo.transact(fn -> dequeue_with_lease(state, queue_id, limit) end)

    handle_dequeue_result(state, result)
  end

  defp dequeue_with_lease(state, queue_id, limit) do
    case Store.obtain_queue_lease(
           state.repo,
           state.root,
           queue_id,
           state.holder_id,
           state.queue_lease_duration
         ) do
      {:ok, queue_lease} ->
        result = do_dequeue(state, queue_id, limit)
        # Release the queue lease after dequeuing to allow subsequent dequeue attempts
        Store.release_queue_lease(state.repo, state.root, queue_lease)
        result

      {:error, :queue_leased} ->
        {:skip, :queue_leased}
    end
  end

  defp do_dequeue(state, queue_id, limit) do
    items = Store.peek(state.repo, state.root, queue_id, limit: limit)
    migrating? = Store.migration_in_progress?(state.repo, state.root, queue_id)
    leases = obtain_item_leases(state, items)
    update_pointer_for_remaining(state, queue_id)

    # A legacy queue advances one bounded index-migration chunk per store call.
    # Requeueing the queue message makes progress without an unbounded loop in
    # this callback; Store.peek/4 still dispatches nothing until the index is
    # complete.
    if migrating?, do: send(self(), {:queue_ready, queue_id})

    {:ok, {items, leases}}
  end

  defp obtain_item_leases(state, items) do
    items
    |> Enum.reduce([], fn item, acc ->
      case Store.obtain_lease(state.repo, state.root, item, state.holder_id, state.lease_duration) do
        {:ok, lease} -> [lease | acc]
        {:error, _} -> acc
      end
    end)
    |> Enum.reverse()
  end

  # Per QuiCK Algorithm 2 lines 6-9: After dequeuing, update pointer to min vesting_time
  defp update_pointer_for_remaining(state, queue_id) do
    case Store.min_vesting_time(state.repo, state.root, queue_id, advance_migration?: false) do
      nil -> :ok
      min_vesting -> Store.update_queue_pointer(state.repo, state.root, queue_id, min_vesting)
    end
  end

  defp handle_dequeue_result(state, {:ok, {items, leases}}), do: dispatch_jobs(state, items, leases)

  defp handle_dequeue_result(state, {:skip, :queue_leased}), do: state

  defp handle_dequeue_result(state, {:error, reason}) do
    Logger.warning("Failed to process queue: #{inspect(reason)}")
    state
  end

  defp dispatch_jobs(state, items, leases) do
    items_by_id = Map.new(items, &{&1.id, &1})

    Enum.reduce(leases, state, fn lease, acc_state ->
      item = Map.get(items_by_id, lease.item_id)

      if item do
        task =
          Task.Supervisor.async(
            acc_state.worker_pool,
            Worker,
            :execute,
            [
              item,
              acc_state.workers,
              [
                repo: acc_state.repo,
                root: acc_state.root,
                lease: lease,
                lease_duration: acc_state.lease_duration
              ]
            ]
          )

        # The Worker owns its linked extender; this Manager owns the Worker task.
        %{acc_state | task_info: Map.put(acc_state.task_info, task.ref, {:worker, lease, task.pid})}
      else
        acc_state
      end
    end)
  end

  defp start_job_action(state, lease, handler_result) do
    case handler_result do
      {:cancelled, {:lease_lost, reason}} ->
        Logger.warning(
          "Skipping queue action for job #{Base.encode16(lease.item_id, case: :lower)} after lease loss: #{inspect(reason)}"
        )

        process_pending(state)

      {:deferred, {:lease_check_unavailable, reason}} ->
        Logger.warning(
          "Skipping queue action for job #{Base.encode16(lease.item_id, case: :lower)} because lease preflight is unavailable: #{inspect(reason)}"
        )

        # The handler never ran, so leave the active lease unchanged rather than
        # consuming retry budget. The item becomes eligible again at lease expiry.
        process_pending(state)

      _ ->
        action = action_for_worker_result(lease, handler_result)

        task =
          Task.Supervisor.async(
            state.worker_pool,
            Action,
            :run,
            [
              state.repo,
              state.root,
              lease,
              action,
              handler_result,
              [action_hook: state.action_hook, backoff_fn: state.backoff_fn]
            ]
          )

        %{state | task_info: Map.put(state.task_info, task.ref, {:action, lease, task.pid})}
    end
  end

  defp action_for_worker_result(_lease, success)
       when success in [:ok] or (is_tuple(success) and elem(success, 0) == :ok), do: :complete

  defp action_for_worker_result(_lease, {:error, _reason}), do: :requeue

  defp action_for_worker_result(lease, {:discard, reason}) do
    Logger.info("Discarding job #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}")
    :complete
  end

  defp action_for_worker_result(_lease, {:snooze, delay_ms}), do: {:snooze, delay_ms}

  defp handle_action_result(_lease, :ok), do: :ok
  defp handle_action_result(_lease, {:ok, _status}), do: :ok

  defp handle_action_result(lease, {:error, reason}) do
    Logger.warning(
      "Failed to finalize job #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}. " <>
        "The finalization was not applied; the job may retry once it is visible."
    )
  end
end
