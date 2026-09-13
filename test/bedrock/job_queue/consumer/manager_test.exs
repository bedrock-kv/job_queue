defmodule Bedrock.JobQueue.Consumer.ManagerTest do
  use ExUnit.Case, async: false

  import Bedrock.JobQueue.Test.StoreHelpers
  import ExUnit.CaptureLog
  import Mox

  alias Bedrock.Internal.Repo
  alias Bedrock.Internal.Repo.TransactionContext
  alias Bedrock.JobQueue.Consumer.Action
  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  @holder_id <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>

  defmodule SuccessJob do
    @moduledoc false
    def perform(_args, _meta), do: :ok
    def timeout, do: 1000
  end

  defmodule CrashingJob do
    @moduledoc false
    def perform(_args, _meta), do: exit(:crash)
    def timeout, do: 1000
  end

  defmodule BlockingJob do
    @moduledoc false
    def perform(_args, _meta) do
      Process.register(self(), :manager_lifecycle_handler)
      send(:manager_lifecycle_test_process, {:handler_started, self()})

      receive do
        :finish -> :ok
      end
    end

    def timeout, do: 1_000
  end

  defmodule ActionHook do
    @moduledoc false
    def apply(repo, root, lease, action, handler_result, queue_result, test_pid) do
      send(test_pid, {:action_hook, repo, root, lease.item_id, action, handler_result, queue_result})
      :ok
    end
  end

  defmodule FailingWriteHook do
    @moduledoc false
    def apply(repo, _root, _lease, _action, _handler_result, _queue_result) do
      repo.put("action_hook/write", "partial")
      {:error, :hook_failed}
    end
  end

  defmodule AbnormalWriteHook do
    @moduledoc false

    def apply(repo, _root, _lease, _action, _handler_result, _queue_result, mode) do
      repo.put("action_hook/write", "partial")

      case mode do
        :raise -> raise "hook raised"
        :throw -> throw(:hook_thrown)
        :exit -> exit(:hook_exited)
      end
    end
  end

  defmodule RollbackHook do
    @moduledoc false

    def apply(repo, _root, _lease, _action, _handler_result, _queue_result) do
      repo.put("action_hook/write", "partial")
      repo.rollback(:hook_requested_rollback)
    end
  end

  defmodule SelectiveThrowingHook do
    @moduledoc false

    def apply(_repo, _root, lease, _action, _handler_result, _queue_result, failing_item_id, test_pid) do
      if lease.item_id == failing_item_id do
        throw(:hook_thrown)
      else
        send(test_pid, {:unrelated_job_completed, lease.item_id})
        :ok
      end
    end
  end

  defmodule SelectiveLinkedChildCrashHook do
    @moduledoc false

    def apply(_repo, _root, lease, _action, _handler_result, _queue_result, failing_item_id, test_pid) do
      if lease.item_id == failing_item_id do
        child =
          spawn_link(fn ->
            receive do
              :crash -> exit(:hook_child_crashed)
            end
          end)

        send(test_pid, {:linked_hook_child, child})
        :ok
      else
        send(test_pid, {:unrelated_job_completed, lease.item_id})
        :ok
      end
    end
  end

  defmodule UnusedCluster do
    @moduledoc false
    def link!, do: raise("the test always supplies an active transaction")
  end

  defmodule TransactionalRepo do
    use Bedrock.Repo, cluster: UnusedCluster
  end

  defmodule PreflightFailureRepo do
    @state :manager_preflight_failure_repo

    def transact(callback) do
      case Agent.get(@state, & &1.phase) do
        :dequeue ->
          Agent.update(@state, &Map.put(&1, :phase, :preflight))
          callback.()

        :preflight ->
          preflight_result = Agent.get(@state, & &1.preflight_result)
          Agent.update(@state, &Map.put(&1, :phase, :after_preflight))
          preflight_result.()

        :after_preflight ->
          Agent.update(@state, &Map.put(&1, :action_called?, true))
          callback.()
      end
    end

    def get(keyspace, key), do: MockRepo.get(keyspace, key)
    def get_range(key_range, opts), do: MockRepo.get_range(key_range, opts)
    def put(keyspace, key, value), do: MockRepo.put(keyspace, key, value)
    def clear(keyspace, key), do: MockRepo.clear(keyspace, key)
    def max(key, value), do: MockRepo.max(key, value)
    def add(key, value), do: MockRepo.add(key, value)
    def rollback(reason), do: MockRepo.rollback(reason)
  end

  defmodule RecordingTransaction do
    @moduledoc false
    use GenServer

    def start_link(values), do: GenServer.start_link(__MODULE__, {values, self()})
    def writes(transaction), do: GenServer.call(transaction, :writes)

    @impl true
    def init({values, test_pid}), do: {:ok, %{values: values, writes: [], test_pid: test_pid}}

    @impl true
    def handle_call(:nested_transaction, _from, state) do
      send(state.test_pid, :nested_transaction)
      {:reply, :ok, state}
    end

    def handle_call(:commit, _from, state) do
      send(state.test_pid, {:commit, state.writes})
      {:reply, :ok, state}
    end

    def handle_call({:get, key, _opts}, _from, state) do
      case Map.fetch(state.values, key) do
        {:ok, value} -> {:reply, {:ok, value}, state}
        :error -> {:reply, {:error, :not_found}, state}
      end
    end

    def handle_call({:get_range, _start_key, _end_key, _limit, _opts}, _from, state),
      do: {:reply, {:ok, {[], false}}, state}

    def handle_call(:writes, _from, state), do: {:reply, state.writes, state}

    @impl true
    def handle_cast({:clear, key, opts}, state), do: {:noreply, add_write(state, {:clear, key, opts})}

    def handle_cast({:set_key, key, value, opts}, state), do: {:noreply, add_write(state, {:set_key, key, value, opts})}

    def handle_cast({:atomic, operation, key, value}, state),
      do: {:noreply, add_write(state, {:atomic, operation, key, value})}

    def handle_cast({:clear_range, start_key, end_key, opts}, state),
      do: {:noreply, add_write(state, {:clear_range, start_key, end_key, opts})}

    def handle_cast(:rollback, state) do
      send(state.test_pid, :rollback)
      {:noreply, %{state | writes: []}}
    end

    defp add_write(state, write), do: %{state | writes: [write | state.writes]}
  end

  setup do
    pool_name = :"TestPool_#{System.unique_integer()}"
    {:ok, pool} = Task.Supervisor.start_link(name: pool_name, max_children: 5)
    {:ok, store_agent} = start_mock_store()

    stub(MockRepo, :transact, fn callback ->
      try do
        callback.()
      catch
        {Repo, :rollback, reason} -> {:error, reason}
      end
    end)

    stub(MockRepo, :rollback, fn reason -> throw({Repo, :rollback, reason}) end)
    setup_integration_stubs(MockRepo, store_agent)

    workers = %{
      "test:success" => SuccessJob,
      "test:crash" => CrashingJob,
      "test:blocking" => BlockingJob
    }

    %{
      pool: pool,
      pool_name: pool_name,
      root: Keyspace.new("job_queue/test/"),
      workers: workers,
      store: store_agent
    }
  end

  defp start_manager(ctx, opts \\ []) do
    name = :"TestManager_#{System.unique_integer()}"

    {:ok, manager} =
      Manager.start_link(
        Keyword.merge(
          [
            name: name,
            repo: MockRepo,
            root: ctx.root,
            workers: ctx.workers,
            worker_pool: ctx.pool_name,
            concurrency: 5,
            holder_id: @holder_id
          ],
          opts
        )
      )

    manager
  end

  defp enqueue_item(ctx, topic, payload \\ %{}), do: enqueue_item(ctx, "tenant_1", topic, payload)

  defp enqueue_item(ctx, queue_id, topic, payload) do
    item = Item.new(queue_id, topic, payload)
    assert :ok = Store.enqueue(MockRepo, ctx.root, item)
    item
  end

  describe "handle_info/2" do
    test "holds a writer-fence-required queue without pointer mutation or self-reschedule", ctx do
      queue_id = "legacy-hold"
      keyspaces = Store.queue_keyspaces(ctx.root, queue_id)
      legacy = Item.new(queue_id, "test:success", %{})
      store_item(ctx.store, keyspaces.items, legacy)
      manager = start_manager(ctx)

      send(manager, {:queue_ready, queue_id})
      Process.sleep(50)
      _ = :sys.get_state(manager)

      assert :writer_fence_required = Store.priority_index_status(MockRepo, ctx.root, queue_id)

      refute Agent.get(ctx.store, fn state ->
               Enum.any?(state, fn
                 {{prefix, _key}, _value} ->
                   prefix == Keyspace.prefix(keyspaces.priority_index) or
                     prefix == Keyspace.prefix(Store.pointer_keyspace(ctx.root))

                 _ ->
                   false
               end)
             end)
    end

    test "handles task crash with :DOWN message", ctx do
      _item = enqueue_item(ctx, "test:crash")
      manager = start_manager(ctx)

      log =
        capture_log(fn ->
          send(manager, {:queue_ready, "tenant_1"})
          Process.sleep(200)
        end)

      assert log =~ "Job task crashed"
      assert Process.alive?(manager)
    end

    test "ignores unknown task reference", ctx do
      manager = start_manager(ctx)

      # Send a fake task completion with unknown ref
      fake_ref = make_ref()
      send(manager, {fake_ref, :ok})

      # Manager should still be alive
      assert Process.alive?(manager)
    end

    test "ignores unknown DOWN reference", ctx do
      manager = start_manager(ctx)

      # Send a fake DOWN message with unknown ref
      fake_ref = make_ref()
      send(manager, {:DOWN, fake_ref, :process, self(), :normal})

      # Manager should still be alive
      assert Process.alive?(manager)
    end
  end

  describe "queue processing" do
    test "terminates an in-flight handler when the manager stops", ctx do
      Process.register(self(), :manager_lifecycle_test_process)

      on_exit(fn ->
        if handler = Process.whereis(:manager_lifecycle_handler) do
          Process.exit(handler, :kill)
        end
      end)

      _item = enqueue_item(ctx, "test:blocking")
      manager = start_manager(ctx)
      manager_ref = Process.monitor(manager)
      Process.unlink(manager)

      send(manager, {:queue_ready, "tenant_1"})

      assert_receive {:handler_started, handler_pid}
      handler_ref = Process.monitor(handler_pid)

      :ok = GenServer.stop(manager, :shutdown)

      assert_receive {:DOWN, ^manager_ref, :process, ^manager, :shutdown}
      assert_receive {:DOWN, ^handler_ref, :process, ^handler_pid, _reason}
    end

    test "handles no available workers", ctx do
      # Fill up worker slots
      _item = enqueue_item(ctx, "test:success")
      manager = start_manager(ctx, concurrency: 0)

      send(manager, {:queue_ready, "tenant_1"})

      # Sync to ensure message processed
      _ = :sys.get_state(manager)
      assert Process.alive?(manager)
    end

    test "does not requeue a job when worker lease preflight is unavailable", ctx do
      assert_preflight_does_not_requeue(ctx, fn -> {:error, :transaction_failed} end)
    end

    test "does not requeue a job when worker lease preflight raises", ctx do
      assert_preflight_does_not_requeue(ctx, fn -> raise "preflight unavailable" end)
    end

    test "runs action hook inside successful queue action", ctx do
      item = enqueue_item(ctx, "test:success")
      manager = start_manager(ctx, action_hook: {ActionHook, :apply, [self()]})

      send(manager, {:queue_ready, "tenant_1"})

      assert_receive {:action_hook, MockRepo, root, item_id, :complete, :ok, :ok}
      assert root == ctx.root
      assert item_id == item.id
    end

    test "keeps processing unrelated jobs when an action hook throws", ctx do
      failed_item = enqueue_item(ctx, "failed_queue", "test:success", %{})
      unrelated_item = enqueue_item(ctx, "unrelated_queue", "test:success", %{})

      manager =
        start_manager(ctx,
          action_hook: {SelectiveThrowingHook, :apply, [failed_item.id, self()]}
        )

      log =
        capture_log(fn ->
          send(manager, {:queue_ready, failed_item.queue_id})
          send(manager, {:queue_ready, unrelated_item.queue_id})

          assert_receive {:unrelated_job_completed, unrelated_item_id}, 500
          assert unrelated_item_id == unrelated_item.id
          assert_eventually(fn -> manager_idle?(manager) end, timeout: 500)
        end)

      assert Process.alive?(manager)
      assert log =~ "Failed to finalize job"
    end

    test "isolates a linked hook-child crash and keeps unrelated jobs tracked", ctx do
      previous_trap_exit = Process.flag(:trap_exit, true)
      on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

      transaction_calls = :counters.new(1, [])
      test_pid = self()

      expect(MockRepo, :transact, 4, fn callback ->
        :counters.add(transaction_calls, 1, 1)

        case :counters.get(transaction_calls, 1) do
          1 ->
            callback.()

          2 ->
            result = callback.()
            send(test_pid, :action_transaction_ready)

            receive do
              :commit -> result
            end

          _ ->
            callback.()
        end
      end)

      failed_item = enqueue_item(ctx, "failed_queue", "test:success", %{})

      manager =
        start_manager(ctx,
          action_hook: {SelectiveLinkedChildCrashHook, :apply, [failed_item.id, self()]}
        )

      log =
        capture_log(fn ->
          send(manager, {:queue_ready, failed_item.queue_id})

          assert_receive {:linked_hook_child, child}, 500
          assert_receive :action_transaction_ready, 500
          send(child, :crash)

          assert_eventually(fn -> manager_idle?(manager) end, timeout: 500)

          unrelated_item = enqueue_item(ctx, "unrelated_queue", "test:success", %{})
          send(manager, {:queue_ready, unrelated_item.queue_id})

          assert_receive {:unrelated_job_completed, unrelated_item_id}, 500
          assert unrelated_item_id == unrelated_item.id
          assert_eventually(fn -> manager_idle?(manager) end, timeout: 500)
        end)

      assert Process.alive?(manager)
      assert log =~ "Failed to finalize job"
    end
  end

  describe "action hook failures" do
    for action <- [:complete, :requeue] do
      test "rolls back #{action} and hook writes when the hook fails" do
        action = unquote(action)
        {lease, values} = lease_transaction_values(action)
        {:ok, transaction} = RecordingTransaction.start_link(values)
        TransactionContext.put_builder(TransactionalRepo, transaction)

        on_exit(fn -> TransactionContext.clear(TransactionalRepo) end)

        assert {:error, {:action_hook_failed, :hook_failed}} =
                 Action.run(
                   TransactionalRepo,
                   Keyspace.new("job_queue/test/"),
                   lease,
                   action,
                   handler_result_for(action),
                   action_hook: {FailingWriteHook, :apply},
                   backoff_fn: fn _attempt -> 1_000 end
                 )

        assert_receive :nested_transaction
        assert_receive :rollback
        refute_receive {:commit, _}
        assert RecordingTransaction.writes(transaction) == []
      end
    end

    for {mode, failure_type} <- [raise: :exception, throw: :throw, exit: :exit] do
      test "rolls back queue and hook writes when a hook #{mode}s" do
        mode = unquote(mode)
        failure_type = unquote(failure_type)
        {lease, values} = lease_transaction_values(:complete)
        {:ok, transaction} = RecordingTransaction.start_link(values)
        TransactionContext.put_builder(TransactionalRepo, transaction)

        on_exit(fn -> TransactionContext.clear(TransactionalRepo) end)

        assert {:error, {:action_hook_failed, {^failure_type, reason}}} =
                 Action.run(
                   TransactionalRepo,
                   Keyspace.new("job_queue/test/"),
                   lease,
                   :complete,
                   :ok,
                   action_hook: {AbnormalWriteHook, :apply, [mode]},
                   backoff_fn: fn _attempt -> 1_000 end
                 )

        assert_abnormal_hook_reason(mode, reason)
        assert_action_rolled_back(transaction)
      end
    end

    test "preserves a hook-requested Bedrock rollback" do
      {lease, values} = lease_transaction_values(:complete)
      {:ok, transaction} = RecordingTransaction.start_link(values)
      TransactionContext.put_builder(TransactionalRepo, transaction)

      on_exit(fn -> TransactionContext.clear(TransactionalRepo) end)

      assert {:error, :hook_requested_rollback} =
               Action.run(
                 TransactionalRepo,
                 Keyspace.new("job_queue/test/"),
                 lease,
                 :complete,
                 :ok,
                 action_hook: {RollbackHook, :apply},
                 backoff_fn: fn _attempt -> 1_000 end
               )

      assert_action_rolled_back(transaction)
    end
  end

  defp lease_transaction_values(action) do
    root = Keyspace.new("job_queue/test/")
    now = System.system_time(:millisecond)
    item = Item.new("tenant_1", "test:success", %{}, id: "item-id", vesting_time: now)
    lease = Lease.new(item, @holder_id, now: now)
    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    values = %{
      Keyspace.pack(keyspaces.leases, item.id) => :erlang.term_to_binary(lease),
      Keyspace.pack(keyspaces.priority_index, {"initialized"}) => "ready"
    }

    values =
      if action == :requeue do
        leased_item = %{
          item
          | lease_id: lease.id,
            lease_expires_at: lease.expires_at,
            vesting_time: lease.expires_at
        }

        Map.put(values, Keyspace.pack(keyspaces.items, lease.item_key), :erlang.term_to_binary(leased_item))
      else
        values
      end

    {lease, values}
  end

  defp handler_result_for(:complete), do: :ok
  defp handler_result_for(:requeue), do: {:error, :failed}

  defp assert_preflight_does_not_requeue(ctx, preflight_result) do
    item = enqueue_item(ctx, "test:success")

    {:ok, _state} =
      Agent.start_link(
        fn -> %{phase: :dequeue, preflight_result: preflight_result, action_called?: false} end,
        name: :manager_preflight_failure_repo
      )

    on_exit(fn ->
      if Process.whereis(:manager_preflight_failure_repo) do
        try do
          Agent.stop(:manager_preflight_failure_repo)
        catch
          :exit, _reason -> :ok
        end
      end
    end)

    manager = start_manager(ctx, repo: PreflightFailureRepo)
    send(manager, {:queue_ready, item.queue_id})

    assert_eventually(fn -> manager_idle?(manager) end, timeout: 500)
    refute Agent.get(:manager_preflight_failure_repo, & &1.action_called?)

    keyspaces = Store.queue_keyspaces(ctx.root, item.queue_id)
    assert lease_value = MockRepo.get(keyspaces.leases, item.id)
    lease = :erlang.binary_to_term(lease_value)
    lease_id = lease.id
    assert leased_item_value = MockRepo.get(keyspaces.items, lease.item_key)
    assert %Item{error_count: 0, lease_id: ^lease_id} = :erlang.binary_to_term(leased_item_value)
  end

  defp assert_action_rolled_back(transaction) do
    assert_receive :nested_transaction
    assert_receive :rollback
    refute_receive {:commit, _}
    assert RecordingTransaction.writes(transaction) == []
  end

  defp assert_abnormal_hook_reason(:raise, %RuntimeError{message: "hook raised"}), do: :ok
  defp assert_abnormal_hook_reason(:throw, :hook_thrown), do: :ok
  defp assert_abnormal_hook_reason(:exit, :hook_exited), do: :ok

  defp manager_idle?(manager) do
    if Process.alive?(manager) do
      %{task_info: task_info} = :sys.get_state(manager)
      task_info == %{}
    else
      false
    end
  catch
    :exit, _reason -> false
  end
end
