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

  defmodule UnusedCluster do
    @moduledoc false
    def link!, do: raise("the test always supplies an active transaction")
  end

  defmodule TransactionalRepo do
    use Bedrock.Repo, cluster: UnusedCluster
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

    def handle_call(:writes, _from, state), do: {:reply, state.writes, state}

    @impl true
    def handle_cast({:clear, key, opts}, state), do: {:noreply, add_write(state, {:clear, key, opts})}

    def handle_cast({:set_key, key, value, opts}, state), do: {:noreply, add_write(state, {:set_key, key, value, opts})}

    def handle_cast({:atomic, operation, key, value}, state),
      do: {:noreply, add_write(state, {:atomic, operation, key, value})}

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
      "test:crash" => CrashingJob
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
    keyspaces = Store.queue_keyspaces(ctx.root, queue_id)
    store_item(ctx.store, keyspaces.items, item)
    item
  end

  describe "handle_info/2" do
    test "handles task crash with :DOWN message", ctx do
      _item = enqueue_item(ctx, "test:crash")
      manager = start_manager(ctx)

      log =
        capture_log(fn ->
          send(manager, {:queue_ready, "tenant_1"})
          Process.sleep(200)
        end)

      assert log =~ "Job task crashed"
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
    test "handles no available workers", ctx do
      # Fill up worker slots
      _item = enqueue_item(ctx, "test:success")
      manager = start_manager(ctx, concurrency: 0)

      send(manager, {:queue_ready, "tenant_1"})

      # Sync to ensure message processed
      _ = :sys.get_state(manager)
      assert Process.alive?(manager)
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
          assert_eventually(fn -> match?(%{task_info: %{}}, :sys.get_state(manager)) end, timeout: 500)
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
    item = Item.new("tenant_1", "test:success", %{}, id: "item-id", vesting_time: 1_000)
    lease = Lease.new(item, @holder_id, now: 2_000)
    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    values = %{
      Keyspace.pack(keyspaces.leases, item.id) => :erlang.term_to_binary(lease)
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

  defp assert_action_rolled_back(transaction) do
    assert_receive :nested_transaction
    assert_receive :rollback
    refute_receive {:commit, _}
    assert RecordingTransaction.writes(transaction) == []
  end

  defp assert_abnormal_hook_reason(:raise, %RuntimeError{message: "hook raised"}), do: :ok
  defp assert_abnormal_hook_reason(:throw, :hook_thrown), do: :ok
  defp assert_abnormal_hook_reason(:exit, :hook_exited), do: :ok
end
