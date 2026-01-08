defmodule Bedrock.JobQueue.Consumer.ManagerTest do
  use ExUnit.Case, async: false

  import Bedrock.JobQueue.Test.StoreHelpers
  import ExUnit.CaptureLog
  import Mox

  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  @holder_id <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>

  defmodule SuccessJob do
    def perform(_args, _meta), do: :ok
    def timeout, do: 1000
  end

  defmodule CrashingJob do
    def perform(_args, _meta), do: exit(:crash)
    def timeout, do: 1000
  end

  setup do
    pool_name = :"TestPool_#{System.unique_integer()}"
    {:ok, pool} = Task.Supervisor.start_link(name: pool_name, max_children: 5)
    {:ok, store_agent} = start_mock_store()

    stub(MockRepo, :transact, fn callback -> callback.() end)
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

  defp enqueue_item(ctx, topic, payload \\ %{}) do
    item = Item.new("tenant_1", topic, payload)
    keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
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
  end
end
