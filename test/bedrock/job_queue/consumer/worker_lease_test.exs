defmodule Bedrock.JobQueue.Consumer.WorkerLeaseTest do
  use ExUnit.Case, async: false

  import Mox

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  defmodule BlockingJob do
    def perform(_args, _meta) do
      send(:worker_lease_test_process, {:perform_started, self()})

      receive do
        :finish -> :ok
      end
    end

    def timeout, do: 1_000
  end

  defmodule NeverRunJob do
    def perform(_args, _meta) do
      send(:worker_lease_test_process, :handler_ran)
      :ok
    end
  end

  test "kills the running handler when renewal proves the lease was lost" do
    Process.register(self(), :worker_lease_test_process)

    item = Item.new("tenant_1", "test:blocking", %{})
    lease = Lease.new(item, "holder", duration_ms: 30_000)
    root = Keyspace.new("job_queue/test/")
    encoded_lease = :erlang.term_to_binary(lease)
    test_pid = self()

    expect(MockRepo, :transact, fn callback -> callback.() end)
    expect(MockRepo, :get, fn _keyspace, _item_id -> encoded_lease end)

    expect(MockRepo, :transact, fn callback ->
      send(test_pid, {:renewal_attempt, self()})

      receive do
        :confirm_loss -> callback.()
      end
    end)

    expect(MockRepo, :get, fn _keyspace, _item_id -> nil end)

    task =
      Task.async(fn ->
        Worker.execute(item, %{"test:blocking" => BlockingJob},
          repo: MockRepo,
          root: root,
          lease: lease,
          lease_duration: 30_000,
          lease_extender_opts: [interval: 0]
        )
      end)

    assert_receive {:perform_started, handler_pid}
    handler_ref = Process.monitor(handler_pid)
    assert_receive {:renewal_attempt, extender_pid}

    send(extender_pid, :confirm_loss)

    assert_receive {task_ref, {:cancelled, {:lease_lost, :lease_not_found}}}
    assert task_ref == task.ref
    assert_receive {:DOWN, ^handler_ref, :process, ^handler_pid, :killed}
  end

  test "checks ownership before invoking the handler" do
    Process.register(self(), :worker_lease_test_process)

    item = Item.new("tenant_1", "test:never_run", %{})
    lease = Lease.new(item, "holder", duration_ms: 30_000)

    expect(MockRepo, :transact, fn callback -> callback.() end)
    expect(MockRepo, :get, fn _keyspace, _item_id -> nil end)

    assert {:cancelled, {:lease_lost, :lease_not_found}} =
             Worker.execute(item, %{"test:never_run" => NeverRunJob},
               repo: MockRepo,
               root: Keyspace.new("job_queue/test/"),
               lease: lease
             )

    refute_received :handler_ran
  end
end
