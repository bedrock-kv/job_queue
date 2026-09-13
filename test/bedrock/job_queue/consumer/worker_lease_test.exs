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

  defmodule RetryRepo do
    def transact(callback) do
      case Agent.get(:worker_lease_retry_repo, & &1.phase) do
        :preflight ->
          Agent.update(:worker_lease_retry_repo, &Map.put(&1, :phase, :renewing))
          callback.()

        :renewing ->
          %{test_pid: test_pid} = Agent.get(:worker_lease_retry_repo, & &1)
          send(test_pid, {:renewal_attempt, self()})

          receive do
            :fail_transiently ->
              Agent.update(:worker_lease_retry_repo, &Map.put(&1, :phase, :retrying))
              {:error, :transaction_failed}
          end

        :retrying ->
          {:error, :transaction_failed}
      end
    end

    def get(_keyspace, _item_id) do
      Agent.get(:worker_lease_retry_repo, & &1.encoded_lease)
    end
  end

  defmodule RaisingRetryRepo do
    def transact(callback) do
      case Agent.get(:worker_lease_raising_retry_repo, & &1.phase) do
        :preflight ->
          Agent.update(:worker_lease_raising_retry_repo, &Map.put(&1, :phase, :renewing))
          callback.()

        :renewing ->
          %{test_pid: test_pid} = Agent.get(:worker_lease_raising_retry_repo, & &1)
          send(test_pid, {:raising_renewal_attempt, self()})

          receive do
            :raise_transiently ->
              Agent.update(:worker_lease_raising_retry_repo, &Map.put(&1, :phase, :retrying))
              raise "renewal unavailable"
          end

        :retrying ->
          %{test_pid: test_pid} = Agent.get(:worker_lease_raising_retry_repo, & &1)
          send(test_pid, {:renewal_retried, self()})

          receive do
            :fail_at_expiry -> {:error, :transaction_failed}
          end
      end
    end

    def get(_keyspace, _item_id) do
      Agent.get(:worker_lease_raising_retry_repo, & &1.encoded_lease)
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

  test "defers without running the handler when preflight is unavailable" do
    Process.register(self(), :worker_lease_test_process)

    item = Item.new("tenant_1", "test:never_run", %{})
    lease = Lease.new(item, "holder", duration_ms: 30_000)

    expect(MockRepo, :transact, fn _callback -> {:error, :transaction_failed} end)

    assert {:deferred, {:lease_check_unavailable, :transaction_failed}} =
             Worker.execute(item, %{"test:never_run" => NeverRunJob},
               repo: MockRepo,
               root: Keyspace.new("job_queue/test/"),
               lease: lease
             )

    refute_received :handler_ran
  end

  test "defers without running the handler when preflight raises" do
    Process.register(self(), :worker_lease_test_process)

    item = Item.new("tenant_1", "test:never_run", %{})
    lease = Lease.new(item, "holder", duration_ms: 30_000)

    expect(MockRepo, :transact, fn _callback -> raise "preflight unavailable" end)

    assert {:deferred, {:lease_check_unavailable, {:exception, %RuntimeError{}}}} =
             Worker.execute(item, %{"test:never_run" => NeverRunJob},
               repo: MockRepo,
               root: Keyspace.new("job_queue/test/"),
               lease: lease
             )

    refute_received :handler_ran
  end

  test "kills the running handler when a transient renewal failure reaches expiry" do
    Process.register(self(), :worker_lease_test_process)

    now = System.system_time(:millisecond)
    {:ok, clock} = Agent.start_link(fn -> now end)
    item = Item.new("tenant_1", "test:blocking", %{}, now: now)
    lease = Lease.new(item, "holder", now: now, duration_ms: 10_000)
    root = Keyspace.new("job_queue/test/")
    encoded_lease = :erlang.term_to_binary(lease)
    test_pid = self()

    {:ok, _repo_state} =
      Agent.start_link(
        fn -> %{phase: :preflight, encoded_lease: encoded_lease, test_pid: test_pid} end,
        name: :worker_lease_retry_repo
      )

    on_exit(fn ->
      if Process.whereis(:worker_lease_retry_repo) do
        try do
          Agent.stop(:worker_lease_retry_repo)
        catch
          :exit, _reason -> :ok
        end
      end
    end)

    task =
      Task.async(fn ->
        Worker.execute(item, %{"test:blocking" => BlockingJob},
          repo: RetryRepo,
          root: root,
          lease: lease,
          lease_duration: 10_000,
          lease_extender_opts: [interval: 50, clock: fn -> Agent.get(clock, & &1) end]
        )
      end)

    assert_receive {:perform_started, handler_pid}
    handler_ref = Process.monitor(handler_pid)
    assert_receive {:renewal_attempt, extender_pid}

    Agent.update(clock, fn _ -> lease.expires_at end)
    send(extender_pid, :fail_transiently)

    assert_receive {task_ref, {:cancelled, {:lease_lost, :lease_expired}}}
    assert task_ref == task.ref
    assert_receive {:DOWN, ^handler_ref, :process, ^handler_pid, :killed}
  end

  test "survives a transient raising renewal failure until lease expiry" do
    Process.register(self(), :worker_lease_test_process)

    now = System.system_time(:millisecond)
    {:ok, clock} = Agent.start_link(fn -> now end)
    item = Item.new("tenant_1", "test:blocking", %{}, now: now)
    lease = Lease.new(item, "holder", now: now, duration_ms: 10_000)
    root = Keyspace.new("job_queue/test/")
    encoded_lease = :erlang.term_to_binary(lease)
    test_pid = self()

    {:ok, _repo_state} =
      Agent.start_link(
        fn -> %{phase: :preflight, encoded_lease: encoded_lease, test_pid: test_pid} end,
        name: :worker_lease_raising_retry_repo
      )

    on_exit(fn ->
      if Process.whereis(:worker_lease_raising_retry_repo) do
        try do
          Agent.stop(:worker_lease_raising_retry_repo)
        catch
          :exit, _reason -> :ok
        end
      end
    end)

    task =
      Task.async(fn ->
        Worker.execute(item, %{"test:blocking" => BlockingJob},
          repo: RaisingRetryRepo,
          root: root,
          lease: lease,
          lease_duration: 10_000,
          lease_extender_opts: [interval: 50, clock: fn -> Agent.get(clock, & &1) end]
        )
      end)

    assert_receive {:perform_started, handler_pid}
    handler_ref = Process.monitor(handler_pid)
    assert_receive {:raising_renewal_attempt, extender_pid}

    send(extender_pid, :raise_transiently)
    assert_receive {:renewal_retried, ^extender_pid}
    assert Process.alive?(task.pid)
    assert Process.alive?(handler_pid)
    refute_received {:DOWN, ^handler_ref, :process, ^handler_pid, _reason}

    Agent.update(clock, fn _ -> lease.expires_at end)
    send(extender_pid, :fail_at_expiry)

    assert_receive {task_ref, {:cancelled, {:lease_lost, :lease_expired}}}
    assert task_ref == task.ref
    assert_receive {:DOWN, ^handler_ref, :process, ^handler_pid, :killed}
  end
end
