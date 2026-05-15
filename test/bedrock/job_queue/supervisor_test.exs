defmodule Bedrock.JobQueue.SupervisorTest do
  use ExUnit.Case, async: false

  import Mox

  alias Bedrock.JobQueue.Supervisor, as: JQSupervisor
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  # Test JobQueue module that provides the config
  defmodule TestJobQueue do
    def __config__ do
      %{
        otp_app: :test,
        repo: MockRepo,
        workers: %{"test:job" => __MODULE__}
      }
    end
  end

  describe "start_link/2" do
    test "starts supervisor with consumer child" do
      root = Keyspace.new("job_queue/test_job_queue/")

      # Mock the transact call for init_root
      expect(MockRepo, :transact, fn _callback ->
        # Simulate successful directory creation
        :persistent_term.put({Bedrock.JobQueue.Internal, TestJobQueue}, root)
        {:ok, root}
      end)

      assert {:ok, pid} = JQSupervisor.start_link(TestJobQueue)
      assert is_pid(pid)
      assert Process.alive?(pid)

      # Verify supervisor has expected name
      assert Process.whereis(TestJobQueue) == pid

      # Clean up
      Supervisor.stop(pid)
      :persistent_term.erase({Bedrock.JobQueue.Internal, TestJobQueue})
    end

    test "accepts custom concurrency and batch_size options" do
      root = Keyspace.new("job_queue/test_job_queue/")

      expect(MockRepo, :transact, fn _callback ->
        :persistent_term.put({Bedrock.JobQueue.Internal, TestJobQueue}, root)
        {:ok, root}
      end)

      assert {:ok, pid} = JQSupervisor.start_link(TestJobQueue, concurrency: 2, batch_size: 5)
      assert Process.alive?(pid)

      Supervisor.stop(pid)
      :persistent_term.erase({Bedrock.JobQueue.Internal, TestJobQueue})
    end

    test "accepts a precomputed root keyspace without initializing the directory" do
      root = Keyspace.new("job_queue/precomputed/")

      assert {:ok, pid} = JQSupervisor.start_link(TestJobQueue, root: root)
      assert Process.alive?(pid)

      Supervisor.stop(pid)
    end
  end

  describe "init/1" do
    test "returns supervisor child spec" do
      root = Keyspace.new("job_queue/test/")

      {:ok, {sup_flags, children}} = JQSupervisor.init({TestJobQueue, root, []})

      assert sup_flags.strategy == :one_for_one
      assert length(children) == 1

      [child] = children
      assert child.id == Bedrock.JobQueue.Consumer
    end

    test "forwards consumer runtime options" do
      root = Keyspace.new("job_queue/test/")
      backoff_fn = fn attempt -> attempt * 100 end

      opts = [
        action_hook: fn -> :ok end,
        concurrency: 2,
        batch_size: 3,
        scan_interval: 4,
        lease_duration: 5,
        queue_lease_duration: 6,
        backoff_fn: backoff_fn,
        gc_interval: 7,
        gc_grace_period: 8
      ]

      assert {:ok, {_sup_flags, [child]}} = JQSupervisor.init({TestJobQueue, root, opts})
      assert %{start: {Bedrock.JobQueue.Consumer, :start_link, [consumer_opts]}} = child

      assert consumer_opts[:repo] == MockRepo
      assert consumer_opts[:root] == root
      assert consumer_opts[:workers] == %{"test:job" => TestJobQueue}
      assert is_function(consumer_opts[:action_hook], 0)
      assert consumer_opts[:concurrency] == 2
      assert consumer_opts[:batch_size] == 3
      assert consumer_opts[:scan_interval] == 4
      assert consumer_opts[:lease_duration] == 5
      assert consumer_opts[:queue_lease_duration] == 6
      assert consumer_opts[:backoff_fn] == backoff_fn
      assert consumer_opts[:gc_interval] == 7
      assert consumer_opts[:gc_grace_period] == 8
    end
  end
end
