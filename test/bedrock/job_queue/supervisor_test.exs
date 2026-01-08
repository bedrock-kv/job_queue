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
  end
end
