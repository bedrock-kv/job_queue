defmodule Bedrock.JobQueue.Consumer.WorkerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Item

  defmodule SuccessJob do
    def perform(_args, _meta), do: :ok
    def timeout, do: 1000
  end

  defmodule SlowJob do
    def perform(_args, _meta) do
      Process.sleep(500)
      :ok
    end

    def timeout, do: 50
  end

  defmodule CrashingJob do
    def perform(_args, _meta) do
      raise "intentional crash"
    end

    def timeout, do: 1000
  end

  defmodule ExitingJob do
    def perform(_args, _meta) do
      exit(:normal)
    end

    def timeout, do: 1000
  end

  defmodule NoTimeoutJob do
    def perform(_args, _meta), do: :ok
    # No timeout/0 defined - should use default 30s
  end

  describe "execute/2" do
    test "returns result from successful job" do
      item = Item.new("tenant", "test:success", %{})
      workers = %{"test:success" => SuccessJob}

      assert Worker.execute(item, workers) == :ok
    end

    test "returns {:discard, :no_handler} when no worker configured" do
      item = Item.new("tenant", "unknown:topic", %{})
      workers = %{}

      log =
        capture_log(fn ->
          assert Worker.execute(item, workers) == {:discard, :no_handler}
        end)

      assert log =~ "No worker configured for topic"
    end

    test "returns {:error, :timeout} when job exceeds timeout" do
      item = Item.new("tenant", "test:slow", %{})
      workers = %{"test:slow" => SlowJob}

      assert Worker.execute(item, workers) == {:error, :timeout}
    end

    test "returns {:error, {:exit, reason}} when job exits" do
      item = Item.new("tenant", "test:exit", %{})
      workers = %{"test:exit" => ExitingJob}

      assert {:error, {:exit, :normal}} = Worker.execute(item, workers)
    end

    test "uses default timeout when job module doesn't define timeout/0" do
      # Just verify it doesn't crash - actual timeout behavior tested elsewhere
      item = Item.new("tenant", "test:no_timeout", %{})
      workers = %{"test:no_timeout" => NoTimeoutJob}

      assert Worker.execute(item, workers) == :ok
    end
  end
end
