defmodule Bedrock.JobQueue.ApplicationTest do
  use ExUnit.Case, async: true

  alias Bedrock.JobQueue.Application

  describe "start/2" do
    test "starts the application supervisor" do
      assert {:ok, pid} = Application.start(:normal, [])
      assert is_pid(pid)
      assert Process.alive?(pid)

      # Clean up
      Supervisor.stop(pid)
    end
  end
end
