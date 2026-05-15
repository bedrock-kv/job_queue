defmodule Bedrock.JobQueue.ConsumerTest do
  use ExUnit.Case, async: true

  alias Bedrock.JobQueue.Consumer
  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Consumer.Scanner
  alias Bedrock.Keyspace

  describe "init/1" do
    test "forwards timing and lifecycle options to manager and scanner children" do
      root = Keyspace.new("job_queue/test/")
      backoff_fn = fn attempt -> attempt * 10 end

      assert {:ok, {_flags, children}} =
               Consumer.init(
                 repo: MockRepo,
                 workers: %{"topic" => __MODULE__},
                 root: root,
                 concurrency: 2,
                 batch_size: 3,
                 scan_interval: 4,
                 lease_duration: 5,
                 queue_lease_duration: 6,
                 backoff_fn: backoff_fn,
                 gc_interval: 7,
                 gc_grace_period: 8
               )

      assert task_supervisor_opts(children)[:max_children] == 2

      manager_opts = child_opts(children, Manager)
      assert manager_opts[:repo] == MockRepo
      assert manager_opts[:root] == root
      assert manager_opts[:workers] == %{"topic" => __MODULE__}
      assert manager_opts[:concurrency] == 2
      assert manager_opts[:batch_size] == 3
      assert manager_opts[:lease_duration] == 5
      assert manager_opts[:queue_lease_duration] == 6
      assert manager_opts[:backoff_fn] == backoff_fn

      scanner_opts = child_opts(children, Scanner)
      assert scanner_opts[:repo] == MockRepo
      assert scanner_opts[:root] == root
      assert scanner_opts[:concurrency] == 2
      assert scanner_opts[:batch_size] == 3
      assert scanner_opts[:interval] == 4
      assert scanner_opts[:gc_interval] == 7
      assert scanner_opts[:gc_grace_period] == 8
    end
  end

  defp child_opts(children, module) do
    assert %{start: {^module, :start_link, [opts]}} =
             Enum.find(children, fn child -> child.id == module end)

    opts
  end

  defp task_supervisor_opts(children) do
    assert %{start: {Task.Supervisor, :start_link, [opts]}} =
             Enum.find(children, fn
               %{start: {Task.Supervisor, :start_link, [_opts]}} -> true
               _ -> false
             end)

    opts
  end
end
