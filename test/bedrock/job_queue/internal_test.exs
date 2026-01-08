defmodule Bedrock.JobQueue.InternalTest do
  use ExUnit.Case, async: false

  import Mox

  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Item
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  # Test module that simulates a JobQueue module
  defmodule TestJobQueue do
    def __config__ do
      %{repo: MockRepo}
    end
  end

  describe "enqueue/5" do
    test "enqueues item with immediate processing" do
      test_pid = self()
      now = 1_000_000

      # Set up persistent_term for root_keyspace fallback
      :persistent_term.erase({Internal, TestJobQueue})

      # 1. repo.transact wraps the callback
      expect(MockRepo, :transact, fn callback ->
        result = callback.()
        send(test_pid, {:transact_result, result})
        result
      end)

      # 2. Store.enqueue calls repo.put for item (keyspace, key, value)
      expect(MockRepo, :put, fn keyspace, item_key, _value ->
        assert Keyspace.prefix(keyspace) =~ "items"
        assert is_tuple(item_key)
        {priority, vesting_time, id} = item_key
        assert priority == 100
        assert vesting_time == now
        assert is_binary(id)
        :ok
      end)

      # 3. Store.update_pointer calls repo.max (packed_key, timestamp_binary)
      expect(MockRepo, :max, fn key, value ->
        assert is_binary(key)
        assert String.contains?(key, "pointers/")
        assert is_binary(value)
        :ok
      end)

      # 4. Store.update_stats calls repo.add (packed_key, delta_binary)
      expect(MockRepo, :add, fn key, delta_binary ->
        assert is_binary(key)
        assert String.contains?(key, "stats")
        # Delta is encoded as 64-bit signed little endian
        <<delta::64-signed-little>> = delta_binary
        assert delta == 1
        :ok
      end)

      result = Internal.enqueue(TestJobQueue, "tenant_1", "test:topic", %{data: 1}, now: now)

      assert {:ok, %Item{}} = result
      assert_receive {:transact_result, {:ok, %Item{topic: "test:topic"}}}
    end

    test "enqueues item with :at scheduling option" do
      test_pid = self()
      now = 1_000_000
      scheduled_at = DateTime.from_unix!(2_000, :second)
      expected_vesting = 2_000_000

      :persistent_term.erase({Internal, TestJobQueue})

      expect(MockRepo, :transact, fn callback ->
        result = callback.()
        send(test_pid, {:transact_result, result})
        result
      end)

      expect(MockRepo, :put, fn _keyspace, item_key, _value ->
        {_priority, vesting_time, _id} = item_key
        assert vesting_time == expected_vesting
        :ok
      end)

      expect(MockRepo, :max, fn _key, _value -> :ok end)
      expect(MockRepo, :add, fn _key, _delta -> :ok end)

      result = Internal.enqueue(TestJobQueue, "tenant_1", "test:topic", %{}, at: scheduled_at, now: now)

      assert {:ok, %Item{}} = result
      assert_receive {:transact_result, {:ok, %Item{vesting_time: ^expected_vesting}}}
    end

    test "enqueues item with :in delay option" do
      test_pid = self()
      now = 1_000_000
      delay_ms = 5_000
      expected_vesting = now + delay_ms

      :persistent_term.erase({Internal, TestJobQueue})

      expect(MockRepo, :transact, fn callback ->
        result = callback.()
        send(test_pid, {:transact_result, result})
        result
      end)

      expect(MockRepo, :put, fn _keyspace, item_key, _value ->
        {_priority, vesting_time, _id} = item_key
        assert vesting_time == expected_vesting
        :ok
      end)

      expect(MockRepo, :max, fn _key, _value -> :ok end)
      expect(MockRepo, :add, fn _key, _delta -> :ok end)

      result = Internal.enqueue(TestJobQueue, "tenant_1", "test:topic", %{}, in: delay_ms, now: now)

      assert {:ok, %Item{}} = result
      assert_receive {:transact_result, {:ok, %Item{vesting_time: ^expected_vesting}}}
    end

    test "enqueues item with custom priority" do
      test_pid = self()
      now = 1_000_000

      :persistent_term.erase({Internal, TestJobQueue})

      expect(MockRepo, :transact, fn callback ->
        result = callback.()
        send(test_pid, {:transact_result, result})
        result
      end)

      expect(MockRepo, :put, fn _keyspace, item_key, _value ->
        {priority, _vesting_time, _id} = item_key
        assert priority == 0
        :ok
      end)

      expect(MockRepo, :max, fn _key, _value -> :ok end)
      expect(MockRepo, :add, fn _key, _delta -> :ok end)

      result = Internal.enqueue(TestJobQueue, "tenant_1", "test:topic", %{}, priority: 0, now: now)

      assert {:ok, %Item{priority: 0}} = result
    end
  end

  describe "stats/3" do
    test "returns queue statistics on success" do
      :persistent_term.erase({Internal, TestJobQueue})

      # Store.stats returns %{pending_count: N, processing_count: N}
      expected_stats = %{pending_count: 5, processing_count: 3}

      # 1. repo.transact wraps the callback
      # The callback returns {:ok, stats}, transact returns that directly
      expect(MockRepo, :transact, fn callback ->
        callback.()
      end)

      # 2. Store.stats calls repo.get twice (for pending and processing)
      # Returns encoded counter or nil
      expect(MockRepo, :get, fn keyspace, key ->
        assert Keyspace.prefix(keyspace) =~ "stats"
        assert key == "pending"
        # Counter encoded as 64-bit little endian
        <<5::64-signed-little>>
      end)

      expect(MockRepo, :get, fn keyspace, key ->
        assert Keyspace.prefix(keyspace) =~ "stats"
        assert key == "processing"
        <<3::64-signed-little>>
      end)

      result = Internal.stats(TestJobQueue, "tenant_1", [])

      assert result == expected_stats
    end

    test "returns error when transaction fails" do
      :persistent_term.erase({Internal, TestJobQueue})

      expect(MockRepo, :transact, fn _callback ->
        {:error, :transaction_failed}
      end)

      result = Internal.stats(TestJobQueue, "tenant_1", [])

      assert result == {:error, :transaction_failed}
    end
  end

  describe "init_root/2" do
    test "initializes root directory and caches keyspace" do
      # Clean up any previous state
      :persistent_term.erase({Internal, TestJobQueue})

      expected_prefix = <<1, 2, 3>>

      # init_root calls repo.transact which wraps Directory operations
      expect(MockRepo, :transact, fn _callback ->
        # Simulate successful directory creation by putting keyspace in persistent_term
        :persistent_term.put({Internal, TestJobQueue}, Keyspace.new(expected_prefix))
        {:ok, Keyspace.new(expected_prefix)}
      end)

      result = Internal.init_root(MockRepo, TestJobQueue)

      assert {:ok, %Keyspace{}} = result

      # Verify keyspace was cached
      cached = :persistent_term.get({Internal, TestJobQueue}, nil)
      assert cached != nil
      assert Keyspace.prefix(cached) == expected_prefix

      # Clean up
      :persistent_term.erase({Internal, TestJobQueue})
    end
  end

  describe "root_keyspace/1" do
    test "returns cached keyspace when available" do
      expected_prefix = <<10, 20, 30>>
      expected_keyspace = Keyspace.new(expected_prefix)

      :persistent_term.put({Internal, TestJobQueue}, expected_keyspace)

      result = Internal.root_keyspace(TestJobQueue)

      assert result == expected_keyspace
      assert Keyspace.prefix(result) == expected_prefix

      # Clean up
      :persistent_term.erase({Internal, TestJobQueue})
    end

    test "returns fallback keyspace when not cached" do
      :persistent_term.erase({Internal, TestJobQueue})

      result = Internal.root_keyspace(TestJobQueue)

      # Fallback creates keyspace from module name
      # Module.split returns ["Bedrock", "JobQueue", "InternalTest", "TestJobQueue"]
      expected_prefix = "job_queue/bedrock_jobqueue_internaltest_testjobqueue/"
      assert Keyspace.prefix(result) == expected_prefix
    end

    test "fallback handles nested module names" do
      defmodule Deeply.Nested.Module do
        def __config__, do: %{repo: MockRepo}
      end

      :persistent_term.erase({Internal, Deeply.Nested.Module})

      result = Internal.root_keyspace(Deeply.Nested.Module)

      # Module.split returns ["Bedrock", "JobQueue", "InternalTest", "Deeply", "Nested", "Module"]
      expected_prefix = "job_queue/bedrock_jobqueue_internaltest_deeply_nested_module/"
      assert Keyspace.prefix(result) == expected_prefix
    end
  end
end
