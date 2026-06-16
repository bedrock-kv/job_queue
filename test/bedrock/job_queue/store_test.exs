defmodule Bedrock.JobQueue.StoreTest do
  use ExUnit.Case, async: true

  import Bedrock.JobQueue.Test.StoreHelpers
  import Mox

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :verify_on_exit!

  # Stub transact to execute callbacks immediately
  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    :ok
  end

  defp root, do: Keyspace.new("job_queue/")

  # ============================================================================
  # Pure tests (no repo needed)
  # ============================================================================

  describe "queue_keyspaces/2" do
    test "creates keyspaces with proper structure" do
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      assert %{
               dead_letter: dead_letter,
               items: items,
               leases: leases,
               stats: stats
             } = keyspaces
      assert dead_letter.key_encoding == nil
      assert items.key_encoding == TupleEncoding
      assert leases.key_encoding == nil
      assert stats.key_encoding == nil

      # Verify prefix contains expected path components
      assert String.contains?(Keyspace.prefix(dead_letter), "dead_letter/")
      assert String.contains?(Keyspace.prefix(items), "items/")
      assert String.contains?(Keyspace.prefix(leases), "leases/")
      assert String.contains?(Keyspace.prefix(stats), "stats/")
      refute String.starts_with?(Keyspace.prefix(dead_letter), Keyspace.prefix(items))
    end
  end

  describe "pointer_keyspace/1" do
    test "creates pointer keyspace with tuple encoding" do
      pointers = Store.pointer_keyspace(root())

      assert pointers.key_encoding == TupleEncoding
      assert String.contains?(Keyspace.prefix(pointers), "pointers/")
    end
  end

  describe "tuple key ordering" do
    test "item keys preserve priority ordering" do
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      # Keys are packed with the keyspace prefix
      high_priority_key = Keyspace.pack(keyspaces.items, {10, 1000, <<1>>})
      low_priority_key = Keyspace.pack(keyspaces.items, {100, 1000, <<1>>})

      assert high_priority_key < low_priority_key
    end

    test "item keys with same priority sort by vesting_time" do
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      earlier = Keyspace.pack(keyspaces.items, {100, 1000, <<1>>})
      later = Keyspace.pack(keyspaces.items, {100, 2000, <<1>>})

      assert earlier < later
    end

    test "pointer keys sort by vesting_time" do
      pointers = Store.pointer_keyspace(root())

      earlier = Keyspace.pack(pointers, {1000, "tenant_a"})
      later = Keyspace.pack(pointers, {2000, "tenant_b"})

      assert earlier < later
    end
  end

  describe "peek/4 priority ordering" do
    test "ignores non-item rows under the item scan prefix" do
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{}, priority: 100, vesting_time: 1000)
      encoded_item = :erlang.term_to_binary(item)
      packed_item_key = Keyspace.pack(keyspaces.items, Item.key(item))
      legacy_dead_letter_key =
        Keyspace.prefix(keyspaces.items) <>
          TupleEncoding.pack("../dead_letter/") <> TupleEncoding.pack("1000/#{item.id}")
      legacy_dead_letter_item = :erlang.term_to_binary(%{item | id: "dead-lettered"})

      expect(MockRepo, :get_range, fn
        %Keyspace{}, _opts ->
          flunk("tuple-encoded item keyspaces must be scanned as raw ranges")

        {start_key, end_key}, _opts when is_binary(start_key) and is_binary(end_key) ->
          assert packed_item_key >= start_key
          assert packed_item_key < end_key

          [
            {legacy_dead_letter_key, legacy_dead_letter_item},
            {packed_item_key, encoded_item}
          ]
      end)

      assert [%Item{id: item_id}] = Store.peek(MockRepo, root(), queue_id, limit: 10, now: 2000)
      assert item_id == item.id
    end

    test "returns items in priority order (lowest number first)" do
      # Create items with different priorities
      high_priority = Item.new("tenant_1", "topic", %{}, priority: 10, vesting_time: 1000)
      medium_priority = Item.new("tenant_1", "topic", %{}, priority: 50, vesting_time: 1000)
      low_priority = Item.new("tenant_1", "topic", %{}, priority: 200, vesting_time: 1000)
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      # Encode items
      items = [
        {
          Keyspace.pack(keyspaces.items, Item.key(low_priority)),
          :erlang.term_to_binary(low_priority)
        },
        {
          Keyspace.pack(keyspaces.items, Item.key(high_priority)),
          :erlang.term_to_binary(high_priority)
        },
        {
          Keyspace.pack(keyspaces.items, Item.key(medium_priority)),
          :erlang.term_to_binary(medium_priority)
        }
      ]

      # Mock returns items in arbitrary order - peek should sort by key
      expect(MockRepo, :get_range, fn {_start_key, _end_key}, _opts ->
        # Return sorted by key (simulating DB behavior)
        Enum.sort_by(items, fn {key, _} -> key end)
      end)

      result = Store.peek(MockRepo, root(), "tenant_1", limit: 10, now: 2000)

      # Should be in priority order: 10, 50, 200
      priorities = Enum.map(result, & &1.priority)
      assert priorities == [10, 50, 200]
    end

    test "maintains priority order with same priority but different vesting_times" do
      # Same priority, different vesting times
      earlier =
        Item.new("tenant_1", "topic", %{data: "earlier"}, priority: 100, vesting_time: 1000)

      later = Item.new("tenant_1", "topic", %{data: "later"}, priority: 100, vesting_time: 2000)
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      items = [
        {
          Keyspace.pack(keyspaces.items, Item.key(later)),
          :erlang.term_to_binary(later)
        },
        {
          Keyspace.pack(keyspaces.items, Item.key(earlier)),
          :erlang.term_to_binary(earlier)
        }
      ]

      expect(MockRepo, :get_range, fn {_start_key, _end_key}, _opts ->
        Enum.sort_by(items, fn {key, _} -> key end)
      end)

      result = Store.peek(MockRepo, root(), "tenant_1", limit: 10, now: 3000)

      # Earlier vesting_time should come first
      vesting_times = Enum.map(result, & &1.vesting_time)
      assert vesting_times == [1000, 2000]
    end
  end

  describe "Item visibility" do
    test "items with past vesting_time and no lease are visible" do
      now = 10_000
      item = Item.new("queue", "topic", %{}, vesting_time: 9_000)

      assert Item.visible?(item, now)
    end

    test "items with future vesting_time are not visible" do
      now = 10_000
      item = Item.new("queue", "topic", %{}, vesting_time: 20_000)

      refute Item.visible?(item, now)
    end

    test "items with lease are not visible" do
      now = 10_000
      item = %{Item.new("queue", "topic", %{}, vesting_time: 9_000) | lease_id: <<1, 2, 3>>}

      refute Item.visible?(item, now)
    end

    test "items with expired lease are visible again" do
      now = 10_000

      item = %{
        Item.new("queue", "topic", %{}, vesting_time: 9_000)
        | lease_id: <<1, 2, 3>>,
          lease_expires_at: 9_000
      }

      assert Item.visible?(item, now)
    end
  end

  describe "Lease creation" do
    test "creates lease with duration" do
      item = Item.new("queue", "topic", %{})
      lease = Lease.new(item, "holder", duration_ms: 5000)

      assert lease.item_id == item.id
      assert lease.queue_id == item.queue_id
      assert lease.holder == "holder"
      assert lease.expires_at > lease.obtained_at
      assert lease.expires_at - lease.obtained_at >= 5000
    end

    test "creates lease with default duration" do
      item = Item.new("queue", "topic", %{})
      lease = Lease.new(item, "holder")

      # Default is 30 seconds
      assert lease.expires_at - lease.obtained_at >= 30_000
    end

    test "stores item_key for O(1) lookup" do
      item = Item.new("queue", "topic", %{}, priority: 50)
      lease = Lease.new(item, "holder", duration_ms: 5000)

      # item_key should be {priority, new_vesting_time, id}
      {priority, vesting_time, id} = lease.item_key
      assert priority == 50
      assert vesting_time == lease.expires_at
      assert id == item.id
    end
  end

  describe "Lease expiration" do
    test "fresh lease is not expired" do
      now = 10_000
      item = Item.new("queue", "topic", %{})
      lease = Lease.new(item, "holder", duration_ms: 5000, now: now)

      refute Lease.expired?(lease, now: now)
      assert Lease.remaining_ms(lease, now: now) == 5000
    end

    test "expired lease reports expired" do
      item = Item.new("queue", "topic", %{})
      # Created at 4000, expires at 9000
      lease = Lease.new(item, "holder", duration_ms: 5000, now: 4_000)
      now = 10_000

      assert Lease.expired?(lease, now: now)
      assert Lease.remaining_ms(lease, now: now) == 0
    end
  end

  describe "QueueLease creation" do
    test "creates queue lease with duration" do
      lease = QueueLease.new("tenant_1", "holder_123", duration_ms: 5000)

      assert lease.queue_id == "tenant_1"
      assert lease.holder == "holder_123"
      assert lease.expires_at > lease.obtained_at
      assert lease.expires_at - lease.obtained_at >= 5000
      assert is_binary(lease.id) and byte_size(lease.id) == 16
    end

    test "creates queue lease with default duration" do
      lease = QueueLease.new("tenant_1", "holder")

      # Default is 5 seconds
      assert lease.expires_at - lease.obtained_at >= 5000
    end
  end

  describe "QueueLease expiration" do
    test "fresh queue lease is not expired" do
      now = 10_000
      lease = QueueLease.new("tenant_1", "holder", duration_ms: 5000, now: now)

      refute QueueLease.expired?(lease, now: now)
      assert QueueLease.remaining_ms(lease, now: now) == 5000
    end

    test "expired queue lease reports expired" do
      # Created at 4000, expires at 9000
      lease = QueueLease.new("tenant_1", "holder", duration_ms: 5000, now: 4_000)
      now = 10_000

      assert QueueLease.expired?(lease, now: now)
      assert QueueLease.remaining_ms(lease, now: now) == 0
    end
  end

  describe "queue_lease_keyspace/1" do
    test "creates queue lease keyspace" do
      ks = Store.queue_lease_keyspace(root())

      assert String.contains?(Keyspace.prefix(ks), "queue_leases/")
    end
  end

  # ============================================================================
  # Mox-based Store operation tests
  # ============================================================================

  describe "obtain_queue_lease/5" do
    test "succeeds on empty queue" do
      MockRepo
      |> expect_queue_lease_get("tenant_1", nil)
      |> expect_queue_lease_put("tenant_1")

      result = Store.obtain_queue_lease(MockRepo, root(), "tenant_1", "holder", 5000)

      assert {:ok, %QueueLease{queue_id: "tenant_1", holder: "holder"}} = result
    end

    test "fails when queue already leased" do
      now = 10_000
      # Create a non-expired lease (created at same time, expires at 15_000)
      existing = QueueLease.new("tenant_1", "holder1", duration_ms: 5000, now: now)
      encoded = :erlang.term_to_binary(existing)

      expect_queue_lease_get(MockRepo, "tenant_1", encoded)
      result = Store.obtain_queue_lease(MockRepo, root(), "tenant_1", "holder2", 5000, now: now)

      assert {:error, :queue_leased} = result
    end

    test "succeeds after previous lease expires" do
      # Create a lease that expires at 9_000
      expired = QueueLease.new("tenant_1", "holder1", duration_ms: 5000, now: 4_000)
      encoded = :erlang.term_to_binary(expired)
      now = 10_000

      MockRepo
      |> expect_queue_lease_get("tenant_1", encoded)
      |> expect_queue_lease_put("tenant_1")

      result = Store.obtain_queue_lease(MockRepo, root(), "tenant_1", "holder2", 5000, now: now)

      assert {:ok, %QueueLease{holder: "holder2"}} = result
    end
  end

  describe "release_queue_lease/3" do
    test "removes the lease" do
      lease = QueueLease.new("tenant_1", "holder", duration_ms: 5000)
      encoded = :erlang.term_to_binary(lease)

      MockRepo
      |> expect_queue_lease_get("tenant_1", encoded)
      |> expect_queue_lease_clear("tenant_1")

      result = Store.release_queue_lease(MockRepo, root(), lease)

      assert :ok = result
    end

    test "fails with mismatched lease" do
      stored = QueueLease.new("tenant_1", "holder1", duration_ms: 5000)
      encoded = :erlang.term_to_binary(stored)

      # Try to release with different lease ID
      fake = QueueLease.new("tenant_1", "attacker", duration_ms: 5000)

      expect_queue_lease_get(MockRepo, "tenant_1", encoded)
      result = Store.release_queue_lease(MockRepo, root(), fake)

      assert {:error, :lease_mismatch} = result
    end

    test "fails when lease not found" do
      lease = QueueLease.new("tenant_1", "holder", duration_ms: 5000)

      expect_queue_lease_get(MockRepo, "tenant_1", nil)
      result = Store.release_queue_lease(MockRepo, root(), lease)

      assert {:error, :lease_not_found} = result
    end
  end

  describe "enqueue/3" do
    test "writes item, updates pointer, increments stats" do
      item = Item.new("tenant_1", "topic", %{n: 1})

      expect_enqueue(MockRepo, item)
      result = Store.enqueue(MockRepo, root(), item)

      assert :ok = result
    end
  end

  describe "dequeue/5" do
    test "returns empty list when no visible items" do
      expect_dequeue_empty(MockRepo, "tenant_1")

      result =
        Store.dequeue(MockRepo, root(), "tenant_1", "holder", limit: 5, lease_duration: 5000)

      assert {:ok, []} = result
    end

    test "reclaims items whose lease expired" do
      now = 10_000
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{n: 1}, vesting_time: 8_000)
      expired_lease = Lease.new(item, "old_holder", duration_ms: 1_000, now: 8_000)

      leased_item = %{
        item
        | lease_id: expired_lease.id,
          lease_expires_at: expired_lease.expires_at,
          vesting_time: expired_lease.expires_at
      }

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, leased_item)

      assert [%Item{id: item_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert item_id == item.id

      assert {:ok, [%Lease{holder: "new_holder"}]} =
               Store.dequeue(MockRepo, root(), queue_id, "new_holder",
                 now: now,
                 lease_duration: 5_000
               )
    end
  end

  describe "complete/3" do
    test "uses the stored lease item key when the caller lease is stale" do
      now = 10_000
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{n: 1}, vesting_time: now)
      stale_lease = Lease.new(item, "holder", duration_ms: 5_000, now: now)
      extended_expires_at = stale_lease.expires_at + 5_000
      current_item_key = {item.priority, extended_expires_at, item.id}
      stored_lease = %{stale_lease | expires_at: extended_expires_at, item_key: current_item_key}

      current_item = %{
        item
        | lease_id: stale_lease.id,
          lease_expires_at: extended_expires_at,
          vesting_time: extended_expires_at
      }

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, current_item)
      MockRepo.put(keyspaces.leases, stale_lease.item_id, :erlang.term_to_binary(stored_lease))

      assert :ok = Store.complete(MockRepo, root(), stale_lease)
      assert MockRepo.get(keyspaces.items, current_item_key) == nil
      assert MockRepo.get(keyspaces.leases, stale_lease.item_id) == nil
    end
  end

  describe "requeue/4" do
    test "uses base_delay for the first retry visibility time" do
      now = 10_000
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{n: 1}, max_retries: 3, vesting_time: now)
      lease = Lease.new(item, "holder", duration_ms: 5_000, now: now)

      leased_item = %{
        item
        | lease_id: lease.id,
          lease_expires_at: lease.expires_at,
          vesting_time: lease.expires_at
      }

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, leased_item)
      MockRepo.put(keyspaces.leases, lease.item_id, :erlang.term_to_binary(lease))

      assert {:ok, :requeued} = Store.requeue(MockRepo, root(), lease, now: now, base_delay: 1_000)
      assert [] = Store.peek(MockRepo, root(), queue_id, now: now + 999)
      assert [%Item{id: item_id, error_count: 1}] =
               Store.peek(MockRepo, root(), queue_id, now: now + 1_000)

      assert item_id == item.id
    end

    test "uses the stored lease item key when the caller lease is stale" do
      now = 10_000
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{n: 1}, max_retries: 3, vesting_time: now)
      stale_lease = Lease.new(item, "holder", duration_ms: 5_000, now: now)
      extended_expires_at = stale_lease.expires_at + 5_000
      current_item_key = {item.priority, extended_expires_at, item.id}
      stored_lease = %{stale_lease | expires_at: extended_expires_at, item_key: current_item_key}

      current_item = %{
        item
        | lease_id: stale_lease.id,
          lease_expires_at: extended_expires_at,
          vesting_time: extended_expires_at
      }

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, current_item)
      MockRepo.put(keyspaces.leases, stale_lease.item_id, :erlang.term_to_binary(stored_lease))

      assert {:ok, :requeued} =
               Store.requeue(MockRepo, root(), stale_lease, now: now, base_delay: 1_000)

      assert MockRepo.get(keyspaces.items, current_item_key) == nil

      assert [%Item{id: item_id, error_count: 1, lease_id: nil}] =
               Store.peek(MockRepo, root(), queue_id, now: now + 1_000)

      assert item_id == item.id
    end

    test "dead letters max-retry items outside the item scan prefix" do
      now = 10_000
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{n: 1}, max_retries: 1, vesting_time: now)
      lease = Lease.new(item, "holder", duration_ms: 5_000, now: now)

      leased_item = %{
        item
        | lease_id: lease.id,
          lease_expires_at: lease.expires_at,
          vesting_time: lease.expires_at
      }

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, leased_item)
      MockRepo.put(keyspaces.leases, lease.item_id, :erlang.term_to_binary(lease))

      assert {:ok, :dead_lettered} = Store.requeue(MockRepo, root(), lease, now: now)
      assert [] = Store.peek(MockRepo, root(), queue_id, now: now + 1_000)
      assert MockRepo.get(keyspaces.items, Item.key(leased_item)) == nil
      assert MockRepo.get(keyspaces.leases, lease.item_id) == nil

      dead_letter_entries =
        Agent.get(store, fn state ->
          Enum.filter(state, fn {{prefix, _key}, _value} ->
            prefix == Keyspace.prefix(keyspaces.dead_letter)
          end)
        end)

      assert [{_storage_key, encoded_item}] = dead_letter_entries
      assert %Item{id: item_id} = :erlang.binary_to_term(encoded_item)
      assert item_id == item.id
    end
  end

  describe "gc_stale_pointers/3" do
    test "handles empty pointer list gracefully" do
      # First call: get stale pointers (empty for this test)
      expect(MockRepo, :get_range, fn _range, _opts -> [] end)

      # The function should handle empty pointer list gracefully
      result = Store.gc_stale_pointers(MockRepo, root(), limit: 10)

      assert result == 0
    end
  end
end
