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
    # The scheduling index adds internal point and range reads. Individual
    # tests retain strict expectations for the queue operation under test while
    # these defaults model an empty index where they do not care about it.
    stub(MockRepo, :get, fn _keyspace, _key -> nil end)
    stub(MockRepo, :put, fn _keyspace, _key, _value -> :ok end)
    stub(MockRepo, :clear, fn _keyspace, _key -> :ok end)
    stub(MockRepo, :get_range, fn _range, _opts -> [] end)
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
               identity_metadata: identity_metadata,
               identities: identities,
               items: items,
               leases: leases,
               priority_index: priority_index,
               stats: stats
             } = keyspaces
      assert dead_letter.key_encoding == nil
      assert identity_metadata.key_encoding == nil
      assert identities.key_encoding == nil
      assert items.key_encoding == TupleEncoding
      assert leases.key_encoding == nil
      assert priority_index.key_encoding == TupleEncoding
      assert stats.key_encoding == nil

      # Verify prefix contains expected path components
      assert String.contains?(Keyspace.prefix(dead_letter), "dead_letter/")
      assert String.contains?(Keyspace.prefix(identity_metadata), "identity_metadata/")
      assert String.contains?(Keyspace.prefix(identities), "identities/")
      assert String.contains?(Keyspace.prefix(items), "items/")
      assert String.contains?(Keyspace.prefix(leases), "leases/")
      assert String.contains?(Keyspace.prefix(priority_index), "priority_index/")
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
    test "finds ready lower-priority work beyond future higher-priority rows" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      queue_id = "many-future-items"
      now = 10_000

      for sequence <- 1..100 do
        future =
          Item.new(queue_id, "future", %{sequence: sequence},
            id: <<sequence::128>>,
            priority: 0,
            vesting_time: 20_000
          )

        assert :ok = Store.enqueue(MockRepo, root(), future, now: now)
      end

      ready = Item.new(queue_id, "ready", %{}, priority: 100, vesting_time: now)
      assert :ok = Store.enqueue(MockRepo, root(), ready, now: now)

      assert [%Item{id: ready_id}] = Store.peek(MockRepo, root(), queue_id, limit: 10, now: now)
      assert ready_id == ready.id
    end

    test "keeps priority ordering among ready work from multiple priorities" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "mixed-priorities"

      low = Item.new(queue_id, "low", %{}, priority: 200, vesting_time: now)
      high = Item.new(queue_id, "high", %{}, priority: 10, vesting_time: now)
      future = Item.new(queue_id, "future", %{}, priority: 0, vesting_time: now + 10_000)

      for item <- [low, high, future] do
        assert :ok = Store.enqueue(MockRepo, root(), item, now: now)
      end

      assert [first, second] = Store.peek(MockRepo, root(), queue_id, limit: 10, now: now)
      assert [first.id, second.id] == [high.id, low.id]
    end

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

    test "does not create a partial priority index for an existing queue" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "legacy-queue"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      existing = Item.new(queue_id, "existing", %{}, priority: 0, vesting_time: now)
      incoming = Item.new(queue_id, "incoming", %{}, priority: 100, vesting_time: now)

      # Simulate a queue written before the scheduling index was introduced.
      store_item(store, keyspaces.items, existing)

      assert :ok = Store.enqueue(MockRepo, root(), incoming, now: now)
      assert MockRepo.get(keyspaces.priority_index, {0, 0}) == nil

      assert [first, second] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert [first.id, second.id] == [existing.id, incoming.id]
    end

    test "uses a custom ID as a queue-scoped idempotency key before leasing" do
      now = 10_000
      queue_id = "tenant_1"
      first = Item.new(queue_id, "email:send", %{attempt: 1}, id: "email-42", vesting_time: now)

      retry =
        Item.new(queue_id, "email:send", %{attempt: 2},
          id: "email-42",
          vesting_time: now + 1_000
        )

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      assert :ok = Store.enqueue(MockRepo, root(), first, now: now)
      assert :ok = Store.enqueue(MockRepo, root(), retry, now: now + 1_000)
      assert %{pending_count: 1, processing_count: 0} = Store.stats(MockRepo, root(), queue_id)
      assert 1 == item_count(store, queue_id)
    end

    test "does not create another custom-ID item while the first is leased" do
      now = 10_000
      queue_id = "tenant_1"
      first = Item.new(queue_id, "email:send", %{}, id: "email-42", vesting_time: now)
      retry = Item.new(queue_id, "email:send", %{}, id: "email-42", vesting_time: now + 1_000)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      assert :ok = Store.enqueue(MockRepo, root(), first, now: now)
      assert {:ok, [_lease]} = Store.dequeue(MockRepo, root(), queue_id, "worker", now: now)
      assert :ok = Store.enqueue(MockRepo, root(), retry, now: now + 1_000)
      assert %{pending_count: 0, processing_count: 1} = Store.stats(MockRepo, root(), queue_id)
      assert 1 == item_count(store, queue_id)
      assert 1 == lease_count(store, queue_id)
    end

    test "keeps a custom ID idempotent after completion" do
      now = 10_000
      queue_id = "tenant_1"
      first = Item.new(queue_id, "email:send", %{}, id: "email-42", vesting_time: now)
      retry = Item.new(queue_id, "email:send", %{}, id: "email-42", vesting_time: now + 1_000)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      assert :ok = Store.enqueue(MockRepo, root(), first, now: now)
      assert {:ok, [lease]} = Store.dequeue(MockRepo, root(), queue_id, "worker", now: now)
      assert :ok = Store.complete(MockRepo, root(), lease)
      assert :ok = Store.enqueue(MockRepo, root(), retry, now: now + 1_000)
      assert %{pending_count: 0, processing_count: 0} = Store.stats(MockRepo, root(), queue_id)
      assert 0 == item_count(store, queue_id)
      assert 1 == identity_count(store, queue_id)
    end

    test "continues to enqueue distinct generated IDs" do
      now = 10_000
      queue_id = "tenant_1"
      first = Item.new(queue_id, "email:send", %{}, vesting_time: now)
      second = Item.new(queue_id, "email:send", %{}, vesting_time: now)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      assert :ok = Store.enqueue(MockRepo, root(), first, now: now)
      assert :ok = Store.enqueue(MockRepo, root(), second, now: now)
      assert %{pending_count: 2, processing_count: 0} = Store.stats(MockRepo, root(), queue_id)
      assert 2 == item_count(store, queue_id)
      assert 0 == identity_count(store, queue_id)
    end

    test "allows the same custom ID in different queues" do
      now = 10_000
      first = Item.new("tenant_1", "email:send", %{}, id: "email-42", vesting_time: now)
      second = Item.new("tenant_2", "email:send", %{}, id: "email-42", vesting_time: now)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      assert :ok = Store.enqueue(MockRepo, root(), first, now: now)
      assert :ok = Store.enqueue(MockRepo, root(), second, now: now)
      assert 1 == item_count(store, "tenant_1")
      assert 1 == item_count(store, "tenant_2")
      assert 1 == identity_count(store, "tenant_1")
      assert 1 == identity_count(store, "tenant_2")
    end

    test "migrates a queued legacy custom ID without creating another item" do
      now = 10_000
      queue_id = "tenant_1"
      legacy = legacy_item(queue_id, "email-42", vesting_time: now)
      retry = Item.new(queue_id, "email:send", %{retry: true}, id: "email-42", vesting_time: now + 1_000)
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, legacy)

      assert :ok = Store.enqueue(MockRepo, root(), retry, now: now)
      assert 1 == item_count(store, queue_id)
      assert 1 == identity_count(store, queue_id)
    end

    test "migrates a leased legacy custom ID without sharing its lease record" do
      now = 10_000
      queue_id = "tenant_1"
      legacy = legacy_item(queue_id, "email-42", vesting_time: now)
      lease = Lease.new(legacy, "worker", now: now)

      leased_legacy = %{
        legacy
        | lease_id: lease.id,
          lease_expires_at: lease.expires_at,
          vesting_time: lease.expires_at
      }

      retry = Item.new(queue_id, "email:send", %{retry: true}, id: "email-42", vesting_time: now + 1_000)
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, leased_legacy)
      MockRepo.put(keyspaces.leases, lease.item_id, :erlang.term_to_binary(lease))

      assert :ok = Store.enqueue(MockRepo, root(), retry, now: now)

      assert 1 == item_count(store, queue_id)
      assert 1 == lease_count(store, queue_id)
      assert 1 == identity_count(store, queue_id)
    end

    test "rejects an unknown custom ID in a legacy queue after completion" do
      now = 10_000
      queue_id = "tenant_1"
      retry = Item.new(queue_id, "email:send", %{retry: true}, id: "email-42", vesting_time: now)
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_counter(store, keyspaces.stats, "pending", 0)
      store_counter(store, keyspaces.stats, "processing", 0)

      assert {:error, :legacy_custom_id_unknown} = Store.enqueue(MockRepo, root(), retry, now: now)

      assert 0 == item_count(store, queue_id)
      assert 0 == identity_count(store, queue_id)
    end

    test "decodes a legacy serialized item without a custom-ID marker" do
      legacy = legacy_item("tenant_1", "email-42", vesting_time: 1_000)
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      assert %Item{id: "email-42", vesting_time: 1_000} = legacy
      refute Map.has_key?(legacy, :custom_id?)
      assert Item.key(legacy) == {100, 1_000, "email-42"}
      assert Item.visible?(legacy, 1_000)
      assert :ok = Store.enqueue(MockRepo, root(), legacy, now: 1_000)
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

      assert :ok =
               Store.complete(MockRepo, root(), stale_lease, now: extended_expires_at - 1)
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

  describe "min_vesting_time/4" do
    test "returns the true minimum beyond the former scan bound" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      queue_id = "many-scheduled-items"
      now = 10_000

      for sequence <- 1..1_000 do
        future =
          Item.new(queue_id, "future", %{sequence: sequence},
            id: <<sequence::128>>,
            priority: 0,
            vesting_time: 20_000
          )

        assert :ok = Store.enqueue(MockRepo, root(), future, now: now)
      end

      ready = Item.new(queue_id, "ready", %{}, priority: 100, vesting_time: now)
      assert :ok = Store.enqueue(MockRepo, root(), ready, now: now)

      assert Store.min_vesting_time(MockRepo, root(), queue_id) == now
    end

    test "tracks each queue independently after a lease moves its vesting time" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      first = Item.new("first", "job", %{}, priority: 10, vesting_time: now)
      second = Item.new("second", "job", %{}, priority: 10, vesting_time: now + 500)

      assert :ok = Store.enqueue(MockRepo, root(), first, now: now)
      assert :ok = Store.enqueue(MockRepo, root(), second, now: now)
      assert {:ok, _lease} = Store.obtain_lease(MockRepo, root(), first, "worker", 1_000, now: now)

      assert Store.min_vesting_time(MockRepo, root(), "first") == now + 1_000
      assert Store.min_vesting_time(MockRepo, root(), "second") == now + 500
    end
  end

  defp item_count(store, queue_id) do
    count_entries(store, root() |> Store.queue_keyspaces(queue_id) |> Map.fetch!(:items))
  end

  defp lease_count(store, queue_id) do
    count_entries(store, root() |> Store.queue_keyspaces(queue_id) |> Map.fetch!(:leases))
  end

  defp identity_count(store, queue_id) do
    count_entries(store, root() |> Store.queue_keyspaces(queue_id) |> Map.fetch!(:identities))
  end

  defp legacy_item(queue_id, id, opts) do
    queue_id
    |> Item.new("email:send", %{legacy: true}, Keyword.put(opts, :id, id))
    |> Map.from_struct()
    |> Map.delete(:custom_id?)
    |> Map.put(:__struct__, Item)
    |> :erlang.term_to_binary()
    |> :erlang.binary_to_term()
  end

  defp count_entries(store, keyspace) do
    prefix = Keyspace.prefix(keyspace)

    Agent.get(store, fn state ->
      Enum.count(state, fn
        {{entry_prefix, _key}, _value} -> entry_prefix == prefix
        _ -> false
      end)
    end)
  end
end
