defmodule Bedrock.JobQueue.StoreTest do
  use ExUnit.Case, async: true

  import Bedrock.JobQueue.Test.StoreHelpers
  import Bitwise
  import Mox

  alias Bedrock.DataPlane.Materializer.Olivine.Index, as: OlivineIndex
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page, as: OlivinePage
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree, as: OlivineTree
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager, as: OlivineIndexManager
  alias Bedrock.DataPlane.Version
  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :verify_on_exit!

  @migration_chunk_size 8
  @migration_tree_point_operations_per_item 197

  # Stub transact to execute callbacks immediately
  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    # The scheduling index adds internal point and range reads. Individual
    # tests retain strict expectations for the queue operation under test while
    # these defaults model an empty index where they do not care about it.
    stub(MockRepo, :get, fn _keyspace, _key -> nil end)
    stub(MockRepo, :put, fn _keyspace, _key, _value -> :ok end)
    stub(MockRepo, :clear, fn _keyspace, _key -> :ok end)
    stub(MockRepo, :clear_range, fn _range -> :ok end)
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

    test "keeps priority indexes isolated between queues" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      future = Item.new("queue-a", "future", %{}, priority: 0, vesting_time: now + 10_000)
      ready_a = Item.new("queue-a", "ready", %{}, priority: 100, vesting_time: now)
      ready_b = Item.new("queue-b", "ready", %{}, priority: 100, vesting_time: now)

      for item <- [future, ready_a, ready_b] do
        assert :ok = Store.enqueue(MockRepo, root(), item, now: now)
      end

      assert [%Item{id: id_a}] = Store.peek(MockRepo, root(), "queue-a", now: now)
      assert id_a == ready_a.id
      assert [%Item{id: id_b}] = Store.peek(MockRepo, root(), "queue-b", now: now)
      assert id_b == ready_b.id
    end

    test "ignores non-item rows while rebuilding an upgraded queue index" do
      {:ok, store} = start_mock_store()
      queue_id = "tenant_1"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      item = Item.new(queue_id, "topic", %{}, priority: 100, vesting_time: 1_000)
      packed_item_key = Keyspace.pack(keyspaces.items, Item.key(item))

      legacy_dead_letter_key =
        Keyspace.prefix(keyspaces.items) <>
          TupleEncoding.pack("../dead_letter/") <> TupleEncoding.pack("1000/#{item.id}")

      setup_integration_stubs(MockRepo, store, [
        {legacy_dead_letter_key, :erlang.term_to_binary(%{item | id: "dead-lettered"})},
        {packed_item_key, :erlang.term_to_binary(item)}
      ])

      assert [] = Store.peek(MockRepo, root(), queue_id, limit: 10, now: 2_000)
      assert :ready = migrate_queue!(queue_id)
      assert [%Item{id: item_id}] = Store.peek(MockRepo, root(), queue_id, limit: 10, now: 2_000)
      assert item_id == item.id
    end

    test "returns items in priority order (lowest number first)" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      # Create items with different priorities
      high_priority = Item.new("tenant_1", "topic", %{}, priority: 10, vesting_time: 1000)
      medium_priority = Item.new("tenant_1", "topic", %{}, priority: 50, vesting_time: 1000)
      low_priority = Item.new("tenant_1", "topic", %{}, priority: 200, vesting_time: 1000)

      for item <- [low_priority, high_priority, medium_priority] do
        assert :ok = Store.enqueue(MockRepo, root(), item, now: 1000)
      end

      result = Store.peek(MockRepo, root(), "tenant_1", limit: 10, now: 2000)

      # Should be in priority order: 10, 50, 200
      priorities = Enum.map(result, & &1.priority)
      assert priorities == [10, 50, 200]
    end

    test "maintains priority order with same priority but different vesting_times" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      # Same priority, different vesting times
      earlier =
        Item.new("tenant_1", "topic", %{data: "earlier"}, priority: 100, vesting_time: 1000)

      later = Item.new("tenant_1", "topic", %{data: "later"}, priority: 100, vesting_time: 2000)

      for item <- [later, earlier] do
        assert :ok = Store.enqueue(MockRepo, root(), item, now: 1_000)
      end

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

    test "atomically initializes an empty queue before accepting its first job" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      item = Item.new("brand-new", "topic", %{}, vesting_time: now)

      assert {:ok, ^item} = Store.enqueue_with_item(MockRepo, root(), item, now: now)
      assert :ready = Store.priority_index_status(MockRepo, root(), item.queue_id)
      assert [%Item{id: item_id}] = Store.peek(MockRepo, root(), item.queue_id, now: now)
      assert item_id == item.id
    end

    test "holds a nonempty pre-index queue until an administrator confirms its writer fence" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "legacy-fence-required"
      keyspaces = Store.queue_keyspaces(root(), queue_id)
      legacy = Item.new(queue_id, "legacy", %{}, priority: 0, vesting_time: now)
      new_item = Item.new(queue_id, "new", %{}, priority: 1, vesting_time: now)
      store_item(store, keyspaces.items, legacy)

      # A root written before the lifecycle marker is untrusted too: an older
      # release could have built only a partial tree.
      MockRepo.put(keyspaces.priority_index, {"root"}, <<now::64-little>>)

      assert :writer_fence_required = Store.priority_index_status(MockRepo, root(), queue_id)

      assert {:error, :priority_index_migration_required} =
               Store.enqueue(MockRepo, root(), new_item, now: now)

      assert [] = Store.peek(MockRepo, root(), queue_id, now: now)

      assert {:error, :priority_index_migration_required} =
               Store.min_vesting_time(MockRepo, root(), queue_id)

      assert {:error, :priority_index_migration_required} =
               Store.obtain_lease(MockRepo, root(), legacy, "worker", 1_000, now: now)

      lease = Lease.new(legacy, "worker", duration_ms: 1_000, now: now)

      assert {:error, :priority_index_migration_required} =
               Store.extend_lease(MockRepo, root(), lease, 1_000, now: now)

      assert {:error, :priority_index_migration_required} = Store.complete(MockRepo, root(), lease)
      assert {:error, :priority_index_migration_required} = Store.requeue(MockRepo, root(), lease, now: now)

      assert MockRepo.get(keyspaces.priority_index, {"migration"}) == nil
      assert MockRepo.get(keyspaces.priority_index, {"initialized"}) == nil
    end

    test "only an explicit offline migration advances a static legacy queue" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "fenced-migration"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      for priority <- 0..15 do
        store_item(
          store,
          keyspaces.items,
          Item.new(queue_id, "future", %{},
            id: <<priority::128>>,
            priority: priority,
            vesting_time: now + 10_000
          )
        )
      end

      ready = Item.new(queue_id, "ready", %{}, priority: 16, vesting_time: now)
      store_item(store, keyspaces.items, ready)

      # Normal reads never turn legacy data into an implicit rolling migration.
      assert [] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert :writer_fence_required = Store.priority_index_status(MockRepo, root(), queue_id)

      assert {:error, :writer_fence_required} = Store.migrate_priority_index(MockRepo, root(), queue_id)
      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert :migrating = Store.priority_index_status(MockRepo, root(), queue_id)
      assert {:error, :writer_fence_required} = Store.migrate_priority_index(MockRepo, root(), queue_id)
      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert :ready = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert :ready = Store.priority_index_status(MockRepo, root(), queue_id)

      assert [%Item{id: ready_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert ready_id == ready.id
      assert Store.min_vesting_time(MockRepo, root(), queue_id) == now
    end

    test "holds every normal queue operation until an offline migration completes" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "static-offline-migration"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      legacy_items =
        for priority <- 0..7 do
          item =
            Item.new(queue_id, "legacy", %{},
              id: <<priority::128>>,
              priority: priority,
              vesting_time: now + 10_000
            )

          store_item(store, keyspaces.items, item)
          item
        end

      ready = Item.new(queue_id, "ready", %{}, priority: 100, vesting_time: now)
      store_item(store, keyspaces.items, ready)

      # The first administrative call is one bounded item chunk. From this
      # point the declared offline fence holds both old and current writers.
      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert :migrating = Store.priority_index_status(MockRepo, root(), queue_id)
      assert [] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert {:error, :priority_index_migration_required} = Store.min_vesting_time(MockRepo, root(), queue_id)

      current = Item.new(queue_id, "current", %{}, priority: 200, vesting_time: now)
      lease = Lease.new(hd(legacy_items), "worker", now: now)
      queue_lease = QueueLease.new(queue_id, "worker", now: now)

      assert {:error, :priority_index_migration_required} = Store.enqueue(MockRepo, root(), current, now: now)

      assert {:error, :priority_index_migration_required} =
               Store.obtain_queue_lease(MockRepo, root(), queue_id, "worker", 1_000, now: now)

      assert {:error, :priority_index_migration_required} = Store.release_queue_lease(MockRepo, root(), queue_lease)

      assert {:error, :priority_index_migration_required} =
               Store.obtain_lease(MockRepo, root(), hd(legacy_items), "worker", 1_000, now: now)

      assert {:error, :priority_index_migration_required} = Store.extend_lease(MockRepo, root(), lease, 1_000, now: now)
      assert {:error, :priority_index_migration_required} = Store.complete(MockRepo, root(), lease)
      assert {:error, :priority_index_migration_required} = Store.requeue(MockRepo, root(), lease, now: now)

      assert {:error, :priority_index_migration_required} =
               Store.update_queue_pointer(MockRepo, root(), queue_id, now, now: now)

      assert :ready = migrate_queue!(queue_id)
      assert [%Item{id: ready_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert ready_id == ready.id
      assert Store.min_vesting_time(MockRepo, root(), queue_id) == now
    end

    test "migrates an empty queue with one raw forward range" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store, [], observer: self())

      queue_id = "empty-offline-migration"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      # An unrelated row cannot affect a raw range bounded by the item prefix.
      MockRepo.put(keyspaces.identities, "unrelated", "value")

      assert :empty = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      current = Item.new(queue_id, "current", %{}, priority: 100, vesting_time: 10_000)
      assert :empty = Store.priority_index_status(MockRepo, root(), queue_id)

      operations = drain_store_operations()
      refute Enum.any?(operations, &match?({:select, _}, &1))

      assert [{:get_range, {_start_key, _end_key}, opts}] =
               Enum.filter(operations, &match?({:get_range, {_, _}, _}, &1))

      assert opts[:limit] == @migration_chunk_size

      assert :ok = Store.enqueue(MockRepo, root(), current, now: 10_000)
    end

    test "holds an old rolling-upgrade migration marker until the administrator re-fences it" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000

      for {queue_id, marker} <- [
            {"cursor-only-migration-marker", {:building, nil}},
            {"frontier-migration-marker", {:building, nil, "old-frontier"}}
          ] do
        keyspaces = Store.queue_keyspaces(root(), queue_id)
        item = Item.new(queue_id, "legacy", %{}, priority: 0, vesting_time: now)
        store_item(store, keyspaces.items, item)

        # A prior rolling-upgrade marker cannot establish this migration's
        # offline static-queue precondition, so it never becomes a dispatch source.
        MockRepo.put(keyspaces.priority_index, {"migration"}, :erlang.term_to_binary(marker))

        assert :writer_fence_required = Store.priority_index_status(MockRepo, root(), queue_id)
        assert [] = Store.peek(MockRepo, root(), queue_id, now: now)
        assert {:error, :writer_fence_required} = Store.migrate_priority_index(MockRepo, root(), queue_id)

        assert :ready = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
        assert [%Item{id: item_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
        assert item_id == item.id
      end
    end

    test "migrates an upgraded queue in fixed chunks before dispatching or reporting an exact minimum" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "legacy-queue"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      # Simulate an upgraded queue: all of these rows predate the scheduling
      # index, so no index keys have been written yet.
      for priority <- 0..999 do
        future =
          Item.new(queue_id, "future", %{priority: priority},
            id: <<priority::128>>,
            priority: priority,
            vesting_time: 20_000
          )

        store_item(store, keyspaces.items, future)
      end

      ready = Item.new(queue_id, "ready", %{}, priority: 1_000, vesting_time: now)
      store_item(store, keyspaces.items, ready)

      # Reads hold this legacy queue until an administrator explicitly confirms
      # the queue is offline. The partial tree is never a dispatch source and
      # never reports a false exact minimum.
      assert [] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert {:error, :priority_index_migration_required} = Store.min_vesting_time(MockRepo, root(), queue_id)

      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert {:error, :priority_index_migration_required} = Store.min_vesting_time(MockRepo, root(), queue_id)
      assert MockRepo.get(keyspaces.priority_index, {"migration"})

      total_rows = 1_001
      nonempty_chunks = div(total_rows + @migration_chunk_size - 1, @migration_chunk_size)

      # The administrator advances one chunk per transaction; the final short
      # raw range proves static coverage and completes immediately.
      for _ <- 1..(nonempty_chunks - 2) do
        assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      end

      assert :ready = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert [%Item{id: ready_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert ready_id == ready.id
      assert MockRepo.get(keyspaces.priority_index, {"root"})
      assert MockRepo.get(keyspaces.priority_index, {"migration"}) == nil
      assert Store.min_vesting_time(MockRepo, root(), queue_id) == now
    end

    test "bounds one migration transaction independently of legacy queue size" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store, [], observer: self())

      queue_id = "bounded-migration"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      for priority <- 0..999 do
        store_item(
          store,
          keyspaces.items,
          Item.new(queue_id, "future", %{},
            id: <<priority::128>>,
            priority: priority,
            vesting_time: 20_000
          )
        )
      end

      # A manager-style minimum read never starts a migration by itself. A
      # marker-less queue reports the precise fence-required error and does no
      # raw scan; the administrator starts the one-chunk transition.
      assert {:error, :priority_index_migration_required} =
               Store.min_vesting_time(MockRepo, root(), queue_id)

      assert MockRepo.get(keyspaces.priority_index, {"migration"}) == nil
      refute Enum.any?(drain_store_operations(), &match?({:get_range, {_, _}, _}, &1))

      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)

      operations = drain_store_operations()

      assert [{:get_range, {_start_key, _end_key}, opts}] =
               Enum.filter(operations, &match?({:get_range, {_, _}, _}, &1))

      assert opts[:limit] == @migration_chunk_size
      refute Enum.any?(operations, &match?({:select, _}, &1))

      index_point_operations =
        Enum.count(operations, fn
          {operation, %Keyspace{} = keyspace, _key} when operation in [:get, :put, :clear] ->
            String.contains?(Keyspace.prefix(keyspace), "priority_index/")

          _ ->
            false
        end)

      assert index_point_operations <=
               @migration_chunk_size * @migration_tree_point_operations_per_item + 8

      # Reads do not advance the cursor or expose partial state.
      assert {:error, :priority_index_migration_required} = Store.min_vesting_time(MockRepo, root(), queue_id)

      refute Enum.any?(drain_store_operations(), &match?({:get_range, {_, _}, _}, &1))
    end

    test "replays a migration chunk idempotently after a retry" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "retrying-migration"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      items =
        for priority <- 0..8 do
          Item.new(queue_id, "item", %{},
            id: <<priority::128>>,
            priority: priority,
            vesting_time: if(priority == 8, do: now, else: now + 10_000)
          )
        end

      ready = List.last(items)
      Enum.each(items, &store_item(store, keyspaces.items, &1))

      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)

      # A real transaction retry rolls back both the tree writes and marker
      # update. Replaying this already-merged chunk is stricter: it proves the
      # merge itself is idempotent even if only the cursor is retried.
      {:offline_building, _cursor} =
        keyspaces.priority_index
        |> MockRepo.get({"migration"})
        |> :erlang.binary_to_term()

      MockRepo.put(
        keyspaces.priority_index,
        {"migration"},
        :erlang.term_to_binary({:offline_building, nil})
      )

      assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert :ready = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert [%Item{id: ready_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert ready_id == ready.id
      assert Store.min_vesting_time(MockRepo, root(), queue_id) == now
    end

    test "uses raw forward pages across the pinned Olivine page boundary" do
      keys = for key <- 0..128, do: "queue/items/#{String.pad_leading(Integer.to_string(key), 3, "0")}"
      first_key = "queue/items/000"
      last_key = "queue/items/128"
      manager = olivine_index_manager_pages([Enum.take(keys, 128), Enum.drop(keys, 128)])

      assert {:ok, pages} =
               OlivineIndexManager.pages_for_range(
                 manager,
                 first_key,
                 Bedrock.Key.key_after(last_key),
                 Version.zero()
               )

      assert Enum.map(pages, &OlivinePage.id/1) == [0, 1]
    end

    test "migrates a static cross-page legacy queue without selector frontier capture" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "cross-page-static-migration"
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      for priority <- 0..128 do
        store_item(
          store,
          keyspaces.items,
          Item.new(queue_id, "future", %{priority: priority},
            id: <<priority::128>>,
            priority: priority,
            vesting_time: 20_000
          )
        )
      end

      ready = Item.new(queue_id, "ready", %{}, priority: 129, vesting_time: now)
      store_item(store, keyspaces.items, ready)

      for _ <- 1..16 do
        assert :more = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      end

      assert :ready = Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline)
      assert [%Item{id: ready_id}] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert ready_id == ready.id
      assert Store.min_vesting_time(MockRepo, root(), queue_id) == now
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

    test "holds a queued legacy custom ID until the writer-fenced migration" do
      now = 10_000
      queue_id = "tenant_1"
      legacy = legacy_item(queue_id, "email-42", vesting_time: now)
      retry = Item.new(queue_id, "email:send", %{retry: true}, id: "email-42", vesting_time: now + 1_000)
      keyspaces = Store.queue_keyspaces(root(), queue_id)

      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)
      store_item(store, keyspaces.items, legacy)

      assert {:error, :priority_index_migration_required} = Store.enqueue(MockRepo, root(), retry, now: now)
      assert 1 == item_count(store, queue_id)
      assert 0 == identity_count(store, queue_id)
    end

    test "holds a leased legacy custom ID until the writer-fenced migration" do
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

      assert {:error, :priority_index_migration_required} = Store.enqueue(MockRepo, root(), retry, now: now)

      assert 1 == item_count(store, queue_id)
      assert 1 == lease_count(store, queue_id)
      assert 0 == identity_count(store, queue_id)
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

      assert :ready = migrate_queue!(queue_id)

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

      assert :ready = migrate_queue!(queue_id)

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

      assert :ready = migrate_queue!(queue_id)

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

      assert :ready = migrate_queue!(queue_id)

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

      assert :ready = migrate_queue!(queue_id)

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

    test "never starts one migration per stale legacy pointer" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store, [], observer: self())

      now = 10_000
      pointers = Store.pointer_keyspace(root())

      Agent.update(store, fn state ->
        Enum.reduce(1..100, state, fn sequence, acc ->
          queue_id = "legacy-gc-#{sequence}"
          Map.put(acc, {Keyspace.prefix(pointers), {0, queue_id}}, <<0::64-little>>)
        end)
      end)

      assert 0 = Store.gc_stale_pointers(MockRepo, root(), limit: 100, grace_period: 1, now: now)

      operations = drain_store_operations()

      assert 1 == Enum.count(operations, &match?({:get_range, {_, _}, _}, &1))

      refute Enum.any?(operations, fn
               {:clear_range, _range} ->
                 true

               {:put, %Keyspace{} = keyspace, _key} ->
                 String.contains?(Keyspace.prefix(keyspace), "priority_index/")

               _ ->
                 false
             end)
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

  describe "priority domain" do
    test "rejects priorities outside the tuple encoding domain with a validation error" do
      for invalid_priority <- [-(1 <<< 64), 1 <<< 64] do
        assert_raise ArgumentError, ~r/priority must be an integer between/, fn ->
          Item.new("invalid-priority", "topic", %{}, priority: invalid_priority)
        end

        invalid_item = %{
          Item.new("invalid-priority", "topic", %{}, priority: 0)
          | priority: invalid_priority
        }

        assert_raise ArgumentError, ~r/priority must be an integer between/, fn ->
          Store.enqueue(MockRepo, root(), invalid_item)
        end
      end
    end

    test "preserves negative and large priorities accepted by the item key encoding" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "signed-priorities"
      lowest = Item.new(queue_id, "lowest", %{}, priority: -((1 <<< 64) - 1), vesting_time: now)
      negative = Item.new(queue_id, "negative", %{}, priority: -1, vesting_time: now)
      ordinary = Item.new(queue_id, "ordinary", %{}, priority: 100, vesting_time: now)
      large = Item.new(queue_id, "large", %{}, priority: 1 <<< 63, vesting_time: now)

      for item <- [lowest, negative, ordinary, large] do
        assert :ok = Store.enqueue(MockRepo, root(), item, now: now)
      end

      assert [first, second, third, fourth] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert [first.id, second.id, third.id, fourth.id] == [lowest.id, negative.id, ordinary.id, large.id]
    end

    test "keeps same-vesting maximum-priority items after lower priorities in ID order" do
      {:ok, store} = start_mock_store()
      setup_integration_stubs(MockRepo, store)

      now = 10_000
      queue_id = "maximum-priority"
      maximum_priority = (1 <<< 64) - 1
      first = Item.new(queue_id, "first", %{}, id: <<0>>, priority: 0, vesting_time: now)

      maximum_first =
        Item.new(queue_id, "maximum-first", %{},
          id: <<1>>,
          priority: maximum_priority,
          vesting_time: now
        )

      maximum_second =
        Item.new(queue_id, "maximum-second", %{},
          id: <<2>>,
          priority: maximum_priority,
          vesting_time: now
        )

      for item <- [maximum_second, first, maximum_first] do
        assert :ok = Store.enqueue(MockRepo, root(), item, now: now)
      end

      assert [low, max_first, max_second] = Store.peek(MockRepo, root(), queue_id, now: now)
      assert [low.id, max_first.id, max_second.id] == [first.id, maximum_first.id, maximum_second.id]
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

  defp migrate_queue!(queue_id) do
    case Store.migrate_priority_index(MockRepo, root(), queue_id, writer_fence: :offline) do
      :more -> migrate_queue!(queue_id)
      status when status in [:ready, :empty] -> status
    end
  end

  defp olivine_index_manager_pages(key_pages) do
    page_map =
      key_pages
      |> Enum.with_index()
      |> Map.new(fn {keys, id} ->
        page = OlivinePage.new(id, Enum.map(keys, &{&1, <<0::64>>}))
        next_id = if id + 1 == length(key_pages), do: 0, else: id + 1
        {id, {page, next_id}}
      end)

    keys = List.flatten(key_pages)

    index = %{
      OlivineIndex.new()
      | tree: OlivineTree.from_page_map(page_map),
        page_map: page_map,
        min_key: hd(keys),
        max_key: List.last(keys)
    }

    %{OlivineIndexManager.new() | versions: [{Version.zero(), {index, %{}}}]}
  end

  defp drain_store_operations(operations \\ []) do
    receive do
      {:store_operation, operation} -> drain_store_operations([operation | operations])
    after
      0 -> Enum.reverse(operations)
    end
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
