defmodule Bedrock.JobQueue.Consumer.LeaseExtenderTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mox

  alias Bedrock.JobQueue.Consumer.LeaseExtender
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  @holder_id <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>

  setup do
    root = Keyspace.new("job_queue/test/")
    item = Item.new("tenant_1", "test:job", %{})
    lease = Lease.new(item, @holder_id)
    leased_item = %{item | lease_id: lease.id, lease_expires_at: lease.expires_at}
    keyspaces = Store.queue_keyspaces(root, "tenant_1")

    %{root: root, item: item, lease: lease, leased_item: leased_item, keyspaces: keyspaces}
  end

  describe "start/5" do
    test "spawns a linked process and can be stopped before extension", ctx do
      # No repo calls expected - stopped before interval fires
      pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 100_000)

      assert is_pid(pid)
      assert Process.alive?(pid)

      LeaseExtender.stop(pid)
      Process.sleep(10)
      refute Process.alive?(pid)
    end
  end

  describe "stop/1" do
    test "handles already dead process" do
      pid = spawn(fn -> :ok end)
      Process.sleep(10)

      assert LeaseExtender.stop(pid) == :ok
    end
  end

  describe "lease extension loop" do
    test "extends lease after interval with correct repo call sequence", ctx do
      test_pid = self()

      # Expect exactly one extension cycle:
      # 1. transact wraps the callback
      expect(MockRepo, :transact, fn callback ->
        result = callback.()
        send(test_pid, :extension_complete)
        {:ok, result}
      end)

      # 2. verify_lease: get lease from leases keyspace
      expect(MockRepo, :get, fn ks, key ->
        assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.leases)
        assert key == ctx.item.id
        :erlang.term_to_binary(ctx.lease)
      end)

      # 3. do_extend_lease: get item from items keyspace
      expect(MockRepo, :get, fn ks, key ->
        assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items)
        assert key == ctx.lease.item_key
        :erlang.term_to_binary(ctx.leased_item)
      end)

      # 4. clear old item key
      expect(MockRepo, :clear, fn ks, key ->
        assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items)
        assert key == ctx.lease.item_key
        :ok
      end)

      # 5. put updated item with new key
      expect(MockRepo, :put, fn ks, {priority, vesting_time, id}, value ->
        assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items)
        assert priority == ctx.item.priority
        assert id == ctx.item.id
        assert vesting_time > ctx.lease.expires_at
        decoded = :erlang.binary_to_term(value)
        assert decoded.id == ctx.item.id
        :ok
      end)

      # 6. put updated lease
      expect(MockRepo, :put, fn ks, key, value ->
        assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.leases)
        assert key == ctx.item.id
        decoded = :erlang.binary_to_term(value)
        assert decoded.id == ctx.lease.id
        assert decoded.expires_at > ctx.lease.expires_at
        :ok
      end)

      # 7. update pointer via max
      expect(MockRepo, :max, fn key, _timestamp ->
        assert is_binary(key)
        assert String.contains?(key, "pointers/")
        :ok
      end)

      # Start with short interval
      pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 10)

      # Wait for extension
      assert_receive :extension_complete, 100

      LeaseExtender.stop(pid)
    end

    test "logs success message on successful extension", ctx do
      test_pid = self()

      expect(MockRepo, :transact, fn callback ->
        result = {:ok, callback.()}
        send(test_pid, :done)
        result
      end)
      expect(MockRepo, :get, fn _, _ -> :erlang.term_to_binary(ctx.lease) end)
      expect(MockRepo, :get, fn _, _ -> :erlang.term_to_binary(ctx.leased_item) end)
      expect(MockRepo, :clear, fn _, _ -> :ok end)
      expect(MockRepo, :put, fn _, _, _ -> :ok end)
      expect(MockRepo, :put, fn _, _, _ -> :ok end)
      expect(MockRepo, :max, fn _, _ -> :ok end)

      log =
        capture_log(fn ->
          pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 10)
          assert_receive :done, 100
          LeaseExtender.stop(pid)
        end)

      assert log =~ "Extended lease for item"
    end

    test "logs warning when lease not found", ctx do
      test_pid = self()

      expect(MockRepo, :transact, fn callback ->
        result = {:ok, callback.()}
        send(test_pid, :done)
        result
      end)
      # verify_lease returns nil -> :lease_not_found
      expect(MockRepo, :get, fn ks, key ->
        assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.leases)
        assert key == ctx.item.id
        nil
      end)

      log =
        capture_log(fn ->
          pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 10)
          assert_receive :done, 100
          LeaseExtender.stop(pid)
        end)

      assert log =~ "Failed to extend lease"
      assert log =~ ":lease_not_found"
    end

    test "logs warning when transaction fails", _ctx do
      test_pid = self()

      expect(MockRepo, :transact, fn _callback ->
        send(test_pid, :done)
        {:error, :transaction_failed}
      end)

      log =
        capture_log(fn ->
          pid = LeaseExtender.start(MockRepo, Keyspace.new("test/"), %Lease{
            id: "lease_id",
            item_id: <<1, 2, 3>>,
            item_key: {100, 0, <<1, 2, 3>>},
            queue_id: "tenant_1",
            holder: @holder_id,
            obtained_at: System.system_time(:millisecond),
            expires_at: System.system_time(:millisecond) + 30_000
          }, 30_000, interval: 10)
          assert_receive :done, 100
          LeaseExtender.stop(pid)
          # Allow time for log to be captured
          Process.sleep(10)
        end)

      assert log =~ "Transaction failed extending lease"
    end
  end
end
