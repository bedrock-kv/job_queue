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
        result
      end)

      # 2. Verify the lease and item, then advance an uninitialized index by
      # one bounded migration chunk.
      expect(MockRepo, :get, 6, fn ks, key ->
        cond do
          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.leases) ->
            assert key == ctx.item.id
            :erlang.term_to_binary(ctx.lease)

          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items) ->
            assert key == ctx.lease.item_key
            :erlang.term_to_binary(ctx.leased_item)

          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.priority_index) ->
            assert key in [{"migration"}, {"initialized"}, {"root"}]
            nil

          true ->
            flunk("Unexpected get: #{inspect({ks, key})}")
        end
      end)

      # 4. clear the old item key and the completed migration marker.
      expect(MockRepo, :clear, 2, fn ks, key ->
        if Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items) do
          assert key == ctx.lease.item_key
        else
          assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.priority_index)
          assert key == {"migration"}
        end

        :ok
      end)

      # 5. Write the item, lease, and two lifecycle markers.
      expect(MockRepo, :put, 4, fn ks, key, value ->
        cond do
          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items) ->
            {priority, vesting_time, id} = key
            assert priority == ctx.item.priority
            assert id == ctx.item.id
            assert vesting_time > ctx.lease.expires_at
            assert :erlang.binary_to_term(value).id == ctx.item.id

          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.leases) ->
            assert key == ctx.item.id
            updated_lease = :erlang.binary_to_term(value)
            assert updated_lease.id == ctx.lease.id
            assert updated_lease.expires_at > ctx.lease.expires_at

          true ->
            assert Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.priority_index)
            assert key in [{"migration"}, {"initialized"}]
        end

        :ok
      end)

      # 7. update pointer via max
      expect(MockRepo, :max, fn key, _timestamp ->
        assert is_binary(key)
        assert String.contains?(key, "pointers/")
        :ok
      end)

      expect(MockRepo, :clear_range, fn keyspace ->
        assert Keyspace.prefix(keyspace) == Keyspace.prefix(ctx.keyspaces.priority_index)
        :ok
      end)

      expect(MockRepo, :get_range, fn _range, _opts -> [] end)

      # Start with short interval
      pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 10)

      # Wait for extension
      assert_receive :extension_complete, 100

      LeaseExtender.stop(pid)
    end

    test "logs success message on successful extension", ctx do
      test_pid = self()

      expect(MockRepo, :transact, fn callback ->
        result = callback.()
        send(test_pid, :done)
        result
      end)

      expect(MockRepo, :get, 6, fn ks, _key ->
        cond do
          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.priority_index) ->
            nil

          Keyspace.prefix(ks) == Keyspace.prefix(ctx.keyspaces.items) ->
            :erlang.term_to_binary(ctx.leased_item)

          true ->
            :erlang.term_to_binary(ctx.lease)
        end
      end)
      expect(MockRepo, :clear, 2, fn _, _ -> :ok end)
      expect(MockRepo, :put, 4, fn _, _, _ -> :ok end)
      expect(MockRepo, :max, fn _, _ -> :ok end)
      expect(MockRepo, :clear_range, fn _keyspace -> :ok end)
      expect(MockRepo, :get_range, fn _range, _opts -> [] end)

      log =
        capture_log(fn ->
          pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 50)
          assert_receive :done, 100
          Process.sleep(10)
          Logger.flush()
          LeaseExtender.stop(pid)
        end)

      assert log =~ "Extended lease for item"
    end

    test "logs warning when lease not found", ctx do
      test_pid = self()

      expect(MockRepo, :transact, fn callback ->
        result = callback.()
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
          pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 50)
          assert_receive :done, 100
          Process.sleep(10)
          Logger.flush()
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
          pid =
            LeaseExtender.start(
              MockRepo,
              Keyspace.new("test/"),
              %Lease{
                id: "lease_id",
                item_id: <<1, 2, 3>>,
                item_key: {100, 0, <<1, 2, 3>>},
                queue_id: "tenant_1",
                holder: @holder_id,
                obtained_at: System.system_time(:millisecond),
                expires_at: System.system_time(:millisecond) + 30_000
              },
              30_000,
              interval: 50
            )

          assert_receive :done, 100
          Process.sleep(10)
          Logger.flush()
          LeaseExtender.stop(pid)
        end)

      assert log =~ "Failed to extend lease"
      assert log =~ ":transaction_failed"
    end

    test "does not report lease loss for a transient renewal failure", ctx do
      test_pid = self()

      expect(MockRepo, :transact, fn _callback ->
        send(test_pid, :renewal_attempted)

        receive do
          :finish_transient_failure -> {:error, :transaction_failed}
        end
      end)

      pid = LeaseExtender.start(MockRepo, ctx.root, ctx.lease, 30_000, interval: 0)
      ref = Process.monitor(pid)

      assert_receive :renewal_attempted
      refute_received {:lease_lost, _lease_id, _reason}

      LeaseExtender.stop(pid)
      send(pid, :finish_transient_failure)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
      refute_received {:lease_lost, _lease_id, _reason}
    end
  end
end
