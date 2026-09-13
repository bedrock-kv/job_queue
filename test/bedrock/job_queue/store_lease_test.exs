defmodule Bedrock.JobQueue.StoreLeaseTest do
  use ExUnit.Case, async: false

  import Mox

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  test "extends an expired lease when it has not been replaced" do
    root = Keyspace.new("job_queue/test/")
    item = Item.new("tenant_1", "test:job", %{}, now: 1_000)
    lease = Lease.new(item, "holder", now: 1_000, duration_ms: 100)

    leased_item = %{
      item
      | lease_id: lease.id,
        lease_expires_at: lease.expires_at,
        vesting_time: lease.expires_at
    }

    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :erlang.term_to_binary(lease)
    end)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.items
      assert key == lease.item_key
      :erlang.term_to_binary(leased_item)
    end)

    expect(MockRepo, :clear, fn keyspace, key ->
      assert keyspace == keyspaces.items
      assert key == lease.item_key
      :ok
    end)

    expect(MockRepo, :put, fn keyspace, key, _value ->
      assert keyspace == keyspaces.items
      assert key == {item.priority, 1_201, item.id}
      :ok
    end)

    expect(MockRepo, :put, fn keyspace, key, _value ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :ok
    end)

    expect(MockRepo, :max, fn _key, _value -> :ok end)

    assert {:ok, %Lease{expires_at: 1_201}} =
             Store.extend_lease(MockRepo, root, lease, 100, now: 1_101)
  end
end
