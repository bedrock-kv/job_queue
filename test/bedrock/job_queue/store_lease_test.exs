defmodule Bedrock.JobQueue.StoreLeaseTest do
  use ExUnit.Case, async: false

  import Mox

  alias Bedrock.JobQueue.Consumer.Action
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :set_mox_global
  setup :verify_on_exit!

  test "refuses to extend an expired lease" do
    root = Keyspace.new("job_queue/test/")
    item = Item.new("tenant_1", "test:job", %{}, now: 1_000)
    lease = Lease.new(item, "holder", now: 1_000, duration_ms: 100)

    assert {:error, :lease_expired} = Store.extend_lease(MockRepo, root, lease, 100, now: 1_100)
  end

  test "refuses to complete an expired lease without mutating queue state" do
    {root, item, lease} = expired_lease()
    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :erlang.term_to_binary(lease)
    end)

    assert {:error, :lease_expired} = Store.complete(MockRepo, root, lease, now: lease.expires_at)
  end

  test "refuses to requeue an expired lease without mutating queue state" do
    {root, item, lease} = expired_lease()
    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :erlang.term_to_binary(lease)
    end)

    assert {:error, :lease_expired} = Store.requeue(MockRepo, root, lease, now: lease.expires_at)
  end

  test "action finalization preserves an expired lease" do
    {root, item, lease} = expired_lease()
    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    expect(MockRepo, :transact, fn callback -> callback.() end)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :erlang.term_to_binary(lease)
    end)

    assert {:error, :lease_expired} =
             Action.run(MockRepo, root, lease, :complete, :ok, backoff_fn: fn _attempt -> 1_000 end)
  end

  defp expired_lease do
    root = Keyspace.new("job_queue/test/")
    item = Item.new("tenant_1", "test:job", %{}, now: 1_000)
    lease = Lease.new(item, "holder", now: 1_000, duration_ms: 100)
    {root, item, lease}
  end
end
