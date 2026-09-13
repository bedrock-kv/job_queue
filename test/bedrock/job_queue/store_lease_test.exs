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

  test "does not revive an expired stored lease from a caller lease with a future expiry" do
    {root, item, expired_lease} = expired_lease()
    caller_lease = %{expired_lease | expires_at: expired_lease.expires_at + 1_000}
    keyspaces = Store.queue_keyspaces(root, item.queue_id)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :erlang.term_to_binary(expired_lease)
    end)

    assert {:error, :lease_expired} =
             Store.extend_lease(MockRepo, root, caller_lease, 100, now: expired_lease.expires_at)
  end

  test "does not extend when the lease expires during its item read" do
    root = Keyspace.new("job_queue/test/")
    now = System.system_time(:millisecond)
    {:ok, clock} = Agent.start_link(fn -> now end)
    item = Item.new("tenant_1", "test:job", %{}, now: now)
    lease = Lease.new(item, "holder", now: now, duration_ms: 30_000)
    leased_item = %{item | lease_id: lease.id, lease_expires_at: lease.expires_at}
    keyspaces = Store.queue_keyspaces(root, item.queue_id)
    test_pid = self()

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      :erlang.term_to_binary(lease)
    end)

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.items
      assert key == lease.item_key
      send(test_pid, {:item_read_blocked, self()})

      receive do
        :finish_item_read -> :erlang.term_to_binary(leased_item)
      end
    end)

    task =
      Task.async(fn ->
        Store.extend_lease(MockRepo, root, lease, 100, clock: fn -> Agent.get(clock, & &1) end)
      end)

    assert_receive {:item_read_blocked, reader_pid}
    Agent.update(clock, fn _ -> lease.expires_at end)
    send(reader_pid, :finish_item_read)

    assert_receive {task_ref, {:error, :lease_expired}}
    assert task_ref == task.ref
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

  test "refuses to complete when the stored lease expires during its read" do
    assert_finalization_expires_after_blocked_read(:complete)
  end

  test "refuses to requeue when the stored lease expires during its read" do
    assert_finalization_expires_after_blocked_read(:requeue)
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

  test "action finalization refuses to complete when the stored lease expires during its read" do
    assert_finalization_expires_after_blocked_read(:action_complete)
  end

  defp assert_finalization_expires_after_blocked_read(action) do
    root = Keyspace.new("job_queue/test/")
    now = System.system_time(:millisecond)
    {:ok, clock} = Agent.start_link(fn -> now end)
    item = Item.new("tenant_1", "test:job", %{}, now: now)
    lease = Lease.new(item, "holder", now: now, duration_ms: 30_000)
    keyspaces = Store.queue_keyspaces(root, item.queue_id)
    test_pid = self()
    read_ref = make_ref()

    if action == :action_complete do
      expect(MockRepo, :transact, fn callback -> callback.() end)
    end

    expect(MockRepo, :get, fn keyspace, key ->
      assert keyspace == keyspaces.leases
      assert key == item.id
      send(test_pid, {:lease_read_blocked, read_ref, self()})

      receive do
        {:finish_lease_read, ^read_ref} -> :erlang.term_to_binary(lease)
      end
    end)

    task =
      Task.async(fn ->
        case action do
          :complete -> Store.complete(MockRepo, root, lease, clock: fn -> Agent.get(clock, & &1) end)
          :requeue -> Store.requeue(MockRepo, root, lease, clock: fn -> Agent.get(clock, & &1) end)

          :action_complete ->
            Action.run(MockRepo, root, lease, :complete, :ok,
              clock: fn -> Agent.get(clock, & &1) end,
              backoff_fn: fn _attempt -> 1_000 end
            )
        end
      end)

    assert_receive {:lease_read_blocked, ^read_ref, reader_pid}
    Agent.update(clock, fn _ -> lease.expires_at end)
    send(reader_pid, {:finish_lease_read, read_ref})

    assert_receive {task_ref, {:error, :lease_expired}}
    assert task_ref == task.ref
  end

  defp expired_lease do
    root = Keyspace.new("job_queue/test/")
    item = Item.new("tenant_1", "test:job", %{}, now: 1_000)
    lease = Lease.new(item, "holder", now: 1_000, duration_ms: 100)
    {root, item, lease}
  end
end
