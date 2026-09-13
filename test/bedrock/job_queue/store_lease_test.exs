defmodule Bedrock.JobQueue.StoreLeaseTest do
  use ExUnit.Case, async: false

  import Mox

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
end
