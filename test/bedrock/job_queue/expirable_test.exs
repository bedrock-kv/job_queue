defmodule Bedrock.JobQueue.ExpirableTest do
  use ExUnit.Case, async: true

  alias Bedrock.JobQueue.Expirable

  describe "expired?/2" do
    test "returns true when current time is past expires_at" do
      struct = %{expires_at: 1000}
      assert Expirable.expired?(struct, now: 2000)
    end

    test "returns false when current time is before expires_at" do
      struct = %{expires_at: 2000}
      refute Expirable.expired?(struct, now: 1000)
    end

    test "uses system time when no :now option provided" do
      # Set expires_at far in the future
      struct = %{expires_at: System.system_time(:millisecond) + 60_000}
      refute Expirable.expired?(struct)
    end
  end

  describe "remaining_ms/2" do
    test "returns remaining time when not expired" do
      struct = %{expires_at: 5000}
      assert Expirable.remaining_ms(struct, now: 3000) == 2000
    end

    test "returns 0 when expired" do
      struct = %{expires_at: 1000}
      assert Expirable.remaining_ms(struct, now: 5000) == 0
    end

    test "uses system time when no :now option provided" do
      future = System.system_time(:millisecond) + 10_000
      struct = %{expires_at: future}
      remaining = Expirable.remaining_ms(struct)
      assert remaining > 0 and remaining <= 10_000
    end
  end
end
