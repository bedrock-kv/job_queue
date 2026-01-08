defmodule Bedrock.JobQueue.PayloadTest do
  use ExUnit.Case, async: true

  alias Bedrock.JobQueue.Payload

  describe "encode/1" do
    test "passes through binary payloads unchanged" do
      binary = "already encoded"
      assert Payload.encode(binary) == binary
    end

    test "JSON encodes non-binary payloads" do
      payload = %{user_id: 123, action: "create"}
      encoded = Payload.encode(payload)
      assert is_binary(encoded)
      assert Jason.decode!(encoded) == %{"user_id" => 123, "action" => "create"}
    end
  end

  describe "decode/1" do
    test "decodes valid JSON with existing atom keys" do
      # user_id and action are existing atoms from test setup
      encoded = Jason.encode!(%{user_id: 123, action: "test"})
      decoded = Payload.decode(encoded)
      assert decoded == %{user_id: 123, action: "test"}
    end

    test "returns raw wrapper for invalid JSON" do
      invalid = "not json at all"
      assert Payload.decode(invalid) == %{raw: "not json at all"}
    end

    test "passes through non-binary payloads unchanged" do
      payload = %{already: :decoded}
      assert Payload.decode(payload) == payload
    end
  end
end
