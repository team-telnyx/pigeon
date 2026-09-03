defmodule Pigeon.APNS.ErrorTest do
  use ExUnit.Case, async: true

  alias Pigeon.APNS.Error

  test "preserves an unknown APNs reason while mapping it to unknown_error" do
    body = ~s({"reason":"NewAPNsReason","timestamp":123})

    assert Error.parse(body) == :unknown_error

    assert Error.decode(body) == %{
             "reason" => "NewAPNsReason",
             "timestamp" => 123
           }
  end

  test "preserves a non-JSON APNs response" do
    assert Error.parse("upstream failure") == :unknown_error

    assert Error.decode("upstream failure") == %{
             "raw_response" => "upstream failure"
           }
  end
end
