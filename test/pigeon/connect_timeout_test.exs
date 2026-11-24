defmodule Pigeon.ConnectTimeoutTest do
  use ExUnit.Case, async: true

  alias Pigeon.APNS
  alias Pigeon.FCM
  alias Pigeon.LegacyFCM

  @cert File.read!("test/support/FakeAPNSCert.pem")
  @key File.read!("test/support/FakeAPNSKey.pem")

  # connect_socket_options/1 lives on each Configurable impl module.
  defp socket_options(config),
    do: Pigeon.Configurable.impl_for!(config).connect_socket_options(config)

  describe "APNS.Config" do
    test "defaults to 10s connect timeout and 3 attempts" do
      config = APNS.Config.new(cert: @cert, key: @key, mode: :prod)
      assert config.connect_timeout == 10_000
      assert config.connect_attempts == 3
    end

    test "accepts overrides" do
      config =
        APNS.Config.new(
          cert: @cert,
          key: @key,
          mode: :prod,
          connect_timeout: 5_000,
          connect_attempts: 2
        )

      assert config.connect_timeout == 5_000
      assert config.connect_attempts == 2
    end

    test "connect_socket_options carries :connect_timeout for the client" do
      config =
        APNS.Config.new(
          cert: @cert,
          key: @key,
          mode: :prod,
          connect_timeout: 5_000
        )

      opts = socket_options(config)
      assert Keyword.get(opts, :connect_timeout) == 5_000
    end
  end

  describe "APNS.JWTConfig" do
    test "defaults and connect_socket_options" do
      config =
        APNS.JWTConfig.new(
          mode: :prod,
          key: File.read!("test/support/FakeAPNSAuthKey.p8"),
          key_identifier: "ABC1234567",
          team_id: "DEF1234567"
        )

      assert config.connect_timeout == 10_000
      assert config.connect_attempts == 3

      opts = socket_options(config)
      assert Keyword.get(opts, :connect_timeout) == 10_000
    end
  end

  describe "FCM.Config" do
    test "defaults, overrides and connect_socket_options" do
      config =
        FCM.Config.new(
          project_id: "example-project",
          service_account_json: "{}",
          connect_timeout: 7_000,
          connect_attempts: 1
        )

      assert config.connect_timeout == 7_000
      assert config.connect_attempts == 1

      {:ok, opts} = socket_options(config)
      assert Keyword.get(opts, :connect_timeout) == 7_000
    end
  end

  describe "LegacyFCM.Config" do
    test "defaults and connect_socket_options" do
      config = LegacyFCM.Config.new(key: "fcm_key")
      assert config.connect_timeout == 10_000
      assert config.connect_attempts == 3

      {:ok, opts} = socket_options(config)
      assert Keyword.get(opts, :connect_timeout) == 10_000
    end
  end
end
