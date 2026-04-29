defmodule Pigeon.APNS do
  @moduledoc """
  `Pigeon.Adapter` for Apple Push Notification Service (APNS) push notifications.

  ## Getting Started

  1. Create an `APNS` dispatcher.

  ```
  # lib/apns.ex
  defmodule YourApp.APNS do
    use Pigeon.Dispatcher, otp_app: :your_app
  end
  ```

  2. (Optional) Add configuration to your `config.exs`.

  ```
  # config.exs

  config :your_app, YourApp.APNS,
    adapter: Pigeon.APNS,
    cert: File.read!("cert.pem"),
    key: File.read!("key_unencrypted.pem"),
    mode: :dev
  ```

  Or use token based authentication:

  ```
  config :your_app, YourApp.APNS,
    adapter: Pigeon.APNS,
    key: File.read!("AuthKey.p8"),
    key_identifier: "ABC1234567",
    mode: :dev,
    team_id: "DEF8901234"
  ```

  3. Start your dispatcher on application boot.

  ```
  defmodule YourApp.Application do
    @moduledoc false

    use Application

    @doc false
    def start(_type, _args) do
      children = [
        YourApp.APNS
      ]
      opts = [strategy: :one_for_one, name: YourApp.Supervisor]
      Supervisor.start_link(children, opts)
    end
  end
  ```

  If you skipped step two, include your configuration.

  ```
  defmodule YourApp.Application do
    @moduledoc false

    use Application

    @doc false
    def start(_type, _args) do
      children = [
        {YourApp.APNS, apns_opts()}
      ]
      opts = [strategy: :one_for_one, name: YourApp.Supervisor]
      Supervisor.start_link(children, opts)
    end

    defp apns_opts do
      [
        adapter: Pigeon.APNS,
        cert: File.read!("cert.pem"),
        key: File.read!("key_unencrypted.pem"),
        mode: :dev
      ]
    end
  end
  ```

  4. Create a notification. **Note: Your push topic is generally the app's bundle identifier.**

  ```
  n = Pigeon.APNS.Notification.new("your message", "your device token", "your push topic")
  ```
   
  5. Send the packet. Pushes are synchronous and return the notification with an
   updated `:response` key.

  ```
  YourApp.APNS.push(n)
  ```

  ## Configuration Options

  #### Certificate Authentication

  - `:cert` - Push certificate. Must be the full-text string of the file contents.
  - `:key` - Push private key. Must be the full-text string of the file contents.

  #### Token Authentication

  - `:key` - JWT private key. Must be the full-text string of the file contents.
  - `:key_identifier` - A 10-character key identifier (kid) key, obtained from
    your developer account.
  - `:team_id` - Your 10-character Team ID, obtained from your developer account.

  #### Shared Options

  - `:mode` - If set to `:dev` or `:prod`, will set the appropriate `:uri`.
  - `:ping_period` - Interval between server pings. Necessary to keep long
    running APNS connections alive. Defaults to 10 minutes.
  - `:port` - Push server port. Can be any value, but APNS only accepts
    `443` and `2197`.
  - `:uri` - Push server uri. If set, overrides uri defined by `:mode`.
    Useful for test environments.

  ## Generating Your Certificate and Key .pem

  1. In Keychain Access, right-click your push certificate and select _"Export..."_
  2. Export the certificate as `cert.p12`
  3. Click the dropdown arrow next to the certificate, right-click the private
     key and select _"Export..."_
  4. Export the private key as `key.p12`
  5. From a shell, convert the certificate.

  ```
  openssl pkcs12 -legacy -clcerts -nokeys -out cert.pem -in cert.p12
  ```

  6. Convert the key. Be sure to set a PEM pass phrase here. The pass phrase must be 4 or 
     more characters in length or this will not work. You will need that pass phrase added 
     here in order to remove it in the next step.

  ```
  openssl pkcs12 -legacy -nocerts -out key.pem -in key.p12
  ```

  7. Remove the PEM pass phrase from the key.

  ```
  openssl rsa -in key.pem -out key_unencrypted.pem
  ```

  8. `cert.pem` and `key_unencrypted.pem` can now be used in your configuration.
  """

  defstruct queue: Pigeon.NotificationQueue.new(),
            stream_id: 1,
            socket: nil,
            config: nil

  @behaviour Pigeon.Adapter

  alias Pigeon.{Configurable, NotificationQueue}
  alias Pigeon.APNS.ConfigParser
  alias Pigeon.Http2.{Client, Stream}

  require Logger

  @impl true
  def init(opts) do
    config = ConfigParser.parse(opts)
    Configurable.validate!(config)

    state = %__MODULE__{config: config}

    case connect_socket(config) do
      {:ok, socket} ->
        Configurable.schedule_ping(config)
        {:ok, %{state | socket: socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_push(notification, %{config: config, queue: queue} = state) do
    headers = Configurable.push_headers(config, notification, [])
    payload = Configurable.push_payload(config, notification, [])

    Client.default().send_request(state.socket, headers, payload)

    new_q = NotificationQueue.add(queue, state.stream_id, notification)

    state =
      state
      |> inc_stream_id()
      |> Map.put(:queue, new_q)

    {:noreply, state}
  end

  def handle_info({:kadabra_closed, _pid}, state) do
    Logger.debug("Pigeon.APNS: Kadabra connection closed, shutting down gracefully")
    {:stop, :normal, state}
  end

  def handle_info({:kadabra_error, reason, _pid}, state) do
    Logger.warning("Pigeon.APNS: Kadabra SSL error: #{inspect(reason)}, shutting down")
    {:stop, {:ssl_error, reason}, state}
  end

  def handle_info({:kadabra_connection_error, error, reason}, state) do
    Logger.error("Pigeon.APNS: Kadabra connection error: #{error} - #{inspect(reason)}")
    {:stop, {:connection_error, error}, state}
  end

  def handle_info(:ping, state) do
    try do
      Client.default().send_ping(state.socket)
      Configurable.schedule_ping(state.config)
      {:noreply, state}
    catch
      :exit, {:noproc, _} ->
        Logger.debug("Pigeon.APNS: Kadabra process not found (already dead), shutting down gracefully")
        {:stop, :normal, state}

      :exit, {:timeout, _} ->
        Logger.warning("Pigeon.APNS: Ping timeout, shutting down")
        {:stop, :ping_timeout, state}

      error ->
        Logger.error("Pigeon.APNS: Unexpected error during ping: #{inspect(error)}")
        {:stop, {:ping_failed, error}, state}
    end
  end

  def handle_info({:closed, pool_pid}, %{config: config, socket: socket} = state) do
    # Close the old socket ONLY if it is the one that sent this message and
    # is still alive. This prevents two leak paths:
    #
    # 1. Original leak: old socket still alive when :closed arrives (race
    #    between message delivery and process exit). Without closing, the old
    #    ConnectionPool + Connection survive as orphans.
    #
    # 2. PR #178 leak: calling close unconditionally crashes the worker when
    #    the socket is already dead (during ConnectionSupervisor.stop_connection).
    #    The Dispatcher restarts the worker, creating a fresh orphan.
    #
    # 3. Infinite loop: Connection.terminate/2 sends a second :closed message
    #    when we explicitly close it. socket == pool_pid prevents closing the
    #    newly-reconnected socket.

    if is_pid(socket) and socket == pool_pid and Process.alive?(socket) do
      try do
        Client.default().close(socket)
      catch
        :exit, {:noproc, _} -> :ok
      end
    end

    case connect_socket(config) do
      {:ok, new_socket} ->
        Configurable.schedule_ping(config)

        state =
          state
          |> reset_stream_id()
          |> Map.put(:socket, new_socket)

        {:noreply, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(msg, state) do
    case Client.default().handle_end_stream(msg, state) do
      {:ok, %Stream{} = stream} -> process_end_stream(stream, state)
      _else -> {:noreply, state}
    end
  end

  defp connect_socket(config), do: connect_socket(config, 0)

  defp connect_socket(_config, 3), do: {:error, :timeout}

  defp connect_socket(config, tries) do
    case Configurable.connect(config) do
      {:ok, socket} -> {:ok, socket}
      {:error, _reason} -> connect_socket(config, tries + 1)
    end
  end

  @doc false
  def process_end_stream(%Stream{id: stream_id} = stream, state) do
    %{queue: queue, config: config} = state

    case NotificationQueue.pop(queue, stream_id) do
      {nil, new_queue} ->
        # Do nothing if no queued item for stream
        {:noreply, %{state | queue: new_queue}}

      {notif, new_queue} ->
        Configurable.handle_end_stream(config, stream, notif)
        {:noreply, %{state | queue: new_queue}}
    end
  end

  @doc false
  def inc_stream_id(%{stream_id: stream_id} = state) do
    %{state | stream_id: stream_id + 2}
  end

  @doc false
  def reset_stream_id(state) do
    %{state | stream_id: 1}
  end
end
