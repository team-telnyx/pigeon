defmodule Pigeon.DispatcherWorkerTest do
  use ExUnit.Case, async: false

  defmodule SocketAdapter do
    @behaviour Pigeon.Adapter

    @impl true
    def init(opts) do
      {:ok, %{socket: Keyword.fetch!(opts, :socket)}}
    end

    @impl true
    def handle_info(_msg, state), do: {:noreply, state}

    @impl true
    def handle_push(notification, state) do
      Pigeon.Tasks.process_on_response(notification)
      {:noreply, state}
    end
  end

  defmodule CloseClient do
    @behaviour Pigeon.Http2.Client

    def start, do: :ok
    def connect(_uri, _scheme, _options), do: {:error, :not_used}
    def send_ping(_pid), do: :ok
    def send_request(_pid, _headers, _data), do: :ok
    def handle_end_stream(msg, _state), do: msg

    def close(pid) do
      send(
        Application.fetch_env!(:pigeon, :close_observer),
        {:closed_socket, pid}
      )

      :ok
    end
  end

  setup do
    previous_client = Application.get_env(:pigeon, :http2_client)
    previous_observer = Application.get_env(:pigeon, :close_observer)
    previous_trap_exit = Process.flag(:trap_exit, true)

    Application.put_env(:pigeon, :http2_client, CloseClient)
    Application.put_env(:pigeon, :close_observer, self())

    on_exit(fn ->
      Process.flag(:trap_exit, previous_trap_exit)
      restore_env(:http2_client, previous_client)
      restore_env(:close_observer, previous_observer)
    end)

    :ok
  end

  test "closes owned HTTP/2 socket when dispatcher worker shuts down" do
    socket = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn -> if Process.alive?(socket), do: Process.exit(socket, :kill) end)

    {:ok, dispatcher} =
      Pigeon.Dispatcher.start_link(
        adapter: SocketAdapter,
        socket: socket,
        pool_size: 1
      )

    Supervisor.stop(dispatcher, :shutdown)

    assert_receive {:closed_socket, ^socket}, 1_000
  end

  defp restore_env(key, nil), do: Application.delete_env(:pigeon, key)
  defp restore_env(key, value), do: Application.put_env(:pigeon, key, value)
end
