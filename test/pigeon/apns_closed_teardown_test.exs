defmodule Pigeon.APNSClosedTeardownTest do
  use ExUnit.Case, async: false

  alias Pigeon.APNS
  alias Pigeon.APNS.Config

  # Mock HTTP/2 client whose `close/1` exits with the *doubly-wrapped* reason seen
  # in production teardown (worker -> ConnectionPool -> Connection nested
  # GenServer.call(:close)). Its head is a `{:shutdown, _}` tuple, not the
  # `:shutdown` atom, so the specific `:exit, {:shutdown, _}` catch clause misses
  # it. `connect/3` returns a fresh live socket so the reconnect branch completes.
  defmodule NestedShutdownClient do
    @behaviour Pigeon.Http2.Client

    def start, do: :ok

    def connect(_uri, _scheme, _options) do
      {:ok, spawn(fn -> Process.sleep(:infinity) end)}
    end

    def send_ping(_pid), do: :ok
    def send_request(_pid, _headers, _data), do: :ok
    def handle_end_stream(msg, _state), do: msg

    def close(pid) do
      exit(
        {{:shutdown, {GenServer, :call, [pid, :close, 5000]}},
         {GenServer, :call, [pid, :close, 5000]}}
      )
    end
  end

  setup do
    previous_client = Application.get_env(:pigeon, :http2_client)
    Application.put_env(:pigeon, :http2_client, NestedShutdownClient)

    on_exit(fn ->
      case previous_client do
        nil -> Application.delete_env(:pigeon, :http2_client)
        client -> Application.put_env(:pigeon, :http2_client, client)
      end
    end)

    :ok
  end

  test "a :closed teardown whose close/1 exits with a nested :shutdown does not crash the worker" do
    socket = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(socket) do
        Process.exit(socket, :kill)
      end
    end)

    config = %Config{uri: "api.push.apple.com"}
    state = %APNS{socket: socket, config: config}

    # Before the fix this re-raised the nested exit and crashed the dispatcher
    # worker (which the Dispatcher then restarted into a fresh orphan). Now it is
    # swallowed and the worker reconnects.
    assert {:noreply, %APNS{socket: new_socket}} =
             APNS.handle_info({:closed, socket}, state)

    assert is_pid(new_socket)
    refute new_socket == socket
  end
end
