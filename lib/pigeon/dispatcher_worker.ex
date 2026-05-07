defmodule Pigeon.DispatcherWorker do
  @moduledoc false

  use GenServer

  require Logger

  def start_link(opts) do
    opts[:adapter] || raise "adapter is not specified"
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    case opts[:adapter].init(opts) do
      {:ok, state} ->
        # Dynamic dispatchers often own HTTP/2 connection pools that are
        # supervised outside of the dispatcher tree (for example Kadabra's
        # global :kadabra supervisor). Trap shutdown exits so terminate/2 runs
        # when the dispatcher supervisor stops this worker and we can close the
        # owned pool instead of orphaning it.
        Process.flag(:trap_exit, true)

        Pigeon.Registry.register(opts[:supervisor])
        {:ok, %{adapter: opts[:adapter], state: state}}

      {:error, reason} ->
        {:error, reason}

      {:stop, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_info({:"$push", notification}, %{adapter: adapter, state: state}) do
    case adapter.handle_push(notification, state) do
      {:noreply, new_state} ->
        {:noreply, %{adapter: adapter, state: new_state}}

      {:stop, reason, new_state} ->
        {:stop, reason, %{adapter: adapter, state: new_state}}
    end
  end

  def handle_info(msg, %{adapter: adapter, state: state}) do
    case adapter.handle_info(msg, state) do
      {:noreply, new_state} ->
        {:noreply, %{adapter: adapter, state: new_state}}

      {:stop, reason, new_state} ->
        {:stop, reason, %{adapter: adapter, state: new_state}}
    end
  end

  @impl GenServer
  def terminate(_reason, %{state: state}) do
    close_state_socket(state)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp close_state_socket(%{socket: socket}) when is_pid(socket) do
    if Process.alive?(socket) do
      try do
        Pigeon.Http2.Client.default().close(socket)
      catch
        :exit, {:noproc, _} = reason ->
          Logger.debug(
            "Pigeon.DispatcherWorker: HTTP/2 socket already closed during shutdown: #{inspect(reason)}"
          )

          :ok

        :exit, {:normal, _} = reason ->
          Logger.debug(
            "Pigeon.DispatcherWorker: HTTP/2 socket closed normally during shutdown: #{inspect(reason)}"
          )

          :ok

        :exit, {:shutdown, _} = reason ->
          Logger.debug(
            "Pigeon.DispatcherWorker: HTTP/2 socket shutdown during worker termination: #{inspect(reason)}"
          )

          :ok
      end
    end

    :ok
  end

  defp close_state_socket(_state), do: :ok
end
