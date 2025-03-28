defmodule PostgresqlMessageBroker.Persistence.NotificationListener do
  @moduledoc """
  GenServer proxy for PostgreSQL's notify/listen. This server can be used to subscribe to any channel, and messages
  delivered to that channel will be sent to your process's message box. Uses a single db connection.

  https://www.postgresql.org/docs/current/sql-notify.html
  """

  alias __MODULE__, as: Self

  use GenServer

  defmodule State do
    @moduledoc false

    alias __MODULE__, as: Self

    @enforce_keys [
      :notifications_pid,
      :channel_subscriptions
    ]
    defstruct @enforce_keys

    @type t :: %Self{
            notifications_pid: pid(),
            channel_subscriptions: %{String.t() => [pid()]}
          }

    @spec new(pid()) :: Self.t()
    def new(notifications_pid) when is_pid(notifications_pid) do
      %Self{
        notifications_pid: notifications_pid,
        channel_subscriptions: %{}
      }
    end

    @spec subscribe_pid_to_channel(Self.t(), pid(), String.t()) :: Self.t()
    def subscribe_pid_to_channel(%Self{} = self, pid, channel) when is_pid(pid) and is_binary(channel) do
      self.channel_subscriptions
      |> Map.update(channel, [pid], fn pid_list ->
        [pid | pid_list]
      end)
      |> then(&%{self | channel_subscriptions: &1})
    end

    @spec unsubscribe_pid(Self.t(), pid()) :: Self.t()
    def unsubscribe_pid(%Self{} = self, pid) when is_pid(pid) do
      self.channel_subscriptions
      |> Enum.flat_map(fn {channel, pid_list} ->
        new_pid_list = Enum.reject(pid_list, &(&1 == pid))

        if Enum.empty?(new_pid_list) do
          []
        else
          [{channel, new_pid_list}]
        end
      end)
      |> Enum.into(%{})
      |> then(&%{self | channel_subscriptions: &1})
    end
  end

  defmodule Notification do
    @moduledoc false

    alias __MODULE__, as: Self

    @enforce_keys [:channel, :payload]
    defstruct @enforce_keys

    @type t :: %Self{
            channel: String.t(),
            payload: String.t()
          }
  end

  # Client

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    genserver_opts = Keyword.take(opts, [:name])
    repo = Keyword.fetch!(opts, :repo)
    true = is_atom(repo)
    GenServer.start_link(Self, repo, genserver_opts)
  end

  @spec listen(GenServer.server(), String.t()) :: :ok
  def listen(server, channel) when is_binary(channel) do
    :ok = GenServer.call(server, {:listen, channel})
  end

  # Server

  @impl GenServer
  def init(repo) when is_atom(repo) do
    Process.flag(:trap_exit, true)
    {:ok, notifications_pid} = Postgrex.Notifications.start_link(repo.config())
    {:ok, State.new(notifications_pid)}
  end

  @impl GenServer
  def handle_call({:listen, channel}, {caller_pid, _tag}, %State{} = state) do
    if not is_map_key(state.channel_subscriptions, channel) do
      {:ok, _listen_reference} = Postgrex.Notifications.listen(state.notifications_pid, channel)
    end

    Process.link(caller_pid)

    state = State.subscribe_pid_to_channel(state, caller_pid, channel)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:notification, notification_pid, listen_ref, channel, payload}, %State{} = state)
      when is_binary(channel) and is_binary(payload) do
    case Map.fetch(state.channel_subscriptions, channel) do
      :error ->
        Postgrex.Notifications.unlisten(notification_pid, listen_ref)

      {:ok, pid_list} when is_list(pid_list) ->
        for pid <- pid_list do
          send(pid, %Notification{channel: channel, payload: payload})
        end
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {:EXIT, pid, _reason},
        %State{notifications_pid: notifications_pid} = state
      )
      when pid != notifications_pid do
    state = State.unsubscribe_pid(state, pid)
    {:noreply, state}
  end
end
