defmodule PostgresqlMessageQueue.Messaging.OutboxWatcher do
  @moduledoc """
  Watches the message outbox for new messages (see `PostgresqlMessageQueue.Messaging.store_message_in_outbox/1`). When new messages
  arrive, notifies the OutboxProcessor process for the relevant queues.
  """

  alias __MODULE__, as: Self
  alias PostgresqlMessageQueue.Messaging.OutboxProcessor
  alias PostgresqlMessageQueue.Persistence.Repo
  alias PostgresqlMessageQueue.Persistence.NotificationListener

  use GenServer

  require Logger

  defmodule State do
    @moduledoc false

    alias __MODULE__, as: Self

    @enforce_keys [:notify_queues]
    defstruct @enforce_keys

    @type t :: %Self{notify_queues: MapSet.t(String.t())}

    @spec new() :: Self.t()
    def new() do
      %Self{notify_queues: MapSet.new()}
    end
  end

  # Client

  @spec start_link(Keyword.t()) :: {:ok, pid()}
  def start_link(opts \\ []) do
    GenServer.start_link(Self, opts, name: Self)
  end

  @spec notify_on_new_message(String.t()) :: :new_subscription | :already_subscribed
  def notify_on_new_message(queue) when is_binary(queue) do
    GenServer.call(Self, {:notify_on_new_message, queue})
  end

  # Server

  @impl GenServer
  def init(_opts) do
    :ok = NotificationListener.listen(Repo.NotificationListener, "outbox_messages_inserted")
    {:ok, State.new()}
  end

  @impl GenServer
  def handle_call({:notify_on_new_message, queue}, _from, %State{} = state) do
    {reply, state} =
      if queue in state.notify_queues do
        {:already_subscribed, state}
      else
        state = add_notify_queue(state, queue)
        {:new_subscription, state}
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_info(
        %NotificationListener.Notification{
          channel: "outbox_messages_inserted",
          payload: queue_name
        },
        %State{} = state
      ) do
    state =
      if queue_name in state.notify_queues do
        Logger.info(log_prefix() <> "Notifying of new message(s) in queue: #{queue_name}")
        :ok = OutboxProcessor.check_for_new_messages(queue_name)
        remove_notify_queue(state, queue_name)
      else
        state
      end

    {:noreply, state}
  end

  @spec add_notify_queue(State.t(), String.t()) :: State.t()
  defp add_notify_queue(%State{} = state, queue) when is_binary(queue) do
    already_subscribed_info =
      if Enum.empty?(state.notify_queues) do
        "(none)"
      else
        Enum.join(state.notify_queues, ", ")
      end

    Logger.info(
      log_prefix() <>
        "Subscribing queue: #{queue}. Already subscribed: #{already_subscribed_info}."
    )

    notify_queues = MapSet.put(state.notify_queues, queue)
    %{state | notify_queues: notify_queues}
  end

  @spec remove_notify_queue(State.t(), String.t()) :: State.t()
  defp remove_notify_queue(%State{} = state, queue) when is_binary(queue) do
    notify_queues = MapSet.delete(state.notify_queues, queue)
    %{state | notify_queues: notify_queues}
  end

  @spec log_prefix() :: String.t()
  defp log_prefix() do
    "Messaging.OutboxWatcher [#{inspect(self())}] "
  end
end
