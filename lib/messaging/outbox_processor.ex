defmodule PostgresqlMessageBroker.Messaging.OutboxProcessor do
  @moduledoc """
  Processes messages in the outbox by calling `PostgresqlMessageBroker.Messaging.process_outbox_batch/1`. When there are no more
  messages in the queue, OutboxWatcher is used to wait for notification of a new message.
  """

  alias __MODULE__, as: Self
  alias PostgresqlMessageBroker.Messaging
  alias PostgresqlMessageBroker.Messaging.Message
  alias PostgresqlMessageBroker.Messaging.OutboxBroadwayProducer
  alias PostgresqlMessageBroker.Persistence.Repo

  require Logger

  use Broadway

  defmodule Context do
    @moduledoc false

    alias __MODULE__, as: Self

    @enforce_keys [
      :queue,
      :handler
    ]
    defstruct @enforce_keys

    @type t :: %Self{
            queue: String.t(),
            handler: :default | {:some, (Message.t() -> :ok)}
          }

    @opts_schema [
                   queue: [
                     type: :string,
                     required: true
                   ],
                   handler: [
                     type: {:fun, 1}
                   ]
                 ]
                 |> NimbleOptions.new!()

    @spec new(Keyword.t()) :: Self.t()
    def new(opts) do
      opts =
        Keyword.take(opts, [
          :queue,
          :handler
        ])

      NimbleOptions.validate!(opts, @opts_schema)

      queue = Keyword.fetch!(opts, :queue)

      handler =
        case Keyword.fetch(opts, :handler) do
          {:ok, handler} -> {:some, handler}
          :error -> :default
        end

      %Self{
        queue: queue,
        handler: handler
      }
    end
  end

  @spec log_prefix(Context.t()) :: String.t()
  def log_prefix(%Context{} = context) do
    "OutboxProcessor [queue:#{context.queue}] "
  end

  # Client

  @doc """
  Pay special attention to `concurrency`: this specifies the number of processes that may call
  `Messaging.process_outbox_batch/2` simultaneously. This must be 1 (the default) if it is important for messages to be
  processed in-order.
  """
  @spec start_link(Keyword.t()) :: {:ok, pid()}
  def start_link(opts \\ []) do
    opts = Keyword.put_new(opts, :concurrency, 1)
    queue = Keyword.fetch!(opts, :queue)

    Broadway.start_link(__MODULE__,
      name: name_from_queue(queue),
      context: Context.new(opts),
      producer: [
        module: {OutboxBroadwayProducer, opts}
      ],
      processors: [
        default: [
          concurrency: opts[:concurrency],
          # If max_demand > 1, then failed messages may be retried out-of-order, because the next message has already
          # been queued up by the time the failed message is returned to the queue.
          max_demand: 1
        ]
      ]
    )
  end

  @doc """
  Forces a check for new messages. Useful especially in tests, where the insert trigger won't fire
  due to the database transaction that wraps the test.
  """
  @spec check_for_new_messages(String.t()) :: :ok | {:error, :not_running}
  def check_for_new_messages(queue) when is_binary(queue) do
    name_from_queue(queue)
    |> Broadway.producer_names()
    |> Enum.each(&OutboxBroadwayProducer.process_demand/1)

    :ok
  catch
    :exit, {:noproc, _mfa} -> {:error, :not_running}
  end

  @spec name_from_queue(String.t()) :: atom()
  defp name_from_queue(queue) when is_binary(queue) do
    :"#{__MODULE__}-#{queue}"
  end

  # Server

  @impl Broadway
  def handle_message(:default = _producer, %Broadway.Message{} = message, %Context{} = context) do
    in_db_transaction(fn ->
      :ok =
        case context.handler do
          :default -> Messaging.deliver_messages_to_handlers!([message.data])
          {:some, handler_func} -> handler_func.(message.data)
        end
    end)

    message
  end

  @spec in_db_transaction((-> :ok)) :: :ok | {:error, any()}
  defp in_db_transaction(func) when is_function(func, 0) do
    Repo.transaction(fn ->
      :ok = func.()
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
