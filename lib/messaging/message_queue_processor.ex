defmodule PostgresqlMessageQueue.Messaging.MessageQueueProcessor do
  @moduledoc """
  Processes messages in the message queue by calling
  `PostgresqlMessageQueue.Messaging.process_message_queue_batch/1`. When there are no more
  messages in the queue, MessageQueueWatcher is used to wait for notification of a new message.

  ## Options

  * `queue`: Each processor targets a single queue, specified by this string.
  * `concurrency`: The number of messages that may be processed from the queue simultaneously.
      Be aware that if this number is > 1, your messages are no longer guaranteed to be processed
      in strict order. The default value is 1.
  * `handler`: A custom message handler function. If specified, each message will be passed to
      this function as a `Messaging.Message.t()` struct, and the function must return `:ok`. If
      the function fails, the message will be returned to the queue. If this is not specified, the
      `Messaging.deliver_message_to_handlers!/1` function will be used by default.
  * `batch_size`: Specifies how many messages to fetch from the queue with each db query.
      Increasing this could improve throughput, but a failure to process any message in the batch
      will cause the whole batch to be retried.
  * `backoff_ms`: Specifies retry backoff behaviour in case of a failure when processing a
      message. This can be an integer, but it's more useful to provide a function, which will
      receive an integer argument representing the number of attempts that have been made to
      process the message, and must return the number of milliseconds to wait before trying again.
      By default, there is no backoff mechanism, and all messages are retried instantly.

  See also the documentation for `PostgresqlMessageQueue.Messaging.process_message_queue_batch/1`.
  """

  alias __MODULE__, as: Self
  alias PostgresqlMessageQueue.Messaging
  alias PostgresqlMessageQueue.Messaging.Message
  alias PostgresqlMessageQueue.Messaging.MessageQueueBroadwayProducer
  alias PostgresqlMessageQueue.Persistence.Repo

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
    "MessageQueueProcessor [queue:#{context.queue}] "
  end

  # Client

  @doc """
  Pay special attention to `concurrency`: this specifies the number of processes that may call
  `Messaging.process_message_queue_batch/2` simultaneously. This must be 1 (the default) if it is
  important for messages to be processed in-order.
  """
  @spec start_link(Keyword.t()) :: {:ok, pid()}
  def start_link(opts \\ []) do
    opts = Keyword.put_new(opts, :concurrency, 1)
    queue = Keyword.fetch!(opts, :queue)

    Broadway.start_link(__MODULE__,
      name: name_from_queue(queue),
      context: Context.new(opts),
      producer: [
        module: {MessageQueueBroadwayProducer, opts}
      ],
      processors: [
        default: [
          concurrency: opts[:concurrency],
          # If max_demand > 1, then failed messages may be retried out-of-order, because the next
          # message has already been queued up by the time the failed message is returned to the
          # queue.
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
    |> Enum.each(&MessageQueueBroadwayProducer.process_demand/1)

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
