defmodule PostgresqlMessageQueue.Messaging do
  @moduledoc """
  Handles everything to do with internal message storage and delivery.
  """

  alias __MODULE__, as: Self
  alias PostgresqlMessageQueue.Messaging.MessageQueueProcessor
  alias PostgresqlMessageQueue.Persistence.Repo

  require AyeSQL
  require Ecto.Query
  require Logger

  @broadcast_listeners Application.compile_env(:postgresql_message_queue, Self, [])
                       |> Keyword.get(:broadcast_listeners, [])

  def global_queue(), do: "global"

  AyeSQL.defqueries(Queries, "messaging.sql", repo: Repo)

  defmodule SerializationError do
    defexception [:message, :message_struct]
    @type t :: %__MODULE__{}

    @impl Exception
    def message(%__MODULE__{} = struct) do
      "Failed to serialize message: #{inspect(struct.message_struct)}. " <>
        inspect(struct.message)
    end
  end

  defmodule Message do
    @moduledoc """
    A message that conforms to a specific schema, represented by the `type` and `schema_version` fields.
    """

    @enforce_keys [:type, :schema_version, :payload]
    defstruct @enforce_keys ++ [metadata: %{}]

    @type t :: %__MODULE__{
            type: String.t(),
            schema_version: pos_integer(),
            payload: map(),
            metadata: map()
          }
  end

  defmodule MessageHandler do
    @moduledoc """
    Behaviour defining a module intended to handle incoming `#{Message}`s.
    """

    @callback handle_message(Message.t()) :: :ok | {:ok, any()}

    defmodule UnexpectedResultError do
      @moduledoc false

      @enforce_keys [:message, :handler, :result]
      defexception @enforce_keys

      @type t :: %__MODULE__{
              message: Message.t(),
              handler: module(),
              result: any()
            }

      @impl Exception
      def message(%__MODULE__{} = self) do
        "Unexpected result from message handler:\n" <>
          "Handler: #{self.handler}\n" <>
          "Message: #{inspect(self.message, pretty: true)}\n" <>
          "Result: #{inspect(self.result, pretty: true)}"
      end
    end
  end

  @type delay_time :: {integer(), :day | :hour | :minute | System.time_unit()}
  @type broadcast_opts :: [to_queue: String.t(), after: DateTime.t() | delay_time()]
  @type message_handler_func :: ([Message.t()] -> :ok)

  @doc """
  Same as `unicast_messages_sync/2`, but raises a `#{SerializationError}` instead of returning an
  `:error` tuple.
  """
  @spec unicast_messages_sync!([Message.t()], atom(), Keyword.t()) :: :ok
  def unicast_messages_sync!(messages, message_handler_module, opts \\ []) do
    unicast_messages_sync(messages, message_handler_module, opts)
    |> case do
      :ok -> :ok
      {:error, %SerializationError{} = error} -> raise error
    end
  end

  @doc """
  Sends the given messages directly to the specified module, which must implement the
  `#{MessageHandler}` behaviour. The messages are handled synchronously, wrapped in a single
  database transaction. This is particularly useful for tests.
  """
  @spec unicast_messages_sync([Message.t()], atom(), opts) ::
          :ok | {:error, SerializationError.t()}
        when opts: [filters: [String.t()]]
  def unicast_messages_sync(messages, message_handler_module, opts \\ [])
      when is_list(messages) and
             is_atom(message_handler_module) do
    Repo.transaction(fn ->
      for %Message{} = message <- messages do
        type_path = String.split(message.type, ".")
        type_filters = Keyword.get(opts, :filters, [])

        if type_filters == [] ||
             Enum.any?(type_filters, &type_filter_matches_path?(&1, type_path)) do
          case normalize_message_payload(message) do
            {:ok, message} -> deliver_message_to_handler!(message, message_handler_module)
            {:error, reason} -> Repo.rollback(reason)
          end
        end
      end
    end)
    |> case do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec normalize_message_payload(Message.t()) ::
          {:ok, Message.t()} | {:error, SerializationError.t()}
  defp normalize_message_payload(%Message{} = message) do
    with {:ok, payload_json} <- Jason.encode(message.payload),
         {:ok, payload_normalized} <- Jason.decode(payload_json) do
      {:ok, %{message | payload: payload_normalized}}
    else
      {:error, reason} -> {:error, %SerializationError{message: reason, message_struct: message}}
    end
  end

  @doc """
  Same as `broadcast_messages!/1`, but raises a `#{SerializationError}` instead of returning an `:error` tuple.
  """
  @spec broadcast_messages!([Message.t()], broadcast_opts()) :: :ok
  def broadcast_messages!(messages, opts \\ []) do
    case broadcast_messages(messages, opts) do
      :ok -> :ok
      {:error, reason} when is_exception(reason) -> raise reason
    end
  end

  @doc """
  Stores the given messages in the specified message queue. A queue must be specified with the
  `:to_queue` key in `opts`, and can be any arbitrary string. You can use `global_queue()` for
  convenience.

  This function will raise an exception if there is no active database transaction. This is
  intended to help ensure that messages are stored atomically along with other associated changes.
  For instance, an event representing a change in state should be committed to the queue in the
  same transaction as the update that stores the state change.

  ## In this function:
  * Store the messages in the message_queue (see `store_message_in_message_queue/1`)

  ## Asynchronously
  * `#{MessageQueueProcessor}` calls `process_message_queue_batch/1` automatically when messages
  are stored in the message_queue.
  """
  @spec broadcast_messages([Message.t()], broadcast_opts()) ::
          :ok | {:error, SerializationError.t()}
  def broadcast_messages(messages, opts \\ []) when is_list(messages) do
    queue = Keyword.fetch!(opts, :to_queue)

    process_after =
      case Keyword.fetch(opts, :after) do
        :error -> :now
        {:ok, %DateTime{} = datetime} -> {:after, datetime}
        {:ok, {count, unit}} -> {:after, DateTime.add(DateTime.utc_now(), count, unit)}
      end

    if not is_binary(queue) or String.trim(queue) == "" do
      raise "Message queue must be a non-empty string."
    end

    # Store each message in turn, but stop at the first error.
    result =
      Enum.reduce_while(messages, :ok, fn message, :ok ->
        case store_message_in_message_queue(message, queue, process_after) do
          :ok -> {:cont, :ok}
          {:error, _reason} = result -> {:halt, result}
        end
      end)

    result
  end

  @doc """
  Stores the given messages in the message_queue, which is a database table that acts as a transactional
  staging area for messages awaiting dispatch to the message queue. The `#{MessageQueueProcessor}` is
  responsible for picking up the messages and delivering them using `process_message_queue_batch/1`.
  """
  @spec store_message_in_message_queue(Message.t(), String.t(), :now | {:after, DateTime.t()}) ::
          :ok | {:error, SerializationError.t()}
  def store_message_in_message_queue(%Message{} = message, queue, process_after \\ :now)
      when is_binary(queue) do
    if not Repo.in_transaction?() do
      raise "Messages stored in the message_queue must be sent within a transaction."
    end

    db_record = %{
      queue: queue,
      type: message.type,
      schema_version: message.schema_version,
      payload: message.payload,
      metadata: message.metadata,
      inserted_at: DateTime.utc_now(),
      processable_after:
        case process_after do
          :now -> nil
          {:after, %DateTime{} = datetime} -> datetime
        end
    }

    {1, nil} = Repo.insert_all("message_queue_messages", [db_record])

    :ok
  rescue
    e in Protocol.UndefinedError ->
      if e.protocol == Jason.Encoder do
        {:error,
         %SerializationError{
           message: "Failed to serialize to JSON.",
           message_struct: message
         }}
      else
        reraise(e, __STACKTRACE__)
      end
  end

  @doc """
  List messages in the message queue awaiting delivery to the message queue. This is useful mainly
  for tests, when the `#{MessageQueueProcessor}` is not running. You can use this function to
  check that the code under test has dispatched the expected messages.
  """
  @spec peek_at_message_queue_messages(
          skip_locked: boolean(),
          limit: pos_integer()
        ) :: %{queue => [Message.t()]}
        when queue: String.t()
  def peek_at_message_queue_messages(opts \\ []) do
    skip_locked = Keyword.get(opts, :skip_locked, false)
    limit = Keyword.get(opts, :limit, 10)

    lock_param =
      if skip_locked do
        [_lock: :for_update_skip_locked]
      else
        []
      end

    {:ok, messages} = Queries.peek_at_message_queue_messages([limit: limit] ++ lock_param)

    Enum.group_by(
      messages,
      & &1.queue,
      &%Message{
        type: &1.type,
        schema_version: &1.schema_version,
        payload: &1.payload,
        metadata: &1.metadata
      }
    )
  end

  @doc """
  Pulls a batch of messages from the message queue (see `store_message_in_message_queue/1`), and
  publishes them using the message queue. If no errors occurred, the messages are then removed
  from the message_queue. The batch_size determines how many messages are retrieved.

  ### Parallel Consumers

  Message records are locked in the database while the messages are being processed. Any other
  processes that call this function during that time will skip over those messages and retrieve
  the following ones. This could lead to out-of-order processing, so if message order is
  important, you need to ensure only one process at a time calls this function.

  ### Batch Size

  Processing efficiency may be improved by increasing the batch size. However, any exception that
  arises during processing of the messages will cause the entire batch transaction to revert, and
  all of the messages will be re-processed on the following call, including those in the batch
  that had been processed successfully the first time. So pay attention to idempotency, ensuring
  that message handlers are able to handle duplicate messages. Since handlers are called within
  a database transaction that wraps the whole batch, any effects they had in the database should
  have been reverted, including any new messages they broadcast. Effects outside the database
  should be avoided when processing messages in batches, to avoid duplicating the external
  interaction. Instead, broadcast a new message that will be handled by a queue with a batch size
  of 1.
  """
  @spec process_message_queue_batch(String.t(),
          batch_size: pos_integer(),
          handler: message_handler_func()
        ) ::
          non_neg_integer()
  def process_message_queue_batch(queue, opts \\ []) when is_binary(queue) and is_list(opts) do
    batch_size = Keyword.get(opts, :batch_size, 1)

    handler =
      case Keyword.fetch(opts, :handler) do
        {:ok, handler} when is_function(handler, 1) -> handler
        :error -> &deliver_messages_to_handlers!/1
      end

    # Note that the transaction only commits after all the messages are processed, so the messages are in fact not
    # deleted until everything has been processed successfully.
    Repo.transaction(fn ->
      {:ok, messages} =
        Queries.get_and_delete_message_queue_batch(
          queue: queue,
          limit: batch_size,
          processing_datetime: DateTime.utc_now()
        )

      messages = Enum.map(messages, &struct(Message, &1))
      :ok = handler.(messages)
      Enum.count(messages)
    end)
    |> then(fn
      {:ok, num_processed} -> num_processed
    end)
  end

  @spec get_queue_processable_state(String.t()) :: :processable | {:after, DateTime.t()} | :empty
  def get_queue_processable_state(queue) when is_binary(queue) do
    Ecto.Query.from(
      x in "message_queue_messages",
      where: x.queue == ^queue,
      select: %{
        count: count(x),
        min_processable_after: min(x.processable_after)
      }
    )
    |> Repo.one()
    |> case do
      %{count: 0} ->
        :empty

      %{min_processable_after: nil} ->
        :processable

      %{min_processable_after: %NaiveDateTime{} = naive_datetime} ->
        datetime = DateTime.from_naive!(naive_datetime, "Etc/UTC")

        if DateTime.compare(datetime, DateTime.utc_now()) in [:lt, :eq] do
          :processable
        else
          {:after, datetime}
        end
    end
  end

  @doc """
  Delivers the provided messages to a set of modules implementing the `MessageHandler` behaviour. The handler
  configuration looks like this:

     [
       {MyContext.MyMessageHandler, ["MyContext.Commands.*", "AnotherContext.Events.*"]},
       {AnotherContext.EventProcessor, ["AnotherContext.Events.*"]}
     ]

  See `type_filter_matches_path?/2` for details on the message type filtering.
  """
  @spec deliver_messages_to_handlers!([Message.t()], [{module(), [String.t()]}]) :: :ok
  def deliver_messages_to_handlers!(messages, handler_configs \\ @broadcast_listeners)
      when is_list(messages) and is_list(handler_configs) do
    Enum.each(messages, &deliver_message_to_handlers!(&1, handler_configs))
  end

  @spec deliver_message_to_handlers!(Message.t(), [{module(), [String.t()]}]) :: :ok
  defp deliver_message_to_handlers!(%Message{} = message, handler_configs)
       when is_list(handler_configs) do
    type_path = String.split(message.type, ".")

    Logger.info("Messaging: #{inspect(message)}")

    for {message_handler_module, type_filters} <- handler_configs do
      if Enum.any?(type_filters, &type_filter_matches_path?(&1, type_path)) do
        deliver_message_to_handler!(message, message_handler_module)
      end
    end

    :ok
  end

  @spec deliver_message_to_handler!(Message.t(), module) :: :ok
  defp deliver_message_to_handler!(%Message{} = message, message_handler_module)
       when is_atom(message_handler_module) do
    message_handler_module.handle_message(message)
    |> case do
      :ok ->
        :ok

      {:ok, _value} ->
        :ok

      result ->
        raise %MessageHandler.UnexpectedResultError{
          message: message,
          handler: message_handler_module,
          result: result
        }
    end
  end

  # NOTE: See tests for examples
  @spec type_filter_matches_path?(String.t(), [String.t()]) :: boolean()
  def type_filter_matches_path?(type_filter, type_path) when is_list(type_path) do
    String.split(type_filter, ".")
    |> Enum.reduce_while(type_path, fn
      element, [element | type_path_rest] ->
        {:cont, type_path_rest}

      "*", [_element | type_path_rest] ->
        {:cont, type_path_rest}

      _element, _type_path ->
        {:halt, :no_match}
    end)
    |> case do
      [] -> true
      _ -> false
    end
  end
end
