defmodule PostgresqlMessageQueue.Messaging.MessageQueueBroadwayProducer do
  @moduledoc """
  A Broadway producer that emits messages from a specified queue.
  """

  alias __MODULE__, as: Self

  alias PostgresqlMessageQueue.Messaging
  alias PostgresqlMessageQueue.Messaging.Message
  alias PostgresqlMessageQueue.Messaging.MessageQueueWatcher

  use GenStage

  require Logger

  @behaviour Broadway.Acknowledger

  defmodule State do
    @moduledoc false

    alias __MODULE__, as: Self

    @enforce_keys [
      :queue,
      :batch_size,
      :pending_demand,
      :num_batches_in_flight,
      :max_batches_in_flight,
      :can_reorder_messages?,
      :backoff_ms,
      :num_consecutive_failed_batches,
      :backoff_state
    ]
    defstruct @enforce_keys

    @type t :: %Self{
            queue: String.t(),
            batch_size: pos_integer(),
            pending_demand: non_neg_integer(),
            num_batches_in_flight: non_neg_integer(),
            max_batches_in_flight: pos_integer(),
            can_reorder_messages?: boolean(),
            backoff_ms: :none | {:some, non_neg_integer() | (attempt :: integer() -> integer())},
            num_consecutive_failed_batches: non_neg_integer(),
            backoff_state: :none | {:paused_until, DateTime.t()}
          }

    @opts_schema [
                   queue: [
                     type: :string,
                     required: true
                   ],
                   concurrency: [
                     type: :pos_integer,
                     required: true
                   ],
                   batch_size: [
                     type: :pos_integer
                   ],
                   backoff_ms: [
                     type: {:or, [:integer, {:fun, 1}]}
                   ]
                 ]
                 |> NimbleOptions.new!()

    @spec new(Keyword.t()) :: Self.t()
    def new(opts) do
      opts =
        Keyword.take(opts, [
          :queue,
          :concurrency,
          :batch_size,
          :backoff_ms
        ])

      NimbleOptions.validate!(opts, @opts_schema)

      concurrency = Keyword.fetch!(opts, :concurrency)
      batch_size = Keyword.get(opts, :batch_size, 1)

      backoff_ms =
        case Keyword.fetch(opts, :backoff_ms) do
          {:ok, value} -> {:some, value}
          :error -> :none
        end

      # This is chosen so that it's always possible to fetch enough batches in parallel to cover
      # all the demand we could receive. The value of concurrency is the maximum demand we
      # could receive (thanks to max_demand configured on the call to `Broadway.start_link` in
      # `MessageQueueProcessor`).
      #
      # NOTE: It's particularly important that when concurrency = 1, we should only be retrieving
      # one batch at a time, because in this case we're expecting messages to be processed in
      # strict sequence.
      max_batches_in_flight = ceil(concurrency / batch_size)

      %Self{
        queue: opts[:queue],
        batch_size: batch_size,
        pending_demand: 0,
        num_batches_in_flight: 0,
        max_batches_in_flight: max_batches_in_flight,
        can_reorder_messages?: concurrency > 1,
        backoff_ms: backoff_ms,
        num_consecutive_failed_batches: 0,
        backoff_state: :none
      }
    end

    @spec adjust_pending_demand(Self.t(), integer()) :: Self.t()
    def adjust_pending_demand(%Self{} = self, amount) do
      new_pending_demand = max(self.pending_demand + amount, 0)
      %{self | pending_demand: new_pending_demand}
    end
  end

  # Client

  @spec process_demand(GenStage.stage()) :: :ok
  def process_demand(stage) do
    send(stage, :process_demand)
    :ok
  end

  # Server

  @impl GenStage
  def init(opts \\ []) do
    state = State.new(opts)

    # Monitor the MessageQueueWatcher, because if we are relying on it to inform us of new
    # messages and its db connection dies, we need to check our queue and subscribe again. We
    # don't handle this message, so we'll just die.
    Process.monitor(MessageQueueWatcher)

    Logger.info(log_prefix(state) <> "Started")

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(demand, %State{} = state) when demand > 0 do
    state = State.adjust_pending_demand(state, demand)
    send(self(), :process_demand)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call({:process_messages, messages}, from, %State{} = state) do
    receive_message = fn ->
      receive do
        value -> value
      end
    end

    num_messages = Enum.count(messages)

    # We spin up a task that listens for exactly as many `:successful` messages from the `ack/3`
    # function as we have messages. Once all the messages have been received, we reply to this
    # call. That way the call blocks until all the messages have been acked.
    {:ok, batch_ack_pid} =
      Task.start_link(fn ->
        Enum.reduce_while(1..num_messages//1, :successful, fn
          _index, :successful ->
            {:cont, receive_message.()}

          _index, {:failed, _messages} = status ->
            {:halt, status}
        end)
        |> then(fn batch_status ->
          GenStage.reply(from, batch_status)
        end)
      end)

    # Note how when wrapping our message in the broadway message struct, we define how to ack the
    # message by storing the pid of the task we created above, which is waiting for these ack
    # messages.
    broadway_messages =
      Enum.map(messages, fn %Message{} = message ->
        %Broadway.Message{
          data: message,
          acknowledger: {Self, batch_ack_pid, _data = :none}
        }
      end)

    # Now we've got the messages from this batch, we know how much demand has been fulfilled.
    state = State.adjust_pending_demand(state, -Enum.count(messages))

    {:noreply, broadway_messages, state}
  end

  @impl GenStage
  def handle_info(:process_demand, %State{} = state) do
    paused_state =
      case state.backoff_state do
        :none ->
          :not_paused

        {:paused_until, %DateTime{} = until} ->
          remaining_backoff_ms = DateTime.diff(until, DateTime.utc_now(), :millisecond)

          if remaining_backoff_ms > 0 do
            {:paused_for_ms, remaining_backoff_ms}
          else
            :not_paused
          end
      end

    state =
      case paused_state do
        {:paused_for_ms, remaining_ms} ->
          Process.send_after(self(), :process_demand, remaining_ms)
          state

        :not_paused ->
          batches_required = ceil(state.pending_demand / state.batch_size)
          max_new_batches = state.max_batches_in_flight - state.num_batches_in_flight
          num_new_batches = min(batches_required, max_new_batches)

          # This will be quick, because starting to process a batch involves just launching an
          # async task that will send us the messages for the batch.
          Enum.reduce(1..num_new_batches//1, state, fn _index, state ->
            start_processing_new_batch(state)
          end)
      end

    {:noreply, [], state}
  end

  def handle_info({:finished_processing_batch, result}, %State{} = state) do
    state = %{state | num_batches_in_flight: state.num_batches_in_flight - 1}

    state =
      case result do
        :failed ->
          retry_datetime =
            DateTime.utc_now()
            |> DateTime.add(backoff_ms_for_producer(state), :millisecond)

          %{
            state
            | num_consecutive_failed_batches: state.num_consecutive_failed_batches + 1,
              backoff_state: {:paused_until, retry_datetime}
          }

        _other ->
          %{state | num_consecutive_failed_batches: 0, backoff_state: :none}
      end

    cond do
      state.pending_demand <= 0 ->
        # We can just wait for more demand to arrive.
        :ok

      result != :empty_queue ->
        # The queue isn't empty, and there is some demand, so let's process it.
        send(self(), :process_demand)

      result == :empty_queue ->
        # We ran out of messages in the queue, but still have pending demand. We want to suspend
        # processing until there is a message to process.

        # Note that the MessageQueueWatcher notification happens only once, when the next message
        # is inserted into the database (and any wrapping transaction commits), and arrives as a
        # call to MessageQueueProcessor.check_for_new_messages/1.
        MessageQueueWatcher.notify_on_new_message(state.queue)

        # Messages could have arrived between this batch returning empty and the above
        # subscription coming into effect.
        case Messaging.get_queue_processable_state(state.queue) do
          :empty ->
            # We can just wait for the MessageQueueWatcher to tell us when a new message arrives.
            :ok

          :processable ->
            send(self(), :process_demand)

          {:after, %DateTime{} = processable_after} ->
            # The only messages in the queue are not processable yet, but we know when they will
            # become processable.
            processable_in_ms = DateTime.diff(processable_after, DateTime.utc_now(), :millisecond)
            Process.send_after(self(), :process_demand, processable_in_ms)
            :ok
        end
    end

    {:noreply, [], state}
  end

  @impl Broadway.Acknowledger
  def ack(batch_ack_pid = _ack_ref, successful, failed)
      when is_list(successful) and is_list(failed) do
    if failed == [] do
      # The recipient is just counting messages to ack the whole batch, so we don't care about
      # specific message details.
      Enum.each(successful, fn %Broadway.Message{} -> send(batch_ack_pid, :successful) end)
    else
      send(batch_ack_pid, {:failed, failed})
    end

    :ok
  end

  @spec start_processing_new_batch(State.t()) :: State.t()
  defp start_processing_new_batch(%State{} = state) do
    server_pid = self()

    # This will be called to handle the new batch of messages. The whole batch of messages will be
    # acked so long as this returns :ok. Otherwise, the whole batch will fail and be re-delivered.
    message_handler = fn messages ->
      case GenStage.call(server_pid, {:process_messages, messages}, :infinity) do
        :successful -> :ok
        {:failed, messages} -> handle_failed_messages(messages, state)
      end
    end

    Task.start_link(fn ->
      try do
        # This call will block until the handler returns, which doesn't happen until all the
        # messages from the batch are processed.
        num_processed =
          Messaging.process_message_queue_batch(
            state.queue,
            batch_size: state.batch_size,
            handler: message_handler
          )

        result =
          cond do
            num_processed == 0 -> :empty_queue
            num_processed > 0 -> :processed_one_or_more
          end

        send(server_pid, {:finished_processing_batch, result})
      rescue
        e ->
          Logger.warning(
            log_prefix(state) <>
              Exception.message(e) <> "\n" <> Exception.format_stacktrace(__STACKTRACE__)
          )

          send(server_pid, {:finished_processing_batch, :failed})
      end
    end)

    # We've launched our async task requesting messages. At some point the handler will be called,
    # and we'll see the messages in a `:process_messages` call, which will cause the demand to be
    # fulfilled.
    %{state | num_batches_in_flight: state.num_batches_in_flight + 1}
  end

  # Tries to "rescue" the failed message by re-enqueuing it to be retried later, avoiding the need
  # to return the whole batch to the queue for a retry. This could by disabled (e.g. when messages
  # must be processed in order, or simply when no backoff is configured), or it could fail (if we
  # can't re-enqueue the message for some reason). If this fails, the whole batch will be returned
  # to the queue.
  @spec handle_failed_messages([Broadway.Message.t()], State.t()) :: :ok | {:error, any()}
  defp handle_failed_messages(messages, %State{} = state) do
    error_response = fn extra_detail when is_list(extra_detail) ->
      detail = [messages: messages, batch_size: state.batch_size] ++ extra_detail
      {:error, {:failed_messages, detail}}
    end

    case state do
      %State{
        can_reorder_messages?: true,
        backoff_ms: {:some, _backoff_ms}
      } ->
        Enum.reduce_while(messages, :ok, fn
          message, :ok -> {:cont, reenqueue_failed_message_with_backoff(message, state)}
          _message, other -> {:halt, other}
        end)
        |> case do
          :ok -> :ok
          other -> error_response.(backoff_error: other)
        end

      _other ->
        error_response.([])
    end
  end

  @spec reenqueue_failed_message_with_backoff(Broadway.Message.t(), State.t()) :: :ok
  defp reenqueue_failed_message_with_backoff(
         %Broadway.Message{data: %Message{} = message, status: message_error},
         %State{} = state
       ) do
    attempt = Map.get(message.metadata, "attempt", 1)
    backoff_ms = backoff_ms_for_attempt(state, attempt)

    # The stacktrace is logged before sending the {:finished_processing_batch, :failed} message.
    # It is a bit noisy, so we can strip it out here to keep messages clear.
    # https://hexdocs.pm/broadway/Broadway.Message.html#t:t/0
    message_error =
      with {cause, reason, stacktrace}
           when cause in [:throw, :error, :exit] and is_list(stacktrace) <- message_error do
        {cause, reason}
      end

    Logger.warning(
      log_prefix(state) <>
        "Failed to process message: #{inspect(message)} due to: #{inspect(message_error)}. Will retry in #{backoff_ms}ms."
    )

    new_metadata = Map.put(message.metadata, "attempt", attempt + 1)
    message = %{message | metadata: new_metadata}

    # Note that because this function is called as part of the handler for
    # `Messaging.process_message_queue_batch/2`, the re-enqueued message is inserted in the same
    # transaction that is locking the original messages. So either the transaction commits,
    # deleting the original message record, or the whole transaction is rolled back and the
    # re-enqueued message is not inserted.
    Messaging.broadcast_messages!(
      [message],
      to_queue: state.queue,
      after: {backoff_ms, :millisecond}
    )
  end

  @spec backoff_ms_for_producer(State.t()) :: integer()
  defp backoff_ms_for_producer(%State{} = state) do
    attempt = state.num_consecutive_failed_batches + 1
    backoff_ms_for_attempt(state, attempt)
  rescue
    e ->
      Logger.error(Exception.format(:error, e, __STACKTRACE__))
      1_000
  end

  @spec backoff_ms_for_attempt(State.t(), integer()) :: integer()
  defp backoff_ms_for_attempt(%State{} = state, attempt) when is_integer(attempt) do
    case state.backoff_ms do
      :none -> 0
      {:some, backoff_ms} when is_integer(backoff_ms) -> backoff_ms
      {:some, backoff_ms_func} when is_function(backoff_ms_func, 1) -> backoff_ms_func.(attempt)
    end
  end

  @spec log_prefix(State.t()) :: String.t()
  defp log_prefix(%State{} = state) do
    "MessageQueueBroadwayProducer [#{inspect(self())} queue:#{state.queue}] "
  end
end
