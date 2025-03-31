defmodule PostgresqlMessageQueue.Messaging.MessageQueueProcessorTest do
  @moduledoc false

  alias PostgresqlMessageQueue.Messaging
  alias PostgresqlMessageQueue.Messaging.Message
  alias PostgresqlMessageQueue.Messaging.MessageHandler
  alias PostgresqlMessageQueue.Messaging.MessageQueueProcessor
  alias PostgresqlMessageQueue.Persistence.Repo

  # We do some global operations in the database to test what is visible in different transactions
  use PostgresqlMessageQueue.Tests.DataCase, async: false
  @moduletag use_ecto_sandbox: false

  defmodule TestMessageHandler do
    @moduledoc false

    @behaviour MessageHandler

    @impl MessageHandler
    def handle_message(
          %Message{
            type: "Test.Command.RespondToTestPid",
            payload: %{"test_pid" => serialized_pid}
          } = message
        )
        when is_binary(serialized_pid) do
      Base.decode64!(serialized_pid)
      |> :erlang.binary_to_term()
      |> send({:received_message, message})

      :ok
    end
  end

  test "message delivery with custom handler using message handler module" do
    queue = UUID.uuid4()

    serialized_test_pid =
      self()
      |> :erlang.term_to_binary()
      |> Base.encode64()

    handler =
      &Messaging.deliver_messages_to_handlers!([&1], [{TestMessageHandler, ["Test.Command.*"]}])

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler})

    Repo.transaction(fn ->
      [
        %Message{
          type: "Test.Command.RespondToTestPid",
          schema_version: 1,
          payload: %{test_pid: serialized_test_pid}
        }
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert {:received_message,
            %Message{
              type: "Test.Command.RespondToTestPid",
              schema_version: 1,
              payload: %{"test_pid" => ^serialized_test_pid}
            }} = next_message()
  end

  test "handler should be wrapped in db transaction" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})

        # This message should never actually be committed to the database due to the handler returning an error tuple,
        # so it'll never be processed.
        [%Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}]
        |> Messaging.broadcast_messages!(to_queue: queue)

        receive do
          {:continue, value} -> value
        after
          1_000 -> {:error, :timeout}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler})

    Repo.transaction(fn ->
      [%Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert {:received_event, :first, handler_pid} = next_message()

    # Only the FirstEvent message should still be in the queue. (It's currently being processed.) The SecondEvent
    # message should be invisible inside the handler's transaction.
    assert %{^queue => [%Message{type: "Test.Event.FirstEvent"}]} =
             Messaging.peek_at_message_queue_messages()

    # The message handler should fail, and the message is retried. The queue should still look the same after the
    # handler failed the message.
    send(handler_pid, {:continue, :error})
    assert {:received_event, :first, ^handler_pid} = next_message()

    assert %{^queue => [%Message{type: "Test.Event.FirstEvent"}]} =
             Messaging.peek_at_message_queue_messages()

    send(handler_pid, {:continue, :ok})
    assert {:received_event, :second} = next_message()
  end

  test "multiple messages" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})

        receive do
          :continue -> :ok
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert {:received_event, :first, handler_pid} = next_message()
    refute_receive {:received_event, :second, _handler_pid}

    # The second message should still be in the database, unlocked.
    assert %{^queue => [%Message{type: "Test.Event.SecondEvent"}]} =
             Messaging.peek_at_message_queue_messages(skip_locked: true)

    send(handler_pid, :continue)

    # NOTE: We expect both events to have been processed by the same pid, because we haven't configured any concurrency.
    assert {:received_event, :second, ^handler_pid} = next_message()
  end

  test "delayed message" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event", payload: %{"index" => index}} ->
        send(test_pid, {:received_event, index})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler})

    Repo.transaction(fn ->
      [%Message{type: "Test.Event", schema_version: 1, payload: %{index: 1}}]
      |> Messaging.broadcast_messages!(to_queue: queue, after: {1, :second})

      [%Message{type: "Test.Event", schema_version: 1, payload: %{index: 2}}]
      |> Messaging.broadcast_messages!(to_queue: queue, after: {2, :second})

      [%Message{type: "Test.Event", schema_version: 1, payload: %{index: 3}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert {:received_event, 3} = next_message()
    refute_receive {:received_event, _}, 900

    assert {:received_event, 1} = next_message(wait_ms: 200)

    refute_receive {:received_event, _}, 900
    assert {:received_event, 2} = next_message(wait_ms: 200)
  end

  test "many messages" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{
        type: "Test.Event.ExampleEvent",
        payload: %{"index" => index}
      } ->
        send(test_pid, {:received_event, index, self()})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler})

    Repo.transaction(fn ->
      for batch_start <- 1..100//20 do
        for index <- batch_start..(batch_start + 19)//1 do
          %Message{type: "Test.Event.ExampleEvent", schema_version: 1, payload: %{index: index}}
        end
        |> Messaging.broadcast_messages!(to_queue: queue)
      end
    end)

    assert {:received_event, 1, handler_pid} = next_message()

    for index <- 2..100//1 do
      assert {:received_event, ^index, ^handler_pid} = next_message()
    end

    refute_receive _any_additional_message
  end

  test "crash in handler" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:processing_first_event, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, :received_second_event)
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # The batch size is 1, so the first event should be processed, and the second event should be pending.
    assert {:processing_first_event, handler_pid} = next_message()
    refute_receive :received_second_event

    # If max_demand isn't set to 1 for the Broadway processor, we can trigger a bug by asking the MessageQueueProcessor to
    # check for new messages at this point, and it will fetch the second event message because demand > 0. As a result,
    # when the first message fails the second will be delivered first, instead of retrying the first.
    MessageQueueProcessor.check_for_new_messages(queue)
    Process.sleep(100)

    # We instruct the handler for the first event to raise an exception. The message should fail and be retried.
    send(handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})

    # The second message should still not be processed, because we need to retry the first message.
    refute_receive :received_second_event

    # A non-:ok response from the handler should have the same result
    assert {:processing_first_event, ^handler_pid} = next_message()
    send(handler_pid, {:run, fn -> :unexpected end})
    refute_receive :received_second_event

    # The next time the event is processed, we let it complete successfully.
    assert {:processing_first_event, ^handler_pid} = next_message()
    send(handler_pid, {:run, fn -> :ok end})

    assert :received_second_event = next_message()
  end

  test "crash in handler, with integer backoff_ms configured" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:processing_first_event, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, :received_second_event)
        :ok
    end

    # In this case the backoff is configured to process the failed message after a fixed interval. This could be 0, in
    # which case the message will be retried immediately. Note that because no concurrency is specified, the queue must
    # be processed sequentially, so the backoff blocks the whole queue.
    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler, backoff_ms: 1_000})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # The batch size is 1, so the first event should be processed, and the second event should be pending.
    assert {:processing_first_event, handler_pid} = next_message()
    refute_receive :received_second_event

    # We instruct the handler for the first event to fail. The message should fail and be retried.
    send(handler_pid, {:run, fn -> {:error, :test} end})

    # The second message should still not be processed, because we need to retry the first message.
    refute_receive :received_second_event

    # We waited up to 100ms for that second event. That first message shouldn't be retried for another ~900ms.
    refute_receive _, 800
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # We fail a second time, to check that exceptions are caught, and the same backoff interval should be used.
    send(handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})
    refute_receive _, 900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # This time we're checking that exceptions in inner db transactions shouldn't cause any issues to the backoff
    # mechanism.
    send(
      handler_pid,
      {:run, fn -> Repo.transaction(fn -> raise "Test: Exception in handler!" end) end}
    )

    refute_receive _, 900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # This time we're checking that inner db transaction rollbacks shouldn't cause any issues to the backoff mechanism.
    send(
      handler_pid,
      {:run, fn -> Repo.transaction(fn -> Repo.rollback("test rollback") end) end}
    )

    refute_receive _, 900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(handler_pid, {:run, fn -> :ok end})
    assert :received_second_event = next_message()
  end

  test "crash in handler, with backoff_ms function configured" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:processing_first_event, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, :received_second_event)
        :ok
    end

    # In this case the backoff is configured to process the failed message after one second, and then on the second and
    # subsequent failures to process it again after two seconds.
    backoff_ms_func = fn
      1 -> 1_000
      _ -> 2_000
    end

    start_link_supervised!(
      {MessageQueueProcessor, queue: queue, handler: handler, backoff_ms: backoff_ms_func}
    )

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # The batch size is 1, so the first event should be processed, and the second event should be pending.
    assert {:processing_first_event, handler_pid} = next_message()
    refute_receive :received_second_event

    # We instruct the handler for the first event to fail. The message should fail and be retried.
    send(handler_pid, {:run, fn -> {:error, :test} end})

    # The second message should still not be processed, because we need to retry the first message.
    refute_receive :received_second_event

    # We waited up to 100ms for that second event. That first message shouldn't be retried for another ~900ms.
    refute_receive _, 800
    assert {:processing_first_event, handler_pid} = next_message(wait_ms: 200)

    # We fail a second time, to check that we progress to the second backoff interval. And this time we use an exception
    # to check this is caught correctly too.
    send(handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})
    refute_receive _, 1_900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # The third time, it should re-use the final backoff interval.
    send(
      handler_pid,
      {:run, fn -> Repo.transaction(fn -> Repo.rollback("test rollback") end) end}
    )

    refute_receive _, 1_900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(handler_pid, {:run, fn -> :ok end})
    assert :received_second_event = next_message()

    # Check that after success, the number of attempts passed into the backoff_ms function is reset.
    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert {:processing_first_event, handler_pid} = next_message()
    send(handler_pid, {:run, fn -> {:error, :test} end})

    refute_receive _, 900
    assert {:processing_first_event, handler_pid} = next_message(wait_ms: 200)
    send(handler_pid, {:run, fn -> :ok end})

    assert :received_second_event = next_message()
  end

  test "crash in handler, with crashing backoff_ms function configured" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:processing_first_event, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, :received_second_event)
        :ok
    end

    # In this case the backoff function will crash, but the message queue processor should fall back to a sensible default.
    backoff_ms_func = fn
      1 -> raise "Test backoff_ms_func exception!"
    end

    start_link_supervised!(
      {MessageQueueProcessor, queue: queue, handler: handler, backoff_ms: backoff_ms_func}
    )

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # The batch size is 1, so the first event should be processed, and the second event should be pending.
    assert {:processing_first_event, handler_pid} = next_message()
    refute_receive :received_second_event

    # We instruct the handler for the first event to fail. The message should fail and be retried.
    send(handler_pid, {:run, fn -> {:error, :test} end})

    # The second message should still not be processed, because we need to retry the first message.
    refute_receive :received_second_event

    # The backoff should apply even if we ask the processor to check for messages.
    :ok = Messaging.MessageQueueProcessor.check_for_new_messages(queue)

    # We waited up to 100ms for that second event. That first message shouldn't be retried for another ~900ms.
    refute_receive _, 800
    assert {:processing_first_event, handler_pid} = next_message(wait_ms: 200)

    # We fail a second time, and the backoff interval should be the same default 1_000.
    send(handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})
    refute_receive _, 900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # The third time, it should still use the default 1_000.
    send(handler_pid, {:run, fn -> {:error, :test} end})
    refute_receive _, 900
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(handler_pid, {:run, fn -> :ok end})
    assert :received_second_event = next_message()
  end

  test "new message arrives during message failure backoff interval" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:processing_first_event, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, :received_second_event)
        :ok
    end

    # In this case the backoff is configured to process the failed message after a fixed interval. This could be 0, in
    # which case the message will be retried immediately. Note that because no concurrency is specified, the queue must
    # be processed sequentially, so the backoff blocks the whole queue.
    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler, backoff_ms: 1_000})

    Repo.transaction(fn ->
      [%Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert {:processing_first_event, handler_pid} = next_message()

    # We instruct the handler for the first event to fail. The message should fail and be re-enqueued at
    # the end of the queue.
    send(handler_pid, {:run, fn -> {:error, :test} end})

    # Wait a little to ensure the processor sees an empty queue and decides to wait until it's time to process the
    # delayed message.
    Process.sleep(200)

    # Now we broadcast the second message.
    Repo.transaction(fn ->
      [%Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # We waited 200ms for the backoff logic. That first message shouldn't be retried for another ~800ms.
    refute_receive _, 700
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(handler_pid, {:run, fn -> :ok end})

    # And now the second event should be processed.
    assert_receive :received_second_event
  end

  test "batch_size = 2" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})

        receive do
          :continue -> :ok
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})
        :ok

      %Message{type: "Test.Event.ThirdEvent"} ->
        send(test_pid, {:received_event, :third, self()})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler, batch_size: 2})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.ThirdEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # NOTE: We expect both events to have been processed by the same pid, because we haven't configured any concurrency.
    assert {:received_event, :first, handler_pid} = next_message()
    refute_received {:received_event, :second, _handler_pid}

    # When the first event is received, the second message should not still be in the message queue, because it should have
    # been collected as part of the same batch. But the third event should be there because it will be in the next
    # batch.
    assert %{^queue => [%Message{type: "Test.Event.ThirdEvent"}]} =
             Messaging.peek_at_message_queue_messages(skip_locked: true)

    send(handler_pid, :continue)

    assert {:received_event, :second, ^handler_pid} = next_message()
    assert {:received_event, :third, ^handler_pid} = next_message()
  end

  test "crash with batch_size = 2" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})
        :ok

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :second_event}
        end

      %Message{type: "Test.Event.ThirdEvent"} ->
        send(test_pid, {:received_event, :third, self()})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler, batch_size: 2})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.ThirdEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # NOTE: We expect all events to be processed by the same pid, because we haven't configured any concurrency.
    assert {:received_event, :first, handler_pid} = next_message()
    assert {:received_event, :second, ^handler_pid} = next_message()
    refute_received {:received_event, :third, _handler_pid}

    # After the handler for the second event fails, we expect the first message to be delivered again, because the first
    # two events are part of the same batch.
    send(handler_pid, {:run, fn -> :error end})

    assert {:received_event, :first, ^handler_pid} = next_message()
    assert {:received_event, :second, ^handler_pid} = next_message()
    send(handler_pid, {:run, fn -> :ok end})
    assert {:received_event, :third, ^handler_pid} = next_message()
  end

  test "concurrency = 2" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})

        receive do
          :continue -> :ok
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})
        :ok

      %Message{type: "Test.Event.ThirdEvent"} ->
        send(test_pid, {:received_event, :third, self()})
        :ok
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler, concurrency: 2})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.ThirdEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert_receive {:received_event, :first, first_handler_pid}
    assert_receive {:received_event, :second, second_handler_pid}
    assert first_handler_pid != second_handler_pid

    # Because the first event handler is waiting for us to tell it to continue, the third event will be processed by the
    # same pid as the second event.
    assert {:received_event, :third, ^second_handler_pid} = next_message()

    send(first_handler_pid, :continue)

    refute_receive _any
  end

  test "crash with concurrency = 2" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})

        receive do
          :fail -> {:error, :deliberate_error_from_test}
          :continue -> :ok
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})

        receive do
          :continue -> :ok
        after
          1_000 -> {:error, :timeout, :second_event}
        end
    end

    start_link_supervised!({MessageQueueProcessor, queue: queue, handler: handler, concurrency: 2})

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert_receive {:received_event, :first, first_handler_pid}
    assert_receive {:received_event, :second, second_handler_pid}
    assert first_handler_pid != second_handler_pid

    send(first_handler_pid, :fail)

    # The first message should be retried
    assert_receive {:received_event, :first, first_handler_pid}
    send(first_handler_pid, :continue)

    # The second message is still processing. It should not have been killed and retried by the failure of the first
    # message.
    refute_receive {:received_event, :second, _second_handler_pid}, 500

    send(second_handler_pid, :continue)
  end

  test "many messages, concurrently, in batches" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{
        type: "Test.Event.ExampleEvent",
        payload: %{"index" => index}
      } ->
        send(test_pid, {:received_event, index, self()})
        :ok
    end

    start_link_supervised!(
      {MessageQueueProcessor, queue: queue, handler: handler, batch_size: 5, concurrency: 5}
    )

    Repo.transaction(fn ->
      for batch_start <- 1..100//20 do
        for index <- batch_start..(batch_start + 19)//1 do
          %Message{type: "Test.Event.ExampleEvent", schema_version: 1, payload: %{index: index}}
        end
        |> Messaging.broadcast_messages!(to_queue: queue)
      end
    end)

    handler_pids =
      for index <- 1..100//1, into: MapSet.new() do
        assert_receive {:received_event, ^index, handler_pid}
        handler_pid
      end

    assert Enum.count(handler_pids) == 5

    refute_receive _any_additional_message
  end

  test "concurrency = 2, crash in handler, with integer backoff_ms configured" do
    queue = UUID.uuid4()
    test_pid = self()

    perform_provided_instruction = fn event_name ->
      receive do
        {:run, func} when is_function(func, 0) -> func.()
      after
        5_000 -> {:error, :timeout, event_name}
      end
    end

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})
        perform_provided_instruction.(:first)

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})
        perform_provided_instruction.(:second)

      %Message{type: "Test.Event.ThirdEvent"} ->
        send(test_pid, {:received_event, :third})
        :ok
    end

    # In this case the backoff is configured to process the failed message after a fixed interval. This could be 0, in
    # which case the message will be processed immediately after any other messages that are already in the queue.
    start_link_supervised!(
      {MessageQueueProcessor, queue: queue, handler: handler, concurrency: 2, backoff_ms: 1_000}
    )

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.ThirdEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert_receive {:received_event, :first, first_handler_pid}
    assert_receive {:received_event, :second, second_handler_pid}
    refute_receive {:received_event, :third}

    # We instruct the handler for the first event to fail. The message should fail and be re-enqueued at
    # the end of the queue.
    send(first_handler_pid, {:run, fn -> {:error, :test} end})

    # The third message should be processed now.
    assert {:received_event, :third} = next_message()

    # We waited up to 100ms for that third event. That first message shouldn't be retried for another ~900ms.
    refute_receive _, 800
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # We fail a second time, to check that exceptions are caught, and the same backoff interval should be used.
    send(first_handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})
    refute_receive _, 900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 400)

    # This time we're checking that exceptions in inner db transactions shouldn't cause any issues to the backoff mechanism.
    send(
      first_handler_pid,
      {:run, fn -> Repo.transaction(fn -> raise "Test: Exception in handler!" end) end}
    )

    refute_receive _, 900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # This time we're checking that inner db transaction rollbacks shouldn't cause any issues to the backoff mechanism.
    send(
      first_handler_pid,
      {:run, fn -> Repo.transaction(fn -> Repo.rollback("test rollback") end) end}
    )

    refute_receive _, 900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(first_handler_pid, {:run, fn -> :ok end})

    # And finally we let the second event handler finish too.
    send(second_handler_pid, {:run, fn -> :ok end})
  end

  test "concurrency = 2, crash in handler, with backoff_ms function configured" do
    queue = UUID.uuid4()
    test_pid = self()

    perform_provided_instruction = fn event_name ->
      receive do
        {:run, func} when is_function(func, 0) -> func.()
      after
        5_000 -> {:error, :timeout, event_name}
      end
    end

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})
        perform_provided_instruction.(:first)

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})
        perform_provided_instruction.(:second)

      %Message{type: "Test.Event.ThirdEvent"} ->
        send(test_pid, {:received_event, :third})
        :ok
    end

    # In this case the backoff is configured to process the failed message after one second, and then on the second and
    # subsequent failures to process it again after two seconds.
    backoff_ms_func = fn
      1 -> 1_000
      _ -> 2_000
    end

    start_link_supervised!(
      {MessageQueueProcessor,
       queue: queue, handler: handler, concurrency: 2, backoff_ms: backoff_ms_func}
    )

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.ThirdEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert_receive {:received_event, :first, first_handler_pid}
    assert_receive {:received_event, :second, second_handler_pid}
    refute_receive {:received_event, :third}

    # We instruct the handler for the first event to fail. The message should fail and be re-enqueued at
    # the end of the queue.
    send(first_handler_pid, {:run, fn -> {:error, :test} end})

    # The third message should be processed now.
    assert {:received_event, :third} = next_message()

    # We waited up to 100ms for that second event. That first message shouldn't be retried for another ~900ms.
    refute_receive _, 800
    assert {:received_event, :first, first_handler_pid} = next_message(wait_ms: 200)

    # We fail a second time, to check that we progress to the second backoff interval. And this time we use an exception
    # to check this is caught correctly too.
    send(first_handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})
    refute_receive _, 1_900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # The third time, it should re-use the final backoff interval.
    send(
      first_handler_pid,
      {:run, fn -> Repo.transaction(fn -> raise "Test: Exception in handler!" end) end}
    )

    refute_receive _, 1_900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(first_handler_pid, {:run, fn -> :ok end})

    # And finally we let the second event handler finish too.
    send(second_handler_pid, {:run, fn -> :ok end})
  end

  test "concurrency = 2, crash in handler, with crashing backoff_ms function configured" do
    queue = UUID.uuid4()
    test_pid = self()

    perform_provided_instruction = fn event_name ->
      receive do
        {:run, func} when is_function(func, 0) -> func.()
      after
        5_000 -> {:error, :timeout, event_name}
      end
    end

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:received_event, :first, self()})
        perform_provided_instruction.(:first)

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, {:received_event, :second, self()})
        perform_provided_instruction.(:second)

      %Message{type: "Test.Event.ThirdEvent"} ->
        send(test_pid, {:received_event, :third})
        :ok
    end

    # In this case the backoff is configured to process the failed message after one second, and then on the second and
    # subsequent failures to process it again after two seconds.
    backoff_ms_func = fn
      1 -> raise "Test backoff_ms_func exception!"
    end

    start_link_supervised!(
      {MessageQueueProcessor,
       queue: queue, handler: handler, concurrency: 2, backoff_ms: backoff_ms_func}
    )

    Repo.transaction(fn ->
      [
        %Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}},
        %Message{type: "Test.Event.ThirdEvent", schema_version: 1, payload: %{}}
      ]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert_receive {:received_event, :first, first_handler_pid}
    assert_receive {:received_event, :second, second_handler_pid}
    refute_receive {:received_event, :third}

    # We instruct the handler for the first event to fail. The message should fail and be re-enqueued at
    # the end of the queue.
    send(first_handler_pid, {:run, fn -> {:error, :test} end})

    # We would expect the third message to be processed now, but because the backoff_ms function failed, the
    # re-enqueueing backoff failed, and instead the whole queue is backing off and will retry the first message.
    refute_receive _, 900
    assert {:received_event, :first, first_handler_pid} = next_message(wait_ms: 200)

    # We fail a second time, and the backoff interval should be the same default 1_000.
    send(first_handler_pid, {:run, fn -> raise "Test: Exception in handler!" end})
    refute_receive _, 900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # The third time, it should still use the default 1_000.
    send(
      first_handler_pid,
      {:run, fn -> Repo.transaction(fn -> raise "Test: Exception in handler!" end) end}
    )

    refute_receive _, 900
    assert {:received_event, :first, ^first_handler_pid} = next_message(wait_ms: 200)

    # The next time the event is processed, we let it complete successfully.
    send(first_handler_pid, {:run, fn -> :ok end})

    # The third message should be processed now.
    assert {:received_event, :third} = next_message()

    # And finally we let the second event handler finish too.
    send(second_handler_pid, {:run, fn -> :ok end})
  end

  test "concurrency = 2, new message arrives during message failure backoff interval" do
    queue = UUID.uuid4()
    test_pid = self()

    handler = fn
      %Message{type: "Test.Event.FirstEvent"} ->
        send(test_pid, {:processing_first_event, self()})

        receive do
          {:run, func} when is_function(func, 0) -> func.()
        after
          1_000 -> {:error, :timeout, :first_event}
        end

      %Message{type: "Test.Event.SecondEvent"} ->
        send(test_pid, :received_second_event)
        :ok
    end

    # In this case the backoff is configured to process the failed message after a fixed interval. This could be 0, in
    # which case the message will be processed immediately after any other messages that are already in the queue.
    start_link_supervised!(
      {MessageQueueProcessor, queue: queue, handler: handler, concurrency: 2, backoff_ms: 1_000}
    )

    Repo.transaction(fn ->
      [%Message{type: "Test.Event.FirstEvent", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # The batch size is 1, so the first event should be processed, and the second event should be pending.
    assert {:processing_first_event, handler_pid} = next_message()

    # We instruct the handler for the first event to fail. The message should fail and be re-enqueued at
    # the end of the queue.
    send(handler_pid, {:run, fn -> {:error, :test} end})

    # Wait a little to ensure the processor sees an empty queue and decides to wait until it's time to process the
    # delayed message.
    Process.sleep(200)

    # Now we broadcast the second message.
    Repo.transaction(fn ->
      [%Message{type: "Test.Event.SecondEvent", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    # It should be processed immediately.
    assert :received_second_event = next_message()

    # We waited up to 100ms for that second event and 200ms for the backoff logic. That first message shouldn't be
    # retried for another ~700ms.
    refute_receive _, 600
    assert {:processing_first_event, ^handler_pid} = next_message(wait_ms: 400)

    # The next time the event is processed, we let it complete successfully.
    send(handler_pid, {:run, fn -> :ok end})
  end

  @spec next_message(wait_ms: integer()) :: any()
  defp next_message(opts \\ []) do
    timeout = Keyword.get(opts, :wait_ms, nil)
    assert_receive message, timeout
    message
  end
end
