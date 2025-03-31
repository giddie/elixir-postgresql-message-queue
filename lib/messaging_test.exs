defmodule PostgresqlMessageQueue.MessagingTest do
  @moduledoc false

  use PostgresqlMessageQueue.Tests.DataCase, async: true

  alias PostgresqlMessageQueue.Messaging
  alias PostgresqlMessageQueue.Messaging.Message
  alias PostgresqlMessageQueue.Persistence.Repo

  test "type_filter_matches_path?" do
    type_path = ["One", "Two", "Three"]

    assert Messaging.type_filter_matches_path?("One.Two.Three", type_path)
    assert Messaging.type_filter_matches_path?("*.Two.Three", type_path)
    assert Messaging.type_filter_matches_path?("One.*.Three", type_path)
    assert Messaging.type_filter_matches_path?("One.Two.*", type_path)

    refute Messaging.type_filter_matches_path?("One.Two", type_path)
    refute Messaging.type_filter_matches_path?("One.Two.Other", type_path)
    refute Messaging.type_filter_matches_path?("*", type_path)
    refute Messaging.type_filter_matches_path?("*.One.Two.Three", type_path)
    refute Messaging.type_filter_matches_path?("One.Two.Three.*", type_path)
    refute Messaging.type_filter_matches_path?("One.Two.Three.Four", type_path)
  end

  test "store_message_in_outbox: serialization error" do
    queue = UUID.uuid4()

    Repo.transaction(fn ->
      assert {:error, %PostgresqlMessageQueue.Messaging.SerializationError{}} =
               Messaging.store_message_in_outbox(
                 %Message{
                   type: "Messaging.Event.Example",
                   schema_version: 1,
                   payload: %{hello: self()}
                 },
                 queue
               )
    end)

    assert %{} == Messaging.peek_at_outbox_messages()
  end

  test "get_queue_processable_state: empty queue" do
    queue = UUID.uuid4()
    assert :empty = Messaging.get_queue_processable_state(queue)
  end

  test "get_queue_processable_state: message with no processable_after" do
    queue = UUID.uuid4()

    Repo.transaction(fn ->
      [%Message{type: "Test.Event", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue)
    end)

    assert :processable = Messaging.get_queue_processable_state(queue)
  end

  test "get_queue_processable_state: message with processable_after in past" do
    queue = UUID.uuid4()
    past_datetime = DateTime.utc_now() |> DateTime.add(-2, :second)

    Repo.transaction(fn ->
      [%Message{type: "Test.Event", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue, after: past_datetime)
    end)

    assert :processable = Messaging.get_queue_processable_state(queue)
  end

  test "get_queue_processable_state: message with processable_after in future" do
    queue = UUID.uuid4()
    future_datetime = DateTime.utc_now() |> DateTime.add(2, :second)

    Repo.transaction(fn ->
      [%Message{type: "Test.Event", schema_version: 1, payload: %{}}]
      |> Messaging.broadcast_messages!(to_queue: queue, after: future_datetime)
    end)

    assert {:after, ^future_datetime} = Messaging.get_queue_processable_state(queue)
  end
end
