defmodule PostgresqlMessageQueue.ExampleUsage do
  @moduledoc """
  This module demonstrates durable asynchronous job processing using the internal message queue.
  """

  alias PostgresqlMessageQueue.Messaging
  alias PostgresqlMessageQueue.Persistence.Repo

  require Logger

  @behaviour Messaging.MessageHandler

  @spec send_greeting!(String.t()) :: :ok
  def send_greeting!(greeting \\ "Hello!") when is_binary(greeting) do
    {:ok, :ok} =
      Repo.transaction(fn ->
        [
          %Messaging.Message{
            type: "ExampleUsage.Events.Greeting",
            schema_version: 1,
            payload: %{greeting: greeting}
          }
        ]
        |> Messaging.broadcast_messages!(to_queue: Messaging.global_queue())
      end)

    :ok
  end

  @spec send_cascade!() :: :ok
  def send_cascade!() do
    {:ok, :ok} =
      Repo.transaction(fn ->
        send_cascade_at_depth!(1, [])
      end)

    :ok
  end

  @spec send_cascade_at_depth!(non_neg_integer(), [non_neg_integer()]) :: :ok
  defp send_cascade_at_depth!(depth, path)
       when is_integer(depth) and depth > 0 and is_list(path) do
    for index <- 1..10 do
      %Messaging.Message{
        type: "ExampleUsage.Events.Cascade",
        schema_version: 1,
        payload: %{depth: depth, path: [index | path]}
      }
    end
    |> Messaging.broadcast_messages!(to_queue: Messaging.global_queue())
  end

  @impl Messaging.MessageHandler
  def handle_message(%Messaging.Message{
        type: "ExampleUsage.Events.Greeting",
        payload: %{"greeting" => greeting}
      }) do
    Logger.info("ExampleUsage: received greeting: #{greeting}")
  end

  def handle_message(%Messaging.Message{
        type: "ExampleUsage.Events.Cascade",
        payload: %{"depth" => depth, "path" => path}
      }) do
    if depth < 4 do
      send_cascade_at_depth!(depth + 1, path)
    else
      :ok
    end
  end
end
