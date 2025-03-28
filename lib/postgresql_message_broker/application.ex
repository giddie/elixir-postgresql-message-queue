defmodule PostgresqlMessageBroker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      PostgresqlMessageBroker.Persistence.Repo,
      {PostgresqlMessageBroker.Persistence.NotificationListener,
       name: PostgresqlMessageBroker.Persistence.Repo.NotificationListener,
       repo: PostgresqlMessageBroker.Persistence.Repo},
      PostgresqlMessageBroker.Messaging.OutboxWatcher,
      outbox_processor_spec()
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PostgresqlMessageBroker.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp outbox_processor_spec() do
    backoff_ms = fn attempt when is_integer(attempt) ->
      base = 2 ** (attempt - 1) * 5 - 5
      jitter = Enum.random(-base..base) |> Integer.floor_div(20)
      base + jitter
    end

    {PostgresqlMessageBroker.Messaging.OutboxProcessor,
     queue: PostgresqlMessageBroker.Messaging.global_queue(),
     concurrency: 5,
     backoff_ms: backoff_ms}
  end
end
