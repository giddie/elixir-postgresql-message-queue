defmodule PostgresqlMessageBroker.Persistence.Repo do
  use Ecto.Repo,
    otp_app: :postgresql_message_broker,
    adapter: Ecto.Adapters.Postgres
end
