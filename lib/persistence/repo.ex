defmodule PostgresqlMessageQueue.Persistence.Repo do
  use Ecto.Repo,
    otp_app: :postgresql_message_queue,
    adapter: Ecto.Adapters.Postgres
end
