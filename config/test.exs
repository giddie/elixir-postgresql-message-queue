import Config

config :logger, level: :warning

config :postgresql_message_broker, PostgresqlMessageBroker.Persistence.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "postgresql_message_broker_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10
