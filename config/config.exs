import Config

config :logger, level: :warning

config :postgresql_message_broker, ecto_repos: [PostgresqlMessageBroker.Persistence.Repo]

config :postgresql_message_broker, PostgresqlMessageBroker.Persistence.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "cqrs_example_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10
