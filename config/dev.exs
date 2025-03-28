import Config

config :postgresql_message_broker, PostgresqlMessageBroker.Persistence.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "postgresql_message_broker",
  pool_size: 50
