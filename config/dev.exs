import Config

config :postgresql_message_queue, PostgresqlMessageQueue.Persistence.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "postgresql_message_queue",
  pool_size: 50
