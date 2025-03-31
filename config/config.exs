import Config

config :logger, level: :info

config :postgresql_message_queue, ecto_repos: [PostgresqlMessageQueue.Persistence.Repo]

config :postgresql_message_queue, PostgresqlMessageQueue.Messaging,
  broadcast_listeners: [
    {PostgresqlMessageQueue.ExampleUsage, ["ExampleUsage.Events.*"]}
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
