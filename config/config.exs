import Config

config :logger, level: :info

config :postgresql_message_broker, ecto_repos: [PostgresqlMessageBroker.Persistence.Repo]

config :postgresql_message_broker, PostgresqlMessageBroker.Messaging,
  broadcast_listeners: [
    {PostgresqlMessageBroker.ExampleUsage, ["ExampleUsage.Events.*"]}
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
