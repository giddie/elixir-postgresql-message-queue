alias PostgresqlMessageBroker.Tests.DatabaseHelpers
alias PostgresqlMessageBroker.Persistence.Repo

ExUnit.start(capture_log: [level: :warning])
DatabaseHelpers.apply_default_ecto_sandbox_mode(Repo)
