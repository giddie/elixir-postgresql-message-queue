alias PostgresqlMessageQueue.Tests.DatabaseHelpers
alias PostgresqlMessageQueue.Persistence.Repo

ExUnit.start(capture_log: [level: :warning])
DatabaseHelpers.apply_default_ecto_sandbox_mode(Repo)
