defmodule PostgresqlMessageQueue.Tests.DatabaseHelpers do
  @moduledoc false

  alias PostgresqlMessageQueue.Persistence.Repo

  require AyeSQL

  AyeSQL.defqueries(Queries, "database_helpers.sql", repo: Repo)

  @spec apply_default_ecto_sandbox_mode(module()) :: :ok
  def apply_default_ecto_sandbox_mode(Repo) do
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)
  end

  @spec wipe_database!(module()) :: :ok
  def wipe_database!(Repo) do
    {:ok, [%{table_names: table_names}]} = Queries.table_names(schema: "public")
    Repo.query!("truncate #{Enum.join(table_names, ", ")}")

    :ok
  end
end
