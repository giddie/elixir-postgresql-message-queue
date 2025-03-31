defmodule PostgresqlMessageQueue.Tests.DataCase do
  @moduledoc """
  This module defines the setup for tests requiring
  access to the application's data layer.

  You may define functions here to be used as helpers in
  your tests.

  Finally, if the test case interacts with the database,
  we enable the SQL sandbox, so changes done to the database
  are reverted at the end of every test. If you are using
  PostgreSQL, you can even run database tests asynchronously
  by setting `use PostgresqlMessageQueue.DataCase, async: true`, although
  this option is not recommended for other databases.
  """

  alias PostgresqlMessageQueue.Tests.DatabaseHelpers

  use ExUnit.CaseTemplate

  using do
    quote do
      alias PostgresqlMessageQueue.Persistence.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import PostgresqlMessageQueue.Tests.DataCase
    end
  end

  setup tags do
    PostgresqlMessageQueue.Tests.DataCase.setup_sandbox(tags)
    :ok
  end

  @doc """
  Sets up the sandbox based on the test tags.
  """
  def setup_sandbox(tags) do
    repo = PostgresqlMessageQueue.Persistence.Repo

    if Map.get(tags, :use_ecto_sandbox, true) do
      pid = Ecto.Adapters.SQL.Sandbox.start_owner!(repo, shared: not tags[:async])
      on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
    else
      if Map.get(tags, :async, false) do
        raise "Async testing isn't possible if we're not using the Ecto sandbox."
      end

      Ecto.Adapters.SQL.Sandbox.mode(repo, :auto)

      on_exit(fn ->
        DatabaseHelpers.wipe_database!(repo)
        DatabaseHelpers.apply_default_ecto_sandbox_mode(repo)
      end)
    end

    :ok
  end
end
