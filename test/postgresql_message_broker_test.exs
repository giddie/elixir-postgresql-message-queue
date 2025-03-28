defmodule PostgresqlMessageBrokerTest do
  use ExUnit.Case
  doctest PostgresqlMessageBroker

  test "greets the world" do
    assert PostgresqlMessageBroker.hello() == :world
  end
end
