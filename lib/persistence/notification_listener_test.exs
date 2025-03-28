defmodule PostgresqlMessageBroker.Persistence.NotificationListenerTest do
  @moduledoc false

  alias PostgresqlMessageBroker.Persistence.NotificationListener
  alias PostgresqlMessageBroker.Persistence.NotificationListener.Notification
  alias PostgresqlMessageBroker.Persistence.Repo

  use PostgresqlMessageBroker.Tests.DataCase, async: false
  @moduletag use_ecto_sandbox: false

  describe "with NotificationListener" do
    setup do
      listener_pid = start_link_supervised!({NotificationListener, repo: Repo})

      [
        listener_pid: listener_pid
      ]
    end

    test "listen", %{} = context do
      assert :ok = NotificationListener.listen(context.listener_pid, "my_channel")
      Repo.query!("NOTIFY my_channel")
      assert_receive %Notification{channel: "my_channel", payload: ""}
      refute_receive _
    end

    test "listening twice causes two notifications to be received", %{} = context do
      assert :ok = NotificationListener.listen(context.listener_pid, "my_channel")
      assert :ok = NotificationListener.listen(context.listener_pid, "my_channel")
      Repo.query!("NOTIFY my_channel")
      assert_receive %Notification{channel: "my_channel", payload: ""}
      assert_receive %Notification{channel: "my_channel", payload: ""}
      refute_receive _
    end

    test "two process each receive a notification", %{} = context do
      receive_one_notification = fn ->
        assert :ok = NotificationListener.listen(context.listener_pid, "my_channel")
        assert_receive %Notification{channel: "my_channel", payload: ""}
        refute_receive _
        :ok
      end

      task_1 = Task.async(receive_one_notification)
      task_2 = Task.async(receive_one_notification)

      Repo.query!("NOTIFY my_channel")

      assert :ok = Task.await(task_1)
      assert :ok = Task.await(task_2)

      refute_receive _
    end

    test "caller exit cleans up state", %{} = context do
      assert :ok =
               Task.async(fn ->
                 NotificationListener.listen(context.listener_pid, "my_channel")
               end)
               |> Task.await()

      state = %NotificationListener.State{} = :sys.get_state(context.listener_pid)
      assert Enum.empty?(state.channel_subscriptions)
    end

    test "killing the db connection process should bring down the server", %{
      listener_pid: listener_pid
    } do
      Process.flag(:trap_exit, true)

      %NotificationListener.State{notifications_pid: notifications_pid} =
        :sys.get_state(listener_pid)

      Process.exit(notifications_pid, :kill)
      assert_receive {:EXIT, ^listener_pid, _reason}
    end
  end

  test "server exit kills listening process" do
    listener_pid = start_supervised!({NotificationListener, repo: Repo})

    task =
      Task.async(fn ->
        Process.flag(:trap_exit, true)
        NotificationListener.listen(listener_pid, "my_channel")
        assert_receive {:EXIT, ^listener_pid, :shutdown}
        :ok
      end)

    stop_supervised!(NotificationListener)

    assert :ok = Task.await(task)
  end
end
