defmodule PostgresqlMessageBroker.Persistence.Repo.Migrations.CreateOutboxMessages do
  use Ecto.Migration

  def change do
    create table("outbox_messages") do
      add :queue, :text, null: false
      add :type, :text, null: false
      add :schema_version, :integer, null: false
      add :payload, :map, null: false
      add :metadata, :map, null: false
      timestamps(updated_at: false, type: :utc_datetime_usec)
      add :processable_after, :utc_datetime_usec
    end

    create index("outbox_messages", [:queue, :processable_after])

    execute(
      """
      create function notify_outbox_messages_inserted() returns trigger as
      $$
        begin
          perform pg_notify('outbox_messages_inserted', new.queue);
          return null;
        end
      $$
      language plpgsql
      """,
      """
      drop function notify_outbox_messages_inserted()
      """
    )

    execute(
      """
      create trigger notify_inserted after insert on outbox_messages
      for each row
      execute function notify_outbox_messages_inserted()
      """,
      """
      drop trigger notify_inserted on outbox_messages
      """
    )
  end
end
