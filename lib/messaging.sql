-- name: peek_at_outbox_messages
  select queue,
         type,
         schema_version,
         payload,
         metadata
    from outbox_messages
order by id
   limit :limit
  :_lock

-- name: for_update_skip_locked
for update skip locked

-- name: get_and_delete_outbox_batch
  with deleted
    as (
           delete
             from outbox_messages
            where id in (
                           select id
                             from outbox_messages
                            where queue = :queue
                              and (
                                       processable_after is null
                                    or processable_after < :processing_datetime
                                  )
                         order by id
                            limit :limit
                              for update skip locked
                        )
        returning id,
                  type,
                  schema_version,
                  payload,
                  metadata,
                  processable_after
       )
select *
  from deleted
 order by id
