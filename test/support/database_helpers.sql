
-- name: table_names
select array_agg(tablename) as table_names
  from pg_catalog.pg_tables
 where schemaname = :schema
   and tablename not in ('schema_migrations')
