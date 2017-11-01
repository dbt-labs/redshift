select
    table_schema as schemaname,
    table_name as tablename

from information_schema.tables
where table_type = 'BASE TABLE'
  and schemaname not ilike 'pg_%'
  and schemaname != 'information_schema'
