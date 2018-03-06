select
  schemaname as schema_name
, tablename as table_name
, tableowner as table_owner
, tablespace as table_space
, hasindexes as has_indexes
, hasrules as has_rules
, hastriggers as has_triggers
from pg_tables