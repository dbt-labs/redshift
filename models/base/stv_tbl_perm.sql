select
  slice
, id -- table id
, name -- table name
, rows
, sorted_rows
, (rows - sorted_rows) as unsorted_rows
, temp
, db_id
, backup
from pg_catalog.stv_tbl_perm
