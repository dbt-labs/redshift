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
from stv_tbl_perm
