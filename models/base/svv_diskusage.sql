select
  db_id
, name
, slice
, col
, tbl
, blocknum
, num_values
, extended_limits
, minvalue
, maxvalue
, sb_pos
, pinned
, on_disk
, backed_up
, modified
, hdr_modified
, unsorted
, tombstone
, preferred_diskno
, temporary
, newblock
, num_readers
, id
, flags
from svv_diskusage
