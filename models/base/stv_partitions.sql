select

  owner
, host
, diskno
, part_begin
, part_end
, used
, tossed
, capacity
, "reads"
, writes
, seek_forward
, seek_back
, is_san
, failed
, mbps
, mount

from pg_catalog.stv_partitions
