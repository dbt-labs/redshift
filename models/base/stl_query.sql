select

  userid as user_id
, query as query_id
, xid as transaction_id
, label
, pid
, database
, starttime as started_at
, endtime as finished_at
, aborted

from stl_query
