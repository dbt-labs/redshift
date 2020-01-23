select

  userid as user_id
, query as query_id
, nodeid
, parentid
, plannode
, info

from pg_catalog.stl_explain
