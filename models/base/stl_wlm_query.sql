select

  userid as user_id
, query as query_id
, xid
, task
, service_class
, slot_count
, service_class_start_time
, queue_start_time
, queue_end_time
, total_queue_time
, exec_start_time
, exec_end_time
, total_exec_time
, service_class_end_time
, final_state

from stl_wlm_query
