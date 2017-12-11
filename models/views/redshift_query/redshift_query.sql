with queries as (

  select * from {{ref('stl_query')}}

), users as (

  select * from {{ref('pg_user')}}

), cost as (

  select * from {{ref('redshift_cost')}}

), timings as (

  select * from {{ref('stl_wlm_query')}}

)



select

  queries.query_id
, queries.transaction_id
, users.username::varchar

, cost.starting_cost
, cost.total_cost

, queries.started_at
, queries.finished_at

, timings.queue_start_time
, timings.queue_end_time
, (timings.total_queue_time::float / 1000000.0) as total_queue_time_seconds

, timings.exec_start_time
, timings.exec_end_time
, (timings.total_exec_time::float / 1000000.0) as total_exec_time_seconds

from queries

left join users
  on queries.user_id = users.user_id

left join cost
  on queries.query_id = cost.query_id

left join timings
  on queries.query_id = timings.query_id
