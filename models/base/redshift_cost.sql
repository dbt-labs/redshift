with stl_explain as (

  select query_id, plannode from {{ref('stl_explain')}}
  where nodeid = 1

), parse_step_one as (

  -- plannode (which contains cost) is formatted like:
  --   XN Seq Scan on nyc_last_update  (cost=0.00..0.03 rows=2 width=40)
  -- we want to rip out the cost part (0.00, 0.03) and make it usable.
  -- cost_string after this step is "0.00..0.03 ..."
  select
    query_id
  , split_part(plannode, 'cost=', 2) as cost_string

  from stl_explain

), parse_step_two as (

  select
    query_id
  , split_part(cost_string, '..', 1) as starting_cost
  , substring(
      split_part(cost_string, '..', 2)
      from 1
      for strpos(split_part(cost_string, '..', 2), ' ')) as total_cost

  from parse_step_one

)


select

  query_id
, starting_cost::float as starting_cost
, total_cost::float as total_cost

from parse_step_two
