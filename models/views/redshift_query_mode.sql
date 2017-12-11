with extract_query_header as (
select
  query,
  nullif(substring(replace(query_text,' ',''),position('--{"' in replace(query_text,' ',''))+2,position('}' in replace(query_text,' ',''))),'') as query_header
from (
select
  query,
  listagg(text,'') within group (order by sequence) as query_text
from (
select *
from (
select
  qt.*,
  row_number() over (partition by qt.query order by qt.sequence desc) as row_number_reverse
from stl_querytext qt
join {{ ref('pg_user') }} pu
on qt.userid = pu.user_id
where pu.username = 'mode_readonly' --Only grab queries run by mode_readonly user
)
where row_number_reverse <= 2 --Only grab last two rows of querytext (this is where the query header will be always be)
)
group by query
)
),

query_header_info_segment as (
select
  query as query_id,
  query_header,
  json_extract_path_text(query_header,'user') as mode_report_run_username,
  substring(json_extract_path_text(query_header,'url'),0,position('/runs/' in json_extract_path_text(query_header,'url'))) as mode_report_url,
  substring(json_extract_path_text(query_header,'url'),0,position('/queries/' in json_extract_path_text(query_header,'url'))) as mode_report_run_url,
  json_extract_path_text(query_header,'url') as mode_query_run_url,
  case
    when json_extract_path_text(query_header,'scheduled') = 'true' then true
    else false
  end as mode_report_run_is_scheduled
from (
select
  *,
  case
    when f_json_ok(query_header) then true --Checks if query_header is JSON formatted
    else false
  end as query_header_is_json
from extract_query_header
)
where query_header_is_json is true
)--,

--output as (
select
  q.*,
  qhis.mode_report_run_username,
  qhis.mode_report_url,
  qhis.mode_report_run_url,
  qhis.mode_query_run_url,
  qhis.mode_report_run_is_scheduled
from {{ ref('redshift_queries') }} q
left join query_header_info_segment qhis
on q.query_id = qhis.query_id;
