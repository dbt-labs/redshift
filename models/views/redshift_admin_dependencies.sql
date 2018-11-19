{# SOURCE: https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_view_dependency.sql #}

select distinct
  srcobj.oid as source_oid
  , srcnsp.nspname as source_schemaname
  , srcobj.relname as source_objectname
  , tgtobj.oid as dependent_oid
  , tgtnsp.nspname as dependent_schemaname
  , tgtobj.relname as dependent_objectname

from

  {{ ref('pg_class') }} as srcobj
  join {{ ref('pg_depend') }} as srcdep on srcobj.oid = srcdep.refobjid
  join {{ ref('pg_depend') }} as tgtdep on srcdep.objid = tgtdep.objid
  join {{ ref('pg_class') }} as tgtobj
    on tgtdep.refobjid = tgtobj.oid
    and srcobj.oid <> tgtobj.oid
  left join {{ ref('pg_namespace') }} as srcnsp
    on srcobj.relnamespace = srcnsp.oid
  left join {{ ref('pg_namespace') }} tgtnsp on tgtobj.relnamespace = tgtnsp.oid

where
  tgtdep.deptype = 'i' --dependency_internal
  and tgtobj.relkind = 'v' --i=index, v=view, s=sequence
