select
  attrelid
, attname
, atttypid
, attstattarget
, attlen
, attnum
, attndims
, attcacheoff
, atttypmod
, attbyval
, attstorage
, attalign
, attnotnull
, atthasdef
, attisdropped
, attislocal
, attinhcount
, attisdistkey
, attispreloaded
, attsortkeyord
, attencodingtype
, attencrypttype
, (case attisdistkey
        when 't' then attname
        else null end) as dist_key
, (case attsortkeyord
        when 1 then attname
        else null end) as sort_key
from pg_attribute
