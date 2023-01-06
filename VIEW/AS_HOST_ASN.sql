CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_ASN" ("TAG_ID", "CLIENT_ID", "ASN_KEY", "ASN_TYPE", "KEEP_TU", "TU_TYPE") AS 
  select  tu_id as tag_id
,       owner_id as client_id
,       asn_key
,       asn_type
,       keep_tu
,       tu_type
from    rhenus_synq.host_asn@as_synq
order by asn_key desc