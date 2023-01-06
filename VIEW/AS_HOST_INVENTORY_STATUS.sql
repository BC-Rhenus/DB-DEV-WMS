CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_INVENTORY_STATUS" ("INVENTORY_STATUS_KEY", "LOCATION_ID", "TU_ID") AS 
  select  INVENTORY_STATUS_KEY,LOCATION_ID,TU_ID
from    rhenus_synq.host_inventory_status@as_synq
order by inventory_status_key desc