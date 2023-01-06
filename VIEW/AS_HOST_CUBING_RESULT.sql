CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_CUBING_RESULT" ("CUBING_RESULT_KEY") AS 
  select  CUBING_RESULT_KEY
from    rhenus_synq.host_cubing_result@as_synq
order by cubing_result_key desc