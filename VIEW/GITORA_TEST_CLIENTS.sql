CREATE OR REPLACE FORCE VIEW "CNL_SYS"."GITORA_TEST_CLIENTS" ("CLIENT_ID") AS 
  SELECT
      client_id
   FROM
      cnl_active_clients