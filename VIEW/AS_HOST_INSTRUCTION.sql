CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_INSTRUCTION" ("CLIENT_ID", "SKU_ID", "INSTRUCTION_KEY", "CLASS_TYPE", "INSTRUCTION_TEXT", "ROLE", "PRODUCT_KEY", "ORDER_KEY", "ORDER_LINE_KEY") AS 
  select  P.OWNER_ID as CLIENT_ID
,       P.PRODUCT_ID as SKU_ID
,       I.INSTRUCTION_KEY
,       I.CLASS_TYPE
,       I.INSTRUCTION_TEXT
,       I.ROLE
,       I.PRODUCT_KEY
,       I.ORDER_KEY
,       I.ORDER_LINE_KEY
from    rhenus_synq.host_instruction@as_synq I
,       rhenus_synq.host_product@as_synq P
where   P.product_key = I.product_key
order by instruction_key desc