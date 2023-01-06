CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_BARCODE" ("CLIENT_ID", "SKU_ID", "PRODUCT_BARCODE_KEY", "CLASS_TYPE", "DESCRIPTION", "PRODUCT_BARCODE_ID", "PRODUCT_UOM_KEY", "PRODUCT_KEY") AS 
  select  P.OWNER_ID as client_id
,       P.PRODUCT_ID as SKU_ID
,       B.PRODUCT_BARCODE_KEY
,       B.CLASS_TYPE
,       B.DESCRIPTION
,       B.PRODUCT_BARCODE_ID
,       B.PRODUCT_UOM_KEY
,       B.PRODUCT_KEY
from    rhenus_synq.host_barcode@as_synq B
,       rhenus_synq.host_product@as_synq P
where   P.product_key = B.product_key
order by product_barcode_key desc