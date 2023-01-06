CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_ASN_LINE" ("CLIENT_ID", "TAG_ID", "SKU_ID", "ASN_LINE_KEY", "ASN_LINE_NUMBER", "PRODUCT_UOM", "PRODUCT_UOM_TREE", "QUANTITY_EXPECTED", "QUANTITY_RECEIVED", "TU_ID", "ASN_KEY") AS 
  select  h.owner_id as client_id
,       h.tu_id as tag_id
,       l.product_id as sku_id
,       l.asn_line_key
,       l.asn_line_number
,       l.product_uom
,       l.product_uom_tree
,       l.quantity_expected
,       l.quantity_received
,       l.tu_id
,       l.asn_key
from    rhenus_synq.host_asn_line@as_synq l
,       rhenus_synq.host_asn@as_synq h
where   h.asn_key = l.asn_key
order by asn_line_key desc