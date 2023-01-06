CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_ORDER_TU_ORDERLINE" ("CLIENT_ID", "ORDER_ID", "ORDER_TU_KEY", "ORDER_LINE_KEY", "ORDER_LINE_NUMER", "SKU_ID", "QUANTITY") AS 
  select  O.OWNER_ID as CLIENT_ID, O.ORDER_ID, T.ORDER_TU_KEY,T.ORDER_LINE_KEY,L.ORDER_LINE_NUMBER, L.PRODUCT_ID as SKU_ID, L.QUANTITY
from    rhenus_synq.host_order_tu_orderline@as_synq T
,       rhenus_synq.host_order_line@as_synq L
,       rhenus_synq.host_order_tu@as_synq O
where   O.order_tu_key = t.order_tu_key
and     L.order_line_key = T.order_line_key
order by order_tu_key desc