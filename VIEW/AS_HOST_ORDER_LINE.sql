CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_ORDER_LINE" ("ORDER_ID", "ORDER_LINE_KEY", "ALLOCATION_TOLERANCE_WINDOW", "CATEGORY", "EXCLUDED_FROM_ALLOCATION", "EXPIRATION_WINDOW", "INVENTORY_SORTING", "MAX_UOM", "MIN_UOM", "ORDER_LINE_NUMBER", "PRODUCT_ID", "QUANTITY", "RELEVANT_DATE_FOR_ALLOCATION", "UOM_TREE", "ORDER_KEY") AS 
  select  O.ORDER_ID,L.ORDER_LINE_KEY,L.ALLOCATION_TOLERANCE_WINDOW,L.CATEGORY,L.EXCLUDED_FROM_ALLOCATION,L.EXPIRATION_WINDOW,L.INVENTORY_SORTING,L.MAX_UOM,MIN_UOM,L.ORDER_LINE_NUMBER,L.PRODUCT_ID,L.QUANTITY,L.RELEVANT_DATE_FOR_ALLOCATION,L.UOM_TREE,L.ORDER_KEY
from    rhenus_synq.host_order_line@as_synq L
,       rhenus_synq.host_order_header@as_synq O
where   L.ORDER_KEY = O.ORDER_KEY
order by order_line_key desc