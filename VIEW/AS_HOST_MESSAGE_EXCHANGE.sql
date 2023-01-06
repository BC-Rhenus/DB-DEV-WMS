CREATE OR REPLACE FORCE VIEW "CNL_SYS"."AS_HOST_MESSAGE_EXCHANGE" ("MESSAGE_KEY", "TABLE_KEY", "CREATE_DATE", "MESSAGE_TYPE", "ORDER_ID", "SKU_ID", "TAG_ID", "MESSAGE_STATUS", "ERROR_TEXT", "MESSAGE_ID", "ERROR_CODE", "SENDER", "RECEIVER", "TRANS_CODE") AS 
  select  h.host_message_key        as message_key
,       h.host_message_table_key  as table_key
,       h.create_date
,       h.message_type
,       (   select  order_id
            from    (   select  order_key key
                        ,       order_id 
                        ,       'OrderMaster' type 
                        from    rhenus_synq.host_order_header@as_synq
                        union
                        select  order_status_change_key key
                        ,       order_id
                        ,       'OrderStatusChangeNotification' type 
                        from    rhenus_synq.host_order_status_change@as_synq
                        union
                        select  cubing_result_key key
                        ,       order_id
                        ,       'CubingResult' type 
                        from    rhenus_synq.host_order_tu@as_synq 
                        where   cubing_result_key is not null
                        union
                        select  order_tu_key key
                        ,       order_id
                        ,       'Manual' type 
                        from    rhenus_synq.host_order_tu@as_synq 
                        where   order_tu_key is not null
                        union
                        select  order_tu_pick_key key
                        ,       order_id
                        ,       'OrderTuPickConfirmation' type 
                        from    rhenus_synq.host_order_tu_pick@as_synq
                    ) orders
            where   orders.key = h.host_message_table_key
            and     (   orders.type = h.message_type or 
                        (   h.message_type in ('ManualCartonPicked','ManualOrderStart') and 
                            orders.type = 'Manual'
                        )
                    )
            and     rownum = 1
        )   order_id
,       (   select  product_id 
            from    rhenus_synq.host_product@as_synq 
            where   product_key = h.host_message_table_key 
            and     h.message_type = 'ProductMaster'
        )   sku_id
,       (   select  tag_id
            from    (   select  asn_key     as key
                        ,       tu_id       as tag_id
                        ,       'ASN'       as type
                        from    rhenus_synq.host_asn@as_synq
                        union
                        select  inventory_status_key as key
                        ,       asn_tu_id   as tag_id
                        ,       'InventoryStatus' as type
                        from    rhenus_synq.host_load_unit@as_synq
                        where   asn_tu_id is not null
                    ) asn_id
            where   asn_id.key  = h.host_message_table_key
            and     (   asn_id.type = h.message_type or
                        (   h.message_type in ('AsnReceivingNotification','AsnCheckInConfirmation') and
                            asn_id.type = 'ASN'
                        )
                    )
        ) tag_id
,       h.message_status
,       h.error_text
,       h.message_id
,       h.error_code
,       h.sender
,       h.receiver
,       h.trans_code
from    rhenus_synq.host_message_exchange@as_synq   h
order by message_key desc