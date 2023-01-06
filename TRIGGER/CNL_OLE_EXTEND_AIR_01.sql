CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OLE_EXTEND_AIR_01" 
	after	insert 
	on 	cnl_sys.cnl_wms_order_line_extend
	for 	each row
declare
begin
	update	cnl_sys.cnl_wms_order_header_extend o
	set	o.contains_hazmat 		= decode(o.contains_hazmat		,'N',:new.contains_hazmat		,o.contains_hazmat)
	,	o.contains_ugly_sku		= decode(o.contains_ugly_sku		,'N',:new.contains_ugly_sku		,o.contains_ugly_sku)
	,	o.contains_kit			= decode(o.contains_kit			,'N',:new.contains_kit			,o.contains_kit)
	,	o.contains_awkward_sku		= decode(o.contains_awkward_sku		,'N',:new.contains_awkward_sku		,o.contains_awkward_sku)
	,	o.contains_two_man_lift		= decode(o.contains_two_man_lift	,'N',:new.contains_two_man_lift		,o.contains_two_man_lift)
	,	o.contains_conveyable_sku	= decode(o.contains_conveyable_sku	,'N',:new.contains_conveyable_sku	,o.contains_conveyable_sku)
	,	o.total_qty_ordered		= nvl(o.total_qty_ordered,0) + :new.qty_ordered
	where	o.client_id 			= :new.client_id
	and	o.order_id			= :new.order_id
	;
end cnl_ole_extend_air_01;