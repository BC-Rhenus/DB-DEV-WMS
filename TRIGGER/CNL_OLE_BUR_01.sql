CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OLE_BUR_01" 
  BEFORE UPDATE ON "DCSDBA"."ORDER_LINE"
  FOR EACH ROW
  WHEN (old.sku_id != new.sku_id or old.qty_ordered != new.qty_ordered) declare
	l_database	varchar2(10);
begin
	select 	name 
	into 	l_database
	from 	v$database
	;
	if	l_database in ('DEVCNLJW','TSTCNLJW')
	then
		update	cnl_sys.cnl_wms_order_line_extend
		set	sku_id 		= :new.sku_id
		, 	qty_ordered	= :new.qty_ordered
		where	order_id	= :new.order_id
		and	client_id	= :new.client_id
		and	line_id		= :new.line_id
		;
	end if;
exception
	when   others
	then
		null;  -- In before Row trigger no raise is allowed
end cnl_ole_bur_01;