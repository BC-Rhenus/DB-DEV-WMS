CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OLE_AIR_01" 
  AFTER INSERT ON "DCSDBA"."ORDER_LINE"
  FOR EACH ROW
  declare
	l_database	varchar2(10);
begin
	select 	name 
	into 	l_database
	from 	v$database
	;
	if	l_database in ('DEVCNLJW','TSTCNLJW')
	then
		insert
		into	cnl_sys.cnl_wms_order_line_extend(order_id, client_id, line_id, sku_id, qty_ordered)
		values	(:new.order_id, :new.client_id, :new.line_id, :new.sku_id, :new.qty_ordered);
	end if;
exception
	when   others
	then
		null;  -- In before Row trigger no raise is allowed
end cnl_ole_air_01;