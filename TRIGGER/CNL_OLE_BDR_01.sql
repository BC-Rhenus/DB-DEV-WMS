CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OLE_BDR_01" 
  BEFORE DELETE ON "DCSDBA"."ORDER_LINE"
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
		delete	cnl_sys.cnl_wms_order_line_extend
		where	order_id 	= :old.order_id
		and	client_id	= :old.client_id
		and	line_id		= :old.line_id
		;
	end if;
exception
	when   others
	then
		null;  -- In before Row trigger no raise is allowed
end CNL_OLE_BDR_01;