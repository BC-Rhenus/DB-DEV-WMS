CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OHR_AIR_01" 
  AFTER INSERT ON "DCSDBA"."ORDER_HEADER"
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
		into	cnl_sys.cnl_wms_order_header_extend(order_id, client_id)
		values	(:new.order_id, :new.client_id);
	end if;
exception
	when   others
	then
		null;  -- In before Row trigger no raise is allowed
end cnl_ohr_air_01;