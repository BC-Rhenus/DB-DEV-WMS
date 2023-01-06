CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OHR_BIR_01" 
  before insert on dcsdba.order_header
  for each row
declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to insert UPLOADED_WS2PC_ID into ORDER_HEADER (Only Centiro on premise)
**********************************************************************************
* $Log: $
**********************************************************************************/
	cursor c_saas(b_client_id varchar2)
	is
		select	count(*)
		from	dcsdba.client_group_clients
		where	client_group = 'CTOSAAS'
		and	client_id = b_client_id
	;
	l_saas	integer;
begin
	-- New Saas solution does not require this trigger. Therefor if client exists in client group CTOSAAS this trigger can be skipped.
	open 	c_saas(:new.client_id);
	fetch	c_saas
	into	l_saas;
	close	c_saas;
	if 	l_saas = 0
	then
		:new.uploaded_ws2pc_id := cnl_external_id_seq1.nextval;
	end if;
exception
   when   others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_ohr_bir_01;