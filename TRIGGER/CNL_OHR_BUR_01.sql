CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OHR_BUR_01" 
  before update of uploaded_ws2pc_id on dcsdba.order_header
  for each row
    WHEN (nvl(new.uploaded_ws2pc_id,-1) <> nvl(old.uploaded_ws2pc_id,-1)) declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to force to keep the original UPLOADED_WS2PC_ID
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
		   if :old.uploaded_ws2pc_id is null
		   then
			  :new.uploaded_ws2pc_id := cnl_external_id_seq1.nextval;
		   else
			  :new.uploaded_ws2pc_id := :old.uploaded_ws2pc_id;
		   end if;
	end if;
exception
   when others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_ohr_bur_01;