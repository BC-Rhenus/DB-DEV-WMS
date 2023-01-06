CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_PAR_BUR_01" 
  before update of uploaded_ws2pc_id on dcsdba.pre_advice_header
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
begin
   if :old.uploaded_ws2pc_id is null
   then
      :new.uploaded_ws2pc_id := cnl_external_id_seq1.nextval;
   else
      :new.uploaded_ws2pc_id := :old.uploaded_ws2pc_id;
   end if;
exception
   when others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_par_bur_01;