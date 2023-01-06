CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_PAR_BIR_01" 
  before insert on dcsdba.pre_advice_header
  for each row
declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to insert UPLOADED_WS2PC_ID into PRE_ADVICE_HEADER
**********************************************************************************
* $Log: $
**********************************************************************************/
begin
   :new.uploaded_ws2pc_id := cnl_external_id_seq1.nextval;
exception
   when   others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_par_bir_01;