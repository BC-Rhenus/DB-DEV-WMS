CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_CAR_BIU_01" 
  before insert or update on dcsdba.carriers
  for each row
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to process the Run Task for Centiro, StreamServe
**********************************************************************************
* $Log: $
**********************************************************************************/
begin
	:new.user_def_note_2 := substr(:new.carrier_id||'_'||:new.service_level,1,30);
exception
   when   others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_car_biu_01;