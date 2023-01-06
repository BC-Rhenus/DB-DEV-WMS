CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OHR_BIUR_01" 
  before insert or update of status, tod on dcsdba.order_header
  for each row
    WHEN ( nvl(new.tod,'ZZZ') = 'ZZZ') declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to make TOD "required"
**********************************************************************************
* $Log: $
**********************************************************************************/

begin
  if inserting
  then
    :new.tod    := 'ZZZ';
  end if;
  --
  if updating
  then
    :new.tod    := 'ZZZ';
    if :new.status = 'Released'
    then
      :new.status := 'Hold';
    end if;
  end if;
exception
  when others
  then
    null;  -- In after Row trigger no raise is allowed
end cnl_ohr_biur_01;