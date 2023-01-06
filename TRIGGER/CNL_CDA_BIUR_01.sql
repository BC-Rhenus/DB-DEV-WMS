CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_CDA_BIUR_01" 
BEFORE INSERT OR UPDATE
ON CNL_SYS.CNL_CONTAINER_DATA
FOR EACH ROW
declare
/**********************************************************************************
 * $Archive: $
 * $Revision: $
 * $Author: $
 * $Date: $
 **********************************************************************************
 * Description: trigger for ID and audit columns in table CNL_CONTAINER_DATA
 **********************************************************************************
 * $Log: $
 *********************************************************************************/
begin
   if inserting
   then
      --************************************************************************
      -- Section: Audit columns when Inserting
      --***********************************************************************
      :new.created_by       := user;
      :new.creation_date    := sysdate;
      :new.last_updated_by  := user;
      :new.last_update_date := sysdate;
   end if;

   if updating
   then
      --************************************************************************
      -- Section: Audit columns when Updating
      --***********************************************************************
      :new.last_updated_by  := user;
      :new.last_update_date := sysdate;
   end if;

end CNL_CDA_BIUR_01;