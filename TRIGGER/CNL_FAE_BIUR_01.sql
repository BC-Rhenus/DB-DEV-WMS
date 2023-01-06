CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_FAE_BIUR_01" 
BEFORE INSERT OR UPDATE
ON  CNL_SYS.CNL_FILES_ARCHIVE
FOR EACH ROW
declare
/**********************************************************************************
 * $Archive: $
 * $Revision: $
 * $Author: $
 * $Date: $
 **********************************************************************************
 * Description: trigger for ID and audit columns in table AUT_COMMUNICATIONS
 **********************************************************************************
 * $Log: $
 *********************************************************************************/
begin
   if inserting
   then
      --****************************************************************************
      -- Section: ID when Inserting
      --**************************************************************************
      if ( :new.id is null )
      then
         select   cnl_fae_seq1.nextval
         into     :new.id
         from     dual;
      end if;
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

end CNL_FAE_BIUR_01;