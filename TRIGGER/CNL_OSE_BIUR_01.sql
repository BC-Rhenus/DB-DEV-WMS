CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OSE_BIUR_01" 
  BEFORE INSERT OR UPDATE ON "CNL_SYS"."CNL_ORDER_SEQUENCES"
  REFERENCING FOR EACH ROW
  declare
/**********************************************************************************
 * $Archive: $
 * $Revision: $
 * $Author: $
 * $Date: $
 **********************************************************************************
 * Description: trigger for ID and audit columns in table CNL_ORDER_SEQUENCES
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
         select   cnl_ose_seq1.nextval
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
      :new.concurrency_key  := SYS_GUID();
   end if;

   if updating
   then
      --************************************************************************
      -- Section: Audit columns when Updating
      --***********************************************************************
      :new.last_updated_by  := user;
      :new.last_update_date := sysdate;
      :new.concurrency_key  := SYS_GUID();
   end if;

end CNL_OSE_BIUR_01;