CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_MAAS_LOGGING_BIR_01" 
  BEFORE INSERT ON  CNL_SYS.CNL_AS_MAAS_LOGGING
 FOR EACH ROW
    WHEN (new.KEY IS NULL) BEGIN
  :NEW.KEY := CNL_AS_MAAS_LOGGING_SEQ1.NEXTVAL;
 END;