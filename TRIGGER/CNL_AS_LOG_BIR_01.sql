CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_LOG_BIR_01" 
  BEFORE INSERT ON  CNL_SYS.CNL_AS_LOG
 FOR EACH ROW
    WHEN (new.LOG_KEY IS NULL) BEGIN
  :NEW.LOG_KEY := CNL_AS_LOG_SEQ1.NEXTVAL;
 END;