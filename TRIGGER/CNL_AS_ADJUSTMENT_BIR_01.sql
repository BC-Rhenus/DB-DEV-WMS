CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_ADJUSTMENT_BIR_01" 
  BEFORE INSERT ON  CNL_SYS.CNL_AS_ADJUSTMENT
 FOR EACH ROW
     WHEN (new.ADJUSTMENT_KEY IS NULL) BEGIN
  :NEW.ADJUSTMENT_KEY := CNL_AS_ADJUSTMENT_SEQ1.NEXTVAL;
 END;