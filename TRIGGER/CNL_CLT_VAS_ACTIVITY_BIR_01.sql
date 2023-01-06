CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_CLT_VAS_ACTIVITY_BIR_01" BEFORE
   INSERT ON cnl_sys.cnl_client_vas_activity
   FOR EACH ROW
BEGIN
      IF ( :new.id IS NULL ) THEN
         SELECT cnl_sys.cnl_clt_vas_activity_seq1.nextval
                  INTO :new.id
         FROM
            dual;
   END IF; 
END;