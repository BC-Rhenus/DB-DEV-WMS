CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_VAS_CODES_BIR_01" 
    before insert on cnl_sys.cnl_as_vas_codes
    for each row
      WHEN (new.id is null) begin
    :new.id := cnl_sys.cnl_as_vas_codes_seq1.nextval;
end;