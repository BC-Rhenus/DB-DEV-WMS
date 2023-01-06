CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_WHH_ERR_LOG_BIR_01" 
    before insert on cnl_sys.cnl_whh_error_log
    for each row
begin
    :new.err_log_key := cnl_sys.cnl_whh_error_log_seq1.nextval;
end;