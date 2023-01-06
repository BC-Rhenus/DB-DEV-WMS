CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_PRINT_LOG_BIR_01" 
	before insert on  CNL_SYS.cnl_print_log
	for each row
begin
	:new.log_key := cnl_print_log_seq1.nextval;
end cnl_print_log_bir_01;