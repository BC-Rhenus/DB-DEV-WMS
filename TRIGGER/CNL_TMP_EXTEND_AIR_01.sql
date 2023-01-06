CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_TMP_EXTEND_AIR_01" 
	after	insert 
	on 	cnl_sys.cnl_wms_tmp_extend_tab
	for 	each row
declare
	pragma autonomous_transaction;
begin
		cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_wms_table_extend_pck.update_extend_table_job_f('''
								   || :new.to_update_table
								   || ''','''
								   || :new.primary_key_string
								   || ''','''
								   || :new.update_string
								   || '''); end;',
						     p_code_i      => 'P_EXT_'||cnl_wms_tmp_extend_seq_1.nextval,
						     p_delay_i     => 5
						   );
end cnl_tmp_extend_air_01;