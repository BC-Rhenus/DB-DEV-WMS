CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OLE_EXTEND_AUDR_01" 
	after	update or delete 
	on 	cnl_sys.cnl_wms_order_line_extend
	for 	each row
declare
	pragma autonomous_transaction;
begin
	if	updating
	then
		cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_wms_table_extend_pck.reset_header_p('''
								   || :new.order_id
								   || ''','''
								   || :new.client_id
								   || '''); end;',
						     p_code_i      => 'P_EXT_'||cnl_wms_tmp_extend_seq_1.nextval,
						     p_delay_i     => 5
						   );
	else
		cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_wms_table_extend_pck.reset_header_p('''
								   || :old.order_id
								   || ''','''
								   || :old.client_id
								   || '''); end;',
						     p_code_i      => 'P_EXT_'||cnl_wms_tmp_extend_seq_1.nextval,
						     p_delay_i     => 5
						   );
	end if;
exception
	when others
	then
		null;
end cnl_ole_extend_audr_01;