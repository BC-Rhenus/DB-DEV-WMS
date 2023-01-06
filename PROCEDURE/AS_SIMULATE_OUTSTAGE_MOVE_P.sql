CREATE OR REPLACE PROCEDURE "CNL_SYS"."AS_SIMULATE_OUTSTAGE_MOVE_P" (p_container_id varchar2)
is
	l_print_doc 	varchar2(1);
	l_close_box 	varchar2(1);
	l_pass      	varchar2(1);
	l_instruction 	varchar2(4000);
	l_ok		varchar2(1);
	l_error		varchar2(4000);
	l_container_id  varchar2(50);
	l_db_alias	varchar2(20);
begin
	select	db_alias
	into 	l_db_alias
	from	dcsdba.system_options
	;
	if	l_db_alias != 'PRDCNLJW'
	then
		l_container_id := upper(p_container_id);
		cnl_sys.cnl_as_mhe_pck.print_doc ( p_wms_unit_id_i	=> l_container_id
						 , p_mhe_position_i    	=> 'MAN57'
						 , p_mht_unit_id_i     	=> null
						 , p_mht_station_id_i  	=> 'NLTLG01-ASMAAS01'
						 , p_print_doc_o       	=> l_print_doc
						 , p_close_box_o       	=> l_close_box
						 , p_pass_trough_o     	=> l_pass
						 , p_instruction_o     	=> l_instruction
						 , p_ok_yn_o           	=> l_ok
						 , p_error_message_o   	=> l_error
						 );
	end if;
end as_simulate_outstage_move_p;