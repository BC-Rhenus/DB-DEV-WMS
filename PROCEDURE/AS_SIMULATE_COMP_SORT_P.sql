CREATE OR REPLACE PROCEDURE "CNL_SYS"."AS_SIMULATE_COMP_SORT_P" ( p_container_id 	varchar2
						   , p_pallet_id	varchar2
						   )
is
	l_ok		varchar2(1);
	l_err_mess	varchar2(4000);
	l_container_id 	varchar2(50);
	l_pallet_id	varchar2(50);
	l_db_alias	varchar2(20);
begin
	select	db_alias
	into 	l_db_alias
	from	dcsdba.system_options
	;
	if	l_db_alias != 'PRDCNLJW'
	then
		l_container_id 	:= upper(p_container_id);
		l_pallet_id 	:= upper(p_container_id);
		cnl_sys.cnl_as_mhe_pck.comp_sort( p_wms_unit_id_i		=> l_container_id
						, p_mhe_position_i 	=> 'MAN1'
						, p_mht_pal_id_i		=> l_pallet_id
						, p_mht_pal_type_i 	=> 'OUTBLOK'
						, p_mht_station_id_i 	=> 'NLTLG01-ASMAAS01'
						, p_ok_yn_o          	=> l_ok
						, p_err_message_o    	=> l_err_mess
						);
	end if;
end as_simulate_comp_sort_p;