CREATE OR REPLACE PROCEDURE "CNL_SYS"."AS_SIMULATE_SORT_ID_P" ( p_container_id varchar2)
is
	l_container_id 	varchar2(50);
	l_sort_pos	varchar2(10);
	l_db_alias	varchar2(20);
begin
	select	db_alias
	into 	l_db_alias
	from	dcsdba.system_options
	;
	if	l_db_alias != 'PRDCNLJW'
	then
		l_container_id := upper(p_container_id);
		cnl_sys.cnl_as_mhe_pck.get_sort_pos( p_wms_unit_id_i    => l_container_id
						   , p_mhe_position_i   => 'MAN1'
						   , p_mht_station_id_i => 'NLTLG01-ASMAAS01'
						   , p_sort_pos_o       => l_sort_pos
						   );
	end if;
end as_simulate_sort_id_p;