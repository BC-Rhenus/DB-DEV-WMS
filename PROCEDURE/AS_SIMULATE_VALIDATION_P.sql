CREATE OR REPLACE PROCEDURE "CNL_SYS"."AS_SIMULATE_VALIDATION_P" ( p_container_id 	varchar2)
is
	l_skip_val	integer;
	l_operator	integer;
	l_tracking	varchar2(50);
	l_container_id	varchar2(50);
	l_db_alias	varchar2(20);
begin
	select	db_alias
	into 	l_db_alias
	from	dcsdba.system_options
	;
	if	l_db_alias != 'PRDCNLJW'
	then
		l_container_id := upper(p_container_id);
		cnl_sys.cnl_as_mhe_pck.validate_parcel( p_wms_unit_id_i		=> l_container_id
						      , p_mhe_position_i     	=> 'MAN114'
						      , p_mht_station_id_i   	=> 'NLTLG01-ASMAAS01'
						      , p_tracking_nr_o 	=> l_tracking
						      , p_operator_o         	=> l_operator
						      , p_skip_val_o         	=> l_skip_val
						      );
	end if;
end as_simulate_validation_p;