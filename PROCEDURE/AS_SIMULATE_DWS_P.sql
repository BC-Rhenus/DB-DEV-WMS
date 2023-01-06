CREATE OR REPLACE PROCEDURE "CNL_SYS"."AS_SIMULATE_DWS_P" ( p_container_id	varchar2
					     , p_container_type	varchar2
					     , p_weight		number
					     , p_height		number
					     , p_width		number
					     , p_depth		number
					     )
is
	l_ok			varchar2(1);
	l_err_mess		varchar2(4000);
	l_print_lab		varchar2(1);
	l_sort_pos		varchar2(3);
	l_container_id		varchar2(50);
	l_container_type 	varchar2(50);
	l_db_alias		varchar2(20);
	l_ctosaas_yn		varchar2(1);
begin
	select	db_alias
	into 	l_db_alias
	from	dcsdba.system_options
	;
	if	l_db_alias != 'PRDCNLJW'
	then
		l_container_id 		:= upper(p_container_id);
		l_container_type 	:= upper(p_container_type);
		--
		cnl_sys.cnl_as_mhe_pck.create_parcel( p_wms_unit_id_i     => l_container_id
						    , p_mhe_position_i    => 'MAN111'
						    , p_mht_unit_id_i     => null
						    , p_mht_station_id_i  => 'NLTLG01-ASMAAS01'
						    , p_lft_status_i      => 'Y'
						    , p_lft_description_i => null
						    , p_package_type_i    => l_container_type
						    , p_weight_i          => p_weight
						    , p_height_i          => p_height
						    , p_width_i           => p_width
						    , p_depth_i           => p_depth
						    , p_ok_yn_o           => l_ok
						    , p_error_message_o   => l_err_mess
						    , p_print_label_yn_o  => l_print_lab
						    , p_sort_pos_o        => l_sort_pos
						    , p_ctosaas_yn_o	  => l_ctosaas_yn
						    );
	end if;
end as_simulate_dws_p;