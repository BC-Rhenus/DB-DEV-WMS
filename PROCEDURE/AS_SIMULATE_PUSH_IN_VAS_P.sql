CREATE OR REPLACE PROCEDURE "CNL_SYS"."AS_SIMULATE_PUSH_IN_VAS_P" (p_container_id varchar2)
is
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
		cnl_sys.cnl_as_mhe_pck.tu_pushed_in_vas_p( p_tu_id_i	=> l_container_id
				     		         , p_site_id_i	=> 'NLTLG01'
							 );
	end if;
end as_simulate_push_in_vas_p;