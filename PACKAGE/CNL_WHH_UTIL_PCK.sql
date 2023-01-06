CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WHH_UTIL_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functions and procedures related to warehouse handlilng application
**********************************************************************************
* $Log: $
**********************************************************************************/
	function chk_user_id_f(p_user_id_i in dcsdba.application_user.user_id%type)
		return dcsdba.application_user.user_id%type;
	--
	function chk_station_id_f( p_station_id_i 	in dcsdba.workstation.station_id%type
				 , p_site_id_i		in dcsdba.workstation.site_id%type
				 )
		return dcsdba.workstation.station_id%type;
	--
	procedure	create_whh_log_p( p_site_id_i			in varchar2 default null
					, p_client_id_i			in varchar2 default null
					, p_order_id_i			in varchar2 default null
					, p_container_id_i		in varchar2 default null
					, p_pallet_id_i			in varchar2 default null
					, p_sku_id_i			in varchar2 default null
					, p_package_name_i		in varchar2 
					, p_procedure_function_i	in varchar2
					, p_extra_parameters_i		in varchar2 default null
					, p_comment_i			in varchar2 default null
					);
	--
	function get_system_profile_f ( p_profile_id_i in varchar2)
		return varchar2;
	--
	procedure create_whh_err_log_p( p_error_data_i	in	cnl_sys.cnl_whh_error_log.error_data%type
				      , p_user_id_i	in	cnl_sys.cnl_whh_error_log.user_id%type
				      );
end cnl_whh_util_pck;