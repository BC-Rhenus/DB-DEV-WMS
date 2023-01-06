CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_UTIL_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Common functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
	function get_constant( p_name_i in  cnl_constants.name%type)
		return varchar2;
	--                   
	procedure clean_cnl_files_archive( p_application_i  in  varchar2
					 , p_archive_days_i in  integer
					 );
	--
  	procedure add_cnl_error( p_sql_code_i		in varchar2 default null
			       , p_sql_error_message_i	in varchar2 default null
			       , p_line_number_i	in varchar2 default null
			       , p_package_name_i	in varchar2 default null
			       , p_routine_name_i	in varchar2 default null
			       , p_routine_parameters_i	in varchar2 default null
			       , p_comments_i		in varchar2 default null
			       );
	function get_system_profile_f ( p_profile_id_i in varchar2)
		return varchar2;

end cnl_util_pck;