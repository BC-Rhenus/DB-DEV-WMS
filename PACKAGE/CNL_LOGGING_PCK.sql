CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_LOGGING_PCK" 
is
/**********************************************************************************
* Author  : M. Swinkels, 29-Dec-2020
* Package to add all procedures that create logging records
**********************************************************************************/
procedure add_print_log_rec_p( p_print_id_i		in number 	default null
			     , p_file_name_i		in varchar2 	default null
			     , p_source_package_i	in varchar2
			     , p_source_routine_i	in varchar2
			     , p_routine_step_i		in varchar2 	default null
			     , p_code_parameters_i 	in varchar2 	default null
			     , p_order_id_i		in varchar2	default null
			     , p_client_id_i		in varchar2	default null
			     , p_pallet_id_i		in varchar2	default null
			     , p_container_id_i		in varchar2	default null
			     , p_site_id_i		in varchar2	default null
			     );
end cnl_logging_pck;