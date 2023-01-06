CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_LOGGING_PCK" 
is
/**********************************************************************************
* Author  : M. Swinkels, 29-Dec-2020
* Procedure that adds a log record in the cnl_print_log table
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
			     )
is
	pragma autonomous_transaction;
begin
	insert into cnl_sys.cnl_print_log
	( print_id
	, file_name
	, dstamp
	, source_package
	, source_routine
	, routine_step
	, code_parameters
	, order_id
	, client_id
	, pallet_id
	, container_id
	, site_id
	)
	values
	( p_print_id_i
	, p_file_name_i
	, sysdate
	, p_source_package_i
	, p_source_routine_i
	, p_routine_step_i
	, p_code_parameters_i
	, p_order_id_i
	, p_client_id_i
	, p_pallet_id_i
	, p_container_id_i
	, p_site_id_i
	);
	commit;
exception
	when others
	then
		null;
end add_print_log_rec_p;
/**********************************************************************************
* Package body initialization
**********************************************************************************/
	begin
		null;
end cnl_logging_pck;