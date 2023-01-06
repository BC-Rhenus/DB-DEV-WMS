CREATE OR REPLACE PROCEDURE "CNL_SYS"."APEX_FIRE_DYNAMIC_SQL" ( p_statement_i 	in varchar2
						 , p_result_o		out varchar2
						 )
is
	pragma autonomous_transaction;
begin
	execute immediate p_statement_i;
	commit;
	p_result_o := 'ok';
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> 'procedure'				-- Package name the error occured
						  , p_routine_name_i		=> 'apex_fire_dynamic_sql'		-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> p_statement_i			-- Additional comments describing the issue
						  );	
		p_result_o := 'Error';
end apex_fire_dynamic_sql;