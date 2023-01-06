CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WHH_UTIL_PCK" is
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Feb-2020
-- Purpose : Set package global variables
------------------------------------------------------------------------------------------------
	g_pck	varchar2(30) := 'cnl_whh_util_pck';

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Nov-2019
-- Purpose : Check if user_id exists in WMS
------------------------------------------------------------------------------------------------
	procedure create_whh_err_log_p( p_error_data_i	in	cnl_sys.cnl_whh_error_log.error_data%type
				      , p_user_id_i	in	cnl_sys.cnl_whh_error_log.user_id%type
				      )
	is
	g_rtn	varchar2(30) := 'create_whh_err_log_p';
	pragma	autonomous_transaction;
	begin
		insert into cnl_whh_error_log( dstamp, error_data, user_id)
		values	( sysdate
			, p_error_data_i
			, p_user_id_i
			);
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_error_data_i||', '||p_user_id_i					-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end create_whh_err_log_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Nov-2019
-- Purpose : Check if user_id exists in WMS
------------------------------------------------------------------------------------------------
	function chk_user_id_f(p_user_id_i in dcsdba.application_user.user_id%type)
		return dcsdba.application_user.user_id%type
	is
		cursor c_user
		is
			select 	distinct 1
			from 	dcsdba.application_user a
			where	a.user_id = p_user_id_i
		;
		r_user		c_user%rowtype;
		l_retval	dcsdba.application_user.user_id%type;
		g_rtn	varchar2(30) := 'chk_user_id_f';
	begin
		if	p_user_id_i is null
		then
			l_retval 	:= 'WHHUSER';
		else
			open	c_user;
			fetch 	c_user into r_user;
			if	c_user%found
			then
				l_retval 	:= p_user_id_i;
			else
				l_retval 	:= 'WHHUSER';
			end if;
			close 	c_user;
		end if;
		return l_retval;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_user_id_i					-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			return 'WHHUSER';
	end chk_user_id_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Nov-2019
-- Purpose : Check if station_id exists in WMS
------------------------------------------------------------------------------------------------
	function chk_station_id_f( p_station_id_i 	in dcsdba.workstation.station_id%type
				 , p_site_id_i		in dcsdba.workstation.site_id%type
				 )
		return dcsdba.workstation.station_id%type
	is
		cursor c_station
		is
			select 	distinct 1
			from 	dcsdba.workstation a
			where	a.station_id	= p_station_id_i
			and	a.site_id	= p_site_id_i
		;
		r_station	c_station%rowtype;
		l_retval	dcsdba.workstation.station_id%type;
		g_rtn		varchar2(30) := 'chk_station_id_f';
	begin
		if	p_site_id_i 	is null
		then
			l_retval 	:= null; -- No site is no station!
		elsif	p_station_id_i 	is null
		then
			l_retval 	:= upper(p_site_id_i)||'WHHSTATION';
		else
			open	c_station;
			fetch 	c_station into r_station;
			if	c_station%found
			then
				l_retval 	:= p_station_id_i;
			else
				l_retval 	:= upper(p_site_id_i)||'WHHSTATION';
			end if;
			close 	c_station;
		end if;
		return l_retval;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_station_id_i||', '||p_site_id_i	-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			return 'WHHSTATION';
	end chk_station_id_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-11-2019
-- Purpose : Create logging.
------------------------------------------------------------------------------------------------
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
					)
	is
		l_extra_parameters	varchar2(4000) 	:= substr(p_extra_parameters_i,1,4000);
		l_comments		varchar2(4000)	:= substr(p_comment_i,1,4000);
		g_rtn			varchar2(30) := 'create_whh_log_p';
		pragma	autonomous_transaction;
	begin
		insert into cnl_sys.cnl_whh_log( dstamp, site_id, client_id, order_id, container_id, pallet_id, sku_id, package_name, procedure_function, extra_parameters, comments)
		values(	sysdate
		,	p_site_id_i
		,	p_client_id_i	
		,	p_order_id_i
		,	p_container_id_i		
		,	p_pallet_id_i		
		,	p_sku_id_i		
		,	p_package_name_i
		,	p_procedure_function_i
		,	l_extra_parameters
		,	l_comments
		);
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null					-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end create_whh_log_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-11-2019
-- Purpose : fetch system profile settings
------------------------------------------------------------------------------------------------
	function get_system_profile_f ( p_profile_id_i in varchar2)
		return varchar2
	is
		cursor c_spf
		is
			select  data_type
			,       text_data
			,       numeric_data
			,       dstamp_data
			,       Password_data
			from    dcsdba.system_profile
			where   profile_id = p_profile_id_i
		;
		--
		l_spf       	c_spf%rowtype;
		l_retval    	varchar2(200);
		g_rtn		varchar2(30) := 'get_system_profile_f';
		--
	begin
		open    c_spf;
		fetch   c_spf 
		into    l_spf;
		if      l_spf.data_type = 'Text' 
		then
			l_retval := l_spf.text_data;
		elsif   l_spf.data_type = 'Number' 
		then
			l_retval := to_char(l_spf.numeric_data);
		elsif   l_spf.data_type = 'Timestamp' 
		then
			l_retval := to_char(l_spf.dstamp_data);
		elsif   l_spf.data_type = 'Password' 
		then
			l_retval := l_spf.password_data;
		end if;
		close   c_spf;
		return l_retval;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_profile_id_i			-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end get_system_profile_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 3-Okt-2019
-- Purpose : Initialize package to load it faster.
------------------------------------------------------------------------------------------------
	begin
	-- initialization
	null;	
	--

end cnl_whh_util_pck;