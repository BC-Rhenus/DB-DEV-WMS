CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_UTIL_PCK" is
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
--
-- Private type declarations
--
--
-- Private constant declarations
--
	g_yes         constant varchar2(1)     := 'Y';
	g_no          constant varchar2(1)     := 'N';
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Get value and description from Constants
------------------------------------------------------------------------------------------------
function get_constant( p_name_i in  cnl_constants.name%type)
	return varchar2
is
	cursor c_cst( b_name cnl_constants.name%type) 
	is
		select cst.value
		from   cnl_constants cst
		where  cst.name = upper( b_name)
	;
	--
	l_retval varchar2(100);
begin
	open  c_cst( b_name => p_name_i);
	fetch c_cst 
	into  l_retval;

	return l_retval;
exception
	when others
	then
		case c_cst%isopen
		when true
		then
			close c_cst;
		end case;
		--
		raise;
end get_constant;

------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 01-Nov-2018
-- Purpose : Clean the Archive directories on the database filesystem
------------------------------------------------------------------------------------------------
procedure clean_cnl_files_archive( p_application_i  in  varchar2
                                 , p_archive_days_i in  integer
                                 )
is
	cursor c_fae
	is
		select fae.id
		,      fae.location
		,      fae.filename
		from   cnl_files_archive fae
		where  fae.application          = upper(p_application_i)
		and    fae.status               != 'Removed'
		and    trunc(fae.creation_date) < trunc(sysdate - p_archive_days_i)
	;

	r_fae         c_fae%rowtype;
	l_arc_fexists boolean := FALSE;
	l_file_length number;
	l_block_size  binary_integer;
begin
	-- delete "Removed" records
	delete cnl_files_archive
	where  status      = 'Removed'
	and    application = upper(p_application_i)
	;
	-- loop through files and remove from filesystem
	for r_fae in c_fae
	loop
		utl_file.fgetattr( location    => r_fae.location 
				 , filename    => r_fae.filename
				 , fexists     => l_arc_fexists
				 , file_length => l_file_length
				 , block_size  => l_block_size
				 );
		if l_arc_fexists
		then
			-- remove the file
			utl_file.fremove( r_fae.location
					, r_fae.filename
					);
			-- set record to status "Removed" so these will be deleted next day
			update cnl_files_archive
			set    status = 'Removed'
			where  id     = r_fae.id
			;
		end if;
	end loop;
end clean_cnl_files_archive;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Feb-2020
-- Purpose : Create record in error log.
------------------------------------------------------------------------------------------------
	procedure add_cnl_error ( 	p_sql_code_i		in varchar2 default null
				,	p_sql_error_message_i	in varchar2 default null
				,	p_line_number_i		in varchar2 default null
				,	p_package_name_i	in varchar2 default null
				,	p_routine_name_i	in varchar2 default null
				,	p_routine_parameters_i	in varchar2 default null
				,	p_comments_i		in varchar2 default null
				)
	is
	pragma autonomous_transaction;
	begin
		insert into cnl_sys.cnl_error( error_date
					     , sql_error_code
					     , sql_error_message
					     , line_number
					     , package_name
					     , routine_name
					     , routine_parameters
					     , comments
					     )
		values( sysdate
		      , p_sql_code_i
		      , p_sql_error_message_i
		      , p_line_number_i
		      , p_package_name_i
		      , p_routine_name_i
		      , p_routine_parameters_i
		      , p_comments_i
		      );
		commit;
	end add_cnl_error;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Feb-2020
-- Purpose : Fetch value from system profile
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
							  , p_package_name_i		=> 'cnl_util_pck'			-- Package name the error occured
							  , p_routine_name_i		=> 'get_system_profile_f'			-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_profile_id_i			-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end get_system_profile_f;

--
--
begin
  -- Initialization
  null;
end cnl_util_pck;