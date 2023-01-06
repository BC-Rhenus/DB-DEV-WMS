CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_TABLE_EXTEND_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $ Martijn swinkels
* $Date: $ 15-Feb-2021
**********************************************************************************
* Description: package used to operate on table set as an extention on existing WMS tables
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
-- Private constant declarations
	g_pck	varchar2(30) := 'cnl_wms_table_extend_pck';
--
-- Private variable declarations
--
-- Private routines
------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : Execute dynamic SQL
-- Date	   : 15-Feb-2021
------------------------------------------------------------------------------------------------
procedure run_sql_p( p_statement_i varchar2)
is
	l_rtn		varchar2(30) 	:= 'run_sql_p';
	l_statement	varchar2(4000)	:= p_statement_i;
	pragma 		autonomous_transaction;
begin
	execute immediate l_statement;
	commit;
exception
	when 	others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_statement_i
						  , p_comments_i		=> 'Unknown error.'
						  );

end run_sql_p;

------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : split string value into 1 for columns and 1 for values
-- Date	   : 15-Feb-2021
------------------------------------------------------------------------------------------------
procedure save_tmp_upd_p( p_table_pk_i 	in varchar2
			, p_string_i	in varchar2
			, p_table_i	in varchar2
			)
is
	l_rtn	varchar2(30) := 'save_tmp_upd_p';
	pragma autonomous_transaction;	
begin
	-- Store data to update after insert
	insert
	into	cnl_sys.cnl_wms_tmp_extend_tab
	(	primary_key_string
	,	update_string
	,	to_update_table
	)
	values
	(	p_table_pk_i
	,	p_string_i
	,	p_table_i
	);
	commit;
exception
	when others 
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> p_table_pk_i||' on table '||p_table_i
						  );
end save_tmp_upd_p;

------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : split string value into 1 for columns and 1 for values
-- Date	   : 15-Feb-2021
------------------------------------------------------------------------------------------------
function split_columns_and_values_f( p_string_i 	in varchar2
				   , p_table_name_i	in varchar2
				   , p_columns_o	out varchar2
				   , p_data_types_o	out varchar2
				   , p_values_o		out varchar2
				   , p_nbr_cols_o	out number
				   )
	return integer
is
	cursor c_data_type( b_column varchar2)
	is
		select	'"'||data_type||'"' data_type
		from 	user_tab_columns
		where	table_name 	= upper(p_table_name_i)
		and	column_name 	= upper(b_column)
	;
	--

	e_miss_value	exception;
	e_unknown_col	exception;

	r_data_type	varchar2(50);

	l_rtn		varchar2(30) := 'split_columns_and_values_f';
	l_num_cols	number;
	l_string	varchar2(4000) := p_string_i;
	l_tmp_column	varchar2(30);
	l_columns	varchar2(4000);
	l_tmp_data_type	varchar2(30);
	l_data_types	varchar2(4000);
	l_values	varchar2(4000);
	l_retval	integer;

	pragma exception_init( e_miss_value, -20001); 
	pragma exception_init( e_unknown_col, -20002); 
begin
	select	regexp_count(p_string_i,'"') 
	into	l_num_cols
	from 	dual;
	--
	if	mod(l_num_cols, 4) > 0
	then
		raise_application_error( -20001, 'Mismatch between number of columns and values.');
	end if;
	--
	<<column_loop>>
	for	i in 1..l_num_cols/4		
	loop
		l_tmp_column	:= substr(l_string,1,(instr(l_string,'"',2)));
		--
		open	c_data_type(replace(upper(l_tmp_column),'"'));
		fetch	c_data_type
		into	r_data_type;
		if	c_data_type%notfound
		then
			raise_application_error( -20001, l_tmp_column||' is not a correct column name');
		end if;
		close 	c_data_type;
		--
		l_columns 	:= l_columns||l_tmp_column;
		l_data_types	:= l_data_types||r_data_type;
		l_string 	:= substr(l_string,instr(l_string,'"',2)+1);
		l_values	:= l_values||substr(l_string,1,(instr(l_string,'"',2)));
		l_string 	:= substr(l_string,instr(l_string,'"',2)+1);
	end loop;

	--
	p_columns_o	:= l_columns;
	p_data_types_o	:= l_data_types;
	p_values_o	:= l_values;
	p_nbr_cols_o	:= l_num_cols/4;
	return 		1;
exception 
	when 	NO_DATA_FOUND
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> 'Column does not exist'
						  );
		return 	0;
	when	e_miss_value
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> 'Numer of columns and values are not matching.'
						  );
		return 	0;
	when	e_unknown_col
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> 'Unknown column added to upate string.'
						  );
		return 	0;
	when 	others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> 'Unknown error.'
						  );
		return 	0;
end split_columns_and_values_f;

------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : Update order header extend table
-- Date	   : 15-Feb-2021
------------------------------------------------------------------------------------------------
procedure update_extend_table_job_f( p_table_name_i	in varchar2 
			           , p_table_pk_i	in varchar2	
			           , p_string_i		in varchar2
			           )
is
	e_error		exception;

	l_columns	varchar2(4000);
	l_values	varchar2(4000);
	l_data_types	varchar2(4000);
	l_nbr_cols	number;

	l_column	varchar2(30);
	l_value		varchar2(1000);
	l_data_type	varchar2(50);
	l_result	integer;

	l_where		varchar2(4000);
	l_update	varchar2(4000);
	l_statement	varchar2(4000);
	l_rtn		varchar2(30) := 'update_order_line_extend_f';

	pragma exception_init( e_error, -20001); 
begin
	-- Extract columns and values from primary key string
	l_result := split_columns_and_values_f( p_string_i	=> p_table_pk_i
					      , p_table_name_i	=> upper(p_table_name_i)
					      , p_columns_o	=> l_columns
					      , p_data_types_o	=> l_data_types
					      , p_values_o	=> l_values
					      , p_nbr_cols_o	=> l_nbr_cols
					      );
	if	l_result = 0
	then
		raise_application_error( -20001, 'An error was raised when splitting primary key value');
	end if;
	-- Build where clause for update statement
	<<pk_loop>>
	for	i in 1..l_nbr_cols
	loop
		if	l_where is null
		then
			l_column 	:= substr(l_columns,2,instr(substr(l_columns,2),'"')-1) || ' = ';
		else
			l_column 	:= 'and '||substr(l_columns,2,instr(substr(l_columns,2),'"')-1) || ' = ';
		end if;

		l_data_type 	:= substr(l_data_types,2,instr(substr(l_data_types,2),'"')-1);

		if	upper(l_data_type) like '%NUMBER%'
		or	l_data_type = 'INTEGER'
		then
			l_value := substr(l_values,2,instr(substr(l_values,2),'"')-1);
			if	l_value = '' 
			or 	l_value is null
			then
				l_value := 'null';
			end if;
		else
			l_value := substr(l_values,2,instr(substr(l_values,2),'"')-1);
			if	l_value = '' 
			or 	l_value is null
			then
				l_value := 'null';
			else
				l_value := ''''||l_value||'''';
			end if;
		end if;
		l_where := l_where || l_column || l_value;
		--
		l_columns 	:= substr(l_columns,(instr(substr(l_columns,2),'"')+2));
		l_values 	:= substr(l_values,(instr(substr(l_values,2),'"')+2));
		l_data_types 	:= substr(l_data_types,(instr(substr(l_data_types,2),'"')+2));		
	end loop;

	l_result 	:= null;
	l_column 	:= null;
	l_value		:= null;

	-- Build update clause
	l_result := split_columns_and_values_f( p_string_i	=> p_string_i
					      , p_table_name_i	=> upper(p_table_name_i)
					      , p_columns_o	=> l_columns
					      , p_data_types_o	=> l_data_types
					      , p_values_o	=> l_values
					      , p_nbr_cols_o	=> l_nbr_cols
					      );
	--
	if	l_result = 0
	then
		raise_application_error( -20001, 'An error was raised when splitting string value');
	end if;

	--
	<<upd_columns_loop>>	
	for	i in 1..l_nbr_cols
	loop
		if	l_update is null
		then
			l_column 	:= substr(l_columns,2,instr(substr(l_columns,2),'"')-1) || ' = ';
		else
			l_column 	:= ', '||substr(l_columns,2,instr(substr(l_columns,2),'"')-1) || ' = ';
		end if;

		l_data_type 	:= substr(l_data_types,2,instr(substr(l_data_types,2),'"')-1);

		if	upper(l_data_type) like '%NUMBER%'
		or	l_data_type = 'INTEGER'
		then
			l_value := substr(l_values,2,instr(substr(l_values,2),'"')-1);
			if	l_value = '' 
			or 	l_value is null
			then
				l_value := 'null';
			end if;
		else
			l_value := substr(l_values,2,instr(substr(l_values,2),'"')-1);
			if	l_value = '' 
			or 	l_value is null
			then
				l_value := 'null';
			else
				l_value := ''''||l_value||'''';
			end if;
		end if;
		l_update := l_update || l_column || l_value;
		--
		l_columns 	:= substr(l_columns,(instr(substr(l_columns,2),'"')+2));
		l_values 	:= substr(l_values,(instr(substr(l_values,2),'"')+2));
		l_data_types 	:= substr(l_data_types,(instr(substr(l_data_types,2),'"')+2));		
	end loop;
	--
	l_statement := 'update '||p_table_name_i||' set '||l_update||' where '||l_where;
	--
	run_sql_p( p_statement_i => l_statement);
	delete	cnl_wms_tmp_extend_tab
	where	primary_key_string 	= p_table_pk_i
	and	update_string		= p_string_i
	and	to_update_table 	= p_table_name_i
	;
	commit;
exception
	when	e_error
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> 'Error during the split of values in the update string.'
						  );
	when 	others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> l_statement
						  );
end update_extend_table_job_f;	

------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : Update order header extend table
-- Date	   : 15-Feb-2021
------------------------------------------------------------------------------------------------
function update_order_header_extend_f( p_order_id_i	in cnl_sys.cnl_wms_order_header_extend.order_id%type
				     , p_client_id_i	in cnl_sys.cnl_wms_order_header_extend.client_id%type
				     , p_string_i	in varchar2
				     )
	return integer
is
	l_rtn		varchar2(30) := 'update_order_header_extend_f';
	l_table		varchar2(30) := 'CNL_WMS_ORDER_HEADER_EXTEND';
	l_table_pk varchar2(4000);
begin 
	-- Build primary key string
	l_table_pk := '"order_id""'||p_order_id_i||'""client_id""'||p_client_id_i||'"';
	-- Save update for after insert
	save_tmp_upd_p( p_table_pk_i 	=> l_table_pk
		      , p_string_i	=> p_string_i
		      , p_table_i	=> l_table
		      );
	return 1;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> l_table_pk||' on table '||l_table
						  );
		return 0;
end update_order_header_extend_f;	

------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : Update order header extend table
-- Date	   : 15-Feb-2021
------------------------------------------------------------------------------------------------
function update_order_line_extend_f( p_order_id_i	in cnl_sys.cnl_wms_order_line_extend.order_id%type
				   , p_client_id_i	in cnl_sys.cnl_wms_order_line_extend.client_id%type
				   , p_line_id_i	in cnl_sys.cnl_wms_order_line_extend.line_id%type
				   , p_string_i		in varchar2
				   )
	return integer
is
	l_rtn		varchar2(30) := 'update_order_line_extend_f';
	l_table		varchar2(30) := 'CNL_WMS_ORDER_LINE_EXTEND';
	l_table_pk varchar2(4000);
begin
	l_table_pk := '"order_id""'||p_order_id_i||'""client_id""'||p_client_id_i||'""line_id""'||p_line_id_i||'"';
	-- Save update for after insert
	save_tmp_upd_p( p_table_pk_i 	=> l_table_pk
		      , p_string_i	=> p_string_i
		      , p_table_i	=> l_table
		      );
	return 1;
exception 
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_string_i
						  , p_comments_i		=> l_table_pk||' on table '||l_table
						  );
		return 0;
end update_order_line_extend_f;	

------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : Update order header extend table after line update
-- Date	   : 01-Mar-2021
------------------------------------------------------------------------------------------------
procedure reset_header_p( p_order_id_i	in cnl_sys.cnl_wms_order_header_extend.order_id%type
			, p_client_id_i in cnl_sys.cnl_wms_order_header_extend.client_id%type
			)
is
	cursor c_lines
	is
		select	qty_ordered
		,	nvl(contains_hazmat,'N') 		contains_hazmat
		,	nvl(contains_ugly_sku,'N')		contains_ugly_sku
		,	nvl(contains_awkward_sku,'N')		contains_awkward_sku
		,	nvl(contains_dual_use_sku,'N')		contains_dual_use_sku
		,	nvl(contains_config_kit,'N')		contains_config_kit
		,	nvl(contains_two_man_lift,'N')		contains_two_man_lift
		,	nvl(contains_conveyable_sku,'N')	contains_conveyable_sku
		,	nvl(contains_kit,'N')			contains_kit
		from	cnl_wms_order_line_extend
		where	order_id 	= p_order_id_i
		and	client_id	= p_client_id_i
	;
	l_rtn				varchar2(30) := 'reset_header_p';
	l_qty_ordered			number := 0;
	l_contains_hazmat		varchar2(1) := 'N';
	l_contains_ugly_sku		varchar2(1) := 'N';
	l_contains_awkward_sku		varchar2(1) := 'N';
	l_contains_dual_use_sku		varchar2(1) := 'N';
	l_contains_config_kit		varchar2(1) := 'N';
	l_contains_two_man_lift		varchar2(1) := 'N';
	l_contains_conveyable_sku	varchar2(1) := 'N';
	l_contains_kit			varchar2(1) := 'N';
begin
	for	i in c_lines
	loop
		l_qty_ordered				:= l_qty_ordered + i.qty_ordered;
		if	l_contains_hazmat 		= 'N'
		then
			l_contains_hazmat		:= i.contains_hazmat;
		end if;
		if	l_contains_ugly_sku		= 'N'
		then
			l_contains_ugly_sku		:= i.contains_ugly_sku;
		end if;
		if	l_contains_awkward_sku		= 'N' 
		then
			l_contains_awkward_sku		:= i.contains_awkward_sku;
		end if;
		if	l_contains_dual_use_sku		= 'N'
		then
			l_contains_dual_use_sku		:= i.contains_dual_use_sku;
		end if;
		if	l_contains_config_kit		= 'N'
		then
			l_contains_config_kit		:= i.contains_config_kit;
		end if;
		if	l_contains_two_man_lift		= 'N'
		then
			l_contains_two_man_lift		:= i.contains_two_man_lift;
		end if;
		if	l_contains_conveyable_sku	= 'N'
		then
			l_contains_conveyable_sku	:= i.contains_conveyable_sku;
		end if;
		if	l_contains_kit			= 'N'
		then
			l_contains_kit			:= i.contains_kit;
		end if;
	end loop;	
		update 	cnl_wms_order_header_extend o
		set	o.contains_hazmat 		= l_contains_hazmat
		,	o.contains_ugly_sku		= l_contains_ugly_sku
		,	o.contains_kit			= l_contains_kit
		,	o.contains_awkward_sku		= l_contains_awkward_sku
		,	o.contains_two_man_lift		= l_contains_two_man_lift
		,	o.contains_conveyable_sku	= l_contains_conveyable_sku
		,	o.total_qty_ordered		= l_qty_ordered
		where	o.order_id 			= p_order_id_i
		and	o.client_id			= p_client_id_i
		;
		commit;
exception
	when others 
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> '"order_id" "'||p_order_id_i||'" "client_id" "'||p_client_id_i||'"'
						  , p_comments_i		=> 'Reset cnl_wms_order_header_extend failed'
						  );
end reset_header_p;
------------------------------------------------------------------------------------------------
-- Author  : Martijn Swinkels
-- Purpose : Initialize
------------------------------------------------------------------------------------------------
begin
	null;
end cnl_wms_table_extend_pck;