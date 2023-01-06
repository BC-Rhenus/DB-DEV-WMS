CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_PCK" is
/********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 07-05-2018
**********************************************************************************
*
* Description: 
* several utilities for AS packages.
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
-- Private constant declarations
--
-- Private variable declarations
	g_pck	varchar2(30) := 'cnl_as_pck';
-- Private routines
--
------------------------------------------------------------------------------------------------
-- function check if value is number
------------------------------------------------------------------------------------------------
function is_number( p_string	in varchar2 )
	return int
is
	v_new_num	number;
begin
	v_new_num	:= to_number(p_string);
	return 1;
exception
	when	value_error 
	then
		return 0;
end is_number;

/***************************************************************************************************************
* function to get table  key
***************************************************************************************************************/                   
function key_f( p_tbl	number )
	return number
is
	cursor c_mex
        is
		select	rhenus_synq.host_message_exchange_seq.nextval@as_synq.rhenus.de 
		from    dual
        ;
        --    
        l_retval 	number;
begin
	if      p_tbl = 1
        then
                open  c_mex;
                fetch c_mex into l_retval;
                close c_mex;
        end if;
        return l_retval;
end key_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 08-May-2018
-- Purpose : Get value from system profile in WMS
------------------------------------------------------------------------------------------------
function get_system_profile( p_profile_id_i	in varchar2 )
        return varchar2
is
        cursor c_spf( b_profile	varchar2 )
        is
		select  data_type
		,       text_data
		,       numeric_data
		,       dstamp_data
		,       Password_data
		from    dcsdba.system_profile
		where   profile_id 	= b_profile
	;
        --
        l_spf       	c_spf%rowtype;
        l_retval    	varchar2(200);
	l_rtn		varchar2(30) := 'get_system_profile';
        --
begin
        open    c_spf( b_profile => p_profile_id_i);
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
        return 	l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_profile_id_i			-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.get_system_profile','Exception check CNL_ERROR');
                l_retval	:= null;
                return		l_retval;
end get_system_profile;

/***************************************************************************************************************
* check user
***************************************************************************************************************/ 
function check_user_id( p_user_i	varchar2 )
	return varchar2
is
        cursor c_chk
        is
		select  count(*)
		from    dcsdba.application_user
		where   user_id = p_user_i
        ;
        --
        l_chk       	number;
        l_retval    	varchar2(50);
	l_rtn		varchar2(30) := 'check_user_id';
        --
begin
        open    c_chk;
        fetch   c_chk 
        into    l_chk;
        close   c_chk;
        if      l_chk = 1 
        then
                l_retval := p_user_i;
        else
                l_retval := 'AUTOSTORE';
        end if;
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_user_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.get_check_user_id','Exception check CNL_ERROR');
		l_retval := 'AUTOSTORE';
		return	l_retval;
end check_user_id;

/***************************************************************************************************************
* check workstation
***************************************************************************************************************/ 
function check_station_id( p_station_i	varchar2 )
        return varchar2
is
        cursor c_chk
        is
		select  count(*)
		from    dcsdba.workstation
		where   station_id = p_station_i
        ;
        --
        l_chk       	number;
        l_retval    	varchar2(50);
	l_rtn		varchar2(30) := 'check_station_id';
        --
begin
        open    c_chk;
        fetch   c_chk 
        into    l_chk;
        close   c_chk;
        if      l_chk = 1 
        then
                l_retval := p_station_i;
        else
                l_retval := 'AUTOSTORE';
        end if;
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_station_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.check_station_id','Exception check CNL_ERROR');
		l_retval := 'AUTOSTORE';
		return l_retval;
end check_station_id;        

/***************************************************************************************************************
* get move_task consol link
***************************************************************************************************************/ 
function get_consol_link
        return number
is 
        l_retval   	number;
	l_rtn		varchar2(30) := 'get_consol_link';
begin
        l_retval := dcsdba.consol_move_task_link_seq.nextval;
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.get_consol_link','Exception check CNL_ERROR');
		l_retval := 0;                                                    
		return l_retval;
end get_consol_link;

/***************************************************************************************************************
* get move_task key
***************************************************************************************************************/ 
function get_move_task_key
        return number
is 
        l_retval	number;
	l_rtn		varchar2(30) := 'get_move_task_key';
begin
        l_retval := dcsdba.move_task_pk_seq.nextval;
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.get_move_task_key','Exception check CNL_ERROR');
		l_retval := 0;                                                    
		return l_retval;
end get_move_task_key;

/***************************************************************************************************************
* get move_task key
***************************************************************************************************************/ 
function get_pick_label_id
        return number
is 
        l_retval	number;
	l_rtn		varchar2(30) := 'get_pick_label_id';
begin
        l_retval := dcsdba.pick_label_seq.nextval;
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.get_pick_label_id','Exception check CNL_ERROR');          
		l_retval := 0;                                                    
		return l_retval;
end get_pick_label_id;

/***************************************************************************************************************
* Get max weight for weight check.
***************************************************************************************************************/
function max_wht_chk( p_site_i 	in varchar2 )
        return number
is
        l_retval 	number;
	l_rtn		varchar2(30) := 'max_why_chk';
begin
        l_retval := round(to_number(cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' 
                                                                                          || p_site_i 
                                                                                          || '_NO-WEIGHT-CHECK_MAX-WEIGHT'))*1000,4);
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_site_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.max_wht_chk','Exception check CNL_ERROR');
		l_retval := 0;                                                    
		return l_retval;
end max_wht_chk;

/***************************************************************************************************************
* Get mix weight for weight check.
***************************************************************************************************************/
function min_wht_chk( p_site_i 	in varchar2 )
        return number
is
        l_retval 	number;
	l_rtn		varchar2(30) := 'min_wht_chk';
begin
        l_retval := round(to_number(get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' 
                                                                       || p_site_i 
                                                                       || '_NO-WEIGHT-CHECK_MIN-WEIGHT'))*1000,4);
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.min_wht_chk','Exception check CNL_ERROR');
		l_retval := 0;                                                    
		return l_retval;
end min_wht_chk;

/***************************************************************************************************************
* Weight check Y/N
***************************************************************************************************************/
function wht_chk_req( p_weight_i    number
		    , p_site_i      varchar2
		    )
        return number
is
        l_retval 	number;
	l_rtn		varchar2(30) := 'wht_chk_req';
begin
        if	p_weight_i > max_wht_chk(p_site_i) or 
		p_weight_i < min_wht_chk(p_site_i) 
        then
		l_retval := 0; -- no weight check
        else
		l_retval := 1; -- weight check
        end if;
        return 	l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> 'p_weight_i '
										|| to_char(p_weight_i)
										|| ', '
										|| 'p_site_i '
										|| p_site_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.wht_chk_req','Exception check CNL_ERROR');
		l_retval := 0;                                                    
		return l_retval;
end wht_chk_req;

/***************************************************************************************************************
* get pick_sequence value
***************************************************************************************************************/              
function picksequence_value( p_weight_i varchar2
                           , p_site_i   varchar2
			   )
        return varchar2
is
        l_retval 	varchar2(100);
	l_rtn		varchar2(30) := 'picksequence_value';
begin
        case
	when	p_weight_i < to_number(get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_i) || '_PICKSEQUENCE_LIGHT-WEIGHT')) 
	then 
		l_retval := 'LIGHT';
	when 	p_weight_i < to_number(get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_i) || '_PICKSEQUENCE_MEDIUM-WEIGHT')) 
	then 
		l_retval := 'MEDIUM';
	else 
		l_retval := 'HEAVY';
        end 	case
	;
        return 	l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> 'p_weight_i '
										|| p_weight_i
										|| ', '
										|| 'p_site_i '
										|| p_site_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.picksequence_value','Exception check CNL_ERROR');
		l_retval := 'MEDIUM';
		return l_retval;
end picksequence_value;

/***************************************************************************************************************
* Get weight tolerance percentage
***************************************************************************************************************/  
function wht_tolerance(	p_site_i	in varchar2 )
        return number
is
        l_retval 	number;
	l_rtn		varchar2(30) := 'wht_tolerance';
begin
        l_retval := to_number(get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_i || '_WEIGHT-TOLERANCE-PERC_WEIGHT-TOLERANCE-PERC'));
        return l_retval;
exception
        when others
        then
 		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_site_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
           cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.wht_tolerance','Exception check CNL_ERROR');
            l_retval := 0;
            return l_retval;
end wht_tolerance;

/***************************************************************************************************************
* Get client groups
***************************************************************************************************************/  
function get_clientgroups( p_site_id_i 	varchar2 )
        return varchar
is
        cursor  c_group( b_site varchar2)
        is
		select  text_data
		from    dcsdba.system_profile
		where   parent_profile_id = '-ROOT-_USER_AUTOSTORE_SITE_' || b_site || '_CLIENTGROUP'
        ;
        --
        r_group     	varchar2(100);
        l_retval    	varchar2(100);
	l_rtn		varchar2(30) := 'get_clientgroups';
begin
        open    c_group(p_site_id_i);
        fetch   c_group
        into    r_group;
        if      c_group%notfound
        then    
                l_retval := null;
                close c_group;
        else
                l_retval := r_group;
                close c_group;
        end if;
        return  l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_site_id_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.get_clientgroups','Exception check CNL_ERROR');
		l_retval := 'NOGROUP';
		return l_retval;
end get_clientgroups;

/***************************************************************************************************************
* check if client is ok to process
***************************************************************************************************************/  
function chk_client( p_site_id_i     varchar2
                   , p_client_id_i   varchar2
		   )
        return number
is
        cursor c_clients(b_group varchar2)
        is
		select  client_id
		from    dcsdba.client_group_clients
		where   client_group = b_group
	;
        --
        l_client_group	varchar2(50) := get_clientgroups(p_site_id_i);
        l_retval        number := 0;
	l_rtn		varchar2(30) := 'chk_client';
begin
        if 	p_client_id_i is null -- Update is done for something not linked to a client.
        then
		l_retval := 1;
        else
		if 	l_client_group is null -- Not all sites have an Autostore so no client group is available.
		then
			l_retval := 0;
		else
			-- Loop trough all clients in client group.
			for 	b in c_clients(l_client_group) 
			loop
				if 	l_retval = 1
				then
					continue;
				else
					if 	b.client_id = p_client_id_i 
					then
						l_retval := 1;
					else 
						l_retval := 0;
					end if;
				end if;
			end loop c_clients;
		end if;
        end if;
        return l_retval;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> 'p_ite_id_i	'
										|| p_site_id_i				-- list of all parameters involved
										|| ', p_client_id_i '
										|| p_client_id_i
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.chk_client','Exception check CNL_ERROR');
		l_retval := 0;
		return l_retval;
end chk_client;

/***************************************************************************************************************
* Check if workstation has seperate containers flag checked.
***************************************************************************************************************/              
function separate_containers( p_station_i	in varchar2 )
        return varchar2
is
        cursor  c_st
        is
		select  nvl(separate_containers,'N')
		from    dcsdba.workstation
		where   station_id = p_station_i
	;
	--
        l_retval	varchar2(1);
begin
	open    c_st;
        fetch   c_st 
        into    l_retval;
        if      c_st%notfound 
        then    l_retval := 'N';
        end if;
        close   c_st;
        return  l_retval;
exception
        when    others
        then    
		l_retval := 'N';
        return  l_retval;
end separate_containers;

/***************************************************************************************************************
* Get site drop location
***************************************************************************************************************/  
function get_drop_location( p_site_i	varchar2 )
        return varchar
is
        l_retval varchar(100);
begin
        l_retval := get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_i) || '_DROP-LOCATION_LOCATION');
        return l_retval;
exception
        when    others
        then    
		l_retval := 'NOLOC';
        return  l_retval;
end get_drop_location;

/***************************************************************************************************************
* Insert inventory transaction
***************************************************************************************************************/  
procedure insert_itl( p_mt_key_i        in  number
                    , p_to_status_i     in  varchar2
                    , p_ok_yn_o         out varchar2
                    )
is
        cursor c_key
        is
		select 	dcsdba.inventory_transaction_pk_seq.nextval 
		from 	dual
	;
        --
        cursor c_mvt( b_key number)
        is
		select  site_id
		,       from_loc_id
		,       to_loc_id
		,       final_loc_id
		,       owner_id
		,       client_id
		,       sku_id
		,       tag_id
		,       work_group
		,       consignment
		,       task_id
		,       qty_to_move
		,       status
		,       config_id
		from    dcsdba.move_task
		where   key = b_key
        ;
        --
        cursor c_con( b_config_id   varchar2
                    , b_client_id   varchar2
                    )
        is
		select  track_level_1
		from    dcsdba.sku_config
		where   config_id = b_config_id
		and     ( client_id = b_client_id or client_id is null )
		and     rownum = 1
        ;
        --
        r_key   	number;
        r_mvt   	c_mvt%rowtype;
        r_con   	varchar2(20);
	l_rtn		varchar2(30) := 'insert_itl';
        --
        pragma 		autonomous_transaction;
        --
begin
	open    c_key;
	fetch   c_key into r_key;
	close   c_key;
	--
	open    c_mvt( p_mt_key_i);
	fetch   c_mvt into r_mvt;
	close   c_mvt;
	--
	open    c_con( r_mvt.config_id
                     , r_mvt.client_id
                     );
	fetch   c_con into r_con;
        close   c_con;
	--                         
	insert 
	into 	dcsdba.inventory_transaction
	( 	key
	, 	site_id
	, 	from_loc_id
	, 	to_loc_id
	, 	final_loc_id
	, 	owner_id
	, 	client_id
	, 	sku_id
	, 	tag_id
	, 	work_group
	, 	consignment
	, 	reference_id
	, 	update_qty
	, 	notes
	, 	tracking_level
	, 	from_status
	, 	to_status
	, 	code, dstamp, station_id, user_id, group_id, uploaded_ab, uploaded_tm, uploaded_vview, session_type
	, 	summary_record, complete_dstamp, ce_rotation_id, uploaded_customs, extra_notes, archived, grid_pick
	) 
	values
	( 	r_key
	, 	r_mvt.site_id
	, 	r_mvt.from_loc_id
	, 	r_mvt.to_loc_id
	, 	r_mvt.final_loc_id
	, 	r_mvt.owner_id
	, 	r_mvt.client_id
	, 	r_mvt.sku_id
	, 	r_mvt.tag_id
	, 	r_mvt.work_group
	, 	r_mvt.consignment
	, 	r_mvt.task_id
	, 	r_mvt.qty_to_move
	, 	r_mvt.status||'=>'||p_to_status_i
	, 	r_con
	, 	r_mvt.status
	, 	p_to_status_i
	, 	'MT Sts Change',sysdate,'AUTOSTORE','AUTOSTORE','TESTER','N','N','N','M','Y',sysdate,'XX/D00000001','Y'
	, 	'Update generated by cubing process','N','N'
	);
	commit;
	p_ok_yn_o := 'Y';
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> 'p_mt_key_i '
										|| to_char(p_mt_key_i)
										|| ', p_to_status_i '
										|| p_to_status_i			-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		p_ok_yn_o := 'N';
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.insert_itl','Exception check CNL_ERROR');
end insert_itl;

/***************************************************************************************************************
* Create container suspect record
***************************************************************************************************************/              
procedure log_container_suspect( p_container_id_i  in  varchar2
                               , p_client_id_i     in  varchar2
                               , p_order_id_i      in  varchar2
                               , p_description_i   in  varchar2
                               )
is
	l_rtn	varchar2(30) := 'log_container_suspect';
begin
	-- l_description := substr(p_description_i,1,4000);
        insert 
	into 	cnl_as_container_suspect 
	( 	key
	, 	dstamp
	, 	container_id
	, 	order_id
	, 	client_id
	, 	description
	)
	values 
	( 	cnl_as_suspect_seq1.nextval
	, 	sysdate
	, 	p_container_id_i
	, 	p_order_id_i
	, 	p_client_id_i
	, 	substr(p_description_i,1,4000)
	)
	;
        --commit;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_container_id_i
										|| ', '
										|| p_client_id_i
										|| ', '
										|| p_order_id_i				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.log_container_suspect','Exception check CNL_ERROR');
end log_container_suspect;

/***************************************************************************************************************
* Create host message exchange
***************************************************************************************************************/              
procedure create_message_exchange( p_message_id_i               in  varchar2
                                 , p_message_status_i           in  varchar2
                                 , p_message_type_i             in  varchar2
                                 , p_trans_code_i               in  varchar2
                                 , p_host_message_table_key_i   in  varchar2
                                 , P_key_o                      out number
                                 )
is
        l_key   number;
	l_rtn	varchar2(30) := 'create_message_exchange';
begin
        l_key := key_f(1);
        insert 
	into 	rhenus_synq.host_message_exchange@as_synq.rhenus.de
	(	message_id
	, 	message_status
	, 	message_type
	, 	trans_code
	, 	host_message_table_key
	, 	create_date
	, 	sender
	, 	receiver
	, 	host_message_key
	)
	values
	( 	l_key
	, 	p_message_status_i
	, 	p_message_type_i
	, 	p_trans_code_i
	, 	p_host_message_table_key_i
	, 	sysdate
	, 	'HOST'
	, 	'SynQ'
	, 	l_key
	)
	;
        p_key_o := l_key;
exception
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_message_id_i
										|| ', '
										|| p_message_status_i
										|| ', '
										|| p_message_type_i
										|| ', '
										|| p_trans_code_i
										|| ', '
										|| p_host_message_table_key_i		-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.create_message_exchange','Exception check CNL_ERROR');
end create_message_exchange;

/***************************************************************************************************************
* process message exchange that confirms our messages to Synq
***************************************************************************************************************/              
procedure process_message_response
is
	-- Process response Synq from WMS => Synq interfaces
	cursor c_hme_host
	is
		select  decode(h.message_type,'ProductMaster',1,'OrderMaster',2,'AsnReceivingNotification',3,'ManualOrderStart',4,'ManualcartonPicked',5) seq
		,	h.message_type
		,       h.message_status
		,       h.host_message_table_key
		,       h.error_code
		,       h.error_text
		,       h.host_message_key
		,       h.message_id
		from    rhenus_synq.host_message_exchange@as_synq.rhenus.de h		
		where	h.sender 		= 'HOST' 
		and 	h.message_status 	in ('SUCCESS','ERROR')
		and 	h.message_type 		in ('ProductMaster','OrderMaster','AsnReceivingNotification','ManualOrderStart','ManualcartonPicked')
		and	to_char(h.create_date,'YYYYMMDD') = to_char(sysdate,'YYYYMMDD')
		order 
		by 	seq	asc
		for 	update 
		of	h.message_status
	;
	l_rtn		varchar2(30) := 'process_message_reponse';
 	--
begin
	-- Loop true all processed messages by Synq.
	<<hme_host_loop>>
	for 	r_hme_host in c_hme_host
	loop
		begin 	-- To prevent exceptions to stop loop!
			if      r_hme_host.message_type 	= 'ProductMaster'
			then    
				update  cnl_sys.cnl_as_masterdata
				set     cnl_if_status   	= 'HmeProductMasterProcessed'
				where   synq_key        	= r_hme_host.host_message_key
				;
			elsif	r_hme_host.message_type 	= 'OrderMaster'
			then
				update  cnl_sys.cnl_as_orders
				set     cnl_if_status   	= 'HMEordermasterProcessed'
				,       update_date     	= sysdate
				where   ord_master_host_message_key = r_hme_host.host_message_key;
			elsif	r_hme_host.message_type 	= 'AsnReceivingNotification'
			then
				update  cnl_sys.cnl_as_inb_tasks
				set     cnl_if_status   	= 'HmeAsnProcessed'
				where   synq_key        	= r_hme_host.host_message_key;
			elsif	r_hme_host.message_type 	= 'ManualOrderStart'
			then
				update  cnl_sys.cnl_as_orders
				set     cnl_if_status   	= 'HMEManualOrderStartProcessed'
				,       update_date     	= sysdate
				where   ord_start_host_message_key = r_hme_host.host_message_key;
			elsif	r_hme_host.message_type 	= 'ManualcartonPicked'
			then
				update  cnl_sys.cnl_as_tu
				set     cnl_if_status   	= 'HMEManualCartonPickedProcessed'
				where   man_pick_host_message_key = r_hme_host.host_message_key;
			end if;				
			--
			update 	rhenus_synq.host_message_exchange@as_synq.rhenus.de
			set	message_status 			= 'CNL'||message_status
			where	host_message_key		= r_hme_host.host_message_key
			;
		exception
			when others
			then
				cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
								  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
								  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
								  , p_package_name_i		=> g_pck				-- Package name the error occured
								  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
								  , p_routine_parameters_i	=> null				-- list of all parameters involved
								  , p_comments_i		=> null					-- Additional comments describing the issue
								  );
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_message_response','Exception check CNL_ERROR');
				continue;
		end;
	end loop; -- hme_host_loop
	commit;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null				-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_message_response','Exception check CNL_ERROR');
		commit;
end process_message_response;

/***************************************************************************************************************
* process message exchange AS pick confirmation
***************************************************************************************************************/              
procedure process_as_pick_confirm
is
	cursor c_hme_synq
	is
		select  h.message_type
		,       h.message_status
		,       h.host_message_table_key
		,       h.error_code
		,       h.error_text
		,       h.host_message_key
		,       h.message_id
		from    rhenus_synq.host_message_exchange@as_synq.rhenus.de h		
		where	h.sender 		= 'SynQ' 
		and 	h.message_status 	= 'UNPROCESSED'
		and 	h.message_type 		= 'OrderTuPickConfirmation'
	;
	--
	cursor  c_otp( b_key    number)
	is
		select  p.shipment_id
		,       p.owner_id
		,       p.order_id
		from    rhenus_synq.host_order_tu_pick@as_synq.rhenus.de p
		where   p.order_tu_pick_key = b_key
	;
	--
	r_otp       	c_otp%rowtype;
	l_error_yn      varchar2(1);
	l_error_code    number;
	l_error_text    varchar2(400);
	l_container     varchar2(30);
	l_ok_yn         varchar2(1);
	l_rtn		varchar2(30) := 'process_as_pick_confirm';
	l_busy	dcsdba.system_profile.text_data%type;
begin
	select	text_data
	into	l_busy
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESPICKCONFIRM_PICKCONFIRMBUSY'
	;
	if	l_busy = 'SLEEPING'
	then	
		-- set parameter to busy
		update	dcsdba.system_profile
		set	text_data = 'BUSY'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESPICKCONFIRM_PICKCONFIRMBUSY'
		;
		commit;

		<<hme_synq_loop>>
		for	r_hme_synq in c_hme_synq
		loop
			begin
				open    c_otp( r_hme_synq.host_message_table_key);
				fetch   c_otp
				into    r_otp;
				close   c_otp;
				--
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_pick_confirm','OrderTuPickConfirmation for client_id ' || r_otp.owner_id
														|| ', order_id ' || r_otp.order_id
														|| ', host_message_id ' || r_hme_synq.message_id                                                                                                                 
														|| ', host_message_status ' || r_hme_synq.message_status
														);
				--
				cnl_sys.cnl_as_outbound_pck.pick_confirmation( r_hme_synq.host_message_table_key
									     , r_hme_synq.host_message_key
									     , l_error_yn
									     , l_error_code
									     , l_error_text
									     , l_container
									     );
				--
				if      nvl(l_error_yn,'N') = 'N'
				then
					update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
					set     message_status 		= 'SUCCESS'
					where   host_message_key 	= r_hme_synq.host_message_key;
				else
					update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
					set     message_status  	= 'ERROR'
					,       error_code      	= l_error_code
					,       error_text      	= l_error_text
					where   host_message_key 	= r_hme_synq.host_message_key;
				end if;
				--
				cnl_sys.cnl_as_outbound_pck.complete_pick_tasks(l_container);
				--
			exception
				when others
				then
					cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
									  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
									  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
									  , p_package_name_i		=> g_pck				-- Package name the error occured
									  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
									  , p_routine_parameters_i	=> r_otp.order_id			-- list of all parameters involved
													|| ', '
													|| l_container
									  , p_comments_i		=> null					-- Additional comments describing the issue
									  );
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_pick_confirm','Exception check CNL_ERROR');
			end;
			--
			commit;
		end loop;	
		-- set parameter to sleeping
		update	dcsdba.system_profile
		set	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESPICKCONFIRM_PICKCONFIRMBUSY'
		;
		commit;
	end if;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_pick_confirm','Exception check CNL_ERROR');
		-- set parameter to sleeping
		update	dcsdba.system_profile
		set	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESPICKCONFIRM_PICKCONFIRMBUSY'
		;
		commit;
end process_as_pick_confirm;

/***************************************************************************************************************
* process message exchange AS cubing results
***************************************************************************************************************/              
procedure process_as_cubing_results
is
	cursor c_hme_synq
	is
		select  h.message_type
		,       h.message_status
		,       h.host_message_table_key
		,       h.error_code
		,       h.error_text
		,       h.host_message_key
		,       h.message_id
		from    rhenus_synq.host_message_exchange@as_synq.rhenus.de h		
		where	h.sender 		= 'SynQ' 
		and 	h.message_status 	= 'UNPROCESSED'
		and 	h.message_type 		= 'CubingResult'
	;
	--
	cursor  c_cub( b_key    number)
	is
		select  t.order_id
		,       t.owner_id
		,       t.tu_id
		,       t.tu_type
		from    rhenus_synq.host_order_tu@as_synq.rhenus.de t
		where   t.cubing_result_key = b_key
	;
	--
	r_cub	c_cub%rowtype;
	l_rtn	varchar2(30) := 'process_as_cubing_results';
	l_busy	dcsdba.system_profile.text_data%type;
begin
	select	text_data
	into	l_busy
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSCUBING_PROCESSINGBUSY'
	;
	if	l_busy = 'SLEEPING'
	then	
		-- set parameter to busy
		update	dcsdba.system_profile
		set	text_data = 'BUSY'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSCUBING_PROCESSINGBUSY'
		;
		commit;

		<<hme_synq_loop>>
		for	r_hme_synq in c_hme_synq
		loop
			begin
				open    c_cub( r_hme_synq.host_message_table_key);
				fetch   c_cub
				into    r_cub;
				close   c_cub;
				--
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_cubing_results','cubing results for client_id ' || r_cub.owner_id
														     || ', order_id ' || r_cub.order_id
														     || ', tu_id ' || r_cub.tu_id
														     || ', tu_type ' || r_cub.tu_type
														     || ', host_message_id ' || r_hme_synq.message_id                                                                                                                 
														     || ', host_message_status ' || r_hme_synq.message_status
														    );
				--
				cnl_sys.cnl_as_cubing_pck.cubing_result( r_hme_synq.host_message_table_key
								       , r_hme_synq.host_message_key
								       ); 
				--
				update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
				set     message_status = 'SUCCESS'
				where   host_message_key = r_hme_synq.host_message_key;
				--
				commit;
				--
				cnl_sys.cnl_as_cubing_pck.wms_update_pick_tasks;
				--
			exception
				when others
				then
					cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
									  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
									  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
									  , p_package_name_i		=> g_pck				-- Package name the error occured
									  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
									  , p_routine_parameters_i	=> r_cub.order_id			-- list of all parameters involved
													|| ', '
													|| r_cub.tu_id
									  , p_comments_i		=> null					-- Additional comments describing the issue
									  );
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_cubing_results','Exception check CNL_ERROR');
			end;
			--
			commit;
		end loop;
		-- set parameter to busy
		update	dcsdba.system_profile
		set	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSCUBING_PROCESSINGBUSY'
		;
		commit;
	end if;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_cubing_results','Exception check CNL_ERROR');
		update	dcsdba.system_profile
		set	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSCUBING_PROCESSINGBUSY'
		;
		commit;
end process_as_cubing_results;

/***************************************************************************************************************
* process message exchange remaining messages
***************************************************************************************************************/              
procedure process_as_other_messages
is
	-- Process interfaces from Synq => WMS
	cursor c_hme_synq
	is
		select  decode(h.message_type,'InventoryStatus',1,'InventoryReconciliation',2,'AsnCheckInConfirmation',3,'OrderStatusChangeNotification',4) seq
		,	h.message_type
		,       h.message_status
		,       h.host_message_table_key
		,       h.error_code
		,       h.error_text
		,       h.host_message_key
		,       h.message_id
		from    rhenus_synq.host_message_exchange@as_synq.rhenus.de h		
		where	h.sender 		= 'SynQ' 
		and 	h.message_status 	= 'UNPROCESSED'
		and 	(	h.message_type 		in ('InventoryStatus','InventoryReconciliation')
			or
				(	h.message_type 		in ('AsnCheckInConfirmation','OrderStatusChangeNotification')
				and	h.create_date 		< sysdate - interval '10' minute
				)
			)
		order 	
		by 	seq 	asc
	;
	--
	cursor  c_inv( b_key    number)
	is
		select  i.location_id
		,       i.tu_id
		from    rhenus_synq.host_inventory_status@as_synq.rhenus.de i
		where   i.inventory_status_key = b_key
	;
	--
	cursor  c_hlu( b_tbl_key    number)
	is
		select  asn_tu_id
		,       reason_code
		,       reason_text
		,       owner_id
		,       prev_quantity
		from    rhenus_synq.host_load_unit@as_synq.rhenus.de
		where   inventory_status_key    = b_tbl_key
	;
	--
	cursor  c_asn( b_key    number)
	is
		select  a.owner_id
		,       a.tu_id
		from    rhenus_synq.host_asn@as_synq.rhenus.de a
		where   asn_key = b_key
	;
	--
	cursor  c_sta( b_key    number)
	is
		select  s.state
		,       s.order_id
		,       s.owner_id
		from    rhenus_synq.host_order_status_change@as_synq.rhenus.de s
		where   s.order_status_change_key = b_key
	;
	-- Check if pick confirmations older than 5 minutes exist
	cursor c_hme_pick
	is
		select  count(*)
		from    rhenus_synq.host_message_exchange@as_synq.rhenus.de h		
		where	h.sender 		= 'SynQ' 
		and 	h.message_status 	= 'UNPROCESSED'
		and 	h.message_type 		= 'OrderTuPickConfirmation'
		and	h.create_date 		< sysdate - interval '5' minute
	;
	--
	r_hlu       	c_hlu%rowtype;
	r_asn       	c_asn%rowtype;
	r_sta		c_sta%rowtype;
	l_ok_yn         varchar2(1);
	l_rtn		varchar2(30) := 'process_as_other_messages';
	l_busy	dcsdba.system_profile.text_data%type;
	l_hme_pick	number;
	--
begin
	select	text_data
	into	l_busy
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSOTHERS_OTHERSBUSY'
	;
	if	l_busy = 'SLEEPING'
	then	
		-- set parameter to busy
		update	dcsdba.system_profile
		set	text_data = 'BUSY'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSOTHERS_OTHERSBUSY'
		;
		commit;

		<<hme_synq_loop>>
		for	r_hme_synq in c_hme_synq
		loop
			if      r_hme_synq.message_type = 'InventoryStatus'
			then
				begin
					open    c_hlu( r_hme_synq.host_message_table_key);
					fetch   c_hlu
					into    r_hlu;
					close   c_hlu;
					--
					if      r_hlu.reason_text = 'Putaway'
					then    
						-- Putaway/relocate
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','InventoryStatus putaway for tag_id ' || r_hlu.asn_tu_id
											|| ', host_message_id ' || r_hme_synq.message_id
											|| ', host_message_status ' || r_hme_synq.message_status
											);
						--
						cnl_sys.cnl_as_inbound_pck.asn_check_in_confirmation( p_hme_tbl_key_i   => r_hme_synq.host_message_table_key
												    , p_client_i        => r_hlu.owner_id
												    , p_ok_yn_o         => l_ok_yn
												    ); 
						--
						if  nvl(l_ok_yn,'Y') = 'Y'
						then
							update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
							set     message_status = 'SUCCESS'
							where   host_message_key = r_hme_synq.host_message_key;
						else
							update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
							set     message_status = 'ERROR'
							where   host_message_key = r_hme_synq.host_message_key;
						end if;
						--
					else
						--Adjustment
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','InventoryStatus adjustment for tag_id ' || r_hlu.asn_tu_id
																     || ', host_message_id ' || r_hme_synq.message_id
																     || ', host_message_status ' || r_hme_synq.message_status
																     );
						--
						l_ok_yn := null;
						-- Site id is currently set to NLTLG01 but should be variable.
						cnl_sys.cnl_as_inventory_pck.autostore_adjustment( p_key_i     => r_hme_synq.host_message_table_key
												 , p_site_id_i => 'NLTLG01'
												 , p_ok_yn_o   => l_ok_yn
												 );
						--
						if  nvl(l_ok_yn,'Y') = 'Y'
						then
							update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
							set     message_status = 'SUCCESS'
							where   host_message_key = r_hme_synq.host_message_key;
						else
							update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
							set     message_status = 'ERROR'
							where   host_message_key = r_hme_synq.host_message_key;
						end if;						
					end if;
					--
				exception 
					when others
					then
						cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
										  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
										  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
										  , p_package_name_i		=> g_pck				-- Package name the error occured
										  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
										  , p_routine_parameters_i	=> null					-- list of all parameters involved
										  , p_comments_i		=> null					-- Additional comments describing the issue
										  );
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','Exception check CNL_ERROR');
						commit;
				end;
				commit;
			end if; 			-- inventory status
			--
			if      r_hme_synq.message_type = 'InventoryReconciliation'
			then
				begin
					update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
					set     message_status 		= 'SUCCESS'
					where   host_message_key 	= r_hme_synq.host_message_key;
				exception
					when others
					then
						cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
										  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
										  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
										  , p_package_name_i		=> g_pck				-- Package name the error occured
										  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
										  , p_routine_parameters_i	=> null					-- list of all parameters involved
										  , p_comments_i		=> null					-- Additional comments describing the issue
										  );
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','Exception check CNL_ERROR');
						commit;
				end;
				commit;
			end if; 		-- InventoryReconciliation
			--
			if      r_hme_synq.message_type = 'AsnCheckInConfirmation'
			then
				begin
					open    c_asn( r_hme_synq.host_message_table_key);
					fetch   c_asn
					into    r_asn;
					close   c_asn;
					--
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','ASNReceivingNotification for tag_id ' || r_asn.tu_id
															|| ', host_message_id ' || r_hme_synq.message_id                                                                                                                 
															|| ', host_message_status ' || r_hme_synq.message_status
															);
					--
					update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
					set     message_status = 'SUCCESS'
					where   host_message_key = r_hme_synq.host_message_key;
					--
					update  cnl_sys.cnl_as_inb_tasks
					set     cnl_if_status = 'Complete'
					where   wms_mt_tag_id = r_asn.tu_id;
				--
				exception
					when others
					then
						cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
										  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
										  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
										  , p_package_name_i		=> g_pck				-- Package name the error occured
										  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
										  , p_routine_parameters_i	=> null					-- list of all parameters involved
										  , p_comments_i		=> null					-- Additional comments describing the issue
										  );
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','Exception check CNL_ERROR');
						commit;
				end;
				commit;
				--
			end if; 		-- end ASNReceivingNotification
			--
			if      r_hme_synq.message_type = 'OrderStatusChangeNotification'
			then
				l_hme_pick	:= 0;
				open	c_hme_pick;
				fetch	c_hme_pick
				into 	l_hme_pick;
				close	c_hme_pick;
				if	l_hme_pick = 0
				then
					begin
						open    c_sta( r_hme_synq.host_message_table_key);
						fetch   c_sta
						into    r_sta;
						close   c_sta;
						--
						if      upper(r_sta.state) = 'COMPLETED' or 
							upper(r_sta.state) = 'CANCELLED'
						then
							cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','OrderStatusChangeNotification for client_id ' || r_sta.owner_id
																	|| ', order_id ' || r_sta.order_id
																	|| ', order_status ' || r_sta.state
																	|| ', host_message_id ' || r_hme_synq.message_id                                                                                                                 
																	|| ', host_message_status ' || r_hme_synq.message_status
																	);
							--
							cnl_sys.cnl_as_outbound_pck.order_status_change( r_sta.order_id
												       , r_sta.owner_id
												       , r_sta.state
												       );
							--
							commit;
						end if;
						--
						update  rhenus_synq.host_message_exchange@as_synq.rhenus.de
						set     message_status = 'SUCCESS'
						where   host_message_key = r_hme_synq.host_message_key;
						--
					exception
						when others
						then
							cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
											  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
											  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
											  , p_package_name_i		=> g_pck				-- Package name the error occured
											  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
											  , p_routine_parameters_i	=> null					-- list of all parameters involved
											  , p_comments_i		=> null					-- Additional comments describing the issue
											  );
							cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','Exception check CNL_ERROR');
							commit;
					end;
					commit;
					--
				end if;
			end if; -- OrderStatusChangeNotification
		end loop; -- hme_synq_loop
		-- set parameter to busy
		update	dcsdba.system_profile
		set	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSOTHERS_OTHERSBUSY'
		;
		commit;
	end if;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.process_as_other_messages','Exception check CNL_ERROR');
		-- set parameter to busy
		update	dcsdba.system_profile
		set	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_AUTOSTORE_PROCESSOTHERS_OTHERSBUSY'
		;
		commit;
end process_as_other_messages;

/***************************************************************************************************************
* Create log record
***************************************************************************************************************/              
procedure create_log_record( p_source_i         in varchar2
			   , p_description_i    in varchar2 
			   )
is
        l_exception	varchar2(4000);
	l_rtn		varchar2(30) := 'create_log_record';
	pragma		autonomous_transaction;
begin
		insert 
		into 	cnl_as_log
		( 	dstamp
		, 	source
		, 	description
		)
		values
		(	sysdate
		, 	substr(p_source_i,1,100)
		, 	substr(p_description_i,1,4000)
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
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		commit;
end create_log_record;

/***************************************************************************************************************
* Housekeeping
***************************************************************************************************************/              
procedure as_housekeeping
is
        cursor c_vas
        is
		select  order_id
		,       client_id
		from    cnl_container_vas_activity
        ;
        --
        cursor c_ord( b_order_id    varchar2
                    , b_client_id   varchar2
                    )
        is
		select  archived
		from    dcsdba.order_header 
		where   order_id = b_order_id
		and     client_id = b_client_id
        ;
        --
        r_ord c_ord%rowtype;
	l_rtn	varchar2(30) := 'as_housekeeping';
begin
        delete  cnl_as_adjustment
        where   processed = 'Y'
        or      to_char(creation_dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD');
        --
        delete  cnl_as_container_suspect
        where   to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -90,'YYYYMMDD');
        --
        delete  cnl_as_inb_tasks
        where   (to_char(dstamp,'YYYYMMDD') = to_char(sysdate -1,'YYYYMMDD') and cnl_if_status = 'Complete')
        or      (to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD')); -- Tasks are one year old.
        --
        delete  cnl_as_log
        where   to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -1,'YYYYMMDD');
        --
        delete  cnl_as_maas_logging
        where   to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -7,'YYYYMMDD');
        --
        delete  cnl_as_manual_lines
        where   finished = 'Y'
        or      (to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD')); -- Tasks are one year old.
        --
        delete  cnl_as_manual_order_started
        where   (to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -90,'YYYYMMDD'));
        --
        delete  cnl_as_masterdata
        where   (cnl_if_status = 'HmeProductMasterProcessed' and to_char(dstamp,'YYYYMMDD') = to_char(sysdate -1,'YYYYMMDD'))
        or      (to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD'));
        --
        delete  cnl_as_orders
        where   (to_char(creation_date,'YYYYMMDD') = to_char(sysdate -7,'YYYYMMDD') and cnl_if_status = 'Completed')
        or      (to_char(creation_date,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD'));
        --
        delete  cnl_as_pick_task
        where   to_char(creation_date,'YYYYMMDD') <= to_char(sysdate -7,'YYYYMMDD')
        and    ( (cnl_if_status   = 'DeAllocated' or  cnl_if_status   = 'Picked' or  cnl_if_status   = 'ErrorKeyNotFound') or
		 wms_mt_key not in (select key from dcsdba.move_task where task_type = 'O'));
        --
        delete cnl_as_picked_attributes
        where   to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -90,'YYYYMMDD');
        --
        delete  cnl_as_processed_host_message a
	where 	to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -14,'YYYYMMDD');
        --
        delete  cnl_as_tu
        where   (to_char(dstamp,'YYYYMMDD') = to_char(sysdate -7,'YYYYMMDD') and cnl_if_status   = 'Sorted')
        or      (to_char(dstamp,'YYYYMMDD') = to_char(sysdate -90,'YYYYMMDD'));
        --      
        for     r_vas in c_vas 
        loop
            open    c_ord(r_vas.order_id,r_vas.client_id);
            fetch   c_ord into r_ord;
            if      c_ord%notfound
            then
                    close c_ord;
                    delete  cnl_container_vas_activity
                    where   order_id    = r_vas.order_id
                    and     client_id   = r_vas.client_id;
            else
                    close   c_ord;
                    if      r_ord.archived = 'Y'
                    then
                            delete  cnl_container_vas_activity
                            where   order_id    = r_vas.order_id
                            and     client_id   = r_vas.client_id;
                    end if;
            end if;
        end loop;
        --
        commit;
        --
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
end as_housekeeping;

/***************************************************************************************************************
* Synq Serial number validation 
***************************************************************************************************************/
procedure serial_validation( p_tu_id_i       in  varchar2
			   , p_owner_id_i    in  varchar2
			   , p_order_id_i    in  varchar2
			   , p_product_id_i  in  varchar2
			   , p_serial_id_i   in  varchar2
			   , p_continue_o    out varchar2
			   , p_suspect_o     out varchar2
			   , p_message_o     out varchar2
			   )
is
	-- Get possible barcodes on product
	cursor	c_chk
	is
		select 	distinct 
			1			hit
		,	'SUP' 			id
		from	dcsdba.supplier_sku	p
		where	p.sku_id 		= p_product_id_i
		and	p.supplier_sku_id 	= p_serial_id_i
		and	p.client_id		= p_owner_id_i
		union
		select	distinct
			1
		,	'TUC' 			id
		from	dcsdba.sku_tuc 		t
		where	t.sku_id 		= p_product_id_i
		and	t.tuc 			= p_serial_id_i
		and	t.client_id 		= p_owner_id_i
		union
		select	1
		,	'SKU' 			id
		from	dcsdba.sku 		t
		where	t.sku_id 		= p_product_id_i
		and	t.sku_id		= p_serial_id_i
		and	t.client_id 		= p_owner_id_i
		union
		select	1
		,	'EAN' 			id
		from	dcsdba.sku 		t
		where	t.sku_id 		= p_product_id_i
		and	t.ean			= p_serial_id_i
		and	t.client_id 		= p_owner_id_i
		and	t.ean			is not null
		union
		select	1
		,	'UPC' 			id
		from	dcsdba.sku 		t
		where	t.sku_id 		= p_product_id_i
		and	t.upc			= p_serial_id_i
		and	t.client_id 		= p_owner_id_i
		and	t.upc			is not null
	;
	-- Get SKU and FA details
	cursor	c_sku
	is
		select	s.serial_format
		,       nvl(s.serial_no_reuse,'N')    
			serial_no_reuse
		,       nvl(s.serial_at_receipt,'N')  
			serial_at_receipt
		,	(	select	'Y' 
					reuse_serial
				from   	dcsdba.function_access f
				where  	f.function_id 	= 'SERIAL_REUSE'
				and    	f.enabled 	= 'G'
			)	
			global_fa_reuse
		from    dcsdba.sku	s
		where   client_id   	= p_owner_id_i
		and     sku_id      	= p_product_id_i
	;
	--
	cursor  c_serial
	is
		select  s.status
		,	s.order_id
		,	s.container_id
		,	s.pallet_id
		from    dcsdba.serial_number s
		where   upper(s.serial_number)  = upper(p_serial_id_i)
		and     upper(client_id)        = upper(p_owner_id_i)
		and     upper(sku_id)           = upper(p_product_id_i)
	;
	--
	cursor	c_characters
	is
		select  allowed_character
		from    dcsdba.serial_number_characters
	;
	-- local variables
	l_mis_match	varchar2(1)	:= 'Y';
	l_format	dcsdba.sku.serial_format%type;
	l_character	varchar2(1);
	l_chk		varchar2(3);

	-- exceptions
	e_barcode	exception;
	e_inv_sku	exception;
	e_ser_rec	exception;
	e_due_in	exception;
	e_pending	exception;
	e_no_reuse	exception;
	e_picked	exception;
	e_length	exception;
	e_format	exception;
	-- Cursor variable
	r_sku       	c_sku%rowtype;
	r_serial    	c_serial%rowtype;
begin
	-- Loop all possible barcodes on box
	<<barcode_loop>>
	for	r_chk in c_chk
	loop
		l_chk	:= r_chk.id;
		raise	e_barcode;--exit barcode_loop when l_next = 'N';
	end loop; -- barcode loop

	-- Fetch SKU details
	open    c_sku;
	fetch   c_sku 
	into 	r_sku;
	if      c_sku%notfound
	then
		close	c_sku;
		raise	e_inv_sku;
	end if;	

	-- Get serial number
	open    c_serial;
	fetch   c_serial 
	into 	r_serial;
	if      c_serial%notfound
	then	-- When no serial is found check if serial at receiptis enabled because that means a serial must exist.
		close   c_serial;
		if	r_sku.serial_at_receipt = 'Y'
		then
			raise	e_ser_rec;
		else
			p_continue_o	:= 'Y';
			p_suspect_o   	:= 'N';
			p_message_o   	:= null;
		end if;
	else
		-- Due in serial
		close	c_serial;
		if      r_serial.status = 'D'
		then	
			raise	e_due_in;
		end if;
		-- Peding serial
		if      r_serial.status = 'P'
		then
			raise	e_pending;
		end if;
		-- Shipped serial
		if	r_serial.status 	= 'S'
		and	(	r_sku.serial_no_reuse		= 'Y'
			or	nvl(r_sku.global_fa_reuse,'N')	= 'N'
			)
		then
			raise	e_no_reuse;
		else
			p_continue_o	:= 'Y';
			p_suspect_o   	:= 'N';
			p_message_o   	:= null;			
		end if;
		-- Serial linked to order
		if	r_serial.status 	= 'I'
		and	r_serial.order_id 	is not null
		then
			raise	e_picked;
		end if;
	end if;
		-- Check format
	if      r_sku.serial_format is not null
	then
		-- Serial is shorter than format
		if      length(p_serial_id_i) < length(r_sku.serial_format)
		or	length(p_serial_id_i) > length(r_sku.serial_format)
		then
			raise	e_length;
		end if;
		-- Start checking each character in serial 
		<<position_loop>>
		for     r_pos in 1..length(p_serial_id_i)
		loop    
			l_format    := substr(r_sku.serial_format,r_pos,1);
			l_character := substr(p_serial_id_i,r_pos,1);

			if	l_character = l_format
			or	l_format 	= '*'
			then
				continue position_loop;
			end if;

			-- CHeck if format requires alpha characters
			if      l_format = 'A'
			then    
				-- Check for special characters
				<<special_char_loop>>
				for     r_character in c_characters
				loop
					if      l_character = r_character.allowed_character
					then   
						continue position_loop;
					end if;
				end loop ;
				-- Check for regular alpha characters
				if      regexp_instr(l_character,'[[:alpha:]]') = 0 
				then    
					raise	e_format;
				else
					continue position_loop;
				end if; 
			end if;

			-- Check if format requires numeric characters
			if      l_format = 'N'
			and	is_number(l_character) = 1
			then
				continue position_loop;
			else
				raise	e_format;
			end if;
		end loop;-- position_loop
	end if;	

	if	p_continue_o is null
	then
		p_continue_o 	:= 'Y';
		p_suspect_o 	:= 'N';
		p_message_o 	:= null;
	end if;
exception
	when 	e_barcode
	then
		p_continue_o	:= 'N';
		p_suspect_o	:= 'N';
		p_message_o	:= 'You scanned the '||l_chk||' instead of the serial number. Please scan again';
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned the wrong barcode and has to try again');
	when	e_inv_sku
	then
		p_continue_o  	:= 'N';
		p_suspect_o   	:= 'N';
		p_message_o	:= 'SKU not found in WMS. Please check SKU id.';
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned the wrong barcode and has to try again');
	when	e_ser_rec
	then
		p_continue_o	:= 'Y';
		p_suspect_o   	:= 'Y';
		p_message_o   	:= 'Serial at receipt enabled so only existing serials are allowed. You can finish this Pick but TU is marked as suspect';
		add_vas_activity( p_container_id_i           => p_tu_id_i
			        , p_client_id_i              => p_owner_id_i
			        , p_order_id_i    	     => p_order_id_i          
			        , p_sku_id_i                 => p_product_id_i
			        , p_activity_name_i          => 'SERIAL-CHECK'
			        , p_activity_sequence_i      => 2
			        , p_activity_instruction_i   => 'A serial number was scanned that did not exist while serial at receipt is enabled for Serial number '
							     ||  p_serial_id_i
			        );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number that does not exist while serial_at_receipt is enabled');
	when	e_due_in
	then	
		p_continue_o 	:= 'Y';
		p_suspect_o   	:= 'Y';
		p_message_o   	:= 'Serial number has status Due in and should not be on stock yet. You may continue this pick but container is marked as suspect';
		add_vas_activity( p_container_id_i           => p_tu_id_i
			        , p_client_id_i              => p_owner_id_i
			        , p_order_id_i    	     => p_order_id_i          
			        , p_sku_id_i                 => p_product_id_i
			        , p_activity_name_i          => 'SERIAL-CHECK'
			        , p_activity_sequence_i      => 2
			        , p_activity_instruction_i   => 'A serial with status due in was scanned during picking for Serial number '
							     ||  p_serial_id_i
			        );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number with status due in so it should not be on stock yet.');
	when	e_pending
	then
		p_continue_o 	:= 'Y';
		p_suspect_o   	:= 'Y';
		p_message_o   	:= 'Serial number has status Pending and can not be picked yet. You may continue this pick but container is marked as suspect';
		add_vas_activity( p_container_id_i           => p_tu_id_i
			        , p_client_id_i              => p_owner_id_i
			        , p_order_id_i    	     => p_order_id_i          
			        , p_sku_id_i                 => p_product_id_i
			        , p_activity_name_i          => 'SERIAL-CHECK'
			        , p_activity_sequence_i      => 2
			        , p_activity_instruction_i   => 'A serial with status pending was scanned during picking for Serial number '
							     ||  p_serial_id_i
			        );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number that has status pending and could not be picked');
	when	e_no_reuse
	then
		p_continue_o 	:= 'Y';
		p_suspect_o	:= 'Y';
		p_message_o   	:= 'Serial number has status Shipped and reusing serials is not allowed. You can finish pick but container is marked as suspect';
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number that already was shipped before while reusing is not allowed.');
	when	e_picked
	then	
		p_continue_o 	:= 'Y';
		p_suspect_o	:= 'Y';
		p_message_o   	:= 'This serial number was already picked for another order. You can finish pick but container is marked as suspect';
		add_vas_activity( p_container_id_i           => p_tu_id_i
			        , p_client_id_i              => p_owner_id_i
			        , p_order_id_i    	     => p_order_id_i          
			        , p_sku_id_i                 => p_product_id_i
			        , p_activity_name_i          => 'SERIAL-CHECK'
			        , p_activity_sequence_i      => 2
			        , p_activity_instruction_i   => 'Serial number '
							     || p_serial_id_i
							     || ' is already picked for order '
							     || r_serial.order_id
							     || ' in container '
							     || r_serial.container_id
							     || ' and on pallet '
							     ||  r_serial.pallet_id
			        );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number that was picked for another order.');
	when	e_length
	then
		p_continue_o  	:= 'N';
		p_suspect_o   	:= 'N';
		p_message_o   	:= 'Entered serial number is to short or to long!';
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number that is not matching the format');
	when	e_format
	then
		p_continue_o  	:= 'N';
		p_suspect_o   	:= 'N';
		p_message_o   	:= 'Entered serial number is not according specified format!';
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.serial_validation','Operator scanned a serial number that is not matching the format');
end serial_validation;

/***************************************************************************************************************
* add vas activity
***************************************************************************************************************/
procedure add_vas_activity( p_container_id_i           in varchar2 default null
                          , p_client_id_i              in varchar2 default null
			  , p_order_id_i               in varchar2 default null
			  , p_sku_id_i                 in varchar2 default null
			  , p_activity_name_i          in varchar2 
			  , p_activity_sequence_i      in varchar2 default null
			  , p_activity_instruction_i   in varchar2 default null
			  )
is
        cursor c_act(b_name varchar2)
        is
		select  id
		from    cnl_sys.cnl_vas_activity
		where   upper(activity_name) = upper(b_name)
        ;
        --
	cursor c_container_vas( b_container_id	varchar2
			      , b_client_id	varchar2
			      , b_order_id	varchar2
			      , b_name		varchar2
			      , b_sku		varchar2
			      )
	is
		select	count(*)
		from	cnl_sys.cnl_container_vas_activity v
		where	(v.container_id = b_container_id or v.container_id is null)
		and	v.client_id = b_client_id
		and	v.order_id = b_order_id
		and	(v.sku_id = b_sku or v.sku_id is null)
		and	upper(v.activity_name) = upper(b_name)
	;
	--
	r_con			number;
        r_act                   c_act%rowtype;
        l_container_id          varchar2(30)    := p_container_id_i;
        l_client_id             varchar2(30)    := p_client_id_i;
        l_order_id              varchar2(30)    := p_order_id_i;
        l_sku_id                varchar2(50)    := p_sku_id_i;
        l_name                  varchar2(50)    := p_activity_name_i;
        l_sequence              number          := p_activity_sequence_i;
        l_instruction           varchar2(4000)  := p_activity_instruction_i;
        l_exception             varchar2(4000);
	l_rtn			varchar2(30) 	:= 'add_vas_activity';
        --
        pragma autonomous_transaction;    
begin
        open    c_act(p_activity_name_i);
        fetch   c_act into r_act;
        if      c_act%notfound
        then
                close c_act;
                null;
        else
                close c_act;
		-- Check if VAS activity already exists 
		open 	c_container_vas( l_container_id, l_client_id, l_order_id, l_name, l_sku_id);
		fetch 	c_container_vas into r_con;
		close 	c_container_vas;
		if	r_con = 0
		then
			insert into cnl_sys.cnl_container_vas_activity( container_id
								      , client_id
								      , order_id
								      , sku_id
								      , activity_name
								      , activity_sequence
								      , activity_instruction
								      )
								   values( l_container_id
									 , l_client_id
									 , l_order_id
									 , l_sku_id
									 , l_name
									 , l_sequence
									 , l_instruction
									 );
		end if;
                commit;
        end if;
exception 
        when others
        then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.add_vas_activity','Exception check CNL_ERROR');
		commit;
end add_vas_activity;

/***************************************************************************************************************
* fetch client vas activity
***************************************************************************************************************/
procedure fetch_client_vas_activity( p_client_id_i      in varchar2
				   , p_container_i      in varchar2 
				   , p_order_id_i       in varchar2
				   )
is
        cursor c_ord
        is
		select  country
		,       customer_id
		from    dcsdba.order_header
		where   client_id   = p_client_id_i
		and     order_id    = p_order_id_i
        ;
        -- Select client VAS activity thatare not for specific SKU's.
        cursor c_clt( b_client      varchar2
                    , b_customer    varchar2
                    , b_country     varchar2
                    )
        is
		select  clt.id
		,       act.activity_name
		,       clt.vas_code_id
		,       clt.activity_sequence
		,       clt.activity_instruction
		from    cnl_client_vas_activity clt
		,       cnl_vas_activity act
		where   clt.sku_id is null
		and     clt.client_id               = b_client
		and     (clt.customer_id = b_customer or clt.customer_id is null) --Customer id matches or is not used
		and     (clt.country = b_country or clt.country is null) -- country matched or is not used
		and     act.id = clt.activity_id
        ;
        --
        cursor c_inv( b_client      varchar2
                    , b_container   varchar2
                    )
        is
		select  distinct sku_id
		from    dcsdba.inventory
		where   container_id    = b_container
		and     client_id       = b_client
        ;
        -- Select SKU specific VAS activities
        cursor c_sku( b_client      varchar2
                    , b_sku         varchar2
		    , b_country	    varchar2
		    , b_customer    varchar2
                    )
        is
		select  clt.id
		,       act.activity_name
		,       clt.vas_code_id
		,       clt.activity_sequence
		,       clt.activity_instruction
		from    cnl_client_vas_activity clt
		,       cnl_vas_activity act
		where   clt.sku_id = b_sku
		and     clt.client_id   = b_client
		and     act.id          = clt.activity_id
		and	    (clt.country = b_country or clt.country is null)
		and	    (clt.customer_id = b_customer or clt.customer_id is null)
        ;
        --       
	cursor c_container_vas( b_container_id	varchar2
			      , b_client_id	varchar2
			      , b_order_id	varchar2
			      , b_name		varchar2
			      , b_sku		varchar2
			      )
	is
		select	count(*)
		from	cnl_sys.cnl_container_vas_activity v
		where	(v.container_id = b_container_id or v.container_id is null)
		and	v.client_id = b_client_id
		and	v.order_id = b_order_id
		and	(v.sku_id = b_sku or v.sku_id is null)
		and	upper(v.activity_name) = upper(b_name)
	;
	--
        r_ord       	c_ord%rowtype;
	r_con	    	number;
	l_rtn		varchar2(30) := 'fetch_client_vas_activity';
begin
        open    c_ord;
        fetch   c_ord into r_ord;
        if      c_ord%notfound
        then
                close   c_ord;
        else
                close   c_ord;
                for     r_clt in c_clt( p_client_id_i, r_ord.customer_id, r_ord.country)
                loop
			-- Check if VAS activity already exists for container.
			open 	c_container_vas( p_container_i, p_client_id_i, p_order_id_i, r_clt.activity_name, null);
			fetch 	c_container_vas into r_con;
			close 	c_container_vas;
			if	r_con = 0
			then
				add_vas_activity( p_container_id_i           => p_container_i
						, p_client_id_i              => p_client_id_i
						, p_order_id_i               => p_order_id_i
						, p_activity_name_i          => r_clt.activity_name
						, p_activity_sequence_i      => r_clt.activity_sequence
						, p_activity_instruction_i   => r_clt.activity_instruction
						);
			end if;
                end loop;
                --
                for     r_inv in c_inv( p_client_id_i, p_container_i)
                loop
                        for     r_sku in c_sku( p_client_id_i, r_inv.sku_id, r_ord.country ,r_ord.customer_id )
                        loop
				-- Check if VAS activity already exists for container.
				open 	c_container_vas( p_container_i, p_client_id_i, p_order_id_i, r_sku.activity_name, r_inv.sku_id);
				fetch 	c_container_vas into r_con;
				close 	c_container_vas;
				if	r_con = 0
				then
					add_vas_activity( p_container_id_i           => p_container_i
							, p_client_id_i              => p_client_id_i
							, p_order_id_i               => p_order_id_i
							, p_sku_id_i                 => r_inv.sku_id
							, p_activity_name_i          => r_sku.activity_name
							, p_activity_sequence_i      => r_sku.activity_sequence
							, p_activity_instruction_i   => r_sku.activity_instruction
							);
				end if;
                        end loop;
                end loop;
        end if;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_pck.fetch_client_vas_activity','Exception check CNL_ERROR');	
end fetch_client_vas_activity;

/***************************************************************************************************************
* Initialization
***************************************************************************************************************/
    begin
    null;    

end cnl_as_pck;