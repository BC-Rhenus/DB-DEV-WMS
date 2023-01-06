CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_ARCHIVE_PURGE_PCK" is
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
	g_pck		varchar2(30) := 'cnl_archive_purge_pck';
	g_database	varchar2(10);
--
-- Private variable declarations
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Archive scheduler jobs and purge archived records
------------------------------------------------------------------------------------------------
	procedure p_cnl_scheduler_job_archive
	is
		cursor	c_arch_purge_settings
		is
			select	purge_days
			from	cnl_archive_purge_settings
			where	table_name = 'CNL_SCHEDULER_JOB_ARCHIVE'
		;
		l_purge_days		cnl_sys.cnl_archive_purge_settings.purge_days%type;
		l_rtn 				varchar2(30) 	:= 'p_cnl_scheduler_job_archive';
		pragma autonomous_transaction;
	begin
		insert
		into	scheduler_job_run_details_arch@purgearchivedb
		select	log_id
		,	log_date
		,	owner
		,	job_name
		,	job_subname
		,	status
		,	error#
		,	req_start_date
		,	actual_start_date
		,	run_duration
		,	instance_id
		,	session_id
		,	slave_pid
		,	cpu_used
		,	credential_owner
		,	credential_name
		,	destination_owner
		,	destination
		,	additional_info
		,	sysdate
		from	dba_scheduler_job_run_details
		where 	owner in ('DCSDBA','CNL_SYS')
		and 	trunc(log_date) = trunc(sysdate -1)
		;
		commit;
		open	c_arch_purge_settings;
		fetch 	c_arch_purge_settings
		into	l_purge_days;
		if	c_arch_purge_settings%notfound
		then
			close	c_arch_purge_settings;
			null;
		else
			close	c_arch_purge_settings;
			delete 	scheduler_job_run_details_arch@purgearchivedb
			where	archive_date = sysdate - l_purge_days
			;
		end if;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_scheduler_job_archive;

------------------------------------------------------------------------------------------------
-- Archive run tasks and purge archived records
------------------------------------------------------------------------------------------------
	procedure p_cnl_run_task_archive
	is
		cursor	c_arch_purge_settings(b_table_name varchar2)
		is
			select	purge_days
			from	cnl_archive_purge_settings
			where	table_name = b_table_name
		;
		l_purge_days_run_task		cnl_sys.cnl_archive_purge_settings.purge_days%type;
		l_purge_days_run_task_err	cnl_sys.cnl_archive_purge_settings.purge_days%type;
		l_rtn 				varchar2(30) 	:= 'p_cnl_run_task_archive';
		pragma autonomous_transaction;
	begin
		insert	
		into	run_task_archive@purgearchivedb
		select	key
		,	site_id
		,	station_id
		,	user_id
		,	status
		,	command
		,	pid
		, 	old_dstamp
		,	dstamp
		,	language
		,	name
		,	time_zone_name
		,	nls_calendar
		,	print_label
		,	java_report
		,	run_light
		,	server_instance
		,	priority
		,	archive
		,	archive_ignore_screen
		,	archive_restrict_user
		,	client_id
		,	email_recipients
		,	email_attachment
		,	email_subject
		,	email_message
		,	master_key
		,	use_db_time_zone
		,	sysdate
		from	dcsdba.run_task
		where	trunc(dstamp) = trunc(sysdate -1)
		;

		insert	
		into	run_task_err_archive@purgearchivedb
		select	e.key
		,	e.line_id
		,	e.text
		,	sysdate
		from	dcsdba.run_task_err e
		where	e.key > (select max(key) from run_task_err_archive@purgearchivedb)
		;


		open	c_arch_purge_settings('CNL_RUN_TASK_ARCHIVE');
		fetch	c_arch_purge_settings
		into	l_purge_days_run_task;
		if	c_arch_purge_settings%notfound
		then
			close	c_arch_purge_settings;
		else
			close	c_arch_purge_settings;
			delete	run_task_archive@purgearchivedb
			where	archive_date < sysdate - l_purge_days_run_task
			;

			open	c_arch_purge_settings('CNL_RUN_TASK_ERR_ARCHIVE');
			fetch	c_arch_purge_settings
			into	l_purge_days_run_task_err;
			if	c_arch_purge_settings%notfound
			then
				close	c_arch_purge_settings;
			else
				close	c_arch_purge_settings;
				delete	run_task_err_archive@purgearchivedb
				where	archive_date < sysdate - l_purge_days_run_task_err
				;
			end if;
		end if;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_run_task_archive;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_adjustment
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_adjustment
	is
		l_rtn	varchar2(30) := 'p_cnl_as_adjustment';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_adjustment
		where   processed = 'Y'
		or      to_char(creation_dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD');
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_adjustment;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_container_suspect
------------------------------------------------------------------------------------------------
        procedure p_cnl_as_container_suspect
	is
		l_rtn varchar2(30) := 'p_cnl_as_container_suspect';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_container_suspect
		where   to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -90,'YYYYMMDD');
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_container_suspect;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_inb_tasks
------------------------------------------------------------------------------------------------
        procedure p_cnl_as_inb_tasks
	is
	l_rtn varchar2(30) := 'p_cnl_as_inb_tasks';
	pragma autonomous_transaction;
	begin
		delete	cnl_as_inb_tasks
		where   ((to_char(dstamp,'YYYYMMDD') = to_char(sysdate -1,'YYYYMMDD') and cnl_if_status = 'Complete') or	
			 (to_char(dstamp,'YYYYMMDD') = to_char(sysdate -7,'YYYYMMDD') and wms_mt_key not in ( select	key 
													      from 	dcsdba.move_task 
													      where 	task_type in ('M','P')
													    )) or
			 (to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD'))); -- Tasks are one year old.
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_inb_tasks;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_log
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_log
	is
		cursor c_set( b_table varchar2)
		is
			select 	nvl(s.purge_days,0)
			,	nvl(s.archive_days,0)
			from 	cnl_sys.cnl_archive_purge_settings s
			where 	s.table_name = b_table
		;
		--
		l_rtn varchar2(30) := 'p_cnl_as_log';
		v_purge_days	number;
		v_archive_days	number;	
		pragma autonomous_transaction;

	begin
		-- Fetch archive purge settings for main table
		open 	c_set('CNL_AS_LOG');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;
		--
		if	nvl(v_purge_days,0) = 0
		then
			null;
		else
			delete  cnl_as_log
			where   dstamp <= sysdate -v_purge_days;
		end if;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_log;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_maas_logging
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_maas_logging
	is
		cursor c_set( b_table varchar2)
		is
			select 	nvl(s.purge_days,0)
			,	nvl(s.archive_days,0)
			from 	cnl_sys.cnl_archive_purge_settings s
			where 	s.table_name = b_table
		;
		--
		l_rtn varchar2(30) := 'p_cnl_as_maas_logging';
		v_purge_days	number;
		v_archive_days	number;	
		pragma autonomous_transaction;
	begin
		-- Fetch archive purge settings for main table
		open 	c_set('CNL_AS_MAAS_LOGGING');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;

		-- Select records to archive
		if	v_archive_days is null or v_archive_days = 0
		then	-- No archive
			null;
		else	-- Set all records that require archiving
			Update	cnl_as_maas_logging o
			set 	o.archive_pending = 'Y'
			where	nvl(o.archived,'N') = 'N'
			and	to_date(to_char(o.dstamp,'YYYYMMDD'),'YYYYMMDD') <= to_date(to_char(sysdate-v_archive_days,'YYYYMMDD'),'YYYYMMDD')
			;

			-- Insert selected data in archive table
			insert into cnl_as_maas_logging_archives
			select 	key
			,	mhe_position_number
			,	dstamp
			,	container_id
			,	mhe_station_id
			,	dws_package_type
			,	dws_box_weight
			,	dws_box_height
			,	dws_box_width
			,	dws_box_depth
			,	sort_pallet_id
			,	sort_pallet_type
			,	print_documents
			,	close_box
			,	bypass
			,	print_label
			,	sortation_loc
			,	tracking_number
			,	'Can''t copy instruction to archive due to database link restriction' instruction
			,	ok
			,	error_message
			,	skip_validation
			,	match_or_contains
			,	archived
			,	sysdate
			,	'N' archive_pending
			from	cnl_as_maas_logging
			where	archive_pending = 'Y'
			;

			-- Update records to archived
			Update 	cnl_as_maas_logging
			set 	archived = 'Y'
			,	archive_pending = 'N'
			,	archived_dstamp = sysdate
			where	archive_pending = 'Y'
			;
		end if;

		-- Purge main table
		if	v_purge_days is null or v_purge_days = 0
		then
			null;
		else	-- purge old records archived just before.
			delete 	cnl_as_maas_logging o
			where	o.archived = 'Y'
			and	to_date(to_char(o.dstamp,'YYYYMMDD'),'YYYYMMDD') <= to_date(to_char(sysdate - v_purge_days,'YYYYMMDD'),'YYYYMMDD')
			;
		end if;

		-- Purge archive table
		v_purge_days 	:= null;
		v_archive_days 	:= null;
		open 	c_set('CNL_MAAS_LOGGING_ARCHIVES');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;
		if	v_purge_days is null or v_purge_days = 0
		then
			null;
		else
			delete 	cnl_as_maas_logging_archives a
			where	a.archived_dstamp < sysdate - v_purge_days
			;
		end if;

		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_maas_logging;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_manual_lines
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_manual_lines
        is
		l_rtn varchar2(30) := 'p_cnl_as_manual_lines';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_manual_lines l
		where   l.finished = 'Y'
		or      to_char(l.dstamp,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD')
		or	( l.finished != 'Y' and	l.mt_key not in ( select	m.key 
								  from 		dcsdba.move_task m
								  where 	m.key = l.mt_key))
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_manual_lines;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_manual_order_start
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_manual_order_started
        is
		l_rtn varchar2(30) := 'p_cnl_as_manual_order_started';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_manual_order_started s
		where   to_char(s.dstamp,'YYYYMMDD') <= to_char(sysdate -90,'YYYYMMDD')
		or	(	select	o.status 
				from 	dcsdba.order_header o 
				where 	o.client_id 	= s.client_id 
				and 	o.order_id 	= s.order_id
			) in ('Shipped','Cancelled','Delivered')
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_manual_order_started;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_masterdata
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_masterdata
	is
		l_rtn varchar2(30) := 'p_cnl_as_masterdata';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_masterdata
		where   (cnl_if_status = 'HmeProductMasterProcessed' and to_char(dstamp,'YYYYMMDD') = to_char(sysdate -1,'YYYYMMDD'))
		or	to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -7,'YYYYMMDD')
		;
		commit;
	end p_cnl_as_masterdata;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_orders
------------------------------------------------------------------------------------------------
        procedure p_cnl_as_orders
	is
		l_rtn varchar2(30) := 'p_cnl_as_orders';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_orders
		where   (to_char(creation_date,'YYYYMMDD') = to_char(sysdate -7,'YYYYMMDD') and cnl_if_status = 'Completed')
		or      (to_char(creation_date,'YYYYMMDD') <= to_char(sysdate -365,'YYYYMMDD'))
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_orders;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_pick_task
------------------------------------------------------------------------------------------------
        procedure p_cnl_as_pick_task
	is
		l_rtn varchar2(30) := 'p_cnl_as_pick_task';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_pick_task
		where   to_char(creation_date,'YYYYMMDD') <= to_char(sysdate -7,'YYYYMMDD')
		and    ( ( cnl_if_status   = 'DeAllocated' 	or  
			   cnl_if_status   = 'Picked' 		or  
			   cnl_if_status   = 'ErrorKeyNotFound'
			 ) or
			 wms_mt_key not in (	select	key 
						from 	dcsdba.move_task 
						where 	task_type = 'O'
						and 	key = wms_mt_key)
			)
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_pick_task;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_picked_attributes
------------------------------------------------------------------------------------------------
	procedure p_cnl_as_picked_attributes
	is
		l_rtn varchar2(30) := 'p_cnl_as_picked_attributes';
		pragma autonomous_transaction;
	begin
		delete cnl_as_picked_attributes
		where   to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -90,'YYYYMMDD')
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_picked_attributes;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_picked_attributes
------------------------------------------------------------------------------------------------
        procedure p_cnl_as_processed_host_mess
	is
		l_rtn varchar2(30) := 'p_cnl_as_processed_host_mess';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_processed_host_message a
		where 	to_char(dstamp,'YYYYMMDD') <= to_char(sysdate -14,'YYYYMMDD')
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_processed_host_mess;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_as_tu
------------------------------------------------------------------------------------------------
        procedure p_cnl_as_tu
	is
		l_rtn varchar2(30) := 'p_cnl_as_tu';
		pragma autonomous_transaction;
	begin
		delete  cnl_as_tu
		where   (to_char(dstamp,'YYYYMMDD') = to_char(sysdate -7,'YYYYMMDD') and cnl_if_status   = 'Sorted')
		or      (to_char(dstamp,'YYYYMMDD') = to_char(sysdate -90,'YYYYMMDD'))
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_as_tu;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_container_vas_activity
------------------------------------------------------------------------------------------------
        procedure p_cnl_container_vas_activity
	is
		l_rtn varchar2(30) := 'p_cnl_container_vas_activity';
		pragma autonomous_transaction;
	begin
		delete 	cnl_container_vas_activity v
		where	v.order_id in ( select	o.order_id
				        from	dcsdba.order_header o
				        where 	o.client_id 	= v.client_id 
				        and	o.order_id 	= v.order_id
				        and	status 		in ( 'Cancelled','Shipped','Delivered'))
		or	( select count(*)
			  from	 dcsdba.order_header o
			  where  o.client_id 	= v.client_id 
			  and	 o.order_id 	= v.order_id) = 0
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_container_vas_activity;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_sys.cnl_wms_qc_order
------------------------------------------------------------------------------------------------
        procedure p_cnl_wms_qc_order
	is
		l_rtn varchar2(30) := 'p_cnl_wms_qc_order';
		pragma autonomous_transaction;
	begin
		delete 	cnl_sys.cnl_wms_qc_order q
		where	q.order_id = ( select	o.order_id
				       from	dcsdba.order_header o
				       where 	o.client_id 	= q.client_id 
				       and	o.order_id 	= q.order_id
				       and	o.status 	in ( 'Cancelled','Shipped','Delivered'))
		or	( select count(*)
			  from	 dcsdba.order_header o
			  where  o.client_id 	= q.client_id 
			  and	 o.order_id 	= q.order_id) = 0
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_wms_qc_order;
------------------------------------------------------------------------------------------------
-- Cleanup table cnl_print_log
------------------------------------------------------------------------------------------------
        procedure p_cnl_print_log
	is
		l_rtn varchar2(30) := 'p_cnl_print_log';
		pragma autonomous_transaction;
	begin
		-- delete data that is a month old.
		delete 	cnl_sys.cnl_print_log
		where	trunc(dstamp) < trunc(sysdate - 30)
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_print_log;
------------------------------------------------------------------------------------------------
-- Build up Autostore "Synq" transaction archive
-- Note that Synq has no archive. It has a process that purges data after three months.
-- Data is archived the same day
------------------------------------------------------------------------------------------------
	procedure p_cnl_synq_trans_archive
	is
		-- Cursor to fetch oldest timestamp in archive
		cursor c_max
		is
			select 	max(to_char(create_date,'YYYYMMDDHH24miSS')) create_date
			from	synq_trans_archive
		;
		-- cursor to fetch archive and purge settings
		cursor c_set( b_table varchar2)
		is
			select 	nvl(s.purge_days,0)
			,	nvl(s.archive_days,0)
			from 	cnl_sys.cnl_archive_purge_settings s
			where 	s.table_name = b_table
		;
		--
		l_rtn varchar2(30) := 'p_cnl_synq_trans_archive';
		v_purge_days	number;
		v_archive_days	number;
		--	
		r_max	varchar2(14);
		pragma autonomous_transaction;
	begin
		-- Fetch archive purge settings
		open 	c_set('SYNQ_TRANS_ARCHIVES');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;

		-- fetch latest archived record
		open	c_max;
		fetch	c_max into r_max;
		if	c_max%notfound
		then
			r_max := to_char(sysdate,'YYYYMMDD');
		end if;
		close 	c_max;

		-- Insert all new records from trans table to trans archive table
		if	nvl(v_archive_days,0) = 0
		then
			null;
		else
			insert 	into synq_trans_archive
			select 	sysdate archive_date
			,	t.* 
			from 	rhenus_synq.trans@as_synq.rhenus.de t 
			where 	to_char(t.create_date,'YYYYMMDDHH24miSS') > r_max -- Later then latest archived record.
			and	to_date(to_char(t.create_date,'YYYYMMDD'),'YYYYMMDD') <= to_date(to_char(sysdate-v_archive_days,'YYYYMMDD'),'YYYYMMDD')  -- X days old
			;
		end if;

		-- Puring the trans table is done by Synq it self.

		-- Purge archived data
		delete 	synq_trans_archive
		where	to_date(to_char(archive_date,'YYYYMMDD'),'YYYYMMDD') < to_date(to_char(sysdate - v_purge_days,'YYYYMMDD'),'YYYYMMDD')
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
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_synq_trans_archive;	
------------------------------------------------------------------------------------------------
-- Build up container data archive and urge old data
-- 
-- Records that no longer have an order in order header will be archived.
------------------------------------------------------------------------------------------------
	procedure p_cnl_container_data
	is
		cursor c_set( b_table varchar2)
		is
			select 	nvl(s.purge_days,0)
			,	nvl(s.archive_days,0)
			from 	cnl_sys.cnl_archive_purge_settings s
			where 	s.table_name = b_table
		;
		--
		l_rtn varchar2(30) := 'p_cnl_container_data';
		v_purge_days	number;
		v_archive_days	number;
		pragma autonomous_transaction;
	begin
		open 	c_set('CNL_CONTAINER_DATA');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;
		if	v_archive_days is null or v_archive_days = 0
		then	-- No archive
			null;
		else	-- Set all records that require archiving
			Update	cnl_container_data o
			set 	o.archive_pending = 'Y'
			where	nvl(o.archived,'N') = 'N'
			and	o.creation_date < sysdate - v_archive_days
			;

			-- Insert selected data in archive table
			insert into cnl_container_data_archives
			select 	container_id
			,	container_type
			,	pallet_id
			,	pallet_type
			,	container_n_of_n
			,	site_id
			,	client_id
			,	owner_id
			,	order_id
			,	customer_id
			,	carrier_id
			,	service_level
			,	wms_weight
			,	wms_height
			,	wms_width
			,	wms_depth
			,	wms_database
			,	dws_unit_id
			,	dws_station_id
			,	dws_lft_status
			,	dws_lft_description
			,	dws_package_type
			,	dws_weight
			,	dws_height
			,	dws_width
			,	dws_depth
			,	dws_dstamp
			,	cto_enabled_yn
			,	cto_pp_filename
			,	cto_pp_dstamp
			,	cto_cp_filename
			,	cto_cp_dstamp
			,	cto_carrier
			,	cto_service
			,	cto_sequence_nr
			,	cto_tracking_nr
			,	cto_tracking_url
			,	cto_error_code
			,	cto_error_message
			,	cto_ppr_dstamp
			,	created_by
			,	creation_date
			,	last_updated_by
			,	last_update_date
			,	archived
			,	sysdate archived_dstamp
			,	'N' archive_pending
			from	cnl_container_data
			where	archive_pending = 'Y'
			;

			-- Update records as archived
			Update 	cnl_container_data
			set 	archived = 'Y'
			,	archive_pending = 'N'
			,	archived_dstamp = sysdate
			where	archive_pending = 'Y'
			;
		end if;
		--
		if	v_purge_days is null or v_purge_days = 0
		then
			null;
		else	-- purge old records archived just before.
			delete 	cnl_container_data o
			where	o.archived = 'Y'
			and	o.creation_date < sysdate - v_purge_days
			;
			commit;
		end if;
		-- Purge archive table
		v_purge_days 	:= null;
		v_archive_days 	:= null;
		open 	c_set('CNL_CONTAINER_DATA_ARCHIVES');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;
		if	v_purge_days is null or v_purge_days = 0
		then
			null;
		else
			delete 	cnl_container_data_archives a
			where	a.archived_dstamp < sysdate - v_purge_days
			;
			commit;
		end if;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_container_data;

------------------------------------------------------------------------------------------------
-- Build up WHH container data archive and purge old data
-- 
------------------------------------------------------------------------------------------------
	procedure p_whh_cnl_container_data
	is
		cursor c_set( b_table varchar2)
		is
			select 	nvl(s.purge_days,0)
			,	nvl(s.archive_days,0)
			from 	cnl_sys.cnl_archive_purge_settings s
			where 	s.table_name = b_table
		;
		--
		l_rtn varchar2(30) := 'p_whh_cnl_container_data';
		v_purge_days	number;
		v_archive_days	number;
		pragma autonomous_transaction;
	begin
		open 	c_set('CNL_WHH_CONTAINER_DATA');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;
		if	v_archive_days is null or v_archive_days = 0
		then	-- No archive
			null;
		else	-- Set all records that require archiving
			Update	cnl_whh_container_data o
			set 	o.archive_pending = 'Y'
			where	nvl(o.archived,'N') = 'N'
			and	o.entry_creation_date < sysdate - v_archive_days
			;
			if g_database in ('DEVCNNJW','TSTCNLJW')
			then
				-- Insert selected data in archive table
				insert 	
				into 	cnl_whh_cont_data_archives
				(	id	
				,	whh_cont_id
				, 	site_id
				, 	client_id
				, 	owner_id
				, 	order_id
				, 	pallet_id
				, 	container_id
				, 	location_id
				, 	qc_req_yn
				, 	qc_batch_yn
				, 	qc_qty_def_yn
				, 	qc_sku_select_yn
				, 	qc_qty_upd_yn
				, 	qc_serial_yn
				, 	initial_check_result
				, 	secondary_check_result
				, 	initial_done_by
				, 	secondary_done_by
				, 	tag_id
				, 	sku_id
				, 	description
				, 	condition_id
				, 	origin_id
				, 	status
				, 	lock_code
				, 	qty_on_hand
				, 	batch_check_result
				, 	qty_check_result
				, 	batch_id_yn
				, 	qty_check_result_sec
				, 	batch_check_result_sec
				, 	serial_number
				, 	batch_serial
				, 	entry_creation_date
				, 	check_type
				, 	archived
				, 	archived_dstamp
				, 	archive_pending
				)
				select	id
				,	0 --whh_cont_id
				, 	site_id
				, 	client_id
				, 	owner_id
				, 	order_id
				, 	pallet_id
				, 	container_id
				, 	location_id
				, 	qc_req_yn
				, 	qc_batch_yn
				, 	qc_qty_def_yn
				, 	qc_sku_select_yn
				, 	qc_qty_upd_yn
				, 	qc_serial_yn
				, 	initial_check_result
				, 	secondary_check_result
				, 	initial_done_by
				, 	secondary_done_by
				, 	tag_id
				, 	sku_id
				, 	description
				, 	condition_id
				, 	origin_id
				, 	status
				, 	lock_code
				, 	qty_on_hand
				, 	batch_check_result
				, 	qty_check_result
				, 	batch_id_yn
				, 	qty_check_result_sec
				, 	batch_check_result_sec
				, 	serial_number
				, 	batch_serial
				, 	entry_creation_date
				, 	check_type
				, 	archived
				, 	sysdate archived_dstamp
				, 	'N' archive_pending
				from	cnl_whh_container_data
				where	archive_pending = 'Y'
				;
			else
				-- Insert selected data in archive table
				insert 	
				into 	cnl_whh_cont_data_archives
				(	id	
				,	whh_cont_id
				, 	site_id
				, 	client_id
				, 	owner_id
				, 	order_id
				, 	pallet_id
				, 	container_id
				, 	location_id
				, 	qc_req_yn
				, 	qc_batch_yn
				, 	qc_qty_def_yn
				, 	qc_sku_select_yn
				, 	qc_qty_upd_yn
				, 	qc_serial_yn
				, 	initial_check_result
				, 	secondary_check_result
				, 	initial_done_by
				, 	secondary_done_by
				, 	tag_id
				, 	sku_id
				, 	description
				, 	condition_id
				, 	origin_id
				, 	status
				, 	lock_code
				, 	qty_on_hand
				, 	batch_check_result
				, 	qty_check_result
				, 	batch_id_yn
				, 	qty_check_result_sec
				, 	batch_check_result_sec
				, 	serial_number
				, 	batch_serial
				, 	entry_creation_date
				, 	check_type
				, 	archived
				, 	archived_dstamp
				, 	archive_pending
				)
				select	id
				,	0--whh_cont_id
				, 	site_id
				, 	client_id
				, 	owner_id
				, 	order_id
				, 	pallet_id
				, 	container_id
				, 	location_id
				, 	qc_req_yn
				, 	qc_batch_yn
				, 	qc_qty_def_yn
				, 	qc_sku_select_yn
				, 	qc_qty_upd_yn
				, 	qc_serial_yn
				, 	initial_check_result
				, 	secondary_check_result
				, 	initial_done_by
				, 	secondary_done_by
				, 	tag_id
				, 	sku_id
				, 	description
				, 	condition_id
				, 	origin_id
				, 	status
				, 	lock_code
				, 	qty_on_hand
				, 	batch_check_result
				, 	qty_check_result
				, 	batch_id_yn
				, 	qty_check_result_sec
				, 	batch_check_result_sec
				, 	serial_number
				, 	batch_serial
				, 	entry_creation_date
				, 	check_type
				, 	archived
				, 	sysdate archived_dstamp
				, 	'N' archive_pending
				from	cnl_whh_container_data
				where	archive_pending = 'Y'
				;
			end if;
			-- Update records as archived
			Update 	cnl_whh_container_data
			set 	archived = 'Y'
			,	archive_pending = 'N'
			,	archived_dstamp = sysdate
			where	archive_pending = 'Y'
			;
		end if;
		--
		if	v_purge_days is null or v_purge_days = 0
		then
			null;
		else	-- purge old records archived just before.
			delete 	cnl_whh_container_data o
			where	o.archived = 'Y'
			and	o.entry_creation_date < sysdate - v_purge_days
			;
			commit;
		end if;
		-- Purge archive table
		v_purge_days 	:= null;
		v_archive_days 	:= null;
		open 	c_set('CNL_WHH_CONTAINER_DATA_ARCHIV');
		fetch	c_set 
		into 	v_purge_days
		,	v_archive_days;
		close	c_set;
		if	v_purge_days is null or v_purge_days = 0
		then
			null;
		else
			delete 	cnl_whh_cont_data_archives a
			where	a.archived_dstamp < sysdate - v_purge_days
			;
			commit;
		end if;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_whh_cnl_container_data;   
------------------------------------------------------------------------------------------------
-- Archive and purge table extension
------------------------------------------------------------------------------------------------
procedure archive_purge_order_extend_p
is
	-- Union with other tables if more are added
	cursor c_records
	is
		select	oe.order_id
		,	oe.client_id
		,	oe.total_qty_ordered
		,	oe.total_unique_sku
		,	oe.pallet_type
		,	oe.vas
		,	oe.mode_of_transport
		,	oe.pre_carrier_type
		,	oe.carrier_type
		,	oe.requires_ext_pack_docs
		,	oe.requires_ext_ship_docs
		,	oe.requires_ext_processing
		,	oe.contains_hazmat
		,	oe.contains_ugly_sku
		,	oe.contains_awkward_sku
		,	oe.contains_dual_use_sku
		,	oe.contains_config_kit
		,	oe.contains_two_man_lift
		,	oe.contains_conveyable_sku
		,	oe.contains_kit
		,	oe.archived
		,	oe.rowid
		from	cnl_sys.cnl_wms_order_header_extend oe
		inner
		join	dcsdba.order_header o
		on	o.order_id		= oe.order_id
		and	o.client_id		= oe.client_id
		and	nvl(o.archived,'N')	= 'Y'
		where	nvl(oe.archived,'N') 	= 'N'
		for 	update 
		of 	oe.archived
	;
	cursor c_lines( b_order_id	varchar2
		      , b_client_id	varchar2
		      )
	is
		select	le.order_id
		,	le.client_id
		,	le.line_id
		,	le.sku_id
		,	le.qty_ordered
		,	le.contains_hazmat
		,	le.contains_ugly_sku
		,	le.contains_awkward_sku
		,	le.contains_dual_use_sku
		,	le.contains_config_kit
		,	le.contains_two_man_lift
		,	le.contains_conveyable_sku
		,	le.contains_kit
		from	cnl_sys.cnl_wms_order_line_extend le
		where	le.order_id	= b_order_id
		and	le.client_id	= b_client_id
	;
	cursor	c_purge
	is
		select	oe.order_id
		,	oe.client_id
		from 	cnl_wms_order_header_extend oe
		left 
		outer 
		join 	dcsdba.order_header o 
		on 	o.order_id 	= oe.order_id 
		and 	o.client_id 	= oe.client_id
		where	o.order_id 	is null
		and	o.client_id	is null
	;	
	cursor	c_arch_purge_settings
	is
		select	purge_days
		from	cnl_archive_purge_settings
		where	table_name = 'ORDER_HEADER_EXTEND_ARCHIVES'
	;
	cursor	c_archive_purge(b_days	number)
	is
		select	rowid
		,	order_id
		,	client_id
		from	order_header_extend_archives
		where	archived_dstamp < sysdate - b_days
	;

	l_rtn	varchar2(30) := 'archive_purge_order_extend_p';
	l_num	number;

begin
	<<header_loop>>
	for	i in c_records
	loop
		update 	cnl_wms_order_header_extend 
		set 	archived 	= 'Y' 
		where 	rowid 		= i.rowid
		;
		-- insert header to archive
		insert
		into	order_header_extend_archives
		(	order_id
		,	client_id
		,	archive_number
		,	total_qty_ordered
		,	total_unique_sku
		,	pallet_type
		,	vas
		,	mode_of_transport
		,	pre_carrier_type
		,	carrier_type
		,	requires_ext_pack_docs
		,	requires_ext_ship_docs
		,	requires_ext_processing
		,	contains_hazmat
		,	contains_ugly_sku
		,	contains_awkward_sku
		,	contains_dual_use_sku
		,	contains_config_kit
		,	contains_two_man_lift
		,	contains_conveyable_sku
		,	contains_kit
		,	archived
		,	archived_dstamp
		)
		values
		(	i.order_id
		,	i.client_id
		,	ord_extend_archive_seq1.nextval
		,	i.total_qty_ordered
		,	i.total_unique_sku
		,	i.pallet_type
		,	i.vas
		,	i.mode_of_transport
		,	i.pre_carrier_type
		,	i.carrier_type
		,	i.requires_ext_pack_docs
		,	i.requires_ext_ship_docs
		,	i.requires_ext_processing
		,	i.contains_hazmat
		,	i.contains_ugly_sku
		,	i.contains_awkward_sku
		,	i.contains_dual_use_sku
		,	i.contains_config_kit
		,	i.contains_two_man_lift
		,	i.contains_conveyable_sku
		,	i.contains_kit
		,	'Y' --archived
		,	sysdate
		)
		;
		-- insert lines to archive
		<<line_loop>>
		for	l in c_lines( b_order_id 	=> i.order_id
				    , b_client_id	=> i.client_id
				    )
		loop
			insert
			into	order_line_extend_archives
			(	order_id
			,	client_id
			,	line_id
			,	archive_number
			,	sku_id
			,	qty_ordered
			,	contains_hazmat
			,	contains_ugly_sku
			,	contains_awkward_sku
			,	contains_dual_use_sku
			,	contains_config_kit
			,	contains_two_man_lift
			,	contains_conveyable_sku
			,	contains_kit
			)
			values
			(	l.order_id
			,	l.client_id
			,	l.line_id
			,	orl_extend_archive_seq1.nextval
			,	l.sku_id
			,	l.qty_ordered
			,	l.contains_hazmat
			,	l.contains_ugly_sku
			,	l.contains_awkward_sku
			,	l.contains_dual_use_sku
			,	l.contains_config_kit
			,	l.contains_two_man_lift
			,	l.contains_conveyable_sku
			,	l.contains_kit
			)
			;
		end loop; -- line_loop
	end loop; -- header_loop
	commit;

	-- delete records for orders that no longer exist in order header
	for	i in c_purge
	loop
		delete	cnl_sys.cnl_wms_order_header_extend o
		where	o.order_id	= i.order_id
		and	o.client_id	= i.client_id
		;
		delete	cnl_sys.cnl_wms_order_line_extend l
		where	l.order_id	= i.order_id
		and	l.client_id	= i.client_id
		;
		commit;
	end loop;

	-- Delete archived records
	open	c_arch_purge_settings;
	fetch 	c_arch_purge_settings
	into	l_num;
	close 	c_arch_purge_settings;
	if 	l_num is null
	or 	l_num = 0
	then
		null;
	else
		for 	r in c_archive_purge(l_num)
		loop		
			delete	order_header_extend_archives 
			where 	rowid = r.rowid
			;
			delete 	order_line_extend_archives 
			where 	order_id = r.order_id 
			and 	client_id = r.client_id
			;
		end loop;
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
						  , p_routine_parameters_i	=> null
						  , p_comments_i		=> 'Something went wrong inserting archive records or during purging'
						  );
end archive_purge_order_extend_p;
------------------------------------------------------------------------------------------------
-- Purge cnl_cto_ship_labels table
-- 
------------------------------------------------------------------------------------------------
procedure p_cnl_cto_ship_labels
is
	l_rtn varchar2(255) := 'p_cnl_cto_ship_labels';
	pragma autonomous_transaction;	
begin
	delete	cnl_cto_ship_labels s
	where	nvl(s.shipment_id,'X') not in	(
						select 	to_char(nvl(o.uploaded_ws2pc_id,0))
						from	dcsdba.order_header o
						where	(	o.client_id 			= s.client_id
							or	s.client_id 			is null
							)
						and	o.from_site_id				= s.site_id
						and	to_char(nvl(o.uploaded_ws2pc_id,0))	= nvl(s.shipment_id,'X')
						and	o.client_id 				in	(
													select 	l.client_id
													from 	dcsdba.client_group_clients l
													where	l.client_group = 'CTOSAAS'
													)
						) 
	and creation_dstamp < sysdate -7
	;
	commit;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i        	=> sqlcode                -- Oracle SQL code or user defined error code
					          , p_sql_error_message_i    	=> sqlerrm                -- SQL error message
						  , p_line_number_i        	=> dbms_utility.format_error_backtrace    -- Procedure or function line number the error occured
						  , p_package_name_i        	=> g_pck                -- Package name the error occured
						  , p_routine_name_i        	=> l_rtn                -- Procedure or function generarting the error
						  , p_routine_parameters_i    	=> null
						  , p_comments_i        	=> null
						  );
end p_cnl_cto_ship_labels;

------------------------------------------------------------------------------------------------
-- Purge cnl_cto_log table
-- 
------------------------------------------------------------------------------------------------
procedure p_cnl_cto_log
is
	l_rtn varchar2(255) := 'p_cnl_cto_log';
	pragma autonomous_transaction;
begin
	delete cnl_cto_log
	where    dstamp < sysdate -7;
	commit;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i        	=> sqlcode                -- Oracle SQL code or user defined error code
					          , p_sql_error_message_i    	=> sqlerrm                -- SQL error message
						  , p_line_number_i       	=> dbms_utility.format_error_backtrace    -- Procedure or function line number the error occured
						  , p_package_name_i        	=> g_pck                -- Package name the error occured
						  , p_routine_name_i        	=> l_rtn                -- Procedure or function generarting the error
						  , p_routine_parameters_i    	=> null
						  , p_comments_i        	=> null
						  );
end p_cnl_cto_log;

------------------------------------------------------------------------------------------------
-- Purge cnl_cto_webservice_body table
-- 
------------------------------------------------------------------------------------------------
procedure p_cnl_cto_webservice_body
is
	l_rtn varchar2(255) :=  'p_cnl_cto_webservice_body';
	pragma autonomous_transaction;
begin
	delete 	cnl_cto_webservice_body
	where   dstamp < sysdate -7;
	commit;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i        	=> sqlcode                -- Oracle SQL code or user defined error code
					          , p_sql_error_message_i    	=> sqlerrm                -- SQL error message
						  , p_line_number_i        	=> dbms_utility.format_error_backtrace    -- Procedure or function line number the error occured
						  , p_package_name_i        	=> g_pck                -- Package name the error occured
						  , p_routine_name_i        	=> l_rtn                -- Procedure or function generarting the error
						  , p_routine_parameters_i    	=> null
						  , p_comments_i        	=> null
						  );
end p_cnl_cto_webservice_body;

------------------------------------------------------------------------------------------------
-- Process housekeeping tasks
------------------------------------------------------------------------------------------------
	procedure p_cnl_process_housekeeping
	is
		l_rtn varchar2(30) := 'p_cnl_process_housekeeping';
	begin
		select 	name 
		into 	g_database
		from 	v$database
		;

		-- purge
		p_cnl_as_adjustment;
		p_cnl_as_container_suspect;
		p_cnl_as_inb_tasks;
		p_cnl_as_log;
		p_cnl_as_maas_logging;
		p_cnl_as_manual_lines;
		p_cnl_as_manual_order_started;
		p_cnl_as_masterdata;
		p_cnl_as_orders;
		p_cnl_as_pick_task;
		p_cnl_as_picked_attributes;
		p_cnl_as_processed_host_mess;
		p_cnl_as_tu;
		p_cnl_container_vas_activity;
		p_cnl_wms_qc_order;
		p_cnl_print_log;
                p_cnl_cto_log;
		p_cnl_cto_webservice_body;
		p_cnl_cto_ship_labels;

		-- Archive
		p_cnl_synq_trans_archive;

		-- Archive and purge
		p_cnl_container_data;
		p_whh_cnl_container_data;
		p_cnl_run_task_archive;
		p_cnl_scheduler_job_archive;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null
							  );
	end p_cnl_process_housekeeping;

------------------------------------------------------------------------------------------------
-- Initialization
------------------------------------------------------------------------------------------------
	begin
		null;
end cnl_archive_purge_pck;