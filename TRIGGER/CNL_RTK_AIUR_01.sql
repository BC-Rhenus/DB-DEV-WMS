CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_RTK_AIUR_01" 
	after insert or update on dcsdba.run_task
	for each row
	 WHEN ( 	new.status = 'In Progress' 
		and 	substr( new.command , (instr( new.command, '"', 1) + 1), (instr( new.command, '"', 2) - 2)) in ( 'CTO_CANCELPARCEL'
														       , 'CTO_PACKPARCEL'
														       , 'PALLET_CLOSING'
														       , 'PARCEL_PACKING'
														       , 'REPACKING'
														       , 'SSV_PLT_ALL'
														       , 'SSV_PLT_CON'
														       , 'SSV_PLT_PAL'
														       , 'SSV_TRL_ALL'
														       )
		) declare
	r_log			varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_PRE-PRINT-LOG_ENABLE');
	l_module		varchar2(50);
	l_action		varchar2(50);
	l_key 			dcsdba.run_task.key%type;
	l_site_id		dcsdba.run_task.site_id%type;
	l_station_id		dcsdba.run_task.station_id%type;
	l_user_id		dcsdba.run_task.user_id%type;
	l_status		dcsdba.run_task.status%type;
	l_command		dcsdba.run_task.command%type;
	l_pid			dcsdba.run_task.pid%type;
	l_old_dstamp		dcsdba.run_task.old_dstamp%type;
	l_dstamp		dcsdba.run_task.dstamp%type;
	l_language		dcsdba.run_task.language%type;
	l_name			dcsdba.run_task.name%type;
	l_time_zone_name	dcsdba.run_task.time_zone_name%type;
	l_nls_calendar		dcsdba.run_task.nls_calendar%type;
	l_print_label		dcsdba.run_task.print_label%type;
	l_java_report		dcsdba.run_task.java_report%type;
	l_run_light		dcsdba.run_task.run_light%type;
	l_server_instance	dcsdba.run_task.server_instance%type;
	l_priority		dcsdba.run_task.priority%type;
	l_archive		dcsdba.run_task.archive%type;
	l_archive_ignore_screen	dcsdba.run_task.archive_ignore_screen%type;
	l_archive_restrict_user	dcsdba.run_task.archive_restrict_user%type;
	l_client_id		dcsdba.run_task.client_id%type;
	l_email_recipients	dcsdba.run_task.email_recipients%type;
	l_email_attachment	dcsdba.run_task.email_attachment%type;
	l_email_subject		dcsdba.run_task.email_subject%type;
	l_email_message		dcsdba.run_task.email_message%type;
	l_master_key		dcsdba.run_task.master_key%type;
	l_use_db_time_zone	dcsdba.run_task.use_db_time_zone%type;
	pragma autonomous_transaction;
begin
	l_key 			:= :new.key;
	l_site_id		:= :new.site_id;
	l_station_id		:= :new.station_id;
	l_user_id		:= :new.user_id;
	l_status		:= :new.status;
	l_command		:= :new.command;
	l_pid			:= :new.pid;
	l_old_dstamp		:= :new.old_dstamp;
	l_dstamp		:= :new.dstamp;
	l_language		:= :new.language;
	l_name			:= :new.name;
	l_time_zone_name	:= :new.time_zone_name;
	l_nls_calendar		:= :new.nls_calendar;
	l_print_label		:= :new.print_label;
	l_java_report		:= :new.java_report;
	l_run_light		:= :new.run_light;
	l_server_instance	:= :new.server_instance;
	l_priority		:= :new.priority;
	l_archive		:= :new.archive;
	l_archive_ignore_screen	:= :new.archive_ignore_screen;
	l_archive_restrict_user	:= :new.archive_restrict_user;
	l_client_id		:= :new.client_id;
	l_email_recipients	:= :new.email_recipients;
	l_email_attachment	:= :new.email_attachment;
	l_email_subject		:= :new.email_subject;
	l_email_message		:= :new.email_message;
	l_master_key		:= :new.master_key;
	l_use_db_time_zone	:= :new.use_db_time_zone;

	if	r_log = 'ON'
	then
		dbms_application_info.read_module( module_name => l_module
						 , action_name => l_action
						 );
		cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> :new.key
							   , p_file_name_i		=> null
							   , p_source_package_i		=> 'cnl_rtk_aiur_01'
							   , p_source_routine_i		=> 'cnl_rtk_aiur_01'
							   , p_routine_step_i		=> 'Trigger is started by '||l_module||' doing '||l_action||' and created a job'
							   , p_code_parameters_i 	=> :new.command
							   , p_order_id_i		=> null
							   , p_client_id_i		=> :new.client_id
							   , p_pallet_id_i		=> null
							   , p_container_id_i		=> null
							   , p_site_id_i		=> :new.site_id
							   );
	end if;
	--
	cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_wms_pck.process_runtask ('
							   || :new.key
							   || ','''
							   || substr( :new.command , (instr( :new.command, '"', 1) + 1), (instr( :new.command, '"', 2) - 2))
							   || ''','''
							   || :new.user_id
							   || ''','''
							   || :new.station_id
							   || ''','''
							   || :new.site_id
							   || ''','''
							   || :new.status
							   || ''','''
							   || :new.command
							   || ''','
							   || 'null' --:new.pid
							   || ','''
							   || :new.old_dstamp
							   || ''','''
							   || :new.dstamp
							   ||''','''
							   || :new.language
							   || ''','''
							   || :new.name
							   || ''','''
							   || :new.time_zone_name
							   || ''','''
							   || :new.nls_calendar
							   || ''','''
							   || :new.print_label
							   || ''','''
							   || :new.java_report
							   || ''','''
							   || :new.run_light
							   || ''','''
							   || :new.server_instance
							   || ''','
							   || 'null' -- :new.priority
							   || ','''
							   || :new.archive
							   || ''','''
							   || :new.archive_ignore_screen
							   || ''','''
							   || :new.archive_restrict_user
							   || ''','''
							   || :new.client_id
							   || ''','''
							   || :new.email_recipients
							   || ''','''
							   || :new.email_attachment
							   || ''','''
							   || :new.email_subject
							   || ''','''
							   || :new.email_message
							   || ''','
							   || :new.master_key
							   || ','''
							   || :new.use_db_time_zone
							   || '''); end;'
							   , p_code_i      => 'P_RTK_' || :new.key
							   , p_delay_i     => 1
							   );
	--
	if	r_log = 'ON'
	then
		cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> :new.key
							   , p_file_name_i		=> null
							   , p_source_package_i		=> 'cnl_rtk_aiur_01'
							   , p_source_routine_i		=> 'cnl_rtk_aiur_01'
							   , p_routine_step_i		=> 'Trigger is finished'
							   , p_code_parameters_i 	=> :new.command
							   , p_order_id_i		=> null
							   , p_client_id_i		=> :new.client_id
							   , p_pallet_id_i		=> null
							   , p_container_id_i		=> null
							   , p_site_id_i		=> :new.site_id
							   );
	end if;

exception
	when   others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> 'cnl_rtk_aiur_01'		-- Package name the error occured
						  , p_routine_name_i		=> 'cnl_rtk_aiur_01'	-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null
						  , p_comments_i		=> 'Exception in trigger'					-- Additional comments describing the issue
						  );
end cnl_rtk_aiur_01;