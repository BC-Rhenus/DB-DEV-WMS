CREATE OR REPLACE PROCEDURE "CNL_SYS"."CNL_ITL_CODE_REPORTS" 
is
	cursor	c_ord
	is
		select	from_site_id
		,	client_id
		,	order_id
		,	owner_id
		,	status
		,	update_value
		,	user_id
		,	station_id
		,	dstamp
		from	(
			-- Fetch orders at status ready to load that require prints
			select	o.from_site_id
			,	o.client_id
			,	o.order_id
			,	o.owner_id
			,	o.status
			,	'R' update_value
			,	nvl(i.user_id,'Automatic')	user_id
			,	nvl(i.station_id,'Automatic')	station_id
			,	i.dstamp
			from	dcsdba.order_header o
			inner
			join	dcsdba.itl_code_reports r
			on	r.report_name 		= 'UREPSTREAMSERVE'
			and	nvl(r.enabled,'N')	= 'N'
			and	nvl(r.shell_script,'N')	= 'Y'
			and	r.client_id		= o.client_id
			and	r.to_status		= o.status
			and	r.site_id		= o.from_site_id
			and	r.to_status		= 'Ready to Load'
			inner
			join	dcsdba.inventory_transaction i
			on	i.code 			= 'Order Status'
			and	i.reference_id		= o.order_id
			and	i.client_id		= o.client_id
			and	i.to_status		= r.to_status
			where	nvl(o.foreign_documentation,'X') not in ('R','C','S','D')  -- Ready to load, Complete, Shipped, Delivered
			union
			-- Fetch orders at status Complete that require prints
			select	o.from_site_id
			,	o.client_id
			,	o.order_id
			,	o.owner_id
			,	o.status
			,	'C' update_value
			,	nvl(i.user_id,'Automatic')	user_id
			,	nvl(i.station_id,'Automatic')	station_id
			,	i.dstamp
			from	dcsdba.order_header o
			inner
			join	dcsdba.itl_code_reports r
			on	r.report_name 		= 'UREPSTREAMSERVE'
			and	nvl(r.enabled,'N')	= 'N'
			and	nvl(r.shell_script,'N')	= 'Y'
			and	r.client_id		= o.client_id
			and	r.to_status		= o.status
			and	r.site_id		= o.from_site_id
			and	r.to_status		= 'Complete'
			inner
			join	dcsdba.inventory_transaction i
			on	i.code 			= 'Order Status'
			and	i.reference_id		= o.order_id
			and	i.client_id		= o.client_id
			and	i.to_status		= r.to_status
			where	nvl(o.foreign_documentation,'X') not in ('C','S','D') -- Complete, Shipped, Delivered
			union
			-- Fetch orders at status Shipped that require prints
			select	o.from_site_id
			,	o.client_id
			,	o.order_id
			,	o.owner_id
			,	o.status
			,	'S' update_value
			,	nvl(i.user_id,'Automatic')	user_id
			,	nvl(i.station_id,'Automatic')	station_id
			,	i.dstamp
			from	dcsdba.order_header o
			inner
			join	dcsdba.itl_code_reports r
			on	r.report_name 		= 'UREPSTREAMSERVE'
			and	nvl(r.enabled,'N')	= 'N'
			and	nvl(r.shell_script,'N')	= 'Y'
			and	r.client_id		= o.client_id
			and	r.to_status		= o.status
			and	r.site_id		= o.from_site_id
			and	r.to_status		= 'Shipped'
			inner
			join	dcsdba.inventory_transaction i
			on	i.code 			= 'Order Status'
			and	i.reference_id		= o.order_id
			and	i.client_id		= o.client_id
			and	i.to_status		= r.to_status
			where	nvl(o.foreign_documentation,'X') not in ('S','D') --Shipped, Delivered
			union
			-- Fetch orders at status Delivered that require prints
			select	o.from_site_id
			,	o.client_id
			,	o.order_id
			,	o.owner_id
			,	o.status
			,	'D' update_value
			,	nvl(i.user_id,'Automatic')	user_id
			,	nvl(i.station_id,'Automatic')	station_id
			,	i.dstamp
			from	dcsdba.order_header o
			inner
			join	dcsdba.itl_code_reports r
			on	r.report_name 		= 'UREPSTREAMSERVE'
			and	nvl(r.enabled,'N')	= 'N'
			and	nvl(r.shell_script,'N')	= 'Y'
			and	r.client_id		= o.client_id
			and	r.to_status		= o.status
			and	r.site_id		= o.from_site_id
			and	r.to_status		= 'Delivered'
			inner
			join	dcsdba.inventory_transaction i
			on	i.code 			= 'Order Status'
			and	i.reference_id		= o.order_id
			and	i.client_id		= o.client_id
			and	i.to_status		= r.to_status
			where	nvl(o.foreign_documentation,'X') not in ('D') -- Ready to load, Complete, Shipped, Delivered
			)
		order
		by	client_id
		,	order_id
		,	dstamp desc
	;
	l_result	integer;
	l_command	dcsdba.run_task.command%type;
	l_order_id	dcsdba.order_header.order_id%type;
	l_client_id	dcsdba.order_header.client_id%type;
	l_working	dcsdba.system_profile.text_data%type;
	l_counter	integer :=0;
begin
	select	text_data
	into	l_working
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_PRINTING_ITLCODEBUSY_ITLCODEBUSY'
	;
	-- To prevent next schduled program selects the same orders we stop it via the system profile setting
	if	l_working = 'SLEEPING'
	then
		update	dcsdba.system_profile
		set 	text_data = 'BUSY'
		where	profile_id = '-ROOT-_USER_PRINTING_ITLCODEBUSY_ITLCODEBUSY'
		;
		commit;

		<<order_loop>>
		for	i in c_ord
		loop
			-- To prevent duplicates when multiple transactions exist. Ordering ensures always the ;ast transaction is used
			if	l_order_id is null
			and	l_client_id is null
			then	-- loop starts
				l_order_id	:= i.order_id;
				l_client_id	:= i.client_id;
			else	
				if	l_order_id 	= i.order_id
				and	l_client_id	= i.client_id
				then	-- order has multiple transactions and has just been processed
					continue order_loop;
				else	-- new order to process
					l_order_id 	:= i.order_id;
					l_client_id 	:= i.client_id;
				end if;
			end if;

			-- Build run task command
			l_command 	:= '"SSV_PLT_ALL" "lp" "P" "1"  "client_id" "'
					|| i.client_id
					|| '" "order_id" "'
					|| i.order_id
					|| '" "owner_id" "'
					|| i.owner_id
					|| '" "site_id" "'
					|| i.from_site_id
					|| '" "order_status" "'
					|| i.status
					|| '"';

			-- Update order to mark it as processed
			update	dcsdba.order_header o
			set	foreign_documentation 	= i.update_value
			where	o.order_id		= i.order_id
			and	o.client_id		= i.client_id
			and	o.from_site_id		= i.from_site_id
			;

			-- Add run task
			l_result := dcsdba.libruntask.createruntask( stationid             => i.station_id
								   , userid                => i.user_id
								   , commandtext           => l_command
								   , nametext              => 'UREPSTREAMSERVE'
								   , siteid                => i.from_site_id
								   , tmplanguage           => 'EN_GB'
								   , p_javareport          => 'Y'
								   , p_archive             => 'Y'
								   , p_runlight            => null
								   , p_serverinstance      => null
								   , p_priority            => null
								   , p_timezonename        => 'Europe/Amsterdam'
								   , p_archiveignorescreen => null
								   , p_archiverestrictuser => null
								   , p_clientid            => i.client_id
								   , p_emailrecipients     => null
								   , p_masterkey           => null
								   , p_usedbtimezone       => 'N'
								   , p_nlscalendar         => 'Gregorian'
								   , p_emailattachment     => null
								   , p_emailsubject        => null
								   , p_emailmessage        => null
								   );
			-- Commit work
			commit;

			-- To prevent an overload of run tasks that all create jobs have a build in pauze to spread the load  
			l_counter := l_counter + 1;
			if	l_counter = 3
			then	--After every 3 records pauze one second then continue.
				l_counter := 0;
				dbms_lock.sleep(1);
			end if;
		end loop;
		-- reset systen profile setting so next job ca start
		update	dcsdba.system_profile
		set 	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_PRINTING_ITLCODEBUSY_ITLCODEBUSY'
		;
		commit;
	end if;		
exception
	when others 
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> 'no package'		-- Package name the error occured
						  , p_routine_name_i		=> 'cnl_itl_code_reports'	-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );	
		update	dcsdba.system_profile
		set 	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_PRINTING_ITLCODEBUSY_ITLCODEBUSY'
		;
		commit;
end cnl_itl_code_reports;