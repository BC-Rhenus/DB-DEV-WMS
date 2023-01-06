CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_RTK_BIUR_01" 
	before insert or update on dcsdba.run_task
	for each row
	 WHEN (	new.status = 'Pending' 
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
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to bypass the Run Task Daemon for Centiro, StreamServe processes
**********************************************************************************
* $Log: $
**********************************************************************************/
		cursor c_saas
		is
			select	count(*)
			from	dcsdba.client_group_clients cc
			where	cc.client_group 	= 'CTOSAAS'
			and	(	(cc.client_id 	= :new.client_id)
				or	(:new.client_id	is null
				and	(	
					select count(distinct c.client_id) 
					from	dcsdba.order_container c
					where	c.pallet_id = nvl(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "'),instr(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "')),'"')-1),'N')
					) = 1
				and	(
					select 	s.client_id
					from	dcsdba.order_container s
					where	s.pallet_id = nvl(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "'),instr(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "')),'"')-1),'N')
					and	rownum = 1
					) = cc.client_id
					)
				)
		;

		cursor c_wsn
		is
			select nvl(user_def_chk_1,'N') linked_to_dws
			from   dcsdba.workstation
			where  station_id = :new.station_id
		;
		--
		cursor c_clt
		is
			select 	s.client_id
			from	dcsdba.order_container s
			where	s.pallet_id = nvl(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "'),instr(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "')),'"')-1),'N')
			and	rownum = 1
		;
		--
		r_log		varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_PRE-PRINT-LOG_ENABLE');
		l_module	varchar2(50);
		l_action	varchar2(50);
		l_linked_to_dws varchar2(1) := 'N'; 
		l_saas		integer;
		l_order_id	varchar2(30);
		l_container_id	varchar2(50);
		l_pallet_id	varchar2(50);
		l_client	varchar2(50);
	begin
		open 	c_saas;
		fetch 	c_saas
		into	l_saas;
		close	c_saas;
		if	substr( :new.command , (instr( :new.command, '"', 1) + 1), (instr( :new.command, '"', 2) - 2)) in ( 'CTO_PACKPARCEL'
														          , 'PALLET_CLOSING'
														          , 'PARCEL_PACKING'
															  )
		and	l_saas > 0
		then
			if	:new.client_id is null
			then
				open 	c_clt;
				fetch	c_clt
				into	l_client;
				close 	c_clt;
			else
				l_client	:= :new.client_id;
			end if;
			-- Ensure it runs using normal task deamon
			:new.java_report := null;
			:new.name:='SCHEDULER';
			if	instr(:new.command,'order_id') > 0
			then
				l_order_id	:= substr(:new.command,instr(:new.command,'"order_id" "')+length('"order_id" "'),instr(substr(:new.command,instr(:new.command,'"order_id" "')+length('"order_id" "')),'"')-1);
			end if;
			if	instr(:new.command,'pallet_id') > 0
			then			
				l_pallet_id	:= substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "'),instr(substr(:new.command,instr(:new.command,'"pallet_id" "')+length('"pallet_id" "')),'"')-1);
			end if;
			if	instr(:new.command,'container_id') > 0
			then			
				l_container_id	:= substr(:new.command,instr(:new.command,'"container_id" "')+length('"container_id" "'),instr(substr(:new.command,instr(:new.command,'"container_id" "')+length('"container_id" "')),'"')-1);
			end if;

			:new.command 	:= '$HOME/reports/CentiroSaas/cto_add_print_parcel.sh '
					|| '-S "' || :new.site_id 			|| '" '
					|| '-C "' || nvl(l_client,'XXXXX')		|| '" '
					|| '-P "' || nvl(l_pallet_id,'XXXXX')		|| '" '
					|| '-c "' || nvl(l_container_id,'XXXXX')	|| '" '
					|| '-O "' || nvl(l_order_id,'XXXXX')		|| '" '
					|| '-R "' || :new.key				|| '" '
					|| '-W "' || nvl(:new.station_id,'XXXXX')	|| '" '
					|| '-T "' || 'addandprint'			|| '" '
					;
		else
			if	r_log = 'ON'
			then
				dbms_application_info.read_module( module_name => l_module
								 , action_name => l_action
								 );

				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> :new.key
									   , p_file_name_i		=> null
									   , p_source_package_i		=> 'cnl_rtk_biur_01'
									   , p_source_routine_i		=> 'cnl_rtk_biur_01'
									   , p_routine_step_i		=> 'Trigger is started by '||l_module||' doing '||l_action
									   , p_code_parameters_i 	=> :new.command
									   , p_order_id_i		=> null
									   , p_client_id_i		=> :new.client_id
									   , p_pallet_id_i		=> null
									   , p_container_id_i		=> null
									   , p_site_id_i		=> :new.site_id
									   );
			end if;
			--
			:new.status  	:= 'In Progress';
			:new.command	:= :new.command || ' "runtaskkey" "'||:new.key||'"';
			--
			if 	instr(:new.command,'locality') = 0
			then
				:new.command := :new.command || 
						' "locality" "' || 
						dcsdba.libsession.getlocality ||
						'" "rdtlocality" "' || 
						dcsdba.libsession.getrdtlocality || 
						'"';
			end if;
			--
			if 	instr(:new.command,'linked_to_dws') = 0
			then
				open	c_wsn;
				fetch 	c_wsn
				into  	l_linked_to_dws;
				close 	c_wsn;
				:new.command := :new.command || 
						' "linked_to_dws" "' || 
						l_linked_to_dws|| 
						'"';
			end if;
			--
			if	r_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> :new.key
									   , p_file_name_i		=> null
									   , p_source_package_i		=> 'cnl_rtk_biur_01'
									   , p_source_routine_i		=> 'cnl_rtk_biur_01'
									   , p_routine_step_i		=> 'Trigger is Finsihed'
									   , p_code_parameters_i 	=> :new.command
									   , p_order_id_i		=> null
									   , p_client_id_i		=> :new.client_id
									   , p_pallet_id_i		=> null
									   , p_container_id_i		=> null
									   , p_site_id_i		=> :new.site_id
									   );
			end if;
		end if;
exception
  when   others
  then
    null;  -- In before Row trigger no raise is allowed
end cnl_rtk_biur_01;