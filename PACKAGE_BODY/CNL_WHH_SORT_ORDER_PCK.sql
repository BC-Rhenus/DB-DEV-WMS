CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WHH_SORT_ORDER_PCK" is
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Set package global variables
------------------------------------------------------------------------------------------------
	g_pck	varchar2(30) := 'cnl_whh_sort_order_pck';

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Get order id inside container
------------------------------------------------------------------------------------------------
	function whh_get_client_id_f( p_container_id_i  in dcsdba.move_task.container_id%type 
				    , p_site_id_i       in dcsdba.move_Task.site_id%type
				    )
		return        dcsdba.move_task.client_id%type
	is
		cursor	c_ord
		is
			select    client_id 
			from      dcsdba.move_task
			where     container_id  =    p_container_id_i
			and       site_id       =    p_site_id_i
			and       task_id       !=   'PALLET'
			and       status        in   ('Consol','WHHConsol')
		;
		--
		g_rtn		varchar2(30):= 'whh_get_client_id_f';
		--
		r_ord 		c_ord%rowtype;
		--
		v_client_id  	dcsdba.move_task.client_id%type;
		--
	begin
		open	c_ord;
		fetch 	c_ord
		into  	r_ord;
		if 	c_ord%notfound 
		then 
			close	c_ord;
			v_client_id	:= 'NOCLIENT';
		else 
			close	c_ord;
			v_client_id	:= r_ord.client_id;
		end if;
		--
		return v_client_id;
	exception
		when others 
		then 
			v_client_id	:= 'NOCLIENT';
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_container_id_i||', '||p_site_id_i	-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			return v_client_id;
	end whh_get_client_id_f; 

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Get order id inside container
------------------------------------------------------------------------------------------------
	function whh_get_order_id_f( p_container_id_i  in dcsdba.move_task.container_id%type 
				   , p_site_id_i       in dcsdba.move_Task.site_id%type
				   )
		return        dcsdba.move_task.task_id%type
	is
		cursor	c_ord
		is
			select    task_id 
			from      dcsdba.move_task
			where     container_id  =   p_container_id_i
			and       site_id       =   p_site_id_i
			and       task_id       !=  'PALLET'
			and       status        in  ('Consol','WHHConsol')
		;
		--
		g_rtn		varchar2(30):= 'whh_get_order_id_f';
		--
		r_ord 		c_ord%rowtype;
		--
		v_order_id  	dcsdba.move_task.task_id%type;
		--
	begin
		open	c_ord;
		fetch 	c_ord
		into  	r_ord;
		if 	c_ord%notfound 
		then 
			close	c_ord;
			v_order_id	:= 'NOORDER';
		else 
			close	c_ord;
			v_order_id	:= r_ord.task_id;
		end if;
		--
		return v_order_id;
	exception
		when others 
		then 
			v_order_id	:= 'NOORDER';
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_container_id_i||', '||p_site_id_i	-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			return v_order_id;
	end whh_get_order_id_f; 

-----------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Figure out if container is last to sort for order.
------------------------------------------------------------------------------------------------
	function whh_last_container_f( p_site_id_i       in  dcsdba.move_task.site_id%type
				     , p_client_id_i     in  dcsdba.order_container.client_id%type
				     , p_container_id_i  in  dcsdba.order_container.container_id%type
				     , p_order_id_i      in  dcsdba.order_container.order_id%type
				     )
		return varchar2 --Y = last, N != last
	is
		cursor	c_ocr
		is
			select	count(*)
			from 	dcsdba.order_container oc
			where 	oc.order_id	= p_order_id_i
			and  	oc.client_id	= p_client_id_i
			and   	oc.container_id != p_container_id_i
		;
		--
		cursor	c_ord
		is
			select	oc.container_id 
			from 	dcsdba.order_container oc
			where 	oc.order_id	= p_order_id_i
			and  	oc.client_id	= p_client_id_i
			and   	oc.container_id != p_container_id_i
		;
		--
		cursor  c_sor( b_container_id	dcsdba.order_container.container_id%type)
		is
			select 	count(*)
			from  	cnl_sys.cnl_wms_sort_order o
			where 	o.container_id 	= b_container_id
			and 	o.client_id 	= p_client_id_i
			and 	o.order_id 	= p_order_id_i
		;
		--
		cursor  c_cnt( b_container_id 	dcsdba.order_container.container_id%type)
		is
			select	count(*)
			from  	cnl_sys.cnl_wms_sort_order o
			where 	o.container_id 	= b_container_id
			and 	o.client_id 	= p_client_id_i
			and 	o.order_id 	= p_order_id_i
		;
		--
		cursor	c_mvt 
		is
			select	count (*)
			from 	dcsdba.move_task mvt
			where 	mvt.task_type		= 'O'
			and 	mvt.status		not in ('Consol','WHHConsol')
			and 	mvt.task_id		= p_order_id_i
			and 	mvt.client_id		= p_client_id_i
			and 	mvt.site_id		= p_site_id_i
			and 	mvt.container_id 	is null
			and 	mvt.pallet_id 		is null
		;
		--
		cursor	c_srt( b_container_id	varchar2)
		is
			select 	count(*)
			from 	dcsdba.move_task m
			where 	m.container_id 	= b_container_id
			and	m.site_id 	= p_site_id_i
			and	m.client_id	= p_client_id_i
			and	m.stage_route_id is not null
			and	m.task_id 	!= 'PALLET'
			and	m.stage_route_id not in (	select 	distinct m2.stage_route_id 
								from	dcsdba.move_task m2
								where 	m2.container_id = p_container_id_i
								and	m2.site_id	= p_site_id_i
								and	m2.client_id	= p_client_id_i
								and	m2.stage_route_id is not null
								and	m2.task_id 	!= 'PALLET'
							)
		;				    		
		g_rtn 		varchar2(30):= 'whh_last_container_f';
		--
		r_ocr		number;
		r_mvt 		number; 
		r_sor 		number;
		r_srt		number;
		--
		v_last_box 	varchar2(1) :='Y';
	begin
		-- Check if current container is only container.
		open	c_ocr;
		fetch 	c_ocr
		into 	r_ocr;
		close 	c_ocr;
		if	r_ocr > 0 
		then	-- More containers found. Start checking each container to find out if it is already sorted
			<<container_loop>>
			for	r_ord in c_ord
			loop
				open	c_sor(r_ord.container_id);
				fetch 	c_sor 
				into 	r_sor;
				close 	c_sor;
				if  	r_sor = 0 
				then	-- Found another container that is not yet sorted.
					open	c_srt(r_ord.container_id);
					fetch 	c_srt
					into 	r_srt;
					close 	c_srt;
					if 	r_srt = 0
					then	-- Container has same route So current container is not the last container.
						v_last_box	:= 'N';
						exit container_loop;
					end if;
				end if;
			end loop;
		end if;
		-- When no other containers found we check for open pick tasks
		if 	v_last_box = 'Y'
		then
			open	c_mvt;
			fetch	c_mvt
			into 	r_mvt;
			close 	c_mvt;
			if 	r_mvt > 0
			then	-- Found pending pick tasks so current container is not last container to sort.
				v_last_box := 'N';
			end if;
		end if;
		--
		return 	v_last_box;
	exception
		when others 
		then 
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_container_id_i||', '||
											   p_site_id_i||', '||
											   p_client_id_i||', '||
											   p_order_id_i				-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
		return 'Y';  -- During exceptions last box will be set to Yes by default. Worst scenario is we have packed to many containers.
		--
	end whh_last_container_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Sortation finished location is emptied
------------------------------------------------------------------------------------------------
	procedure location_reserve_p( p_location_id_i	in dcsdba.location.location_id%type
				    , p_site_id_i	in dcsdba.location.site_id%type
				    , p_reserve_yn_i	in varchar2
				    )
	is
		pragma 		autonomous_transaction;
	begin
		if 	p_reserve_yn_i = 'Y'
		then
			update	dcsdba.location
			set 	user_def_type_8 = 'RESERVED'
			where	site_id 	= p_site_id_i
			and	location_id 	= p_location_id_i
			and	lock_status 	= 'Locked'
			;
		else
			update	dcsdba.location
			set 	user_def_type_8 = null
			where	user_def_type_8 = 'RESERVED'
			and	location_id    	= p_location_id_i
			and	site_id 	= p_site_id_i
			and	lock_status 	= 'Locked'
			;
		end if;
		commit;
	end 	location_reserve_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Fetch a location the container must be sorted to
------------------------------------------------------------------------------------------------
	function whh_get_location_f( p_container_id_i  	in  dcsdba.move_task.container_id%type 
				   , p_workstation_i	in  dcsdba.workstation.station_id%type 
				   , p_message_o       	out varchar2
				   , p_last_cont_o     	out varchar2
				   , p_order_id_o	out dcsdba.order_header.order_id%type
				   , p_client_id_o	out dcsdba.order_header.client_id%type
				   )
		return        dcsdba.location.location_id%type
	is
		-- Search for a location already used for the same order
		cursor	c_loc( b_order_id dcsdba.move_task.task_id%type)
		is
			select	o.* 
			from 	cnl_sys.cnl_wms_sort_order o
			where 	o.order_id	= b_order_id
		;

		-- Select minimal weight and volume restrictions for new empty location 
		cursor	c_odh( b_client_id dcsdba.order_header.client_id%type
			     , b_order_id  dcsdba.order_header.order_id%type
			     , b_site_id   dcsdba.order_header.from_site_id%type
			     )
		is
			select    nvl(greatest(o.order_volume, o.expected_volume),0) min_volume
			,         nvl(greatest(o.order_weight, o.expected_weight),0) min_weight
			from      dcsdba.order_header o
			where     o.client_id		= b_client_id
			and       o.order_id		= b_order_id
			and       o.from_site_id	= b_site_id
		;

		-- Select automation_equipment id
		cursor	c_aeq( b_workstation	dcsdba.workstation.station_id%type
			     , b_site		dcsdba.workstation.site_id%type
			     )
		is
			select	a.automation_id 
			from 	dcsdba.automation_equipment a
			where 	a.automation_type 	= b_workstation
			and	a.site_id 		= b_site
		;

		-- Select zones used for automation equipment
		cursor c_zns( b_site		dcsdba.location.site_id%type
			    , b_automation_id	dcsdba.location.automation_id%type
			    )
		is
			select 	l.work_zone
			,	l.zone_1
			,	l.subzone_1
			,	l.subzone_2
			from	dcsdba.location l
			where	l.site_id 	= b_site
			and	l.automation_id	= b_automation_id
			and	rownum		= 1
		;

		-- select location
		cursor	c_lcn( b_min_volume	number
			     , b_min_weight	number
			     , b_automation_id	dcsdba.location.automation_id%type
			     , b_site		dcsdba.location.site_id%type
			     , b_work_zone	dcsdba.location.work_zone%type
			     , b_zone_1		dcsdba.location.zone_1%type
			     , b_subzone_1	dcsdba.location.subzone_1%type
			     , b_subzone_2	dcsdba.location.subzone_2%type
			     )
		is
			select	l.location_id
			from	dcsdba.location l
			where	l.volume    	>= 	b_min_volume
			and 	l.weight    	>= 	b_min_weight
			and 	l.site_id 	= 	b_site
			and	l.work_zone	= 	b_work_zone
			and (	l.user_def_type_8 is 	null or 
				l.user_def_type_8 != 	'RESERVED'
			    )
			and ( 	l.automation_id	=	b_automation_id or
				(	b_automation_id is null
				and	l.automation_id is not null
				)
			    )
			and (	l.zone_1	= 	b_zone_1 or
				b_zone_1	is null)
			and (	l.subzone_1	=	b_subzone_1 or
				b_subzone_1	is null)
			and (	l.subzone_2	=	b_subzone_2 or
				b_subzone_2	is null)
			and	l.location_id 	not in(	select	s.location_id 
							from 	cnl_sys.cnl_wms_sort_order s
							where	s.location_id = l.location_id)
		order
		by	l.volume 	asc
		,	l.weight 	asc
		,	l.location_id	asc
		;

		-- Check if workstation exists
		cursor	c_wst
		is
			select 	site_id
			from	dcsdba.workstation
			where 	station_id = p_workstation_i
		;

		-- Global variable
		g_rtn 		varchar2(30):= 'whh_get_location_f';

		-- Cursor variable
		r_loc		c_loc%rowtype;
		r_lcn		c_lcn%rowtype;
		r_wst		dcsdba.workstation.site_id%type;

		-- Local variable
		v_site_id	dcsdba.workstation.site_id%type;
		v_client_id	dcsdba.order_header.client_id%type;
		v_location  	dcsdba.location.location_id%type;
		v_automation_id	dcsdba.location.automation_id%type;
		v_work_zone	dcsdba.location.work_zone%type;
		v_zone_1	dcsdba.location.zone_1%type;
		v_subzone_1	dcsdba.location.subzone_1%type;
		v_subzone_2	dcsdba.location.subzone_2%type;
		v_order_id 	dcsdba.move_task.task_id%type;
		v_min_volume	number;
		v_min_weight	number;
		v_message	varchar2(4000);
		v_last_cont	varchar2(1);
		--
	begin
		-- Error when workstation is null or not found
		if	p_workstation_i is null
		then
			v_location	:= 'INVALIDSTATION';
			v_message	:= 'The workstation used is either not filled or does not exist in WMS.';
			v_last_cont	:= 'N';
		else	-- workstation is filled now check if it exists in WMS
			open	c_wst;
			fetch	c_wst
			into	r_wst;
			close 	c_wst;
			--
			if 	r_wst is null
			then	-- Can't find workstation so can't work out site id
				v_location	:= 'INVALIDSTATION';
				v_message	:= 'The workstation used is either not filled or does not exist in WMS.';
				v_last_cont	:= 'N';
			else	-- workstation exists in WMS so set site id
				v_site_id 	:= r_wst;

				-- Fetch order id inside container.
				v_order_id	:= whh_get_order_id_f( p_container_id_i
								     , v_site_id
								     );

				if	v_order_id = 'NOORDER'
				then	-- Could not find a matching order for the current container.
					v_location	:= v_order_id;
					v_message	:= 'Could not link an order to this container and therefor could not select a location.';
					v_last_cont	:= 'N';
				else
					v_client_id 	:= whh_get_client_id_f( p_container_id_i
									     , v_site_id
									     );
					if	v_client_id = 'NOCLIENT'
					then	-- Could not find a matching client for container
						v_location	:= v_client_id;
						v_message	:= 'Could not link a client to this container and therefor could not select a location.';
						v_last_cont	:= 'N';
					else	-- Matching order and client found now look if order already linked to a location.
						open	c_loc( v_order_id);
						fetch 	c_loc
						into 	r_loc;
						close 	c_loc;
						--
						if 	r_loc.key is not null
						then	
							v_client_id 	:= r_loc.client_id;

							-- Order already linked to location
							v_location	:= r_loc.location_id;
							v_message	:= 'Order already linked to location. Please use same location';
							v_last_cont 	:= whh_last_container_f( p_site_id_i       =>  v_site_id
											       , p_client_id_i     =>  v_client_id
											       , p_container_id_i  =>  p_container_id_i
											       , p_order_id_i      =>  v_order_id
											       );
						else	-- New order to sort. Fetch order details needed to find suitable location.
							open	c_odh( v_client_id
								     , v_order_id
								     , v_site_id
								     );
							fetch	c_odh
							into	v_min_volume
							,	v_min_weight;
							close 	c_odh;

							-- Fetch empty location 
							-- Fetch automation_id
							open 	c_aeq( p_workstation_i
								     , v_site_id
								     );
							fetch 	c_aeq
							into	v_automation_id;
							if	c_aeq%notfound
							then
								close 	c_aeq;
								v_location 	:= 'NOEMPTYLOCATION';
								v_message	:= 'No automation id linked to workstation';
								v_last_cont	:= 'N';
							else	-- Fetch zones linked to automation id
								close 	c_aeq;
								open 	c_zns( v_site_id
									     , v_automation_id
									     );
								fetch 	c_zns
								into	v_work_zone
								,	v_zone_1
								,	v_subzone_1
								,	v_subzone_2;
								if	c_zns%notfound
								then	--Can't find locations linked to automation id
									close 	c_zns;
									v_location 	:= 'NOEMPTYLOCATION';
									v_message	:= 'No locations found linked to workstation';
									v_last_cont	:= 'N';
								else	-- Search for empty locations within zones (perfect match)
									close 	c_zns;
									open	c_lcn( v_min_volume
										     , v_min_weight
										     , v_automation_id
										     , v_site_id
										     , v_work_zone
										     , v_zone_1
										     , v_subzone_1
										     , v_subzone_2
										     );
									fetch	c_lcn
									into 	r_lcn;
									if	c_lcn%notfound
									then	-- search outside subzone 2 
										close	c_lcn;
										open	c_lcn( v_min_volume
											     , v_min_weight
											     , null--v_automation_id
											     , v_site_id
											     , v_work_zone
											     , v_zone_1
											     , v_subzone_1
											     , null
											     );
										fetch	c_lcn
										into 	r_lcn;
										if	c_lcn%notfound
										then	-- search outside subzone 1 and 2
											close	c_lcn;
											open	c_lcn( v_min_volume
												     , v_min_weight
												     , null--v_automation_id
												     , v_site_id
												     , v_work_zone
												     , v_zone_1
												     , null
												     , null
												     );
											fetch	c_lcn
											into 	r_lcn;
											if	c_lcn%notfound
											then	-- search outside zone 1
												close	c_lcn;
												open	c_lcn( v_min_volume
													     , v_min_weight
													     , null--v_automation_id
													     , v_site_id
													     , v_work_zone
													     , null
													     , null
													     , null
													     );
												fetch	c_lcn
												into 	r_lcn;
												if	c_lcn%notfound
												then	-- No locations can be found anywhere
													close 	c_lcn;
													v_location 	:= 'NOEMPTYLOCATION';
													v_message	:= 'Can''t find a location big enough for this order minimal volume '
															|| v_min_volume ||' minimal weight '
															|| v_min_weight ||' Work_zone '
															|| v_work_zone	||' client_id '
															|| v_client_id	||' order_id '
															|| v_order_id	||' site_id '
															|| v_site_id
															;	
													v_last_cont	:= 'N';
												else
													close 	c_lcn;
													v_location 	:= r_lcn.location_id;
													v_message	:= 'Add this container to the assigned location for this order';
													v_last_cont	:= whh_last_container_f( p_site_id_i       =>  v_site_id
																	       , p_client_id_i     =>  v_client_id
																	       , p_container_id_i  =>  p_container_id_i
																	       , p_order_id_i      =>  v_order_id
																	       );
												end if;
											else
												close 	c_lcn;
												v_location 	:= r_lcn.location_id;
												v_message	:= 'Add this container to the assigned location for this order';
												v_last_cont	:= whh_last_container_f( p_site_id_i       =>  v_site_id
																       , p_client_id_i     =>  v_client_id
																       , p_container_id_i  =>  p_container_id_i
																       , p_order_id_i      =>  v_order_id
																       );
										end if;
										else
											close 	c_lcn;
											v_location 	:= r_lcn.location_id;
											v_message	:= 'Add this container to the assigned location for this order';
											v_last_cont	:= whh_last_container_f( p_site_id_i       =>  v_site_id
															       , p_client_id_i     =>  v_client_id
															       , p_container_id_i  =>  p_container_id_i
															       , p_order_id_i      =>  v_order_id
															       );
										end if;
									else
										v_location 	:= r_lcn.location_id;
										v_message	:= 'Add this container to the assigned location for this order';
										v_last_cont	:= whh_last_container_f( p_site_id_i       =>  v_site_id
														       , p_client_id_i     =>  v_client_id
														       , p_container_id_i  =>  p_container_id_i
														       , p_order_id_i      =>  v_order_id
														       );
									end if;
								end if;
							end if;
						end if;
					end if;
				end if;
			end if;
		end if;
		-- Set pending out parameters
		p_order_id_o	:= v_order_id;
		p_client_id_o	:= v_client_id;
		p_message_o	:= v_message;
		p_last_cont_o 	:= v_last_cont;
		--		
		location_reserve_p( p_location_id_i 	=> v_location
				  , p_site_id_i		=> v_site_id
				  , p_reserve_yn_i	=> 'Y');
		--
		Return	v_location;
	exception
		when others 
		then 
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_container_id_i||', '||
											   p_workstation_i
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
		return 'ORACLE_ERROR';
	end whh_get_location_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Autonomous transaction to insert , update and delete sortation order records.
------------------------------------------------------------------------------------------------
	procedure iud_order_rec_p( p_location_id_i	in  dcsdba.location.location_id%type
				 , p_order_id_i     	in  dcsdba.order_header.order_id%type
				 , p_container_id_i 	in  dcsdba.move_task.container_id%type default null
				 , p_last_cont_i    	in  varchar2
				 , p_client_id_i    	in  dcsdba.order_header.client_id%type default null
				 , p_action_i       	in  varchar2 -- I or D
				 )
	is
		cursor	c_rec
		is
			select	count(*)
			from	cnl_sys.cnl_wms_sort_order
			where	order_id 	= p_order_id_i
			and	container_id	= p_container_id_i
			and	location_id	= p_location_id_i
			and	client_id	= p_client_id_i
		;
		--
      cursor   c_prio(p_order_id_i varchar2, p_client_id_i varchar2)
      is
			select	priority, export
			from	dcsdba.order_header oh
			where	order_id 	= p_order_id_i
			and	client_id	= p_client_id_i
      ;
      --
		g_rtn		varchar2(30):= 'iud_order_rec';
		--
		r_rec		number;
		--
      l_priority dcsdba.order_header.priority%type;
      --
      l_export dcsdba.order_header.export%type;
      --
		pragma 		autonomous_transaction;
	begin
		open	c_rec;
		fetch	c_rec
		into	r_rec;
		close	c_rec;
		if	r_rec = 0
		then
			if	p_action_i = 'I'
			then
            open c_prio(p_order_id_i => p_order_id_i, p_client_id_i => p_client_id_i);
            fetch c_prio into l_priority, l_export;
            close c_prio;

				insert into cnl_wms_sort_order
				(	location_id
				, 	order_id
				, 	order_complete
				,	client_id
				, 	container_id
            ,  priority
            ,  export

				)
				values 
				(	p_location_id_i
				,	p_order_id_i
				, 	p_last_cont_i
				, 	p_client_id_i
				, 	p_container_id_i
            ,  l_priority
            ,  l_export
				);
				--
				if	p_last_cont_i = 'Y' 
				then
					update	cnl_wms_sort_order 
					set     order_complete = p_last_cont_i
					where   location_id = p_location_id_i
					and     order_id 	= p_order_id_i
					and     client_id	= p_client_id_i
					;
				end if;  
			end if;
		else
			if	p_last_cont_i = 'Y' 
			then
				update	cnl_wms_sort_order 
				set     order_complete = p_last_cont_i
				where   location_id = p_location_id_i
				and     order_id 	= p_order_id_i
				and     client_id	= p_client_id_i
				;
			end if;  
		end if;			
		--
		if	p_action_i = 'D'
		then
			delete	cnl_wms_sort_order
			where  	location_id 	= p_location_id_i;
		end if;
		--
		commit;

	exception
		when others 
		then 
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_location_id_i  ||', '||
											   p_order_id_i     ||', '||
											   p_container_id_i ||', '||
											   p_last_cont_i    ||', '||
											   p_client_id_i    ||', '||   
											   p_action_i				-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end iud_order_rec_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Confirm sortation.
------------------------------------------------------------------------------------------------
	procedure confirm_sortation_p( p_location_id_i  in  dcsdba.location.location_id%type
				     , p_order_id_i     in  dcsdba.order_header.order_id%type
				     , p_container_id_i in  dcsdba.move_task.container_id%type default null
				     , p_last_cont_i    in  varchar2
				     , p_client_id_i    in  dcsdba.order_header.client_id%type default null
				     , p_loc_chkstrng_i in  dcsdba.location.check_string%type
				     , p_site_id_i      in  dcsdba.order_header.from_site_id%type
				     , p_ok_yn_o        out varchar2
				     , p_message_o      out varchar2
				     )
	is
		--
		cursor	c_loc
		is
			select	l.check_string
			from    dcsdba.location l
			where   l.site_id     = p_site_id_i
			and     l.location_id = p_location_id_i
		;
		--
		g_rtn varchar2(30):= 'sort_order_rec_p';
		--
		r_loc c_loc%rowtype;
		--
		v_ok    varchar2(1)       :='N';
		v_mess  varchar2(4000);
		--
	begin
		if	p_loc_chkstrng_i = 'ESCAPE'
		then
			v_mess  := 'Sortation escaped. No sortation done.';
			v_ok    := 'Y';
		else    
			open    c_loc;
			fetch   c_loc
			into    r_loc;
			if      c_loc%found
			then 	-- Found location to match check string
				close c_loc;
				if	p_loc_chkstrng_i	= r_loc.check_string
				then 	-- Check sring matched so sort was succesfull
					v_ok 	:= 'Y';
					-- Insert container to sorted containers
					iud_order_rec_p( p_location_id_i    =>      p_location_id_i
						       , p_order_id_i       =>      p_order_id_i 
						       , p_container_id_i   =>      p_container_id_i
						       , p_last_cont_i      =>      p_last_cont_i 
						       , p_client_id_i      =>      p_client_id_i
						       , p_action_i         =>      'I'
						       );
					v_mess	:='Sortation executed succesfully.';             
				else 	-- Checkstring error
					v_mess 	:= 'Incorrect location, sortation not performed.';
					v_ok 	:= 'N';
				end if;
			else 	-- No matching location found
				close   c_loc;
				v_mess 	:= 'Could not find provided location. Sortation not executed';
				v_ok 	:= 'N';
			end if;
		end if;
		--
		location_reserve_p( p_location_id_i 	=> p_location_id_i
				  , p_site_id_i		=> p_site_id_i
				  , p_reserve_yn_i	=> 'N');
		--
		p_message_o 	:= v_mess;
		p_ok_yn_o 	:= v_ok;
	exception
		when others 
		then 
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_location_id_i  ||', '||
											   p_order_id_i     ||', '||
											   p_container_id_i ||', '||
											   p_last_cont_i    ||', '||
											   p_client_id_i    ||', '||   
											   p_loc_chkstrng_i ||', '||  
											   p_site_id_i     			-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			p_message_o	:= 'An exception was raised please check cnl_sys.cnl_error';
			p_ok_yn_o 	:= 'N';
	end confirm_sortation_p;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Sortation finished location is emptied
------------------------------------------------------------------------------------------------
	procedure delete_sort_order_p( p_location_id_i	in dcsdba.location.location_id%type
				     , p_order_id_i     in  dcsdba.order_header.order_id%type
				     )
	is
	begin
		iud_order_rec_p( p_location_id_i  => p_location_id_i
			       , p_order_id_i     => p_order_id_i
			       , p_last_cont_i    => null
			       , p_action_i       => 'D'
			       );
	end delete_sort_order_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 13-Mar-2020
-- Purpose : Initialize package to load it faster.
------------------------------------------------------------------------------------------------
	begin
	-- initialization
	null;	
	--	
end cnl_whh_sort_order_pck;