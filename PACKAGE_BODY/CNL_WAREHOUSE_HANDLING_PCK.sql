CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WAREHOUSE_HANDLING_PCK" is
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-Nov-2019
-- Purpose : Private constant declarations
------------------------------------------------------------------------------------------------
	
	g_logging	constant varchar2(10)	:= nvl(cnl_whh_util_pck.get_system_profile_f('-ROOT-_USER_WAREHOUSE-HANDLING_LOGGING_LOGGING'),'OFF');
	g_pck		varchar2(30) := 'cnl_warehouse_handling_pck';

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 08-Nov-2019
-- Purpose : Procedure to capture pallets on WHH locations
------------------------------------------------------------------------------------------------
	procedure collect_whh_pallets_p
	is
		cursor 	c_pal
		is
			select	m.pallet_id
			,	m.site_id
			from	dcsdba.move_task m
			where	m.status 	= 'WHHandling'
			and	m.task_id 	= 'PALLET'
			and	m.dstamp	between (sysdate - interval '2' minute) and sysdate
		;
	begin
		for r in c_pal
		loop
			fetch_client_vas_activity(r.pallet_id, r.site_id);
		end loop;
	end collect_whh_pallets_p;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 08-Nov-2019
-- Purpose : Add client VAS actvities to Container VAS activity When container is added to WHH
------------------------------------------------------------------------------------------------
	procedure fetch_client_vas_activity( p_pallet_i		in varchar2
					   , p_site_id_i	in varchar2
					   )
	is
	-- Collect all move tasks
	type move_task_rec is record( order_id		dcsdba.move_task.task_id%type
				    , client_id		dcsdba.move_task.client_id%type
				    , pallet_id		dcsdba.move_task.pallet_id%type
				    , container_id	dcsdba.move_task.container_id%type
				    , sku_id		dcsdba.move_task.sku_id%type
				    , customer_id	dcsdba.move_task.customer_id%type
				    , country		dcsdba.order_header.country%type
				    );
	type move_task_tab is table of move_task_rec;
	l_move_tasks		move_task_tab;	
	l_move_tasks_count	integer;

	-- Collect client VAS tasks
	type client_vas_rec is record( id			cnl_sys.cnl_client_vas_activity.id%type
				     , activity_id		cnl_sys.cnl_client_vas_activity.activity_id%type
				     , sku_id			cnl_sys.cnl_client_vas_activity.sku_id%type
				     , activity_name		cnl_sys.cnl_vas_activity.activity_name%type
				     , activity_sequence	cnl_sys.cnl_client_vas_activity.activity_sequence%type
				     , activity_instruction	cnl_sys.cnl_client_vas_activity.activity_instruction%type
				     );
	type client_vas_tab is table of client_vas_rec;
	l_client_vas		client_vas_tab;	
	l_client_vas_count	integer;

	-- cursor to fetch already existing container vas records.
	cursor c_cva( b_container_id 	varchar2
		    , b_client_id	varchar2
		    , b_order_id	varchar2
		    , b_sku_id		varchar2
		    , b_activity_name	varchar2
		    )
	is
		select	count(*)
		from	cnl_sys.cnl_container_vas_activity c
		where	(	c.container_id	= b_container_id or
				c.container_id 	is null
			)
		and	c.client_id 	= b_client_id
		and	c.order_id	= b_order_id
		and	(	c.sku_id	= b_sku_id or
				c.sku_id is null
			)
		and	c.activity_name	= b_activity_name
	;
	r_cva	integer;
	--
	g_rtn	varchar2(30) := 'fetch_client_vas_activity';
	pragma 	autonomous_transaction;
    begin
	if 	g_logging = 'ON'
	then
		cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
 						 , p_pallet_id_i		=> p_pallet_i
 						 , p_package_name_i		=> 'cnl_whh_util_pck'
						 , p_procedure_function_i	=> 'fetch_client_vas_activity'
						 , p_comment_i			=> 'Starting fetch client VAS activity with attached parameters'
						 );
	end if;
	-- clear table
	l_move_tasks := move_task_tab();    

	-- fetch all consol move_tasks on pallet
	select	distinct m.task_id order_id
	,	m.client_id
	,	m.pallet_id
	,	m.container_id
	,	m.sku_id
	,	m.customer_id
	,	o.country
	bulk collect into l_move_tasks
	from 	dcsdba.move_task m
	,	dcsdba.order_header o
	where	m.site_id 	= p_site_id_i
	and	o.from_site_id  = p_site_id_i
	and	m.pallet_id 	= p_pallet_i
	and	m.task_id 	!= 'PALLET'
	and	o.order_id 	= m.task_id
	and	o.client_id	= m.client_id
	and	o.customer_id	= o.customer_id
	and	m.status 	in ('WHHConsol','Consol')
	order by container_id
	;
	l_move_tasks_count := l_move_tasks.count; -- total records
	--
	if 	g_logging = 'ON'
	then
		cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
 						 , p_pallet_id_i		=> p_pallet_i
 						 , p_package_name_i		=> 'cnl_whh_util_pck'
						 , p_procedure_function_i	=> 'fetch_client_vas_activity'
						 , p_comment_i			=> 'Found '||l_move_tasks_count||' consol tasks for pallet'
						 );
	end if;
	--
	if	l_move_tasks_count is not null
	and	l_move_tasks_count > 0
	then
		<<container_loop>>
		for 	r_con in l_move_tasks.first .. l_move_tasks.last
		loop
			begin -- create routine with exception handler
				-- clear table
				l_client_vas := client_vas_tab();  

				-- fetch client vas records into client vas
				select	cls.id			
				, 	cls.activity_id	
				,	cls.sku_id
				, 	vas.activity_name
				, 	cls.activity_sequence
				, 	cls.activity_instruction
				bulk collect into l_client_vas
				from	cnl_sys.cnl_client_vas_activity cls
				,	cnl_vas_activity vas
				where	vas.id 		= cls.activity_id
				and	cls.client_id 	= l_move_tasks(r_con).client_id
				and	(	cls.customer_id = l_move_tasks(r_con).customer_id or
						cls.customer_id is null
					)
				and	(	cls.country = l_move_tasks(r_con).country or
						cls.country is null
					)
				and	(	cls.sku_id = l_move_tasks(r_con).sku_id or
						cls.sku_id is null
					)
				;
				l_client_vas_count := l_client_vas.count; -- total records.
				--
				if 	g_logging = 'ON'
				then
					cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
									 , p_pallet_id_i		=> p_pallet_i
									 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
									 , p_procedure_function_i	=> 'fetch_client_vas_activity'
									 , p_comment_i			=> 'Found '||l_client_vas_count||' VAS activities. next step check if adding is required'
									 );
				end if;
				--
				if	l_client_vas_count is not null
				and	l_client_vas_count > 0
				then	-- Found client vas activities. Now look if they need to be added
					<<client_vas_loop>>
					for	r_clv in l_client_vas.first .. l_client_vas.last
					loop
						begin -- create routine with exception handler
							open	c_cva( l_move_tasks(r_con).container_id
								     , l_move_tasks(r_con).client_id
								     , l_move_tasks(r_con).order_id
								     , l_move_tasks(r_con).sku_id
								     , l_client_vas(r_clv).activity_name
								     );
							fetch 	c_cva into r_cva;
							close	c_cva;
							if	r_cva > 0
							then	-- VAS activity already in 
								if 	g_logging = 'ON'
								then
									cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
													 , p_pallet_id_i		=> p_pallet_i
													 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
													 , p_procedure_function_i	=> 'fetch_client_vas_activity'
													 , p_comment_i			=> 'VAS activity already added'
													 );
								end if;
								--
								continue;
							else	-- add activity using !!Autostore!! procedure
								if 	g_logging = 'ON'
								then
									cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
													 , p_client_id_i		=> l_move_tasks(r_con).client_id
													 , p_order_id_i			=> l_move_tasks(r_con).order_id
													 , p_container_id_i		=> l_move_tasks(r_con).container_id
													 , p_sku_id_i			=> l_client_vas(r_clv).sku_id
													 , p_pallet_id_i		=> p_pallet_i
													 , p_extra_parameters_i		=> 'Activity name '||l_client_vas(r_clv).activity_name||', activity sequence '||l_client_vas(r_clv).activity_sequence 
													 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
													 , p_procedure_function_i	=> 'fetch_client_vas_activity'
													 , p_comment_i			=> 'Add VAS activity'
													 );
								end if;
								--
								cnl_sys.cnl_as_pck.add_vas_activity( p_container_id_i           => l_move_tasks(r_con).container_id
												   , p_client_id_i              => l_move_tasks(r_con).client_id
												   , p_order_id_i               => l_move_tasks(r_con).order_id
												   , p_sku_id_i                 => l_client_vas(r_clv).sku_id
												   , p_activity_name_i          => l_client_vas(r_clv).activity_name
												   , p_activity_sequence_i      => l_client_vas(r_clv).activity_sequence
												   , p_activity_instruction_i   => l_client_vas(r_clv).activity_instruction
												   );
							end if;
						exception 
							when others
							then
								cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
												  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
												  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
												  , p_package_name_i		=> g_pck				-- Package name the error occured
												  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
												  , p_routine_parameters_i	=> l_move_tasks(r_con).container_id||', '||
																   l_move_tasks(r_con).client_id||', '|| 
																   l_move_tasks(r_con).order_id||', '||
																   l_move_tasks(r_con).sku_id||', '||
																   l_client_vas(r_clv).activity_name			-- list of all parameters involved
												  , p_comments_i		=> null					-- Additional comments describing the issue
												  );
						end;
					end loop; --l_client_vas
				else
					continue;
				end if;
			exception
				when others 
				then
					cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
									  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
									  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
									  , p_package_name_i		=> g_pck				-- Package name the error occured
									  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
									  , p_routine_parameters_i	=> p_pallet_i||', '||p_site_id_i	-- list of all parameters involved
									  , p_comments_i		=> null					-- Additional comments describing the issue
									  );
			end;
		end loop; -- l_move_tasks
	end if;
    end fetch_client_vas_activity;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 29-Okt-2019
-- Purpose : Get SKU id using EAN, UPC, SUP or TUC	
------------------------------------------------------------------------------------------------
	function get_sku_id_f( p_client_id_i		in dcsdba.sku.client_id%type
			     , p_id_i			in varchar2
			     , p_qty_o			out number
			     )
	return	varchar2
	is
		cursor	c_sku
		is
			select	s.sku_id
			from	dcsdba.sku s
			where	(s.sku_id 	= p_id_i or
				 s.ean 	  	= p_id_i or
				 s.upc	  	= p_id_i)
			and	s.client_id 	= p_client_id_i
			and	rownum 		= 1
		;
		--
		cursor c_sup
		is
			select	s.sku_id
			from	dcsdba.supplier_sku s
			where	s.supplier_sku_id 	= p_id_i
			and	s.client_id		= p_client_id_i
		;
		--
		cursor	c_tuc
		is
			select	t.sku_id
			,	t.quantity
			from	dcsdba.sku_tuc t
			where	tuc 		= p_id_i
			and	client_id 	= p_client_id_i
		;
		--
		g_rtn		varchar2(30) := 'get_sku_id_f';
		v_id		varchar2(50);
		v_qty		number;
	begin
		open	c_sku;
		fetch 	c_sku into v_id;
		if	c_sku%notfound
		then
			close	c_sku;
			open	c_sup;
			fetch	c_sup into v_id;
			if	c_sup%notfound
			then
				close 	c_sup;
				open 	c_tuc;
				fetch 	c_tuc into v_id, v_qty;
				if	c_tuc%notfound
				then
					close	c_tuc;
					v_id := p_id_i;
				end if;
			end if;
		end if;
		--
		if v_id is null
		then
			v_id := p_id_i;
		end if;
		--
		if	v_qty is null
		then
			p_qty_o := 1;
		else
			p_qty_o := v_qty;
		end if;
		--
		return 	v_id;
	exception
		when others
		then
			return p_id_i;
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i	=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> p_client_id_i||', '||p_id_i||', '||p_qty_o	-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
	end get_sku_id_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 3-Okt-2019
-- Purpose : Insert QC parameters into QC table
------------------------------------------------------------------------------------------------
	procedure set_qc_parameters_p( p_order_id_i		in dcsdba.order_header.order_id%type
				     , p_client_id_i		in dcsdba.order_header.client_id%type
				     , p_site_id_i		in dcsdba.order_header.from_site_id%type
				     , p_qc_req_yn_i		in cnl_sys.cnl_wms_qc_order.qc_req_yn%type -- QC is required 
				     , p_qc_batch_yn_i		in cnl_sys.cnl_wms_qc_order.qc_batch_yn%type -- QC batch id is required
				     , p_qc_qty_def_yn_i 	in cnl_sys.cnl_wms_qc_order.qc_qty_def_yn%type -- QTY is default 1 during QC
			             , p_qc_sku_select_yn_i	in cnl_sys.cnl_wms_qc_order.qc_sku_select_yn%type -- SKU can be selected from overview during QC
				     , p_qc_qty_upd_yn_i	in cnl_sys.cnl_wms_qc_order.qc_qty_upd_yn%type -- Default QTY can be changed
				     , p_qc_serial_yn_i		in cnl_sys.cnl_wms_qc_order.qc_serial_yn%type -- Serial check is required
				     )
	is
		cursor c_o
		is
			select 	c.*
 			from	cnl_sys.cnl_wms_qc_order c
			where	order_id 	= upper(p_order_id_i)
			and	client_id 	= upper(p_client_id_i)
			and	site_id		= upper(p_site_id_i)
			;
		--
		v_qc_order	cnl_sys.cnl_wms_qc_order%rowtype;
		--
		g_rtn		varchar2(30) := 'set_qc_parameters_p';
		pragma autonomous_transaction;
	begin
		if 	g_logging = 'ON'
		then
			cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
							 , p_client_id_i		=> p_client_id_i
							 , p_order_id_i			=> p_order_id_i
							 , p_container_id_i		=> null
							 , p_sku_id_i			=> null
							 , p_pallet_id_i		=> null
							 , p_extra_parameters_i		=> 'QC_required = '		|| p_qc_req_yn_i ||
											   ', Batch check required = '	|| p_qc_batch_yn_i ||
											   ', Default QTY = '		|| p_qc_qty_def_yn_i ||
											   ', SKU select enabled = '	|| p_qc_sku_select_yn_i ||
											   ', QTY update allowed = '	|| p_qc_qty_upd_yn_i ||
											   ', serial check req = '	|| p_qc_serial_yn_i
							 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
							 , p_procedure_function_i	=> 'set_qc_parameters_p'
							 , p_comment_i			=> 'Setting QC parameters'
							 );
		end if;
		--
		open 	c_o;
		fetch	c_o into v_qc_order;
		close 	c_o;
		--
		if	v_qc_order.order_id is null
		then
			if 	g_logging = 'ON'
			then
				cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
								 , p_client_id_i		=> p_client_id_i
								 , p_order_id_i			=> p_order_id_i
								 , p_container_id_i		=> null
								 , p_sku_id_i			=> null
								 , p_pallet_id_i		=> null
								 , p_extra_parameters_i		=> null
								 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
								 , p_procedure_function_i	=> 'set_qc_parameters_p'
								 , p_comment_i			=> 'Adding order to QC orders'
								 );
			end if;
			--
			insert into cnl_sys.cnl_wms_qc_order(order_id, client_id, site_id, qc_req_yn, qc_batch_yn, qc_qty_def_yn, qc_sku_select_yn, qc_qty_upd_yn, qc_serial_yn)
			values(	upper(p_order_id_i)
			      , upper(p_client_id_i)
			      , upper(p_site_id_i)
			      , upper(p_qc_req_yn_i)
			      , upper(p_qc_batch_yn_i)
			      , upper(p_qc_qty_def_yn_i)
			      , upper(p_qc_sku_select_yn_i)
			      , upper(p_qc_qty_upd_yn_i)
			      , upper(p_qc_serial_yn_i)
			      );
			commit;
		else
			if 	g_logging = 'ON'
			then
				cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
								 , p_client_id_i		=> p_client_id_i
								 , p_order_id_i			=> p_order_id_i
								 , p_container_id_i		=> null
								 , p_sku_id_i			=> null
								 , p_pallet_id_i		=> null
								 , p_extra_parameters_i		=> null
								 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
								 , p_procedure_function_i	=> 'set_qc_parameters_p'
								 , p_comment_i			=> 'Updating existing QC order'
								 );
			end if;
			--
			update 	cnl_sys.cnl_wms_qc_order
			set 	qc_req_yn 		= upper(p_qc_req_yn_i)
			,	qc_batch_yn		= upper(p_qc_batch_yn_i)
			,	qc_qty_def_yn		= upper(p_qc_qty_def_yn_i)
			,	qc_sku_select_yn	= upper(p_qc_sku_select_yn_i)
			,	qc_qty_upd_yn		= upper(p_qc_qty_upd_yn_i)
			, 	qc_serial_yn		= upper(p_qc_serial_yn_i)
			where	order_id 		= upper(p_order_id_i)
			and	client_id 		= upper(p_client_id_i)
			and	site_id 		= upper(p_site_id_i)
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
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_order_id_i||', '||p_client_id_i||', '||p_site_id_i||', '||
											   p_qc_req_yn_i||', '||p_qc_batch_yn_i||' ,'||p_qc_qty_def_yn_i||', '||
											   p_qc_sku_select_yn_i||', '||p_qc_qty_upd_yn_i||', '||p_qc_serial_yn_i	-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end set_qc_parameters_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 10-Okt-2019
-- Purpose : Insert inventory transcation.
------------------------------------------------------------------------------------------------
	procedure create_itl_p( p_status		in out 	integer
			      , p_code 			in 	dcsdba.inventory_transaction.code%type
			      , p_updateqty		in 	dcsdba.inventory_transaction.update_qty%type
			      , p_originalqty 		in 	dcsdba.inventory_transaction.original_qty%type default nuLL
			      , p_clientid 		in 	dcsdba.inventory_transaction.client_id%type
			      , p_skuid 		in 	dcsdba.inventory_transaction.sku_id%type default NULL
			      , p_tagid 		in 	dcsdba.inventory_transaction.tag_id%type
			      , p_batchid 		in 	dcsdba.inventory_transaction.batch_id%type default null
			      , p_conditionid 		in 	dcsdba.inventory_transaction.condition_id%type default null
			      , p_tolocation 		in 	dcsdba.inventory_transaction.to_loc_id%type
			      , p_fromlocation 		in 	dcsdba.inventory_transaction.from_loc_id%type default null
			      , p_finallocation 	in 	dcsdba.inventory_transaction.final_loc_id%type default null
			      , p_referenceid 		in 	dcsdba.inventory_transaction.reference_id%type default null
			      , p_lineid 		in 	dcsdba.inventory_transaction.line_id%type default null
			      , p_reasonid 		in 	dcsdba.inventory_transaction.reason_id%type default null
			      , p_stationid 		in 	dcsdba.inventory_transaction.station_id%type default null
			      , p_userid 		in 	dcsdba.inventory_transaction.user_id%type default null
			      , p_tmpnotes 		in 	dcsdba.inventory_transaction.notes%type default null
			      , p_elapsedtime 		in 	dcsdba.inventory_transaction.elapsed_time%type default null
			      , p_sessiontype 		in 	dcsdba.inventory_transaction.session_type%type default 'm'
			      , p_summaryrecord 	in 	dcsdba.inventory_transaction.summary_record%type default 'y'
			      , p_siteid 		in 	dcsdba.inventory_transaction.site_id%type default null
			      , p_fromsiteid 		in 	dcsdba.inventory_transaction.from_site_id%type default null
			      , p_tositeid 		in 	dcsdba.inventory_transaction.to_site_id%type default null
			      , p_containerid 		in 	dcsdba.inventory_transaction.container_id%type default null
			      , p_palletid 		in 	dcsdba.inventory_transaction.pallet_id%type default null
			      , p_listid 		in 	dcsdba.inventory_transaction.list_id%type default null
			      , p_expirydstamp 		in 	dcsdba.inventory_transaction.expiry_dstamp%type default null
			      , p_ownerid 		in 	dcsdba.inventory_transaction.owner_id%type default null
			      , p_originid 		in 	dcsdba.inventory_transaction.origin_id%type default null
			      , p_workgroup 		in 	dcsdba.inventory_transaction.work_group%type default null
			      , p_consignment 		in 	dcsdba.inventory_transaction.consignment%type default null
			      , p_manufdstamp 		in 	dcsdba.inventory_transaction.manuf_dstamp%type default null
			      , p_taskcategory 		in 	dcsdba.inventory_transaction.task_category%type default null
			      , p_lockstatus 		in 	dcsdba.inventory_transaction.lock_status%type default null
			      , p_qcstatus 		in 	dcsdba.inventory_transaction.qc_status%type default null
			      , p_supplierid 		in 	dcsdba.inventory_transaction.supplier_id%type default null
			      , p_samplingtype 		in 	dcsdba.inventory_transaction.sampling_type%type default null
			      , p_userdeftype1 		in 	dcsdba.inventory_transaction.user_def_type_1%type default null
			      , p_userdeftype2 		in 	dcsdba.inventory_transaction.user_def_type_2%type default null
			      , p_userdeftype3 		in 	dcsdba.inventory_transaction.user_def_type_3%type default null
			      , p_userdeftype4 		in 	dcsdba.inventory_transaction.user_def_type_4%type default null
			      , p_userdeftype5 		in 	dcsdba.inventory_transaction.user_def_type_5%type default null
			      , p_userdeftype6 		in 	dcsdba.inventory_transaction.user_def_type_6%type default null
			      , p_userdeftype7 		in 	dcsdba.inventory_transaction.user_def_type_7%type default null
			      , p_userdeftype8 		in 	dcsdba.inventory_transaction.user_def_type_8%type default null
			      , p_userdefchk1 		in 	dcsdba.inventory_transaction.user_def_chk_1%type default null
			      , p_userdefchk2 		in 	dcsdba.inventory_transaction.user_def_chk_2%type default null
			      , p_userdefchk3 		in 	dcsdba.inventory_transaction.user_def_chk_3%type default null
			      , p_userdefchk4 		in 	dcsdba.inventory_transaction.user_def_chk_4%type default null
			      , p_userdefdate1 		in 	dcsdba.inventory_transaction.user_def_date_1%type default null
			      , p_userdefdate2 		in 	dcsdba.inventory_transaction.user_def_date_2%type default null
			      , p_userdefdate3 		in 	dcsdba.inventory_transaction.user_def_date_3%type default null
			      , p_userdefdate4 		in 	dcsdba.inventory_transaction.user_def_date_4%type default null
			      , p_userdefnum1 		in 	dcsdba.inventory_transaction.user_def_num_1%type default null
			      , p_userdefnum2		in 	dcsdba.inventory_transaction.user_def_num_2%type default null
			      , p_userdefnum3 		in 	dcsdba.inventory_transaction.user_def_num_3%type default null
			      , p_userdefnum4 		in 	dcsdba.inventory_transaction.user_def_num_4%type default null
			      , p_userdefnote1 		in 	dcsdba.inventory_transaction.user_def_note_1%type default null
			      , p_userdefnote2 		in 	dcsdba.inventory_transaction.user_def_note_2%type default null
			      , p_jobid 		in 	dcsdba.inventory_transaction.job_id%type default null
			      , p_jobunit 		in 	dcsdba.inventory_transaction.job_unit%type default null
			      , p_tmpmanning 		in 	dcsdba.inventory_transaction.manning%type default null
			      , p_speccode 		in 	dcsdba.inventory_transaction.spec_code%type default null
			      , p_estimatedtime 	in 	dcsdba.inventory_transaction.estimated_time%type default null
			      , p_completedstamp 	in 	dcsdba.inventory_transaction.complete_dstamp%type default null
			      , p_configid 		in 	dcsdba.inventory_transaction.config_id%type default null
			      , p_ceorigrotationid 	in 	dcsdba.inventory_transaction.ce_orig_rotation_id%type default null
			      , p_cerotationid 		in 	dcsdba.inventory_transaction.ce_rotation_id%type default null
			      , p_ceconsignid 		in 	dcsdba.inventory_transaction.ce_consignment_id%type default null
			      , p_cereceipttype 	in 	dcsdba.inventory_transaction.ce_receipt_type%type default null
			      , p_ceorigin 		in 	dcsdba.inventory_transaction.ce_originator%type default null
			      , p_ceoriginref 		in 	dcsdba.inventory_transaction.ce_originator_reference%type default null
			      , p_cecoo 		in 	dcsdba.inventory_transaction.ce_coo%type default null
			      , p_cecwc 		in 	dcsdba.inventory_transaction.ce_cwc%type default null
			      , p_ceucr 		in 	dcsdba.inventory_transaction.ce_ucr%type default null
			      , p_ceunderbond 		in 	dcsdba.inventory_transaction.ce_under_bond%type default null
			      , p_cedocdstamp 		in 	dcsdba.inventory_transaction.ce_document_dstamp%type default null
			      , p_uploadedcustoms 	in 	dcsdba.inventory_transaction.uploaded_customs%type default 'y'
			      , p_lockcode 		in 	dcsdba.inventory_transaction.lock_code%type default null
			      , p_printlabel 		in 	dcsdba.inventory_transaction.print_label_id%type default null
			      , p_asnid 		in 	dcsdba.inventory_transaction.asn_id%type default null
			      , p_customerid 		in 	dcsdba.inventory_transaction.customer_id%type default null
			      , p_cedutystamp 		in 	dcsdba.inventory_transaction.ce_duty_stamp%type default null
			      , p_palletgrouped 	in 	dcsdba.inventory_transaction.pallet_grouped%type default null
			      , p_consollink 		in 	dcsdba.inventory_transaction.consol_link%type default null
			      , p_jobsiteid 		in 	dcsdba.inventory_transaction.job_site_id%type default null
			      , p_jobclientid 		in 	dcsdba.inventory_transaction.job_client_id%type default null
			      , p_extranotes 		in 	dcsdba.inventory_transaction.extra_notes%type default null
			      , p_stagerouteid 		in 	dcsdba.inventory_transaction.stage_route_id%type default null
			      , p_stagerouteseq 	in 	dcsdba.inventory_transaction.stage_route_sequence%type default null
			      , p_pfconsollink 		in 	dcsdba.inventory_transaction.pf_consol_link%type default null
			      , p_ceavailstatus 	in 	dcsdba.inventory_transaction.ce_avail_status%type default null
			      , p_masterpahid 		in 	dcsdba.inventory_transaction.master_pah_id%type default null
			      , p_masterpalid 		in 	dcsdba.inventory_transaction.master_pal_id%type default null
			      , p_uploaded 		in 	dcsdba.inventory_transaction.uploaded%type default 'n'
			      , p_custshpmntno 		in 	dcsdba.inventory_transaction.customer_shipment_number%type default null
			      , p_shipmentno 		in 	dcsdba.inventory_transaction.shipment_number%type default null
			      , p_fromstatus 		in 	dcsdba.inventory_transaction.from_status%type default null
			      , p_tostatus 		in 	dcsdba.inventory_transaction.to_status%type default null
			      , p_palletconfig 		in 	dcsdba.inventory_transaction.pallet_config%type default null
			      , p_masterorderid 	in 	dcsdba.inventory_transaction.master_order_id%type default null
			      , p_masterorderlineid 	in 	dcsdba.inventory_transaction.master_order_line_id%type default null
			      , p_kitplanid 		in 	dcsdba.inventory_transaction.kit_plan_id%type default null
			      , p_plansequence 		in 	dcsdba.inventory_transaction.plan_sequence%type default null
			      , p_cecollicountexpected 	in 	dcsdba.inventory_transaction.ce_colli_count_expected%type default null
			      , p_cecollicount 		in 	dcsdba.inventory_transaction.ce_colli_count%type default null
			      , p_cesealsok 		in 	dcsdba.inventory_transaction.ce_seals_ok%type default null
			      , p_ceinvoicenumber 	in 	dcsdba.inventory_transaction.ce_invoice_number%type default null
			      , p_olduserdeftype1 	in 	dcsdba.inventory_transaction.old_user_def_type_1%type default null
			      , p_olduserdeftype2 	in 	dcsdba.inventory_transaction.old_user_def_type_2%type default null
			      , p_olduserdeftype3 	in 	dcsdba.inventory_transaction.old_user_def_type_3%type default null
			      , p_olduserdeftype4 	in 	dcsdba.inventory_transaction.old_user_def_type_4%type default null
			      , p_olduserdeftype5 	in 	dcsdba.inventory_transaction.old_user_def_type_5%type default null
			      , p_olduserdeftype6 	in 	dcsdba.inventory_transaction.old_user_def_type_6%type default null
			      , p_olduserdeftype7 	in 	dcsdba.inventory_transaction.old_user_def_type_7%type default null
			      , p_olduserdeftype8 	in 	dcsdba.inventory_transaction.old_user_def_type_8%type default null
			      , p_olduserdefchk1 	in 	dcsdba.inventory_transaction.old_user_def_chk_1%type default null
			      , p_olduserdefchk2 	in 	dcsdba.inventory_transaction.old_user_def_chk_2%type default null
			      , p_olduserdefchk3 	in 	dcsdba.inventory_transaction.old_user_def_chk_3%type default null
			      , p_olduserdefchk4 	in 	dcsdba.inventory_transaction.old_user_def_chk_4%type default null
			      , p_olduserdefdate1 	in 	dcsdba.inventory_transaction.old_user_def_date_1%type default null
			      , p_olduserdefdate2 	in 	dcsdba.inventory_transaction.old_user_def_date_2%type default null
			      , p_olduserdefdate3 	in 	dcsdba.inventory_transaction.old_user_def_date_3%type default null
			      , p_olduserdefdate4 	in 	dcsdba.inventory_transaction.old_user_def_date_4%type default null
			      , p_olduserdefnum1 	in 	dcsdba.inventory_transaction.old_user_def_num_1%type default null
			      , p_olduserdefnum2 	in 	dcsdba.inventory_transaction.old_user_def_num_2%type default null
			      , p_olduserdefnum3 	in 	dcsdba.inventory_transaction.old_user_def_num_3%type default null
			      , p_olduserdefnum4 	in 	dcsdba.inventory_transaction.old_user_def_num_4%type default null
			      , p_olduserdefnote1 	in 	dcsdba.inventory_transaction.old_user_def_note_1%type default null
			      , p_olduserdefnote2 	in 	dcsdba.inventory_transaction.old_user_def_note_2%type default null
			      , p_laborassignment 	in 	dcsdba.inventory_transaction.labor_assignment%type default null
			      , p_laborgridsequence 	in 	dcsdba.inventory_transaction.labor_grid_sequence%type default null
			      , p_kitceconsignid 	in 	dcsdba.inventory_transaction.kit_ce_consignment_id%type default null
			      )
	is
		v_status 	integer;
		g_rtn		varchar2(30) := 'create_itl_p';
		pragma 	autonomous_transaction;
	begin
		dcsdba.libinvtrans.createinvtransproc( status		=>        v_status
						     , transcode        =>        p_code
						     , updateqty        =>        p_updateqty
						     , originalqty      =>        p_originalqty
						     , clientid         =>        p_clientid
						     , skuid            =>        p_skuid
						     , tagid            =>        p_tagid
						     , batchid          =>        p_batchid
						     , conditionid      =>        p_conditionid
						     , tolocation       =>        p_tolocation
						     , fromlocation     =>        p_fromlocation
						     , finallocation    =>        p_finallocation
						     , referenceid      =>        p_referenceid
						     , lineid           =>        p_lineid
						     , reasonid         =>        p_reasonid
						     , stationid        =>        p_stationid
						     , userid           =>        p_userid
						     , tmpnotes         =>        p_tmpnotes
						     , elapsedtime      =>        p_elapsedtime
						     , sessiontype      =>        p_sessiontype
						     , summaryrecord    =>        p_summaryrecord
						     , siteid           =>        p_siteid
						     , fromsiteid       =>        p_fromsiteid
						     , tositeid         =>        p_tositeid
						     , containerid      =>        p_containerid
						     , palletid         =>        p_palletid
						     , listid           =>        p_listid
						     , expirydstamp     =>        p_expirydstamp
						     , ownerid          =>        p_ownerid
						     , originid         =>        p_originid
						     , workgroup        =>        p_workgroup
						     , consignment      =>        p_consignment
						     , manufdstamp      =>        p_manufdstamp
						     , taskcategory     =>        p_taskcategory
						     , lockstatus       =>        p_lockstatus
						     , qcstatus         =>        p_qcstatus
						     , supplierid       =>        p_supplierid
						     , samplingtype     =>        p_samplingtype
						     , userdeftype1     =>        p_userdeftype1
						     , userdeftype2     =>        p_userdeftype2
						     , userdeftype3     =>        p_userdeftype3
						     , userdeftype4     =>        p_userdeftype4
						     , userdeftype5     =>        p_userdeftype5
						     , userdeftype6     =>        p_userdeftype6
						     , userdeftype7     =>        p_userdeftype7
						     , userdeftype8     =>        p_userdeftype8
						     , userdefchk1     	=>        p_userdefchk1
						     , userdefchk2     	=>        p_userdefchk2
						     , userdefchk3     	=>        p_userdefchk3
						     , userdefchk4     	=>        p_userdefchk4
						     , userdefdate1     =>        p_userdefdate1
						     , userdefdate2     =>        p_userdefdate2
						     , userdefdate3     =>        p_userdefdate3
						     , userdefdate4     =>        p_userdefdate4
						     , userdefnum1     	=>        p_userdefnum1
						     , userdefnum2     	=>        p_userdefnum2
						     , userdefnum3     	=>        p_userdefnum3
						     , userdefnum4     	=>        p_userdefnum4
						     , userdefnote1     =>        p_userdefnote1
						     , userdefnote2     =>        p_userdefnote2
						     , jobid    	=>        p_jobid
						     , jobunit     	=>        p_jobunit
						     , tmpmanning     	=>        p_tmpmanning
						     , speccode     	=>        p_speccode
						     , estimatedtime    =>        p_estimatedtime
						     , completedstamp   =>        p_completedstamp
						     , configid     	=>        p_configid
						     , ceorigrotationid =>        p_ceorigrotationid
						     , cerotationid     =>        p_cerotationid
						     , ceconsignid     	=>        p_ceconsignid
						     , cereceipttype    =>        p_cereceipttype
						     , ceorigin     	=>        p_ceorigin
						     , ceoriginref     	=>        p_ceoriginref
						     , cecoo     	=>        p_cecoo
						     , cecwc     	=>        p_cecwc
						     , ceucr     	=>        p_ceucr
						     , ceunderbond     	=>        p_ceunderbond
						     , cedocdstamp     	=>        p_cedocdstamp
						     , uploadedcustoms  =>        p_uploadedcustoms
						     , lockcode     	=>        p_lockcode
						     , printlabel     	=>        p_printlabel
						     , asnid     	=>        p_asnid
						     , customerid     	=>        p_customerid
						     , cedutystamp     	=>        p_cedutystamp
						     , palletgrouped    =>        p_palletgrouped
						     , consollink     	=>        p_consollink
						     , jobsiteid     	=>        p_jobsiteid
						     , jobclientid     	=>        p_jobclientid
						     , extranotes     	=>        p_extranotes
						     , stagerouteid     =>        p_stagerouteid
						     , stagerouteseq    =>        p_stagerouteseq
						     , pfconsollink     =>        p_pfconsollink
						     , ceavailstatus    =>        p_ceavailstatus
						     , masterpahid     	=>        p_masterpahid
						     , masterpalid     	=>        p_masterpalid
						     , uploaded     	=>        p_uploaded
						     , custshpmntno     =>        p_custshpmntno
						     , fromstatus     	=>        p_shipmentno
						     , tostatus     	=>        p_fromstatus
						     , shipmentno     	=>        p_tostatus
						     , palletconfig     =>        p_palletconfig
						     , masterorderid    =>        p_masterorderid
						     , masterorderlineid=>        p_masterorderlineid
						     , kitplanid     	=>        p_kitplanid
						     , plansequence     =>        p_plansequence
						     , cecollicountexpected	=>        p_cecollicountexpected
						     , cecollicount     =>        p_cecollicount
						     , cesealsok     	=>        p_cesealsok
						     , ceinvoicenumber  =>        p_ceinvoicenumber
						     , olduserdeftype1  =>        p_olduserdeftype1
						     , olduserdeftype2  =>        p_olduserdeftype2
						     , olduserdeftype3  =>        p_olduserdeftype3
						     , olduserdeftype4  =>        p_olduserdeftype4
						     , olduserdeftype5  =>        p_olduserdeftype5
						     , olduserdeftype6  =>        p_olduserdeftype6
						     , olduserdeftype7  =>        p_olduserdeftype7
						     , olduserdeftype8  =>        p_olduserdeftype8
						     , olduserdefchk1   =>        p_olduserdefchk1
						     , olduserdefchk2   =>        p_olduserdefchk2
						     , olduserdefchk3   =>        p_olduserdefchk3
						     , olduserdefchk4   =>        p_olduserdefchk4
						     , olduserdefdate1  =>        p_olduserdefdate1
						     , olduserdefdate2  =>        p_olduserdefdate2
						     , olduserdefdate3  =>        p_olduserdefdate3
						     , olduserdefdate4  =>        p_olduserdefdate4
						     , olduserdefnum1   =>        p_olduserdefnum1
						     , olduserdefnum2   =>        p_olduserdefnum2
						     , olduserdefnum3   =>        p_olduserdefnum3
						     , olduserdefnum4   =>        p_olduserdefnum4
						     , olduserdefnote1  =>        p_olduserdefnote1
						     , olduserdefnote2  =>        p_olduserdefnote2
						     , laborassignment  =>        p_laborassignment
						     , laborgridsequence=>        p_laborgridsequence
						     , kitceconsignid   =>        p_kitceconsignid
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
	end create_itl_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 10-Okt-2019
-- Purpose : fetch container checked content
------------------------------------------------------------------------------------------------
	procedure process_container_check_p( p_id_i		in number 
					   , p_container_id_i	in dcsdba.order_container.container_id%type
					   , p_pallet_id_i	in dcsdba.order_container.pallet_id%type default null
					   , p_client_id_i	in dcsdba.order_container.client_id%type
					   , p_site_id_i	in dcsdba.inventory.site_id%type
					   , p_order_id_i	in dcsdba.order_container.order_id%type
					   , p_location_id_i	in dcsdba.inventory.location_id%type default null
					   , p_station_id_i	in dcsdba.workstation.station_id%type
					   , p_user_id_i	in dcsdba.inventory_transaction.user_id%type
					   , p_notes_i		in dcsdba.inventory_transaction.user_def_note_1%type -- Used for reason description secondary QC
					   , p_extra_notes_i	in dcsdba.inventory_transaction.extra_notes%type -- Does not work with standard JDA API
					   , p_elapsed_time_i	in dcsdba.inventory_transaction.elapsed_time%type default null
					   , p_check_type_i	in varchar2 -- 'CHKI','CHKS'
					   , p_check_ok_yn_i	in varchar2 -- 'Y','N'
					   , p_release_cont_i	in varchar2 -- Y or N to release the container in the warehouse for further processing.
					   , p_reset_times_i	in integer
					   , p_qc_serial_yn_i	in varchar2 -- serial check must be processed differently.
					   , p_success_yn_o	out varchar2
					   )
	is
		-- create a record and table type for container content checked.
		type container_content_rec is record( id			cnl_wms.wmh_container_content.id@whh_cnl_wms%type
						    , owner_id			cnl_wms.wmh_container_content.owner_id@whh_cnl_wms%type
						    , tag_id			cnl_wms.wmh_container_content.tag_id@whh_cnl_wms%type
						    , sku_id			cnl_wms.wmh_container_content.sku_id@whh_cnl_wms%type
						    , batch_id			cnl_wms.wmh_container_content.batch_id@whh_cnl_wms%type
						    , serial_number		cnl_wms.wmh_container_content.serial_number@whh_cnl_wms%type
						    , check_value		cnl_wms.wmh_container_content.check_value@whh_cnl_wms%type
						    , qty_on_hand		cnl_wms.wmh_container_content.qty_on_hand@whh_cnl_wms%type
						    , batch_check_result	cnl_wms.wmh_container_content.batch_check_result@whh_cnl_wms%type
						    , qty_check_result		cnl_wms.wmh_container_content.qty_check_result@whh_cnl_wms%type
						    , qty_processed		number
						    , check_finished		varchar2(1)
						    );
		type container_content_tab is table of container_content_rec;
		l_checked	container_content_tab;

		-- Create a record and table for inventory inside container accrording WMS. Als add counter collumn.
		type container_inventory_rec is record( tag_id			dcsdba.inventory.tag_id%type
						      , sku_id			dcsdba.inventory.sku_id%type
						      , location_id		dcsdba.inventory.location_id%type
						      , owner_id		dcsdba.inventory.owner_id%type
						      , origin_id		dcsdba.inventory.origin_id%type
						      , condition_id		dcsdba.inventory.condition_id%type
						      , lock_status		dcsdba.inventory.lock_status%type
						      , lock_code		dcsdba.inventory.lock_code%type
						      , qty_on_hand		dcsdba.inventory.qty_on_hand%type
						      , batch_id		dcsdba.inventory.batch_id%type
						      , qty_checked		number
						      , over_check		varchar2(1)
						      );
		type container_inventory_tab is table of container_inventory_rec;
		l_inventory	container_inventory_tab;

		-- Create record and table for records to add as transaction
		type itl_rec is record( code		dcsdba.inventory_transaction.code%type
				      , sku_id		dcsdba.inventory_transaction.sku_id%type
				      , owner_id	dcsdba.inventory_transaction.owner_id%type
				      , tag_id		dcsdba.inventory_transaction.tag_id%type
				      , client_id	dcsdba.inventory_transaction.client_id%type
				      , site_id		dcsdba.inventory_transaction.site_id%type
				      , from_loc_id	dcsdba.inventory_transaction.from_loc_id%type
				      , to_loc_id	dcsdba.inventory_transaction.to_loc_id%type
				      , final_loc_id	dcsdba.inventory_transaction.final_loc_id%type
				      , original_qty	dcsdba.inventory_transaction.original_qty%type
				      , update_qty	dcsdba.inventory_transaction.update_qty%type
				      , origin_id	dcsdba.inventory_transaction.origin_id%type
				      , condition_id	dcsdba.inventory_transaction.condition_id%type
				      , reference_id	dcsdba.inventory_transaction.reference_id%type
				      , notes		dcsdba.inventory_transaction.notes%type
				      , extra_notes	dcsdba.inventory_transaction.extra_notes%type
				      , lock_status	dcsdba.inventory_transaction.lock_status%type
				      , lock_code	dcsdba.inventory_transaction.lock_code%type
				      , container_id	dcsdba.inventory_transaction.container_id%type
				      , pallet_id	dcsdba.inventory_transaction.pallet_id%type
				      , batch_id	dcsdba.inventory_transaction.batch_id%type
				      , user_id		dcsdba.inventory_transaction.user_id%type
				      , station_id	dcsdba.inventory_transaction.station_id%type
				      , elapsed_time	dcsdba.inventory_transaction.elapsed_time%type
				      , reset_times	dcsdba.inventory_transaction.user_def_type_1%type
				      );
		type itl_tab is table of itl_rec;
		l_itl	itl_tab;

		cursor c_serial_at_pick( b_sku_id 	varchar2
				       , b_client_id 	varchar2
				       )
		is
			select	nvl(s.serial_at_pick,'N') serial_at_pick
			from	dcsdba.sku s
			where	s.sku_id 	= b_sku_id
			and	s.client_id	= b_client_id
		;
		--
		r_serial_at_pick	dcsdba.sku.serial_at_pick%type;
		--
		l_code			dcsdba.inventory_transaction.code%type;
		l_status 		integer; -- Out parameter for calling create_itl_p
		l_inventory_count	integer;
		l_checked_count		integer;
		l_to_process		number;
		l_user_id		dcsdba.application_user.user_id%type;
		l_notes			dcsdba.inventory_transaction.notes%type;
		l_reloop		varchar2(1) := 'N';
		l_iteration		integer := 0;
		l_batch_id 		dcsdba.inventory.batch_id%type;
		l_serial_number		dcsdba.serial_number.serial_number%type;
		--
		g_rtn			varchar2(30) := 'process_container_check_p';
		pragma 			autonomous_transaction;
	begin 
		p_success_yn_o := 'N';

		-- Check user_id
		l_user_id := cnl_whh_util_pck.chk_user_id_f(p_user_id_i);

		-- Set WMS session settings and debug level to 3. If no value debug level will be default 5.
		dcsdba.libmqsdebug.setsessionid(USERENV('SESSIONID'),'sql',l_user_id);
		dcsdba.libmqsdebug.setdebuglevel(3);	
		dcsdba.libsession.InitialiseSession( UserID       => l_user_id
						   , GroupID      => null
						   , StationID    => p_station_id_i
                                                   , WksGroupID   => null
                                                   );
		-- Save parameters when logging is enabled:
		if	g_logging = 'ON'
		then
			begin
				-- Next procedure is a pragma autonomous transaction
				cnl_sys.cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
									 , p_client_id_i		=> p_client_id_i
									 , p_order_id_i			=> p_order_id_i
									 , p_container_id_i		=> p_container_id_i
									 , p_pallet_id_i		=> p_pallet_id_i
									 , p_sku_id_i			=> null
									 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
									 , p_procedure_function_i	=> 'process_container_check_p'
									 , p_extra_parameters_i		=> 'p_id = '|| p_id_i || 
													   ', from_location = '|| p_location_id_i ||
													   ', station_id = ' || p_station_id_i ||
													   ', user_id = ' || p_user_id_i ||
													   ', check type = ' || p_check_type_i ||
													   ', serial check req = ' || p_qc_serial_yn_i ||
													   ', check ok Y/N = ' || p_check_ok_yn_i ||
													   ', Release container Y/N = ' || p_release_cont_i
									 , p_comment_i			=> 'All parameters send by Warehouse handling'
									 );
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
			end;
		end if;

		-- Set real transaction code
		if	p_check_type_i = 'CHKS'
		then
			l_code := 'Container ChkS';
		else
			l_code := 'Container ChkI';
		end if;

		-- Fetch all inventory from WMS that should be inside the container.
		select	i.tag_id
		, 	i.sku_id
		,	i.location_id
		,	i.owner_id
		,	i.origin_id
		,	i.condition_id
		, 	i.lock_status
		,	i.lock_code
		,	i.qty_on_hand
		,	i.batch_id
		,	0 	qty_checked
		,	'N'	over_check -- More QTY can be added than qty on hand
		bulk collect into l_inventory
		from 	dcsdba.inventory i
		where	i.client_id	= p_client_id_i
		and	i.site_id	= p_site_id_i
		and	i.container_id 	= p_container_id_i
		;
		l_inventory_count := l_inventory.count; -- total records

		-- fetch all inventory inside the container according QC. !!!Only when the check failed!!!
		if	p_check_ok_yn_i = 'N'
		then
			select	id
			,	owner_id
			, 	tag_id
			, 	sku_id
			, 	batch_id
			,	serial_number
			,	check_value
			,	qty_on_hand
			,	batch_check_result
			,	qty_check_result
			,	0 	qty_processed
			,	'N'	check_finished
			bulk collect into l_checked
			from	cnl_wms.wmh_container_content@whh_cnl_wms
			where	container_data_id = p_id_i
			;
			l_checked_count := l_checked.count; -- total records
		end if;

		-- initialize itl table
		l_itl := itl_tab();

		-- Search for checked records that should not be inside the container.
		--
		-- When serial checking is yes a check must be done if the checked value is a serial number or a batch number.
		-- When a new record is found this is harder to figure out.
		if	p_check_ok_yn_i = 'N'
		then
			if 	l_checked.count > 0
			then
				<<invalid_loop>>
				for	r in l_checked.first .. l_checked.last
				loop
					begin
						if	l_checked(r).qty_on_hand = 0
						then
							if	nvl(p_qc_serial_yn_i,'N') = 'Y'
							then
								open	c_serial_at_pick( l_checked(r).sku_id
											, p_client_id_i
											);
								fetch 	c_serial_at_pick
								into	r_serial_at_pick;
								close 	c_serial_at_pick;

								if 	r_serial_at_pick = 'Y'
								then	-- entered vale must be a serial number for this specific SKU and therefore result must be added in comment.
									l_batch_id := null;
									l_serial_number := l_checked(r).batch_check_result;
									--
									l_itl.extend;
									l_itl(l_itl.count).code		:= l_code;
									l_itl(l_itl.count).sku_id	:= l_checked(r).sku_id;
									l_itl(l_itl.count).owner_id	:= l_checked(r).owner_id;
									l_itl(l_itl.count).tag_id	:= l_checked(r).tag_id;
									l_itl(l_itl.count).client_id	:= p_client_id_i;
									l_itl(l_itl.count).site_id	:= p_site_id_i;
									l_itl(l_itl.count).from_loc_id	:= p_location_id_i;
									l_itl(l_itl.count).to_loc_id	:= p_location_id_i;
									l_itl(l_itl.count).final_loc_id	:= p_location_id_i;
									l_itl(l_itl.count).original_qty	:= 0;
									l_itl(l_itl.count).update_qty	:= l_checked(r).qty_check_result;
									l_itl(l_itl.count).origin_id	:= null;
									l_itl(l_itl.count).condition_id	:= null;
									l_itl(l_itl.count).reference_id	:= p_order_id_i;
									l_itl(l_itl.count).notes	:= 'This SKU, Batch, serial '||
													   l_serial_number||
													   ' or QTY should not be inside this container';
									l_itl(l_itl.count).extra_notes	:= p_extra_notes_i;
									l_itl(l_itl.count).lock_status	:= null;
									l_itl(l_itl.count).lock_code	:= null;
									l_itl(l_itl.count).container_id	:= p_container_id_i;
									l_itl(l_itl.count).pallet_id	:= p_pallet_id_i;
									l_itl(l_itl.count).batch_id	:= l_batch_id;
									l_itl(l_itl.count).user_id	:= l_user_id;
									l_itl(l_itl.count).station_id	:= p_station_id_i;
									l_itl(l_itl.count).elapsed_time	:= p_elapsed_time_i;
									l_itl(l_itl.count).reset_times	:= to_char(p_reset_times_i);

									l_checked(r).qty_processed := l_checked(r).qty_check_result;
									l_checked(r).check_finished := 'Y';
								else	-- SKU is not serial controlled normal procesing
									l_batch_id 	:= l_checked(r).batch_check_result;
									l_serial_number	:= null;
									--
									l_itl.extend;
									l_itl(l_itl.count).code		:= l_code;
									l_itl(l_itl.count).sku_id	:= l_checked(r).sku_id;
									l_itl(l_itl.count).owner_id	:= l_checked(r).owner_id;
									l_itl(l_itl.count).tag_id	:= l_checked(r).tag_id;
									l_itl(l_itl.count).client_id	:= p_client_id_i;
									l_itl(l_itl.count).site_id	:= p_site_id_i;
									l_itl(l_itl.count).from_loc_id	:= p_location_id_i;
									l_itl(l_itl.count).to_loc_id	:= p_location_id_i;
									l_itl(l_itl.count).final_loc_id	:= p_location_id_i;
									l_itl(l_itl.count).original_qty	:= 0;
									l_itl(l_itl.count).update_qty	:= l_checked(r).qty_check_result;
									l_itl(l_itl.count).origin_id	:= null;
									l_itl(l_itl.count).condition_id	:= null;
									l_itl(l_itl.count).reference_id	:= p_order_id_i;
									l_itl(l_itl.count).notes	:= 'This SKU, Batch or QTY should not be inside this container';
									l_itl(l_itl.count).extra_notes	:= p_extra_notes_i;
									l_itl(l_itl.count).lock_status	:= null;
									l_itl(l_itl.count).lock_code	:= null;
									l_itl(l_itl.count).container_id	:= p_container_id_i;
									l_itl(l_itl.count).pallet_id	:= p_pallet_id_i;
									l_itl(l_itl.count).batch_id	:= l_checked(r).batch_check_result;
									l_itl(l_itl.count).user_id	:= l_user_id;
									l_itl(l_itl.count).station_id	:= p_station_id_i;
									l_itl(l_itl.count).elapsed_time	:= p_elapsed_time_i;
									l_itl(l_itl.count).reset_times	:= to_char(p_reset_times_i);

									l_checked(r).qty_processed := l_checked(r).qty_check_result;
									l_checked(r).check_finished := 'Y';
								end if;
							else	-- No serial check done so nromal processing
								l_batch_id 	:= l_checked(r).batch_check_result;
								l_serial_number	:= null;
								--
								l_itl.extend;
								l_itl(l_itl.count).code		:= l_code;
								l_itl(l_itl.count).sku_id	:= l_checked(r).sku_id;
								l_itl(l_itl.count).owner_id	:= l_checked(r).owner_id;
								l_itl(l_itl.count).tag_id	:= l_checked(r).tag_id;
								l_itl(l_itl.count).client_id	:= p_client_id_i;
								l_itl(l_itl.count).site_id	:= p_site_id_i;
								l_itl(l_itl.count).from_loc_id	:= p_location_id_i;
								l_itl(l_itl.count).to_loc_id	:= p_location_id_i;
								l_itl(l_itl.count).final_loc_id	:= p_location_id_i;
								l_itl(l_itl.count).original_qty	:= 0;
								l_itl(l_itl.count).update_qty	:= l_checked(r).qty_check_result;
								l_itl(l_itl.count).origin_id	:= null;
								l_itl(l_itl.count).condition_id	:= null;
								l_itl(l_itl.count).reference_id	:= p_order_id_i;
								l_itl(l_itl.count).notes	:= 'This SKU, Batch or QTY should not be inside this container';
								l_itl(l_itl.count).extra_notes	:= p_extra_notes_i;
								l_itl(l_itl.count).lock_status	:= null;
								l_itl(l_itl.count).lock_code	:= null;
								l_itl(l_itl.count).container_id	:= p_container_id_i;
								l_itl(l_itl.count).pallet_id	:= p_pallet_id_i;
								l_itl(l_itl.count).batch_id	:= /*l_batch_id;*/l_checked(r).qty_check_result;
								l_itl(l_itl.count).user_id	:= l_user_id;
								l_itl(l_itl.count).station_id	:= p_station_id_i;
								l_itl(l_itl.count).elapsed_time	:= p_elapsed_time_i;
								l_itl(l_itl.count).reset_times	:= to_char(p_reset_times_i);

								l_checked(r).qty_processed := l_checked(r).qty_check_result;
								l_checked(r).check_finished := 'Y';
							end if;
						end if;
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
					end;
				end loop;
			end if;
		end if;

		-- search checked records for matching inventory
		if	p_check_ok_yn_i = 'N'
		then
			if 	l_checked.count > 0
			then
				<<Checked_loop>>
				for 	k in l_checked.first .. l_checked.last
				loop
					begin
						if	l_checked(k).check_finished = 'Y'
						then
							continue;
						end if;

						if	l_inventory.count > 0
						then
							-- When processed qty of any record is still above 0 a reloop is required!
							<<inventory_loop>>
							for	e in l_inventory.first .. l_inventory.last
							loop
								begin
									-- When SKU and Batch do not match move to next inventory record.
									if	l_inventory(e).sku_id 		!= l_checked(k).sku_id or
										nvl(l_inventory(e).batch_id,'N')!= nvl(l_checked(k).batch_id,'N')
									then
										continue; -- Move to next inventory record.
									else	-- found a matching inventory record.
										if	l_inventory(e).over_check = 'N'
										then	-- Inventory record has not been fully checked yet.
											-- QTY checked is equal to inventory QTY to check
											-- Over check is set to yes. Never will it happen that there are two checked records for the same SKU / BATCH.
											if 	l_inventory(e).qty_on_hand - l_inventory(e).qty_checked = l_checked(k).qty_check_result-l_checked(k).qty_processed
											then
												l_inventory(e).qty_checked 	:= l_inventory(e).qty_checked + (l_checked(k).qty_check_result-l_checked(k).qty_processed);
												l_checked(k).qty_processed 	:= l_checked(k).qty_check_result;
												l_checked(k).check_finished	:= 'Y';
												l_inventory(e).over_check 	:= 'Y';
												exit inventory_loop; -- continue with next checked record.
											-- QTY checked is lower then inventory QTY to check
											elsif 	l_inventory(e).qty_on_hand - l_inventory(e).qty_checked > l_checked(k).qty_check_result-l_checked(k).qty_processed
											then
												l_inventory(e).qty_checked 	:= l_inventory(e).qty_checked + (l_checked(k).qty_check_result-l_checked(k).qty_processed);
												l_checked(k).qty_processed 	:= l_checked(k).qty_check_result;
												l_checked(k).check_finished	:= 'Y';
												exit inventory_loop; -- continue with next checked record
											-- QTY checked is higher then inventory QTY
											elsif	l_inventory(e).qty_on_hand - l_inventory(e).qty_checked < l_checked(k).qty_check_result - l_checked(k).qty_processed
											then
												l_checked(k).qty_processed 	:= l_checked(k).qty_processed + (l_inventory(e).qty_on_hand - l_inventory(e).qty_checked); 
												l_inventory(e).qty_checked 	:= l_inventory(e).qty_on_hand;
												l_inventory(e).over_check 	:= 'Y';
												continue; -- Move to next inventory record if any.
											end if;
										else	-- Over check is allowed.
											if	l_checked(k).serial_number is not null
											then
												continue; -- Serials are always qty 1 in checked results and can't be over checked.
											else
												l_inventory(e).qty_checked := l_inventory(e).qty_checked + (l_checked(k).qty_check_result - l_checked(k).qty_processed);
												l_checked(k).qty_processed := l_checked(k).qty_check_result;
												l_checked(k).check_finished:= 'Y';
												exit inventory_loop; -- Continue with next checked record.
											end if;
										end if;
									end if;
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
								end;
							end loop;
						end if;
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
					end;
				end loop;

				-- Check if checked records have been fullt processed.
				<<check_Checked_loop>>
				for 	kk in l_checked.first .. l_checked.last
				loop
					if	l_checked(kk).check_finished = 'N'
					then
						l_reloop := 'Y';
						l_iteration := l_iteration + 1;
						exit check_Checked_loop;
					else
						l_reloop := 'N';
					end if;
				end loop;
				if 	l_reloop = 'Y'
				and	l_iteration < 20
				then
					goto Checked_loop;
				end if;

				-- Add all inventory records to ilt table
				if 	l_inventory.count > 0
				then
					for 	m in l_inventory.first .. l_inventory.last
					loop
						begin
							if	l_inventory(m).qty_on_hand = l_inventory(m).qty_checked
							then
								l_notes := 'QC successful';
							elsif	l_inventory(m).qty_on_hand > l_inventory(m).qty_checked
							then
								l_notes := 'QC failed. Less QTY inside.';
							else
								l_notes := 'QC failed. More QTY inside.';
							end if;

							l_itl.extend;
							l_itl(l_itl.count).code		:= l_code;
							l_itl(l_itl.count).sku_id	:= l_inventory(m).sku_id;
							l_itl(l_itl.count).owner_id	:= l_inventory(m).owner_id;
							l_itl(l_itl.count).tag_id	:= l_inventory(m).tag_id;
							l_itl(l_itl.count).client_id	:= p_client_id_i;
							l_itl(l_itl.count).site_id	:= p_site_id_i;
							l_itl(l_itl.count).from_loc_id	:= l_inventory(m).location_id;
							l_itl(l_itl.count).to_loc_id	:= l_inventory(m).location_id;
							l_itl(l_itl.count).final_loc_id	:= l_inventory(m).location_id;
							l_itl(l_itl.count).original_qty	:= l_inventory(m).qty_on_hand;
							l_itl(l_itl.count).update_qty	:= l_inventory(m).qty_checked;
							l_itl(l_itl.count).origin_id	:= l_inventory(m).origin_id;
							l_itl(l_itl.count).condition_id	:= l_inventory(m).condition_id;
							l_itl(l_itl.count).reference_id	:= p_order_id_i;
							l_itl(l_itl.count).notes	:= l_notes;
							l_itl(l_itl.count).extra_notes	:= p_extra_notes_i;
							l_itl(l_itl.count).lock_status	:= l_inventory(m).lock_status;
							l_itl(l_itl.count).lock_code	:= l_inventory(m).lock_code;
							l_itl(l_itl.count).container_id	:= p_container_id_i;
							l_itl(l_itl.count).pallet_id	:= p_pallet_id_i;
							l_itl(l_itl.count).batch_id	:= l_inventory(m).batch_id;
							l_itl(l_itl.count).user_id	:= l_user_id;
							l_itl(l_itl.count).station_id	:= p_station_id_i;
							l_itl(l_itl.count).elapsed_time	:= p_elapsed_time_i;
							l_itl(l_itl.count).reset_times	:= to_char(p_reset_times_i);
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
						end;
					end loop;
				end if;
			end if;
		end if;

		-- Process all inventory records when no difference are found during QC		
		if	p_check_ok_yn_i = 'Y'
		then
			if 	l_inventory.count > 0
			then
				for 	s in l_inventory.first .. l_inventory.last
				loop
					begin
						l_itl.extend;
						l_itl(l_itl.count).code		:= l_code;
						l_itl(l_itl.count).sku_id	:= l_inventory(s).sku_id;
						l_itl(l_itl.count).owner_id	:= l_inventory(s).owner_id;
						l_itl(l_itl.count).tag_id	:= l_inventory(s).tag_id;
						l_itl(l_itl.count).client_id	:= p_client_id_i;
						l_itl(l_itl.count).site_id	:= p_site_id_i;
						l_itl(l_itl.count).from_loc_id	:= l_inventory(s).location_id;
						l_itl(l_itl.count).to_loc_id	:= l_inventory(s).location_id;
						l_itl(l_itl.count).final_loc_id	:= l_inventory(s).location_id;
						l_itl(l_itl.count).original_qty	:= l_inventory(s).qty_on_hand;
						l_itl(l_itl.count).update_qty	:= l_inventory(s).qty_on_hand;
						l_itl(l_itl.count).origin_id	:= l_inventory(s).origin_id;
						l_itl(l_itl.count).condition_id	:= l_inventory(s).condition_id;
						l_itl(l_itl.count).reference_id	:= p_order_id_i;
						l_itl(l_itl.count).notes	:= 'QC successful';
						l_itl(l_itl.count).extra_notes	:= p_extra_notes_i;
						l_itl(l_itl.count).lock_status	:= l_inventory(s).lock_status;
						l_itl(l_itl.count).lock_code	:= l_inventory(s).lock_code;
						l_itl(l_itl.count).container_id	:= p_container_id_i;
						l_itl(l_itl.count).pallet_id	:= p_pallet_id_i;
						l_itl(l_itl.count).batch_id	:= l_inventory(s).batch_id;
						l_itl(l_itl.count).user_id	:= p_user_id_i;
						l_itl(l_itl.count).station_id	:= p_station_id_i;
						l_itl(l_itl.count).elapsed_time	:= p_elapsed_time_i;
						l_itl(l_itl.count).reset_times	:= to_char(p_reset_times_i);
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
					end;
				end loop;
			end if;
		end if;

		-- Process all records in ITL table
		if	l_itl.count > 0
		then
			for 	a in l_itl.first .. l_itl.last
			loop
				begin
					-- Next procedure is a pragma autonomous transaction
					create_itl_p( p_status		=> l_status
						    , p_code 		=> l_itl(a).code
						    , p_updateqty	=> l_itl(a).update_qty
						    , p_originalqty 	=> l_itl(a).original_qty
						    , p_clientid 	=> l_itl(a).client_id
						    , p_skuid 		=> l_itl(a).sku_id
						    , p_tagid 		=> l_itl(a).tag_id
						    , p_batchid 	=> l_itl(a).batch_id
						    , p_conditionid 	=> l_itl(a).condition_id
						    , p_tolocation 	=> l_itl(a).to_loc_id
						    , p_fromlocation 	=> l_itl(a).from_loc_id
						    , p_finallocation 	=> l_itl(a).final_loc_id
						    , p_referenceid 	=> l_itl(a).reference_id
						    , p_stationid 	=> l_itl(a).station_id
						    , p_userid 		=> l_itl(a).user_id
						    , p_tmpnotes 	=> l_itl(a).notes
						    , p_elapsedtime 	=> l_itl(a).elapsed_time
						    , p_siteid 		=> l_itl(a).site_id
						    , p_containerid 	=> l_itl(a).container_id
						    , p_palletid 	=> l_itl(a).pallet_id
						    , p_ownerid 	=> l_itl(a).owner_id
						    , p_originid 	=> l_itl(a).origin_id
						    , p_lockstatus 	=> l_itl(a).lock_status
						    , p_lockcode 	=> l_itl(a).lock_code
						    , p_userdeftype1	=> 'Times reset during QC = '||l_itl(a).reset_times
						    , p_userdefnote1	=> p_notes_i
						    );
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
				end;
			end loop;
		end if;

		commit;

		-- When QC was the only warehouse handling activity and it is finished release the task
		if	p_release_cont_i = 'Y'
		then
			-- call procedure to release container
			null;
		end if;
		p_success_yn_o := 'Y';
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
	end process_container_check_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 24-Okt-2019
-- Purpose : Set QC done to Yes and when all containers on a paller are finished released pallet
------------------------------------------------------------------------------------------------
	procedure release_marshal_task_p( p_container_id_i	in	dcsdba.move_task.container_id%type default null
					, p_pallet_id_i		in 	dcsdba.move_task.pallet_id%type default null
					, p_site_id_i		in 	dcsdba.move_task.site_id%type
					, p_station_id_i	in 	dcsdba.move_task.station_id%type
					, p_user_id_i		in 	dcsdba.move_task.user_id%type
					, p_force_release	in 	integer -- 0 = Not enforce a release, 1 = enforce release
					, P_ok_yn_o		out	integer -- 0 = not ok, 1 = ok
					, p_comment_o		out	varchar2 --varchar2(200)
					)
	is
		cursor c_pal_chk( b_pallet_id dcsdba.move_task.pallet_id%type)
		is
			select 	distinct 1
			from	dcsdba.move_task m
			where	m.site_id 	= p_site_id_i
			and	m.pallet_id 	= b_pallet_id
			and	m.task_id 	= 'PALLET'
		;
		--
		cursor c_station(b_site_id dcsdba.move_task.site_id%type)
		is
			select	distinct 1
			from	dcsdba.workstation w
			where	w.station_id 	= p_station_id_i
			and	w.site_id 	= b_site_id
		;
		--
		cursor c_container(b_site_id dcsdba.move_task.site_id%type)
		is
			select 	m.pallet_id
			,	m.container_id
			from	dcsdba.move_task m
			where	m.site_id 	= p_site_id_i
			and	m.container_id 	= p_container_id_i
		;
		--
		cursor c_finished( b_pallet dcsdba.move_task.pallet_id%type)
		is
			select	count(*) total_not_checked
			from	dcsdba.move_task m
			where	m.site_id 	= p_site_id_i
			and	m.pallet_id 	= b_pallet
			and	m.status 	in ('Consol','WHHConsol')
			and	nvl(m.repack_qc_done,'N') = 'N'
		;
		--
		r_pal_chk		c_pal_chk%rowtype;
		r_finished		c_finished%rowtype;
		r_container		c_container%rowtype;
		r_station		c_station%rowtype;
		--
		v_site_id		dcsdba.move_task.site_id%type;
		v_container_id		dcsdba.move_task.container_id%type;
		v_pallet_id		dcsdba.move_task.pallet_id%type;
		v_station_id		dcsdba.move_task.station_id%type;
		v_user_id		dcsdba.move_task.user_id%type;
		v_ok_yn			integer := 1;
		v_comment		varchar2(200);
		--
		g_rtn			varchar2(30) := 'release_marshal_task_p';
		pragma autonomous_transaction;
	begin
		if 	g_logging = 'ON'
		then
			cnl_whh_util_pck.create_whh_log_p( p_site_id_i			=> p_site_id_i
							 , p_client_id_i		=> null
							 , p_order_id_i			=> null
							 , p_container_id_i		=> p_container_id_i
							 , p_sku_id_i			=> null
							 , p_pallet_id_i		=> p_pallet_id_i
							 , p_extra_parameters_i		=> 'Executed from workstation '||p_station_id_i||' by user '||p_user_id_i||'. '||' Release enforced? '||p_force_release
							 , p_package_name_i		=> 'cnl_warehouse_handling_pck'
							 , p_procedure_function_i	=> 'release_marshal_task_p'
							 , p_comment_i			=> 'Releasing container from WHH'
							 );
		end if;
		-- Check user_id
		v_user_id := cnl_whh_util_pck.chk_user_id_f(p_user_id_i);

		-- Set WMS session settings and debug level to 3. If no value debug level will be default 5.
		dcsdba.libmqsdebug.setsessionid(USERENV('SESSIONID'),'sql',v_user_id);
		dcsdba.libmqsdebug.setdebuglevel(3);	
		dcsdba.libsession.InitialiseSession( UserID       => v_user_id
						   , GroupID      => null
						   , StationID    => p_station_id_i
                                                   , WksGroupID   => null
                                                   );

		-- Check if pallet exists when force release is yes.
		if	v_ok_yn = 1
		and	p_pallet_id_i is not null
		then
			open	c_pal_chk(p_pallet_id_i);
			fetch 	c_pal_chk into r_pal_chk;
			if	c_pal_chk%notfound
			then
				v_ok_yn := 0;
				v_comment	:= 'The pallet id is unknown and so can''t be released.';
			end if;
			close 	c_pal_chk;
		end if;

		-- check site is valid
		if 	v_ok_yn = 1
		then
			if	p_site_id_i is null
			then
				v_ok_yn 	:= 0;
				v_comment	:= 'Site id is null and is mandatory';
			else
				v_site_id 	:= p_site_id_i;
			end if;
		end if;

		-- check and set workstation
		if	v_ok_yn = 1
		then
			if	p_station_id_i is null
			then
				v_station_id 	:= v_site_id||'WHHSTATION';
			else
				open	c_station(v_site_id);
				fetch 	c_station into r_station;
				if	c_station%found
				then
					v_station_id 	:= p_station_id_I;
				else
					v_station_id 	:= v_site_id||'WHHSTATION';
				end if;
				close 	c_station;
			end if;
		end if;

		-- Process
		if	v_ok_yn = 1
		then
			if 	p_force_release = 1
			then	-- task releasing is enforced
				if 	p_pallet_id_i is null
				then
					v_ok_yn := 0;
					v_comment	:= 'Release is enforced but there is no pallet id mentioned.';
				else
					-- Here the marshal task is released disregarding if containers are QC'd Y/N
					update	dcsdba.move_task m
					set	m.status			= 'Released'
					,	m.last_held_reason_code 	= 'WHHANDLING'
					,	m.last_released_workstation 	= v_station_id
					,	m.last_released_user 		= v_user_id
					where	m.site_id			= v_site_id
					and	m.pallet_id 			= p_pallet_id_i
					and	m.task_id			= 'PALLET'
					and	m.from_loc_id 			like '%WHH'
					and	m.status 			= 'WHHandling'
					;
					update	dcsdba.move_task m
					set	m.status			= 'Consol'
					,	m.last_held_reason_code 	= 'WHHANDLING'
					,	m.last_released_workstation 	= v_station_id
					,	m.last_released_user 		= v_user_id
					where	m.site_id			= v_site_id
					and	m.pallet_id 			= p_pallet_id_i
					and	m.task_id			!= 'PALLET'
					and	m.from_loc_id 			like '%WHH'
					and	m.status 			= 'WHHConsol'
					;
					commit;
				end if;
			else	-- task releasing is not enforced but still optional
				if	p_container_id_i is null
				then
					v_ok_yn		:= 0;
					v_comment	:= 'Container id is not filled and is mandatory when releasing is not enforced';
				else
					-- Check if container exists
					open	c_container(v_site_id);
					fetch	c_container into r_container;
					if	c_container%notfound
					then
						close 	c_container;
						v_ok_yn 	:= 0;
						v_comment	:= 'Container does not exist in WMS';
					else
						close 	c_container;
						v_container_id	:= r_container.container_id;
						v_pallet_id	:= r_container.pallet_id;
						-- Update consol task by marknig it as QC done
						update	dcsdba.move_task m
						set	m.repack_qc_done 	= 'Y'
						,	m.status 		= 'Consol'
						where	m.site_id 		= p_site_id_i
						and	m.container_id 		= v_container_id
						and	m.pallet_id 		= v_pallet_id
						and	m.status 		in ('WHHConsol','Consol')
						;
						commit;

						-- Check if all consol tasks re qc'd
						open	c_finished(v_pallet_id);
						fetch	c_finished into r_finished;
						if	c_finished%notfound
						then
							close 	c_finished;
							v_ok_yn 	:= 0;
							v_comment	:= 'Something went wrong with updating pallet in WMS';
						else
							close 	c_finished;
							if	r_finished.total_not_checked = 0
							then
								update	dcsdba.move_task m
								set	m.status			= 'Released'
								,	m.last_held_reason_code 	= 'WHHANDLING'
								,	m.last_released_workstation 	= v_station_id
								,	m.last_released_user 		= v_user_id
								where	m.site_id			= p_site_id_i
								and	m.pallet_id 			= v_pallet_id
								and	m.task_id			= 'PALLET'
								and	m.from_loc_id 			like '%WHH'
								and	m.status 			= 'WHHandling'
								;
								update	dcsdba.move_task m
								set	m.status			= 'Consol'
								,	m.last_held_reason_code 	= 'WHHANDLING'
								,	m.last_released_workstation 	= v_station_id
								,	m.last_released_user 		= v_user_id
								where	m.site_id			= v_site_id
								and	m.pallet_id 			= v_pallet_id
								and	m.task_id			!= 'PALLET'
								and	m.from_loc_id 			like '%WHH'
								and	m.status 			= 'WHHConsol'
								;
								commit;
							end if;
						end if;
					end if;
				end if;
			end if;
		end if;
		P_ok_yn_o 	:= v_ok_yn;
		p_comment_o	:= v_comment;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> g_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> p_container_id_i||', '||p_pallet_id_i||', '||p_site_id_i||', '||
											   p_station_id_i||', '||p_user_id_i||', '||p_force_release-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end release_marshal_task_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 3-Okt-2019
-- Purpose : Initialize package to load it faster.
------------------------------------------------------------------------------------------------
	begin
	-- initialization
	null;	
	--
end cnl_warehouse_handling_pck;