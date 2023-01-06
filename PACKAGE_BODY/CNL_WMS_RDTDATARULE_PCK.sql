CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_RDTDATARULE_PCK" is
/***********************************************************************************************
* NOTE: ANY CHANGE IN THIS PCK BODY REQUIRES A RESTART OF THE APPLICATION.
*       MERGE RULES AND OR RDT DATA RULES WILL START FAILING IF YOU DON'T.
************************************************************************************************/

------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels, 09-Sep-2016
-- Purpose : Procedure to update Order_Container from Box_Count procedure
------------------------------------------------------------------------------------------------
  procedure upd_ocr ( p_pallet_id_i in varchar2
                    , p_box_count_i in number
                    )
  is
  begin
    update dcsdba.order_container ocr
    set    ocr.transport_boxes    = p_box_count_i
    where  pallet_id              = p_pallet_id_i
    ;
  end upd_ocr;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 05-Jul-2016
-- Purpose : Function to get the qty of boxes on a pallet via RDT Data Rules
------------------------------------------------------------------------------------------------
  function box_count ( p_pallet_id_i in  varchar2
                     , p_box_count_i in  varchar2
                     )
    return varchar2
  is
    l_box_count number := to_number( p_box_count_i);
  begin 
    upd_ocr ( p_pallet_id_i => p_pallet_id_i
            , p_box_count_i => l_box_count
            );

  return p_box_count_i;

  end box_count;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-May-2019
-- Purpose : Function to check whether a pallet or container is biening processed. 
------------------------------------------------------------------------------------------------
	function f_pal_or_con	( p_id 		in varchar2
				, p_site_id 	in varchar2
				)
		return varchar2
	is
		cursor	c_pallet_yn( b_id in varchar2)
		is
			select	count(*)
			from 	dcsdba.order_container ocr
			where	ocr.pallet_id = b_id
		;
		--
		cursor 	c_multi_con_pallet( b_pallet_id in varchar2)
		is
			select	count( distinct ocr.container_id)
			from	dcsdba.order_container ocr
			where 	ocr.pallet_id = b_pallet_id
		;
		--
		cursor 	c_container( b_pallet_id in varchar2)
		is
			select	distinct ocr.container_id
			from	dcsdba.order_container ocr
			where 	ocr.pallet_id = b_pallet_id
		;
		--
		cursor 	c_pallet_type( b_pallet_id in varchar2)
		is
			select	distinct nvl(ocr.config_id,'N')
			from	dcsdba.order_container ocr
			where	ocr.pallet_id = b_pallet_id
		;
		--
		r_pallet_yn 		integer;
		r_multi_con_pallet	number;
		r_container		varchar2(30);
		r_pallet_type		varchar2(30);
		--
		l_retval		varchar2(10); -- PALLET or CONTAINER 
	begin
		open 	c_pallet_yn(p_id);
		fetch 	c_pallet_yn into r_pallet_yn;
		if	c_pallet_yn%found
		then
			close	c_pallet_yn;
			open	c_multi_con_pallet(p_id);
			fetch 	c_multi_con_pallet into r_multi_con_pallet;
			close 	c_multi_con_pallet;
			if	r_multi_con_pallet > 1
			then
				l_retval := 'PALLET';
			else
				open 	c_container(p_id);
				fetch 	c_container into r_container;
				close 	c_container;
				if	r_container != p_id
				then
					l_retval := 'PALLET';
				else
					open	c_pallet_type(p_id);
					fetch 	c_pallet_type into r_pallet_type;
					close	c_pallet_type;
					if	r_pallet_type = 'N'
					then
						l_retval := 'CONTAINER';
					else
						l_retval := 'PALLET';
					end if;
				end if;
			end if;		
		else
			close	c_pallet_yn;
			l_retval := 'CONTAINER';
		end if;
		return l_retval;
	end f_pal_or_con;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-May-2019
-- Purpose : Function to check if short ship is allowed
------------------------------------------------------------------------------------------------
	function f_disallow_short_ship(p_pallet_id in varchar2)
		return integer
	is
		cursor c_disallow_short_ship
		is
			select	distinct 1
			from	dcsdba.order_header o
			where	o.disallow_short_ship in ('F','P')
			and	1 = (	select	distinct 1
					from 	dcsdba.order_line l
					where	l.qty_ordered != nvl(l.qty_tasked,0) + nvl(l.qty_picked,0)
					and	nvl(l.unallocatable,'N') = 'N'
					and	l.order_id = o.order_id
					and	l.client_id = o.client_id)
			and	o.order_id || o.client_id in (	select	ocr.order_id || ocr.client_id
								from	dcsdba.order_container ocr
								where	ocr.pallet_id = p_pallet_id)
		;
		--
		r_disallow_short_ship 	integer :=1;
		l_retval 		integer; -- 1 means short ship is allowed. 0 means short ship is disallowed.
	begin
		open	c_disallow_short_ship;
		fetch 	c_disallow_short_ship into r_disallow_short_ship;
		if	c_disallow_short_ship%found
		then
			close	c_disallow_short_ship;
			l_retval := 0; -- Short ship disallowed
		else
			close	c_disallow_short_ship;
			l_retval := 1; -- Short ship allowed
		end if;
		return l_retval;
	end f_disallow_short_ship;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-May-2019
-- Purpose : Function to check if everything from order is on ship dock in-stage
------------------------------------------------------------------------------------------------
	function f_all_on_shipdock_instage( p_pallet_id in varchar2)
		return integer
	is
		cursor c_orders( b_pallet_id in varchar2)
		is
			select	distinct ocr.order_id
			,	ocr.client_id
			from	dcsdba.order_container ocr
			where	ocr.pallet_id = b_pallet_id
		;
		--
		cursor c_tasks( b_order_id 	in varchar2
			      , b_client_id 	in varchar2
			      )
		is
			select	m.*
			from	dcsdba.move_task m
			where	m.task_id = b_order_id
			and	m.client_id = b_client_id
		;
		--
		cursor	c_shipdock_instage( b_from_loc_id	in varchar2
					  , b_site_id		in varchar2
					  )
		is
			select 	1
			from	dcsdba.location l
			where	l.site_id = b_site_id
			and	l.loc_type = 'ShipDock'
			and	l.In_stage = b_from_loc_id
		;
		--
		r_instage	integer := 0;
		l_retval 	integer := 1; -- 1 means ok. 0 means not ok.
	begin
		for 	r_orders in c_orders(p_pallet_id)
		loop
			if	l_retval = 0
			then
				exit;
			else
				for	r_tasks in c_tasks( r_orders.order_id
							  , r_orders.client_id)
				loop
					open	c_shipdock_instage( r_tasks.from_loc_id, r_tasks.site_id);
					fetch	c_shipdock_instage into r_instage;
					if	c_shipdock_instage%found
					then
						close c_shipdock_instage;
						l_retval := 1;
					else
						close c_shipdock_instage;
						l_retval := 0;
						exit;
					end if;
				end loop;
			end if;
		end loop;
		return l_retval;
	end f_all_on_shipdock_instage;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-May-2019
-- Purpose : Function to check if pallet is a parcel pallet or a regular pallet that requires documents
------------------------------------------------------------------------------------------------
	function f_parcel_pallet_yn( p_pallet_id in varchar2)
		return integer
	is
		cursor c_order_count( b_pallet_id in varchar2)
		is
			select	count(distinct order_id)
			from	dcsdba.order_container
			where	pallet_id = b_pallet_id
		;
		--
		cursor c_container_labelled( b_pallet_id in varchar2)
		is
			select 	count(*)
			from	dcsdba.order_container
			where	pallet_id = b_pallet_id
			and	nvl(labelled,'N') = 'Y'
		;
		--
		r_order_count		number;
		r_container_labelled	number;
		--
		l_retval 		integer;-- 0 = regular parcel 1 = parcel pallet
	begin
		open	c_order_count(p_pallet_id);
		fetch 	c_order_count into r_order_count;
		close 	c_order_count;
		if	r_order_count > 1
		then 	-- Multi order pallet so documents not printable.
			l_retval := 1;
		else
			open	c_container_labelled( p_pallet_id);
			fetch 	c_container_labelled into r_container_labelled;
			close 	c_container_labelled;
			if	r_container_labelled = 0
			then
				l_retval := 0;
			else
				l_retval := 0;
			end if;
		end if;
		return l_retval;
	end f_parcel_pallet_yn;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-May-2019
-- Purpose : Function to check all serial numbers are on the pallet
------------------------------------------------------------------------------------------------
	function f_check_serial_on_pal( p_pallet_id 	in varchar2
				      , p_site_id	in varchar2
				      )
		return integer
	is
		-- Check 1 is to see if the pallet contains orders that require serialized SKU. Note that the serial number check is not per pallet but per order.
		-- All inventory picked for the order is on the ship dock in stage and even when the mismatch is on another pallet you can not close the current 
		-- pallet because it contains a box of the order that has the mismatch.

		-- Get all orders on the pallet.
		cursor c_ord
		is
			select 	distinct m.task_id order_id
			,	m.client_id
			from 	dcsdba.move_task m
			where	m.pallet_id = p_pallet_id
			and	m.site_id = p_site_id
		;
		--
		cursor	c_req( b_order_id 	varchar2
			     , b_client_id 	varchar2
			     )
		is
			select	1
			from 	dcsdba.order_line l
			where	l.sku_id||l.client_id = (	select	s.sku_id||s.client_id
								from	dcsdba.sku s
								where	s.client_id = b_client_id
								and	s.sku_id = l.sku_id
								and	(	s.serial_at_pack = 'Y' or
										s.serial_at_pick = 'Y' or
										s.serial_at_receipt = 'Y'))
			and	l.order_id = b_order_id
			and	l.client_id = b_client_id
		;
		--
		cursor c_sku( b_order_id 	in varchar2
			    , b_client_id	in varchar2)
		is
			select	sum(l.qty_tasked+l.qty_picked)
			from	dcsdba.order_line l
			where	l.order_id 	= b_order_id
			and	l.client_id 	= b_client_id
			and	l.sku_id||l.client_id = (	select 	s.sku_id||s.client_id
								from	dcsdba.sku s
								where	s.sku_id = l.sku_id
								and	s.client_id = l.client_id
								and	( s.serial_at_pack = 'Y' or
									  s.serial_at_pick = 'Y' or
									  s.serial_at_receipt = 'Y'))
		;
		--
		cursor c_ser( b_order_id	in varchar2
			    , b_client_id 	in varchar2
			    )
		is
			select	count(*)
			from	dcsdba.serial_number s
			where	s.client_id = b_client_id
			and	s.order_id = b_order_id
		;
		--
		r_req		c_req%rowtype;
		r_sku		number;
		r_ser		number;
		l_check_req	integer := 0;-- 0 Check not required. 1 Check required
		l_retval 	integer := 1;-- 0 means there is a mismatch, 1 means everything is correct
	begin
		-- Check if pallet contains orders that require serial numbers.
		for r_ord in c_ord
		loop
			open	c_req(r_ord.order_id, r_ord.client_id);
			fetch 	c_req into r_req;
			if	c_req%found
			then
				close	c_req;
				l_check_req := 1;
				exit;
			else
				close	c_req;
			end if;
		end loop;

		-- Check nbr serial numbers required vs nbr serial numbers in serial number table for order.
		if	l_check_req = 1
		then
			for r_ord in c_ord 
			loop
				-- fetch total number of serial numbers required.
				open	c_sku(r_ord.order_id, r_ord.client_id);
				fetch 	c_sku into r_sku;
				close 	c_sku;
				-- fetch total number of serial numbers in serial table for order.
				open 	c_ser(r_ord.order_id, r_ord.client_id);
				fetch 	c_ser into r_ser;
				close 	c_ser;
				-- Compare
				if	r_sku != r_ser
				then
					l_retval := 0;
					exit;
				end if;
			end loop;
		end if;

		-- When a mismatch is found for an order the whole pallet including all other pallets must be blocked.
		return l_retval;			
	end f_check_serial_on_pal;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-May-2019
-- Purpose : Function to check if all containers on the pallet are on the same location
------------------------------------------------------------------------------------------------
	function f_chk_container_location( p_pallet_id 	in varchar2
					 , p_site_id	in varchar2)
		return integer
	is
		cursor c_odh
		is
			select	distinct m.task_id
			,	m.pallet_id
			from	dcsdba.move_task m
			where	m.pallet_id in (select 	distinct m2.pallet_id
						from	dcsdba.move_task m2
						where	m2.task_id in ( select 	distinct m3.task_id
									from	dcsdba.move_task m3
									where	m3.pallet_id = p_pallet_id
									and	m3.task_id != 'PALLET'
									and	m3.site_id = p_site_id)
						and	m2.site_id = p_site_id)
			and	m.site_id = p_site_id
		;
		--
		cursor	c_pal( b_pallet_id varchar2)
		is
			select 	m.to_loc_id
			,	m.from_loc_id
			from	dcsdba.move_task m
			where	m.site_id 	= p_site_id
			and	m.pallet_id 	= b_pallet_id
			and	m.task_id 	= 'PALLET'
		;
		--
		cursor	c_consol( b_pallet_id varchar2)
		is
			select 	m.from_loc_id
			,	m.to_loc_id
			from	dcsdba.move_task m
			where	m.pallet_id = b_pallet_id
			and	m.status = 'Consol'
			and	m.site_id = p_site_id
		;
		--	
		cursor c_to_loc_id
		is
			select 	count(distinct to_loc_id)
			from	dcsdba.move_task
			where	pallet_id = p_pallet_id
			and	site_id = p_site_id
			;
		--
		r_pal		c_pal%rowtype;
		r_to_loc_id 	number;
		l_retval 	integer; -- 1 is ok. 0 is not ok.
	begin
		-- select all pallet and orders that have a relation with the pallet bieng closed.
		for	r_odh in c_odh
		loop
			-- Loop thru all found pallets/order combinations
			open	c_pal(r_odh.pallet_id);
			fetch 	c_pal into r_pal;
			if	c_pal%notfound 
			then	-- No pallet could be found in move task so something must be wrong.
				close 	c_pal;
				l_retval := 0;
				exit; -- exit c_odh
			else
				close 	c_pal;
				for 	r_consol in c_consol(r_odh.pallet_id)
				loop	
					if	r_consol.from_loc_id 	!= r_pal.from_loc_id or
						r_consol.to_loc_id 	!= r_pal.to_loc_id
					then
						l_retval := 0;
						exit; -- exit c_consol
					end if;
				end loop;
			end if;
			-- when l_retval is 0 no need to continue the current loop;
			if	nvl(l_retval,1) = 0 
			then
				exit; -- exit c_odh
			end if;
		end loop;
		if 	l_retval is null
		then
			l_retval := 1;
		end if;
		return l_retval;
	end f_chk_container_location;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 24-07-2020
-- Purpose : Procedure to add list to restrict pick table when pick list details is displayed
------------------------------------------------------------------------------------------------
	procedure add_restrict_pick_p( p_station_id_i 	varchar2
				     , p_site_id_i 	varchar2
				     , p_list_id_i 	varchar2
				     )
	is
		l_key 	integer;
		pragma autonomous_transaction;
	begin
		-- Enclose select into for exception when no data is found.
		begin
			select 	m.key
			into 	l_key
			from	dcsdba.move_task m
			where 	m.list_id 	= p_list_id_i
			and	m.site_id	= p_site_id_i
			and	m.status 	= 'Released'
			and	m.task_type	= 'O'
			and 	m.sequence 	=(	select	min(m2.sequence) 
							from 	dcsdba.move_task m2
							where 	m2.list_id 	= m.list_id
							and	m2.site_id	= m.site_id
							and	m2.status 	= m.status
							and	m2.task_type	= m.task_type
						 )
			and (	select	count(*)
				from	dcsdba.restrict_pick r
				where	r.list_id 	= m.list_id
			    )	= 0
			;
		exception
			when NO_DATA_FOUND
			then
				l_key := 1;
		end;
		--
		insert into dcsdba.restrict_pick( key
						, site_id
						, station_id
						, list_id
						)
		values				( l_key
						, p_site_id_i
						, p_station_id_i
						, p_list_id_i
						);
		commit;				
	exception
		when others
		then
			null;
	end add_restrict_pick_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 24-07-2020
-- Purpose : Function that calls the procedure to add list to restrict pick table when pick list details is displayed
------------------------------------------------------------------------------------------------
	function add_restrict_pick_f( p_station_id_i	varchar2
				    , p_site_id_i 	varchar2
				    , p_list_id_i 	varchar2
				    )
	return integer
	is
	begin
		if 	p_station_id_i 	is null 
		or 	p_site_id_i 	is null
		or 	p_list_id_i 	is null
		then
			null;
		else
			add_restrict_pick_p( p_station_id_i 	=> p_station_id_i
					   , p_site_id_i 	=> p_site_id_i
					   , p_list_id_i 	=> p_list_id_i
					   );
		end if;
		return 1;
	end add_restrict_pick_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 22-10-2020
-- Purpose : Function that searches for overpick/ship issues
------------------------------------------------------------------------------------------------
	function over_pick_f( p_task_id_i	in dcsdba.order_header.order_id%type
			    , p_sku_id_i	in dcsdba.sku.sku_id%type
			    , p_site_id_i	in dcsdba.site.site_id%type
			    , p_station_id_i	in dcsdba.workstation.station_id%type
			    , p_user_id_i	in dcsdba.application_user.user_id%type
			    )
	return integer
	is
		l_retval	integer;
	begin	
		select	distinct 
			1
		into	l_retval
		from	(
			select	sum(nvl(a.qty_to_move,0)) 
				qty_to_move
			,	a.line_id
			,	a.task_id
			,	a.client_id
			,	l.qty_ordered
			,	l.qty_tasked
			,	l.qty_picked 
			from 	dcsdba.move_task a
			inner 
			join 	dcsdba.order_line l 
			on 	l.line_id	= a.line_id 
			and 	l.order_id 	= a.task_id 
			and 	l.sku_id	= a.sku_id 
			and 	l.client_id 	= a.client_id 
			where	a.task_id 	= p_task_id_i
			and	a.sku_id	= p_sku_id_i
			and	a.site_id	= p_site_id_i
			and	a.task_type	in ('O','B')	
			and	a.line_id	= (	select	b.line_id
							from	dcsdba.move_task b
							where	b.station_id	= p_station_id_i
							and	b.user_id	= p_user_id_i
							and	b.task_id	= p_task_id_i
							and	b.sku_id	= p_sku_id_i
							and	b.status 	= 'In Progress'
						   )
			group 
			by 	a.line_id
			,	a.task_id
			,	a.client_id
			,	l.qty_ordered
			,	l.qty_tasked
			,	l.qty_picked 
			having 	l.qty_ordered 	< sum(a.qty_to_move) + nvl(l.qty_picked,0)
			)
		;
		--
		cnl_util_pck.add_cnl_error ( p_sql_code_i 		=> null
					   , p_sql_error_message_i 	=> null
					   , p_line_number_i		=> null
					   , p_package_name_i		=> 'cnl_wms_rdtdatarule_pck'
					   , p_routine_name_i		=> 'over_pick_f'
					   , p_routine_parameters_i	=> 'p_task_id_i = '||p_task_id_i
									|| ' p_sku_id_i	= '||p_sku_id_i
									|| ' p_site_id_i = '||p_site_id_i
									|| ' p_station_id_i = '||p_station_id_i
									|| ' p_user_id_i = '||p_user_id_i
					   , p_comments_i		=> 'An over pick issue has been found for order '||p_task_id_i
					   );
		return l_retval;
	exception
		when others
		then
			return 0;
	end over_pick_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 24-07-2020
-- Purpose : Initialization to load package faster
------------------------------------------------------------------------------------------------
	begin
		null;
end cnl_wms_rdtdatarule_pck;