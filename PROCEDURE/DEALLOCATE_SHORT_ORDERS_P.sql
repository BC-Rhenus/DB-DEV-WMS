CREATE OR REPLACE PROCEDURE "CNL_SYS"."DEALLOCATE_SHORT_ORDERS_P" ( p_client_id_i	dcsdba.client.client_id%type
						     , p_site_id_i	dcsdba.site.site_id%type
						     )
is
	cursor	c_orders
	is
		select	o.order_id 
		from	dcsdba.order_header o
		where   o.disallow_short_ship	= 'F'
		and	(	o.status  		= 'Allocated'
			or	o.status  		= 'Released'
			or	o.status  		= 'Hold'
			)
		and	o.order_id = (	select	distinct
						m.task_id
					from	dcsdba.move_task m
					where	m.task_id 	= o.order_id
					and	m.task_type	= 'O'
				     )
		and	o.from_site_id 		= p_site_id_i
		and     o.client_id		= p_client_id_i
		and     o.order_id		in (	select	l.order_id
							from    dcsdba.order_line l
							where   nvl(l.qty_tasked,0)+ 
								nvl(l.qty_picked,0)	< qty_ordered
							and     l.order_id              = o.order_id
							and	l.client_id		= o.client_id
							-- Unallocatable lines are always short.
                                                        and	nvl(l.unallocatable,'N')= 'N'
							-- Sku must not be a KIT or kit components are shipped
							and	l.sku_id		in (	select	s.sku_id
												from	dcsdba.sku s
												where   s.sku_id	= l.sku_id
												and	s.client_id	= l.client_id
												and	(	nvl(s.kit_sku,'N')		= 'N'
													or      nvl(s.kit_sku,'N')		= 'Y'
											   		and	nvl(s.kit_ship_components,'N') 	= 'N'
													)		
											   )
						   )
	;
	--
	cursor	c_tsk( b_order_id	dcsdba.order_header.order_id%type)
	is 
		select	t.key 
		from 	dcsdba.move_task t 
		where 	t.task_id 	= b_order_id
		and	t.client_id 	= p_client_id_i
		and	t.site_id 	= p_site_id_i
		and	t.task_type	= 'O'
		and	(	t.status 	= 'Released'
			or	t.status 	= 'Hold'
			or	t.status 	= 'Pending'
			)
		and	(	t.from_loc_id	is null
			or	t.from_loc_id 	in (	select	l.location_id 
							from	dcsdba.location l
							where	l.location_id = t.from_loc_id
							and	l.loc_type in ('Tag-FIFO','Bin','Bulk','Tag-Operator','Receive Dock')
						   )
			)
	;
	--
	l_results integer;
begin
	<<orders_loop>>
	for r_order in c_orders
	loop
		<<tasks_in_order_loop>>
		for r_tsk in c_tsk( r_order.order_id) 
		loop
			dcsdba.libsession.InitialiseSession( userid	=> 'SCHEDULER'
                                                           , groupid	=> null
							   , stationid	=> 'SCHEDULER'||p_site_id_i
							   , wksgroupid	=> null
							   );
			dcsdba.libdeallocate.deallocatestock( result	=> l_results
						            , taskkey 	=> r_tsk.key
						            );
		end loop; --tasks_in_order_loop
	end loop; --orders_loop
	commit;
end deallocate_short_orders_p;