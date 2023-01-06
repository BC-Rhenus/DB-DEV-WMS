CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_TASKRELDAE_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality that releases tasks.
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--    
--
-- Private variable declarations
--
--
    v_site varchar2(10);
    v_list varchar2(20);
    v_list_ok integer;
-- Private routines
--
/********************************************************************************************
*   Author     : M. Swinkels, 09-11-2017
*   Purpose    : Get available inventory QTY from dynamic pick_face
*   Description: returns the QTY available taking any relocate, replenishment, already released pick into account
*********************************************************************************************/
    function chk_clients_f( p_client_group varchar2)

        return integer
        is
        cursor c_get_clients( a_client varchar2)
        is
            select  count(client_id)
            from    dcsdba.client_group_clients
            where   client_group = p_client_group
            and     client_id = a_client;
        --
        cursor c_get_list_clients
        is
            select  distinct client_id
            from    dcsdba.move_task
            where   list_id = v_list;
        --
        l_client_exists integer;
        l_retval integer;
    begin
        dbms_output.put_line('Starting to check if all clients on list are valid for task releasing');
        l_client_exists := 1;
        for r in c_get_list_clients loop
            if l_client_exists = 1 then
                open    c_get_clients(r.client_id);
                fetch   c_get_clients into l_client_exists;
                close   c_get_clients;
            end if;
        end loop;
        if l_client_exists > 0 then
            l_retval := 1;
            dbms_output.put_line('All clients in the list ' || v_list || ' are part of client group ' || p_client_group || '. Proceed.');
        else
            l_retval := 0;
            dbms_output.put_line('One or more of the clients in the list ' || v_list || ' are not part of client group ' || p_client_group || ' List will not be released');
        end if;
        return l_retval;            
    end chk_clients_f;

/********************************************************************************************
*   Author     : M. Swinkels, 09-11-2017
*   Purpose    : Release move tasks on a list.
*   Description: Releases move tasks that are on a list and are ok to process.
*********************************************************************************************/
	Procedure release_lists_p( p_site_i		in varchar2
                                 , p_client_group_i 	in varchar2
				 )
        is
		-- Fetch lists that contain pick tasks with status Hold and other criteria
		cursor c_list ( b_site_id 	varchar2
			      , b_client_group 	varchar2
			      )
		is
			select   distinct mt.list_id list_id
			from     dcsdba.move_task mt 
			where    mt.site_id 	= b_site_id	
			and      mt.list_id 	is not null			-- task is on a list
			and (	select	count(mt1.key) 				-- List contains task with status Hold
				from   	dcsdba.move_task mt1
				where  	mt1.list_id 	= mt.list_id
				and    	mt1.site_id 	= mt.site_id
				and    	mt1.status = 'Hold'
			    )	> 0				
			and (	select	count(mt2.key) 				-- List does not contain pending tasks or tasks without a workzone, from location or is no pick or consol pick task.
				from   	dcsdba.move_task mt2
				where  	mt2.list_id 	= mt.list_id
				and    	mt2.site_id 	= mt.site_id
				and (	mt2.status 	= 'Pending' or 
					mt2.work_zone 	is null or 
					mt2.from_loc_id 	is null or
					mt2.task_type 	not in ('O','C')
				    ) 	
			    )	= 0				
			and (	select	max(mt3.dstamp)				-- All tasks on list are 5 min old
				from    dcsdba.move_task mt3
				where   mt3.list_id 	= mt.list_id
				and     mt3.site_id 	= mt.site_id
			    )	< (sysdate - interval '5' minute) 
			and (	select	count(*)				-- status from all tasks on the list have never been updated by a user 
				from    dcsdba.user_updated_tasks uut
				where   uut.key in (	select	mt4.key
							from   	dcsdba.move_task mt4
							where  	mt4.list_id 	= mt.list_id
							and    	mt4.site_id 	= mt.site_id
						   )
			    ) = 0
		;  

		-- Fetch tasks for tasks on the list
		cursor c_task( b_list_id	varchar2
			     , b_site_id	varchar2
			     )
		is
			select  m.*
			,       l.zone_1        	zone_1
			,       l.pick_face     	pick_face
			,       l.dynamic_link  	dynamic_link
			from    dcsdba.move_task	m
			,       dcsdba.location 	l
			where   m.site_id      	= b_site_id
			and     m.list_id      	= b_list_id
			and     l.site_id       = m.site_id
			and     l.location_id   = m.from_loc_id
		; 

		-- Get inventory available outside pick face
		cursor c_inv_no_pf( b_sku_id		varchar2
				  , b_client_id 	varchar2
				  , b_site_id		varchar2
				  , b_list_id		varchar2
				  , b_tag_id 		varchar2
				  , b_location_id 	varchar2
				  , b_key 		integer
				  )
		is
			select	(
					(	select  nvl(sum(i.qty_on_hand),0)
						from    dcsdba.inventory i
						where   i.site_id 	= b_site_id
						and     i.sku_id  	= b_sku_id
						and     i.tag_id  	= b_tag_id
						and     i.location_id 	= b_location_id
					) 
					-
					( 	select  nvl(sum(m.qty_to_move),0)
						from    dcsdba.move_task m
						where   m.site_id     	= b_site_id
						and     m.client_id   	= b_client_id
						and     m.from_loc_id 	= b_location_id
						and     m.tag_id      	= b_tag_id
						and     m.sku_id      	= b_sku_id
						and ((	m.task_type in ('O','C') and ((	m.status = 'Released' and m.list_id != b_list_id) or ( m.list_id = b_list_id and key != b_key))) or
						     (	m.Task_type = 'M' and m.status = 'Released') or
						     (	m.task_type = 'R')
						    )
					)
				) 	qty_available
			from 	dual
		;

		-- Get inventory available in fixed pick face.
		cursor c_inv_f_pf( b_sku_id		varchar2
				 , b_client_id 		varchar2
				 , b_site_id		varchar2
				 , b_list_id		varchar2
				 , b_tag_id 		varchar2
				 , b_location_id 	varchar2
				 , b_key 		integer
				 )                    
		is
			select	(
					(	select  nvl(sum(p.qty_on_hand),0)
						from    dcsdba.pick_face p
						where   p.site_id 	= b_site_id
						and     p.sku_id  	= b_sku_id
						and     p.location_id 	= b_location_id
						and     p.face_type 	= 'F'
					)
					-
					( 	select  nvl(sum(m.qty_to_move),0)
						from    dcsdba.move_task m
						where   m.site_id     	= b_site_id
						and     m.client_id   	= b_client_id
						and     m.from_loc_id 	= b_location_id
						and     m.sku_id      	= b_sku_id
						and ((	m.task_type in ('O','C') and ((m.status = 'Released' and m.list_id != b_list_id) or (m.list_id = b_list_id and m.key != b_key))) or
						     (	m.task_type = 'M' and m.status = 'Released') or
						     (	m.task_type = 'R')
						    )
					)
				) 	qty_available
			from 	dual
		;

		-- Get available QTY multi location dynamic pick face.
		cursor c_inv_md_pf( b_sku_id		varchar2
				  , b_client_id 	varchar2
				  , b_site_id		varchar2
				  , b_list_id		varchar2
				  , b_tag_id 		varchar2
				  , b_location_id 	varchar2
				  , b_key 		integer
				  , b_link 		integer
				  )                    
		is
			select	(
					(	select  nvl(p.qty_on_hand,0)
						from    dcsdba.pick_face p
						where   p.site_id 	= b_site_id
						and     p.sku_id  	= b_sku_id
						and     p.key     	= b_link
						and     p.face_type 	= 'D'
					)
					-
					( 	select  nvl(sum(m.qty_to_move),0)
						from    dcsdba.move_task m
						where   m.site_id    	= b_site_id
						and     m.client_id   	= b_client_id
						and     m.from_loc_id 	in (	select  l.location_id
										from    dcsdba.location l
										where   l.site_id 	= b_site_id
										and     l.dynamic_link 	= b_link
									   )
						and     m.sku_id      	= b_sku_id
						and ((	m.task_type in ('O','C') and ((m.status = 'Released' and m.list_id != b_list_id) or (m.list_id = b_list_id and m.key != b_key))) or
						     (	m.task_type = 'M' and m.status = 'Released') or
						     (	m.task_type = 'R')
						    )
					)
				) qty_available
			from 	dual
		;

		-- Get available QTY single location dynamic pick face.
		cursor c_inv_d_pf( b_sku_id		varchar2
				 , b_client_id		varchar2
				 , b_site_id		varchar2
				 , b_list_id		varchar2
				 , b_tag_id 		varchar2
				 , b_location_id	varchar2
				 , b_key 		integer
				 , b_link 		integer
				 , b_zone 		varchar2
                          )                    
		is
			select	(
					(	select	nvl(p.qty_on_hand,0)
						from    dcsdba.pick_face p
						where	p.site_id 	= b_site_id
						and     p.sku_id  	= b_sku_id
						and     p.zone_1  	= b_zone
						and     p.location_id 	= b_location_id
						and     p.face_type 	= 'D'
					) 
					-
					( 	select  nvl(sum(m.qty_to_move),0)
						from    dcsdba.move_task m
						where   m.site_id     	= b_site_id
						and     m.client_id   	= b_client_id
						and     m.from_loc_id 	in (	select	l.location_id
										from	dcsdba.location l
										where   l.site_id 	= b_site_id
										and     l.zone_1 	= b_zone
									   )
						and     m.sku_id      	= b_sku_id
						and ((	m.task_type = 'O' and ((m.status = 'Released' and m.list_id != b_list_id) or (m.list_id = b_list_id and m.key != b_key))) or
						     (	m.task_type = 'M' and m.status = 'Released') or
						     (	m.task_type = 'R')
						    )
					)
				)	qty_available
			from 	dual
		;

		-- Variables
		l_task_ok	integer;
		l_qty_available number;

	begin
		v_site := p_site_i;

		-- Loop true all lists.
		for	r_list in c_list( v_site
					, p_client_group_i
					) 
		loop
			v_list		:= r_list.list_id;
			v_list_ok 	:= 1;
			-- check for valid clients
			v_list_ok 	:= chk_clients_f(p_client_group_i);
			-- When all clients on list are OK.
			if	v_list_ok = 1 
			then
				-- Start looping true all tasks on the list
				l_task_ok	:= 1;
				for	r_task in c_task( v_list
							, v_site
							)
				loop
					if	l_task_ok	= 1 
					then
						if	r_task.pick_face 	is null 
						then
						-- 	Fetch available qty from non pick face location
							open    c_inv_no_pf( r_task.sku_id
									   , r_task.client_id
									   , v_site
									   , v_list
									   , r_task.tag_id
									   , r_task.from_loc_id
									   , r_task.key
									   );
							fetch   c_inv_no_pf 
							into 	l_qty_available;
							close   c_inv_no_pf;
							--
							if	l_qty_available - r_task.qty_to_move < 0 
							then
								l_task_ok 	:= 0;
							end if;
						-- 	Fetch available qty from fixed pick face
						elsif 	r_task.pick_face 	= 'F' 
						then
							open    c_inv_f_pf( r_task.sku_id
									  , r_task.client_id
									  , v_site
									  , v_list
									  , r_task.tag_id
									  , r_task.from_loc_id
									  , r_task.key
									  );
							fetch   c_inv_f_pf 
							into 	l_qty_available;
							close   c_inv_f_pf;
							--
							if 	l_qty_available - r_task.qty_to_move < 0 
							then
								l_task_ok := 0;
							end if;
						--	Fetch available qty from multi location dynamic pick face
						elsif 	r_task.pick_face 	= 'D' 
						and 	r_task.dynamic_link 	is not null 
						then
							open    c_inv_md_pf( r_task.sku_id
									   , r_task.client_id
									   , v_site
									   , v_list
									   , r_task.tag_id
									   , r_task.from_loc_id
									   , r_task.key
									   , r_task.dynamic_link
									   );
							fetch   c_inv_md_pf 
							into 	l_qty_available;
							close   c_inv_md_pf;
							--
							if 	l_qty_available - r_task.qty_to_move < 0 
							then
								l_task_ok := 0;
							end if;
						--	Fetch available qty from single location dynamic pick face
						elsif 	r_task.pick_face 	= 'D' 
						and 	r_task.dynamic_link 	is null 
						then
							open    c_inv_d_pf( r_task.sku_id
									  , r_task.client_id
									  , v_site
									  , v_list
									  , r_task.tag_id
									  , r_task.from_loc_id
									  , r_task.key
									  , r_task.dynamic_link
									  , r_task.zone_1
									  );
							fetch	c_inv_d_pf 
							into 	l_qty_available;
							close   c_inv_d_pf;
							--
							if 	l_qty_available - r_task.qty_to_move < 0 
							then
								l_task_ok := 0;
							end if;
						end if;
					end if;
				end loop;
				--
				if 	l_task_ok = 0 
				then
					v_list_ok := 0;
				end if;
				--
				if	v_list_ok 	= 1 
				then
					update  dcsdba.move_task
					set     status = 'Released'
					,       last_released_reason_code = 'TASKRELDAEUPDATE'
					,       last_released_workstation = 'Automatic'
					,       last_released_date        = LOCALTIMESTAMP
					,       last_released_user        = 'CNLSYS'
					where   list_id = v_list
					and     site_id = v_site
					and     status  = 'Hold';
					commit;
				end if;
			end if;
	       end loop;
	end release_lists_p;

end cnl_wms_taskreldae_pck;
--show errors;