CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_OUTBOUND_PCK" is
/********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 04-05-2018
**********************************************************************************
*
* Description: 
* Package to share master data with SynQ WCS from Swisslog.
* SynQ WCS is the WMS software from Swisslog that controlls the Autostore storage system
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
--
-- Private routines
--
/***************************************************************************************************************
* Function to replace WMS wildcards with Oracke wildcards
***************************************************************************************************************/                   
    function replace_wildcards(p_string varchar2)
        return varchar2
    is
        l_string varchar2(400);
        l_retval varchar2(400);
    begin
        l_string := replace(p_string,'*','%');
        l_string := replace(l_string,'?','_');
        l_retval := l_retval;
        return l_retval;
    end replace_wildcards;
/***************************************************************************************************************
* function to check if box is the last to go to conveyor for order
***************************************************************************************************************/                   
    function last_box( p_site_id_i         varchar2
                     , p_client_id_i       varchar2
                     , p_order_id_i        varchar2
                     , p_container_id_i    varchar2
                     )
            return number
    is
            cursor c_pending_box( b_drop_loc_id varchar2)
            is
                    select  count(*)
                    from    dcsdba.move_task
                    where   from_loc_id = b_drop_loc_id
                    and     container_id   != p_container_id_i
                    and     task_id         = p_order_id_i
                    and     client_id       = p_client_id_i
                    and     container_id    not in ( select wms_container_id
                                                     from   cnl_sys.cnl_as_tu
                                                     where  wms_order_id    = p_order_id_i
                                                     and    wms_client_id   = p_client_id_i
                                                   )
            ;
            --
            cursor c_all_tasks
            is
                    select  m.*
                    from    dcsdba.move_task m
                    where   m.task_id = p_order_id_i
                    and     m.site_id = p_site_id_i
                    and     (   m.container_id is null or 
                                (   m.container_id != p_container_id_i and
                                    m.container_id  not in (  select u.wms_container_id
                                                              from   cnl_sys.cnl_as_tu u
                                                              where  u.wms_order_id     = p_order_id_i
                                                              and    u.wms_client_id    = p_client_id_i
                                                          )
                                )
                            )
            ;
            --
            cursor c_from_tsk_zone( b_location varchar2)
            is
                    select  zone_1 
                    from    dcsdba.location
                    where   site_id         = p_site_id_i
                    and     location_id     = b_location
            ;
            --
            cursor c_to_tsk_zone( b_location varchar2)
            is
                    select  zone_1 
                    from    dcsdba.location
                    where   site_id     = p_site_id_i
                    and     location_id = b_location
            ;
            --
            cursor  c_to_as( b_location varchar2
                           , b_stage_route varchar2
                           )        
            is
                    select  count(*)
                    from    dcsdba.location_zone_staging
                    where   next_stage      = b_location
                    and     stage_route_id  = b_stage_route
            ;
            --
            cursor c_seq( b_location varchar2
                        , b_stage_route varchar2
                        )
            is
                    select  stage_route_sequence
                    from    dcsdba.location_zone_staging
                    where   next_stage      = b_location
                    and     stage_route_id  = b_stage_route
                    and     rownum = 1
            ;
            --
            cursor  c_from_zone( b_seq number
                               , b_location varchar2
                               , b_stage_route varchar2
                               )
            is
                    select  from_zone
                    from    dcsdba.location_zone_staging
                    where   next_stage      = b_location
                    and     stage_route_id  = b_stage_route
                    and     stage_route_sequence < b_seq
                    and     from_zone is not null
            ;
            --
            cursor  c_from_loc( b_seq number
                               , b_location varchar2
                               , b_stage_route varchar2
                               )
            is
                    select  from_loc_id
                    from    dcsdba.location_zone_staging
                    where   next_stage      = b_location
                    and     stage_route_id  = b_stage_route
                    and     stage_route_sequence < b_seq
                    and     from_loc_id is not null
            ;
            --
            cursor  c_from_zones( b_from_zone varchar2)
            is
                    select  zone_1 
                    from    dcsdba.location_zone 
                    where   zone_1 like b_from_zone
                    and     site_id = p_site_id_i
            ;
            --
            cursor  c_from_locs( b_from_loc_id varchar2)
            is
                    select  location_id 
                    from    dcsdba.location 
                    where   location_id like b_from_loc_id
                    and     site_id = p_site_id_i
            ;
            --
	    cursor c_pallet( b_pallet_id varchar2)
	    is
		select 	m.*
		from	dcsdba.move_task m
		where	m.task_id = 'PALLET'
		and	m.pallet_id = b_pallet_id
		and	m.site_id = p_site_id_i
	    ;
	    --
	    r_pallet		c_pallet%rowtype;
            r_pending_box       number;
            r_to_as             number;
            r_seq               number;
            r_task_from_zone    varchar2(30);
            r_task_to_zone      varchar2(30);
            --
            l_drop_location     varchar2(30);
            l_storage_location  varchar2(30);
            l_stage_route       varchar2(20);
            l_last_box          number := 1;
            l_continue          varchar2(1) := 'Y';
            l_temp_zone         varchar2(30);
            l_temp_loc          varchar2(30);
	    l_from_loc_id	varchar2(30);
	    l_to_loc_id		varchar2(30);
	    l_stage_route_id	varchar2(30);
            --
    begin
            -- Get conveyor drop location and storage location
            l_drop_location     := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_id_i) || '_DROP-LOCATION_LOCATION');
            l_storage_location  := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_id_i) || '_STORAGE-LOCATION_LOCATION');

            -- Check if any box is on conveyor other then current container but did not pass the first scanner yet.
            if      l_continue = 'Y'
            then
                    open    c_pending_box( l_drop_location);
                    fetch   c_pending_box into r_pending_box;
                    close   c_pending_box;
                    if      r_pending_box > 0
                    then
                            l_last_box := 0;
                            l_continue := 'N';
                    end if;
            end if;

            -- Check for tasks that still need to be moved to conveyor and are not from Autostore.
            if      l_continue = 'Y'
            then
                    for     r in c_all_tasks
                    loop
                            if      l_continue = 'N'
                            then
                                    exit;
                            end if;
                            --
                            if      r.from_loc_id = l_storage_location
                            then
                                    continue;
                            elsif   r.to_loc_id    = l_drop_location and
                                    r.from_loc_id  != l_storage_location
                            then
                                    l_last_box := 0;
                                    l_continue := 'N';
                            end if;
                            --
                    end loop;
            end if;

            -- Check if task still needs to go to Conveyor
            if      l_continue = 'Y'
            then
                    for     r in c_all_tasks
                    loop
                            if      l_continue = 'N' 
                            then 
                                    exit; 
                            end if;
			    -- Check if task is part of a marshal task
			    if		r.status = 'Consol'
			    then	-- capture pallet task
				open	c_pallet(r.pallet_id);
				fetch 	c_pallet
				into 	r_pallet;
				close 	c_pallet;
				l_from_loc_id 	:= r_pallet.from_loc_id;
				l_to_loc_id 	:= r_pallet.to_loc_id;
				l_stage_route_id:= r_pallet.stage_route_id;
				if	l_to_loc_id = l_drop_location
				then	
					l_last_box := 0;
					l_continue := 'N';
					exit;
				end if;
			    else
				l_from_loc_id 	:= r.from_loc_id;
				l_to_loc_id	:= r.to_loc_id;
				l_stage_route_id:= r.stage_route_id;
			    end if;
                            -- Capture location zone for move task from location 
                            open        c_from_tsk_zone(l_from_loc_id);
                            fetch       c_from_tsk_zone into r_task_from_zone;
                            close       c_from_tsk_zone;
                            -- Capture location zone form move task to location
                            open        c_to_tsk_zone(l_to_loc_id);
                            fetch       c_to_tsk_zone into r_task_to_zone;
                            close       c_to_tsk_zone;
                            -- Check stage route
                            if      l_stage_route_id is null
                            then
                                    continue; -- No stage route in this task so move to next task.
                            else
                                    l_stage_route := l_stage_route_id;
                                    -- Check if stage route has steps towards conveyor
                                    open        c_to_as(l_drop_location, l_stage_route);
                                    fetch       c_to_as into r_to_as;
                                    close       c_to_as;
                                    if      r_to_as = 0
                                    then
                                            continue; -- no steps towards conveyor
                                    else
                                            -- get stage route sequence of step to Conveyor
                                            open        c_seq(l_drop_location, l_stage_route);
                                            fetch       c_seq into r_seq;
                                            close       c_seq;
                                            -- Get all from zones from stage route with sequence lower then sequence to conveyor
                                            for     l in c_from_zone( r_seq, l_drop_location, l_stage_route)
                                            loop
                                                    if      l_continue = 'N'
                                                    then
                                                            exit;
                                                    end if;
                                                    -- Translate WMS wildcards into oracle wildcards
                                                    l_temp_zone := replace_wildcards(l.from_zone);
                                                    --  fetch all zones using the wild card.
                                                    for     k in c_from_zones(l_temp_zone)
                                                    loop
                                                            if      l_continue = 'N' 
                                                            then 
                                                                    exit; 
                                                            end if;
                                                            --
                                                            if      r_task_from_zone    = k.zone_1 or   -- task is from one of these zones
                                                                    r_task_to_zone      = k.zone_1      -- task is going towards these zones.
                                                            then
                                                                    l_last_box := 0;
                                                                    l_continue := 'N';
                                                            end if;
                                                    end loop;
                                            end loop;
                                            -- Get all from locations from stage route wil sequence lower then sequence to conveyor
                                            for     l in c_from_loc( r_seq, l_drop_location, l_stage_route)
                                            loop
                                                    if      l_continue = 'N'
                                                    then
                                                            exit;
                                                    end if;
                                                    -- Translate WMS wildcards into oracle wildcards
                                                    l_temp_loc := replace_wildcards(l.from_loc_id);
                                                    -- fetch all locations using wildcard
                                                    for     k in c_from_locs(l_temp_loc)
                                                    loop
                                                            if      l_continue = 'N' 
                                                            then 
                                                                    exit; 
                                                            end if;
                                                            --
                                                            if      l_from_loc_id   = k.location_id or  -- task is from this location
                                                                    L_to_loc_id     = k.location_id     -- task is going to this location
                                                            then
                                                                    l_last_box := 0;
                                                                    l_continue := 'N';
                                                            end if;
                                                    end loop;
                                            end loop;
                                    end if;
                            end if;
                    end loop;
            end if;
            return l_last_box;
    exception
            when others
            then
                l_last_box := 1;
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.last_box',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
                return l_last_box;
    end last_box;
/***************************************************************************************************************
* function to get unique synq line id
***************************************************************************************************************/                   
    procedure get_line_id ( p_client_i    in varchar2
                          , p_order_i     in varchar2
                          , p_sku_i       in varchar2
                          , p_qty_i       in number
                          , p_line_o      out number
                          , p_more_o      out varchar2
                          , p_qty_left_o  out number
                          )
    is
        -- get best fit synq line.
        cursor c_best_key( b_client  varchar2
                         , b_order   varchar2
                         , b_sku     varchar2
                         , b_qty     number
                         )
        is
            select  a.mt_key
            from    cnl_sys.cnl_as_manual_lines a
            where   a.client_id                     = b_client
            and     a.order_id                      = b_order
            and     a.sku_id                        = b_sku
            and     a.mt_qty - nvl(a.qty_picked,0)  = b_qty
            and     a.finished                      = 'N'
            and     a.category                      = 'MANUAL'
            and     rownum                          = 1
        ;

        -- second best fit line.
        cursor c_sec_key( b_client  varchar2
                        , b_order   varchar2
                        , b_sku     varchar2
                        , b_qty     number
                        )
        is
            select  a.mt_key
            from    cnl_sys.cnl_as_manual_lines a
            where   a.client_id                     = b_client
            and     a.order_id                      = b_order
            and     a.sku_id                        = b_sku
            and     a.mt_qty - nvl(a.qty_picked,0)  > b_qty
            and     a.finished                      = 'N'
            and     a.category                      = 'MANUAL'
            and     rownum                          = 1
        ;

        -- Any line.
        cursor c_fin_key( b_client  varchar2
                        , b_order   varchar2
                        , b_sku     varchar2
                        , b_qty     number
                        )
        is
            select  a.mt_key
            ,       a.mt_qty - nvl(a.qty_picked,0) mt_qty
            from    cnl_sys.cnl_as_manual_lines a
            where   a.client_id                     = b_client
            and     a.order_id                      = b_order
            and     a.sku_id                        = b_sku
            and     a.mt_qty - nvl(a.qty_picked,0)  > 0
            and     a.finished                      = 'N'
            and     a.category                      = 'MANUAL'
            and     rownum                          = 1
        ;

        --
        r_best  number;
        r_sec   number;
        r_fin   c_fin_key%rowtype;
        --
    begin
        open    c_best_key( p_client_i, p_order_i, p_sku_i, p_qty_i);
        fetch   c_best_key into r_best;
        if      c_best_key%notfound
        then
                close   c_best_key;
                --
                open    c_sec_key( p_client_i, p_order_i, p_sku_i, p_qty_i);
                fetch   c_sec_key into r_sec;
                if      c_sec_key%notfound
                then
                        close   c_sec_key;
                        --
                        open    c_fin_key( p_client_i, p_order_i, p_sku_i, p_qty_i);
                        fetch   c_fin_key into r_fin;
                        if      c_fin_key%notfound
                        then
                                close c_fin_key;
                                --THIS IS AN ISSUE
                        else
                                p_line_o        := r_fin.mt_key;
                                p_more_o        := 'Y';                     -- Not whole line is selected
                                p_qty_left_o    := p_qty_i - r_fin.mt_qty;  -- QTY left to select from lines
                                close       c_fin_key;
                                -- The whole line is picked but more is needed for the task.
                                update      cnl_as_manual_lines
                                set         qty_picked  = mt_qty
                                ,           finished    = 'Y'
                                where       client_id   = p_client_i
                                and         order_id    = p_order_i
                                and         sku_id      = p_sku_i
                                and         mt_key      = r_fin.mt_key;
                        end if;
                else
                        p_line_o        := r_sec;
                        p_more_o        := 'N'; -- The task can be completed with QTY from this line.
                        p_qty_left_o    := 0;   -- No QTY left from task.
                        close       c_sec_key;
                        -- Line is not finished because the line QTY is more than the tasked QTY
                        update      cnl_as_manual_lines
                        set         qty_picked  = nvl(qty_picked,0) + p_qty_i
                        where       client_id   = p_client_i
                        and         order_id    = p_order_i
                        and         sku_id      = p_sku_i
                        and         mt_key      = r_sec;
                end if;
        else
                p_line_o    := r_best;
                p_more_o    := 'N'; -- The task can be completed with QTY from this line.
                p_qty_left_o := 0;  -- No QTY left from task.
                close       c_best_key;
                --
                update      cnl_as_manual_lines
                set         qty_picked  = nvl(qty_picked,0) + p_qty_i
                ,           finished    = 'Y'
                where       client_id   = p_client_i
                and         order_id    = p_order_i
                and         sku_id      = p_sku_i
                and         mt_key      = r_best;
        end if;
    end get_line_id;
/***************************************************************************************************************
* function to get table  key
***************************************************************************************************************/                   
    function key_f(p_tbl number)
        return number
    is
        cursor c_otu
        is
            select rhenus_synq.host_order_tu_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_mex
        is
            select rhenus_synq.host_message_exchange_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_orl
        is
            select rhenus_synq.host_order_line_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        l_retval number;
    begin
        if      p_tbl = 1
        then
                open  c_otu;
                fetch c_otu into l_retval;
                close c_otu;
        elsif   p_tbl = 2
        then
                open  c_mex;
                fetch c_mex into l_retval;
                close c_mex;
        elsif   p_tbl = 3
        then
                open  c_orl;
                fetch c_orl into l_retval;
                close c_orl;
        end if;
        return l_retval;
    end key_f;

/***************************************************************************************************************
* Add orders and lists from WMS that started
* This data is used to inform Synq Rhenus has started picking and so Autostore can start preparing the bins needed.
***************************************************************************************************************/
	procedure wms_get_orders_started( p_order_i	in varchar2
                                        , p_list_i   	in varchar2
					, p_client_i 	in varchar2
					, p_site_i   	in varchar2
                                        )
	is
		-- Check if order exists as order master already.
		cursor c_exi( b_order    varchar2
			    , b_client   varchar2
                            )
		is
			select  count(*)
			from    cnl_sys.cnl_as_manual_order_started
			where   order_id    	= b_order
			and     client_id   	= b_client
			and	cnl_if_status	!= 'Completed'
		; 

		-- get the list id used for picking.
		cursor c_lst( b_order  varchar2
			    , b_client varchar2
			    , b_site   varchar2
			    )
		is
			select  distinct list_id
			from    dcsdba.inventory_transaction
			where   site_id         = b_site
			and     reference_id    = b_order
			and     client_id       = b_client
			and     code            = 'Pick'
			and     to_loc_id       = 'CONTAINER'
			and     list_id         is not null
		;

		-- get all orders on list
		cursor c_ord( b_list    varchar2)
		is
			select  distinct task_id
			,       client_id
			from    dcsdba.move_task
			where   (	list_id 	= b_list 
				or 	(	b_list 	is null 
					and 	list_id is null
					)
				)
			and     task_id 	!= p_order_i
		;

		r_exi       number;
	begin
		if cnl_sys.cnl_as_pck.chk_client( p_site_i
                                                , p_client_i
						) = 1
		then
			-- Check if order already exists as order master.
			open    c_exi( p_order_i
				     , p_client_i
				     );
			fetch   c_exi
			into    r_exi;
			close   c_exi;
			if      r_exi = 0
			then    -- insert order if it does not exist yet.
				insert
				into	cnl_sys.cnl_as_manual_order_started
				(	client_id
				,	order_id
				,	dstamp
				,	processed
				,	cnl_if_status
				)
				values
				(	p_client_i
				,	p_order_i
				,	sysdate
				,	'N'
				,	'OrderStarted'
				);
				--
				cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.wms_get_orders_started'
								    , 'inserting order started. Order ' 
								    || p_order_i 
								    );
			end if;
			-- To prevent the message from beeing to late we run past all orders that are on the same list.
			-- First get all lists part of this order.
			-- The list must be fetched from the transactions because it is possible that only one task existed.
			for     r_lst in c_lst( p_order_i
					      , p_client_i
					      , p_site_i
					      ) 
			loop 
				-- The orders on the list must be fetched from the move task. Not all orders on a list have transactions.
				for     r_o in c_ord( r_lst.list_id) 
				loop
					open    c_exi( r_o.task_id
						     , r_o.client_id
						     );
					fetch   c_exi 
					into    r_exi;
					close   c_exi;
					if      r_exi = 0
					then
						insert
						into	cnl_sys.cnl_as_manual_order_started
						(	client_id
						,	order_id
						,	dstamp
						,	processed
						,	cnl_if_status
						)
						values
						(	r_o.client_id
						,	r_o.task_id
						,	sysdate
						,	'N'
						,	'OrderStarted'
						);
						--
						cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.wms_get_orders_started'
										    , 'inserting order started. Order ' 
										    || r_o.task_id
										    );
					end if;
				end loop c_ord;
			end loop c_lst;
		else
			cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.wms_get_orders_started'
							    , 'Client id ' 
							    || p_client_i 
							    || ' is not valid for Autostore and record is skipped for order ' 
							    || p_order_i
							    );
		end if;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.wms_get_orders_started'
							    , substr('Exception handling: SQLERRM = ' 
							    || sqlerrm 
							    || ' and SQLCODE = ' 
							    || sqlcode,1,4000)
							    );
			commit;
	end wms_get_orders_started;

/***************************************************************************************************************
* Manual order started
***************************************************************************************************************/
	procedure manual_order_start
	is
		-- select all maual order start orders that are known in Synq
		cursor c_str
		is 
			select  c.order_id
			,       c.client_id
			,       o.ord_type
			from    cnl_sys.cnl_as_manual_order_started c
			inner
			join	cnl_sys.cnl_as_orders o
			on 	o.order_id	= c.order_id
			and	o.client_id	= c.client_id
			and	o.cnl_if_status = 'Shared'
			where   c.processed 	= 'N'
		;
		--
		l_otu_key   number;
		l_hos_key   number;
	begin
		for     r_str in c_str
		loop
			l_otu_key := key_f(1);
			if	r_str.ord_type != 'AUTOSTORE'
			then
				insert 
				into 	rhenus_synq.host_order_tu@as_synq.rhenus.de
				( 	order_id
				, 	owner_id
				, 	category
				, 	tu_id
				, 	tu_type
				, 	order_tu_key
				)
				values
				( 	r_str.order_id
				, 	r_str.client_id
				, 	'MANUAL'
				, 	'NOID'
				, 	'NOTYPE'
				, 	l_otu_key
				);
				--
				cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_order_start'
								    , 'inserting order ' 
								    || r_str.order_id 
								    || ' as manual started in SynQ.'
								    );
				l_hos_key := key_f(2);
				--
				cnl_sys.cnl_as_pck.create_message_exchange( p_message_id_i                 => r_str.order_id||r_str.client_id||'OSTRT' || l_hos_key
									  , p_message_status_i             => 'UNPROCESSED'
									  , p_message_type_i               => 'ManualOrderStart'
									  , p_trans_code_i                 => 'NEW'
									  , p_host_message_table_key_i     => l_otu_key
									  , P_key_o                        => l_hos_key
									  );
			end if;
			--
			update  cnl_sys.cnl_as_orders
			set     cnl_if_status   = 'OrderStartShared'
			,       ord_start_host_message_key        = l_hos_key
			,       update_date                       = sysdate  
			where   order_id                          = r_str.order_id
			and     client_id                         = r_str.client_id
			;
			--
			update	cnl_as_manual_order_started
			set	processed 	= 'Y'
			,	cnl_if_status 	= 'OrderStartShared'
			where	client_id	= r_str.client_id
			and	order_id	= r_str.order_id
			and	processed	= 'N'
			;
				--
			commit;
		end loop c_str;
	exception
		when others
		then
			cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_order_start'
							    , substr('Exception handling: SQLERRM = ' 
							    || sqlerrm 
							    || ' and SQLCODE = ' 
							    || sqlcode,1,4000)
							    );
			commit;
	end manual_order_start;
/***************************************************************************************************************
* Manual picking finished
***************************************************************************************************************/
    function get_carton_weight( p_container_id_i  in  varchar2
                              , p_client_id_i     in  varchar2
                              , p_site_id_i       in  varchar2
                              )
        return number
    is
        cursor c_content( b_container   varchar2
                        , b_client      varchar2
                        , b_site        varchar2
                        )
        is
            select  i.sku_id
            ,       i.qty_on_hand
            ,       i.config_id
            from    dcsdba.inventory i
            where   i.container_id      = b_container
            and     i.client_id         = b_client
            and     i.site_id           = b_site
        ;
        --
        cursor c_container_type( b_container   varchar2
                               , b_client      varchar2
                               )
        is
            select  nvl(p.weight,0)*1000 weight
            from    dcsdba.pallet_config p
            where   config_id = ( select    o.container_type
                                  from      dcsdba.order_container o
                                  where     o.client_id     = b_client
                                  and       o.container_id  = b_container
                                )
            and     client_id = b_client
        ;
        --
        cursor  c_sku( b_sku    varchar2
                     , b_client varchar2
                     )
        is
            select  nvl(s.each_weight,0)*1000 weight
            from    dcsdba.sku s
            where   s.sku_id = b_sku
            and     s.client_id = b_client
        ;
        --
        cursor c_config ( b_config  varchar2
                        , b_client  varchar2
                        )
        is
            select  nvl(c.ratio_1_to_2,0)  ratio_1_to_2
            ,       nvl(c.weight_2,0)*1000      weight_2
            ,       nvl(c.ratio_2_to_3,0) * nvl(c.ratio_1_to_2,1) ratio_2_to_3
            ,       nvl(c.weight_3,0)*1000      weight_3
            ,       nvl(c.ratio_3_to_4,0) * nvl(c.ratio_2_to_3,1) * nvl(c.ratio_1_to_2,1) ratio_3_to_4
            ,       nvl(c.weight_4,0)*1000      weight_4
            ,       nvl(c.ratio_4_to_5,0) * nvl(c.ratio_3_to_4,1) * nvl(c.ratio_2_to_3,1) * nvl(c.ratio_1_to_2,1) ratio_4_to_5
            ,       nvl(c.weight_5,0)*1000      weight_5
            ,       nvl(c.ratio_5_to_6,0) * nvl(c.ratio_4_to_5,1) * nvl(c.ratio_3_to_4,1) * nvl(c.ratio_2_to_3,1) * nvl(c.ratio_1_to_2,1) ratio_5_to_6
            ,       nvl(c.weight_6,0)*1000      weight_6
            ,       nvl(c.ratio_6_to_7,0) * nvl(c.ratio_5_to_6,1) * nvl(c.ratio_4_to_5,1) * nvl(c.ratio_3_to_4,1) * nvl(c.ratio_2_to_3,1) * nvl(c.ratio_1_to_2,1) ratio_6_to_7
            ,       nvl(c.weight_7,0)*1000      weight_7
            ,       nvl(c.ratio_7_to_8,0) * nvl(c.ratio_6_to_7,1) * nvl(c.ratio_5_to_6,1) * nvl(c.ratio_4_to_5,1) * nvl(c.ratio_3_to_4,1) * nvl(c.ratio_2_to_3,1) * nvl(c.ratio_1_to_2,1) ratio_7_to_8
            ,       nvl(c.weight_8,0)*1000      weight_8
            from    dcsdba.sku_config c
            where   c.config_id = b_config
            and     c.client_id = b_client
        ;        
        --
        r_type_weight   number;
        r_sku_weight    number;
        r_sku_config    c_config%rowtype;
        --
        l_qty_left      number;
        l_inv_weight    number;
        l_sub_total     number := 0;
        l_total_weight  number;
        l_retval        number;
        l_wht_chk       number := 1;

    begin
            -- Get weight of used container type
            open    c_container_type( p_container_id_i
                                    , p_client_id_i
                                    );
            fetch   c_container_type 
            into    r_type_weight;
            close   c_container_type;

            -- Get inventory inside container
            for     r_content in c_content( p_container_id_i
                                          , p_client_id_i
                                          , p_site_id_i
                                          )
            loop
                    -- get each weight from SKU
                    open    c_sku( r_content.sku_id
                                 , p_client_id_i
                                 );
                    fetch   c_sku 
                    into    r_sku_weight;
                    close   c_sku;

                    -- Check if weight check can be done.
                    if      cnl_sys.cnl_as_pck.wht_chk_req( p_weight_i => r_sku_weight
                                                          , p_site_i   => p_site_id_i
                                                          ) = 0
                    then
                            l_wht_chk := 0; -- No weight check
                    end if;

                    -- get pack config details from inventory.
                    open    c_config( r_content.config_id
                                    , p_client_id_i
                                    );
                    fetch   c_config
                    into    r_sku_config;
                    if      c_config%found
                    then
                            close   c_config;
                            l_inv_weight    := 0;
                            l_qty_left      := r_content.qty_on_hand;

                            -- Get weight from tracking level 8.
                            if      r_sku_config.ratio_7_to_8   >   0  
                            and     l_qty_left                  >=  r_sku_config.ratio_7_to_8
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_7_to_8) * r_sku_config.weight_8;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_7_to_8) * r_sku_config.ratio_7_to_8;
                            end if;

                            -- Get weight from tracking level 7
                            if      r_sku_config.ratio_6_to_7   >   0
                            and     l_qty_left                  >=  r_sku_config.ratio_6_to_7
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_6_to_7) * r_sku_config.weight_7;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_6_to_7) * r_sku_config.ratio_6_to_7;
                            end if;

                            -- Get weight from tracking level 6
                            if      r_sku_config.ratio_5_to_6   >   0
                            and     l_qty_left                  >=  r_sku_config.ratio_5_to_6
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_5_to_6) * r_sku_config.weight_6;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_5_to_6) * r_sku_config.ratio_5_to_6;
                            end if;

                            -- Get weight from tracking level 5
                            if      r_sku_config.ratio_4_to_5   >   0
                            and     l_qty_left                  >=  r_sku_config.ratio_4_to_5
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_4_to_5) * r_sku_config.weight_5;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_4_to_5) * r_sku_config.ratio_4_to_5;
                            end if;

                            -- Get weight from tracking level 4
                            if      r_sku_config.ratio_3_to_4   >   0
                            and     l_qty_left                  >=  r_sku_config.ratio_3_to_4
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_3_to_4) * r_sku_config.weight_4;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_3_to_4) * r_sku_config.ratio_3_to_4;
                            end if;

                            -- Get weight from tracking level 3
                            if      r_sku_config.ratio_2_to_3   >   0
                            and     l_qty_left                  >=  r_sku_config.ratio_2_to_3
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_2_to_3) * r_sku_config.weight_3;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_2_to_3) * r_sku_config.ratio_2_to_3;
                            end if;

                            -- Get weight from tracking level 2
                            if      r_sku_config.ratio_1_to_2   >   0
                            and     l_qty_left >= r_sku_config.ratio_1_to_2
                            then
                                    l_inv_weight    := l_inv_weight + floor(l_qty_left / r_sku_config.ratio_1_to_2) * r_sku_config.weight_2;
                                    l_qty_left      := l_qty_left - floor(l_qty_left / r_sku_config.ratio_1_to_2) * r_sku_config.ratio_1_to_2;
                            end if;

                            -- Get weight from each weight
                            if      l_qty_left > 0 
                            then
                                    l_inv_weight    := l_inv_weight + l_qty_left * r_sku_weight;
                                    l_qty_left      := 0;
                            else
                                    null;
                            end if;
                    else
                            close   c_config;
                            l_inv_weight := r_content.qty_on_hand * r_sku_weight;
                    end if;
                    --
                    l_sub_total := l_sub_total + l_inv_weight;
            end loop;

            -- When no weight check is required value null is send back.
            if      l_wht_chk = 0 
            then
                    l_retval := null;
            else
                    l_total_weight := l_sub_total + r_type_weight;
                    l_retval := nvl(l_total_weight,0);
            end if;

            return l_retval;
    end get_carton_weight;

/***************************************************************************************************************
* Manual picking finished
***************************************************************************************************************/
	procedure manual_pick_finished( p_itl_key_i	in number
				      , p_pallet_i	in varchar2
				      , p_container_i	in varchar2
				      , p_station_i	in varchar2
				      , p_site_i	in varchar2
				      , p_client_i	in varchar2
				      , p_to_location_i	in varchar2
				      , p_consol_link_i	in number
				      )
	is
		-- Fetch all containers on this pallet + other details.
		cursor 	c_containers_on_pallet
		is
			select	distinct
				mt.container_id
			,	mt.task_id		order_id
			,	mt.client_id
			,	tu.wms_container_id	container_already_processed
			,       oc.container_type
			,       oc.config_id		pallet_type
			,	mt.final_loc_id
			,	mt.stage_route_id
			,	oh.work_group
			,	oh.consignment
			,	(
				select	count(distinct order_id)
				from	dcsdba.order_container o
				where	o.container_id 	= mt.container_id
				and	o.pallet_id	= p_pallet_i
				) 			multi_order_container
			from	dcsdba.move_task 	mt
			left
			join	cnl_sys.cnl_as_tu 	tu
			on	mt.container_id		= tu.wms_container_id
			and	mt.client_id		= tu.wms_client_id
			inner
			join	dcsdba.order_container 	oc
			on	mt.task_id 		= oc.order_id
			and	mt.container_id 	= oc.container_id
			and	mt.client_id		= oc.client_id
			inner
			join	dcsdba.order_header	oh
			on	mt.task_id		= oh.order_id
			and	mt.client_id		= oh.client_id
			where	mt.pallet_id		= p_pallet_i
			and	mt.site_id		= p_site_i
			and	mt.task_id 		!= 'PALLET'
		;

		-- Get marshal task from pallet
		cursor c_mvt
		is
			select  m.*
			from    dcsdba.move_task 	m
			where   m.pallet_id   		= p_pallet_i
			and     m.site_id     		= p_site_i
			and     m.task_id     		= 'PALLET'
		;

		-- Get container content
		cursor c_inventory( b_container  varchar2
				  , b_client     varchar2
				  , b_site       varchar2
				  )
		is
			select  i.key
			,       i.qty_on_hand
			,       i.sku_id
			from    dcsdba.inventory i
			where   i.container_id  = b_container
			and     i.client_id     = b_client
			and     i.site_id       = b_site
		;

		-- cursor variables
		r_mvt               c_mvt%rowtype;

		-- Local variables
		l_storage_location  	varchar2(50);
		l_drop_location     	varchar2(50);

		l_otu_key           	number; -- Order tu key
		l_orl_key           	number; -- order line key
		l_hme_key           	number; -- Returned key for host message

		l_order_id		varchar2(50);
		l_exi			number;
		l_del_pal           	number;
		l_chk               	number;
		l_timer             	number := 0;
		l_more_lines        	varchar2(1);
		l_qty_to_move       	number;
		l_line              	number; -- selected line id
		l_more              	varchar2(1); -- If more lines are required.
		l_qty_left          	number;
		l_container_weight  	number;
		l_seperate_cont     	varchar2(1);
		l_mvt_key           	number;
		l_new_consol_link   	number;
		l_last_box          	number;
		l_qc		    	number;
		--	
	begin
		-- Starting manual pick finished procedure for pallet
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.manual_pick_finished','starting procedure for pallet id ' ||p_pallet_i);

		-- Fetch storage location and drop off location
		l_storage_location	:= cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_i) || '_STORAGE-LOCATION_LOCATION');
		l_drop_location		:= cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_i) || '_DROP-LOCATION_LOCATION');

		-- Check if workstation has seperate containers enabled Y/N
		l_seperate_cont 	:= cnl_sys.cnl_as_pck.separate_containers(p_station_i);

		-- Fetch all containers on the pallet and other related data and start looping each container
		<<container_loop>>
		for	r_containers_on_pallet in c_containers_on_pallet
		loop
			-- Check if container is not already processed. If yes move to next container
			if	r_containers_on_pallet.container_already_processed is not null
			then
				cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
								    , 'Checking if container is processed already. ' 
								    ||r_containers_on_pallet.container_id 
								    ||' Is already processed'
								    );
				continue container_loop;
			end if;

			-- Check if container is a multi order container. When yes container must be skipped.
			if	r_containers_on_pallet.multi_order_container > 1 
			then
				cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
								    , 'Checking if container is multi order. ' 
								    ||r_containers_on_pallet.container_id 
								    ||' Is multi order so container is skipped'
								    );
				continue container_loop;
			end if;

			-- Check if order exist in Synq to detirmine what to use as order id.
			-- When the order does not exist in Synq the box is treated as a UFO and the order id must become the container id.
			-- Else we must use the order id
			select	count(*)
			into 	l_exi
			from	rhenus_synq.order_header@as_synq o
			where	o.order_id 	= r_containers_on_pallet.order_id
			and	o.owner_key 	=	( 
							select  w.owner_key
							from	rhenus_synq.owner@as_synq w
							where	w.owner_key 	= o.owner_key
							and	w.owner_id 	= r_containers_on_pallet.client_id
							)
			;
			if	l_exi = 0
			then
				l_order_id := r_containers_on_pallet.container_id;
			else
				l_order_id := r_containers_on_pallet.order_id;
			end if;

			-- When seperate containers is not enabled on workstation fetch the pallet task so it can be used to create new marshal tasks 
			if      nvl(l_seperate_cont,'N') = 'N'
			then
				open    c_mvt;
				fetch   c_mvt 
				into 	r_mvt;
				if      c_mvt%notfound
				then    -- Some thing is wrong exit procedure and record an error
					cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
									    , 'Fetching pallet task details failed. Could not find marshal task for pallet ' 
									    ||p_pallet_i
									    ||'. Stopped procedure.'
									    );
					close c_mvt;
					exit container_loop;
				end if;
				close   c_mvt;
			end if;

			-- Fetch total weight container in grams
			l_container_weight := get_carton_weight( p_container_id_i  => r_containers_on_pallet.container_id
							       , p_client_id_i     => r_containers_on_pallet.client_id
							       , p_site_id_i       => p_site_i
							       );

			-- When the container type = CONTAINER no correct weight check can be performend. New conditions might follow
			-- To prevent a weight check the weight must be set to null.
			if	r_containers_on_pallet.container_type = 'CONTAINER'
			then
				l_container_weight := null;
			end if;

			cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
							    , 'Weight of container ' 
							    ||r_containers_on_pallet.container_id
							    ||' from client_id '
							    ||r_containers_on_pallet.client_id
							    ||' is calculated at '
							    ||l_container_weight
							    ||' grams.'
							    );


			-- Synq must know when no other boxes will arrive on the conveyor for an order. 
			-- This is needed to set the order to status complete and so having the order be deleted at some point.
			l_last_box := last_box( p_site_id_i      => p_site_i
					      , p_client_id_i    => r_containers_on_pallet.client_id
					      , p_order_id_i     => r_containers_on_pallet.order_id
					      , p_container_id_i => r_containers_on_pallet.container_id
					      );
			if	l_last_box = 1
			then
				cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
								    , 'Container ' 
								    ||r_containers_on_pallet.container_id
								    ||' from client_id '
								    ||r_containers_on_pallet.client_id
								    ||' is is flagged as the last box for order '
								    ||r_containers_on_pallet.order_id
								    ||' to be dropped on the conveyor.'
								    );
			end if;

			-- Fetch host_order_tu key
			select	rhenus_synq.host_order_tu_seq.nextval@as_synq.rhenus.de 
			into	l_otu_key
			from    dual
			;

			-- Insert host order tu record
			insert 
			into 	rhenus_synq.host_order_tu@as_synq.rhenus.de
			(	order_tu_key
			, 	category
			, 	order_id
			, 	owner_id
			, 	tu_id
			, 	tu_type
			, 	cubing_result_key
			, 	tu_expected_weight
			, 	last_tu_for_order
			)
			values
			(	l_otu_key
			, 	'MANUAL'
			, 	l_order_id 
			, 	r_containers_on_pallet.client_id
			, 	r_containers_on_pallet.container_id
			, 	nvl(r_containers_on_pallet.container_type,r_containers_on_pallet.pallet_type)
			, 	null
			, 	l_container_weight
			, 	l_last_box
			);

			-- Now add the order lines for each container to Synq by looping true all inventory records inside the container
			<<inventory_loop>>
			for	r_inventory in c_inventory( r_containers_on_pallet.container_id
							  , r_containers_on_pallet.client_id
							  , p_site_i
							  )
			loop
				cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
								    , 'Looping true inventory inside container ' 
								    ||r_containers_on_pallet.container_id
								    );
				l_more_lines 	:= 'Y';
				l_qty_to_move 	:= r_inventory.qty_on_hand;

				<<order_line_loop>>
				while   l_more_lines = 'Y' 
				loop
					-- Get original line id. 
					-- An attempt is made to match the inventory with order lines in Synq.
					-- This don't always match because 1 pick task is equal to one order line in Synq but pick tasks can be split in WMS
					-- This is therefore a best effort
					get_line_id( p_client_i    => r_containers_on_pallet.client_id
						   , p_order_i     => r_containers_on_pallet.order_id
						   , p_sku_i       => r_inventory.sku_id
						   , p_qty_i       => l_qty_to_move
						   , p_line_o      => l_line
						   , p_more_o      => l_more
						   , p_qty_left_o  => l_qty_left
						   );

					-- When nothing is returned we use inventory details as line information.
					if      l_line is null
					then
						l_more 		:= 'N';
						l_line 		:= r_inventory.key;
						l_qty_left 	:= 0;
					end if;

					-- First fetch unique key for new record then start insert
					select	rhenus_synq.host_order_line_seq.nextval@as_synq.rhenus.de 
					into	l_orl_key
					from    dual
					;

					-- Start inserting host order line details
					insert 
					into 	rhenus_synq.host_order_line@as_synq.rhenus.de
					( 	allocation_tolerance_window
					, 	expiration_window
					, 	category
					, 	order_line_number
					, 	product_id
					, 	quantity
					, 	relevant_date_for_allocation
					, 	order_line_key
					, 	uom_tree
					, 	min_uom
					)
					values
					( 	0
					, 	0
					, 	'MANUAL'
					, 	l_line
					, 	r_inventory.sku_id
					, 	l_qty_to_move - l_qty_left
					, 	'NO-DATE'
					, 	l_orl_key
					, 	'DEFAULT'
					,	'EACH'
					);

					l_qty_to_move := l_qty_left;

					-- Now create the link between the container and the lines
					insert 
					into 	rhenus_synq.host_order_tu_orderline@as_synq.rhenus.de
					( 	order_tu_key
					, 	order_line_key
					)
					values
					( 	l_otu_key
					, 	l_orl_key
					);

					-- Is the content linked to more lines?
					if      l_more = 'N'
					then
						l_more_lines := 'N';
					end if;
				end loop; --  order_line_loop
			end loop; --inventory_loop

			-- Create host message for container 
			cnl_sys.cnl_as_pck.create_message_exchange( p_message_id_i               => r_containers_on_pallet.container_id
								  , p_message_status_i           => 'UNPROCESSED'
								  , p_message_type_i             => 'ManualCartonPicked'
								  , p_trans_code_i               => 'NEW'
								  , p_host_message_table_key_i   => l_otu_key
								  , P_key_o                      => l_hme_key
								  );

			cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
							    , 'Created host message for container ' || r_containers_on_pallet.container_id
							    );

			-- Saving container in CNL_AS_TU 
			insert 
			into	cnl_sys.cnl_as_tu
			( 	wms_container_id
			, 	wms_order_id
			, 	wms_container_type
			, 	wms_client_id
			, 	cnl_if_status
			, 	man_pick_host_message_key
			, 	dstamp
			, 	container_weight
			, 	last_box
			)
			values
			( 	r_containers_on_pallet.container_id
			, 	r_containers_on_pallet.order_id
			, 	r_containers_on_pallet.container_type
			, 	r_containers_on_pallet.client_id
			, 	'ManualPickShared'
			, 	l_hme_key
			, 	sysdate
			, 	l_container_weight
			, 	l_last_box
			);

			commit;
		end loop; -- Container_loop

		-- When pallet task was not found the whole orutine can be skipped
		if	r_mvt.key is not null
		then
			-- Start splitting containers when seperate containers = N
			if	nvl(l_seperate_cont,'N') = 'N'
			then
				-- Wait to enure that the move task daemon has commited his work. wait max 120 seconds.
				<<move_task_complete_loop>>
				while   l_timer < 120 
				loop
					select	count(*)
					into	l_chk
					from    dcsdba.move_task mvt
					where   mvt.pallet_id                   = p_pallet_i
					and     mvt.status                      = 'Complete'
					and     mvt.site_id                     = p_site_i
					;
					if      l_chk > 0
					then
						cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
										    , 'Checking if move task daemon is ready processing pallet id '
										    ||p_pallet_i
										    ||' try number '
										    ||to_char(l_timer + 1)
										    ||'. Wait one second and try again.'
										    );
						dbms_lock.sleep(1); --To be sure new tasks have been created.
						l_timer := l_timer + 1;
					else
						exit move_task_complete_loop;
					end if;
				end loop; --move_task_complete_loop

				-- Start looping all containers agin now to split them away from pallet
				<<container_split_loop>>
				for	r_containers_on_pallet in c_containers_on_pallet
				loop
					-- Check if container is a multi order container. When yes container must be skipped.
					if	r_containers_on_pallet.multi_order_container > 1 
					then
						cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
										    , 'Checking if container is multi order. ' 
										    ||r_containers_on_pallet.container_id 
										    ||' Is multi order so container is skipped'
										    );
						continue container_split_loop;
					end if;
					-- Start creating new marshal task
					l_mvt_key           := cnl_sys.cnl_as_pck.get_move_task_key;
					l_new_consol_link   := cnl_sys.cnl_as_pck.get_consol_link;

					cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
									    , 'Creating new marshal task for container '
									    ||r_containers_on_pallet.container_id 
									    ||' with key '
									    ||l_mvt_key
									    ||' and consol link '
									    ||l_new_consol_link
									    ||'. And also update inventory, consol tasks, order container and serial numbers.'
									    );
					insert 
					into 	dcsdba.move_task
					( 	key
					, 	first_key
					, 	consol_link
					, 	pallet_id
					, 	pallet_config
					, 	description
					, 	final_loc_id
					, 	work_group
					, 	consignment
					, 	logging_level
					, 	stage_route_id
					, 	old_qty_to_move, status, print_label_id, qty_to_move, shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number
					, 	label_exceptioned,shipment_group,deconsolidate,kit_plan_id,plan_sequence,to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode
					, 	consol_run_num,labor_assignment,list_to_pallet_id,list_to_container_id,labor_grid_sequence,trolley_slot_id,processing,move_whole,user_def_match_blank_1
					, 	user_def_match_blank_2,user_def_match_blank_3,user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,user_def_match_blank_7
					, 	user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,user_def_match_chk_4,last_held_user
					, 	last_released_user,last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,last_held_date,last_released_date
					, 	spec_code,full_pallet_cluster,shipping_unit,task_type,task_id,line_id,client_id,sku_id,config_id,tag_id,old_tag_id,customer_id,origin_id
					, 	condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,old_to_loc_id,owner_id,sequence,list_id,dstamp,start_dstamp,finish_dstamp
					, 	original_dstamp,priority,face_type,face_key,work_zone,bol_id,reason_code,container_id,to_container_id,to_pallet_id,to_pallet_config
					, 	to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,to_pallet_weight,pallet_grouped,pallet_volume,pallet_height,pallet_depth,pallet_width
					, 	pallet_weight,user_id,station_id,session_type,summary_record,repack,kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id
					, 	trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond,increment_time,estimated_time,uploaded_labor,print_label,old_status,repack_qc_done
					, 	old_task_id,catch_weight,moved_lock_status,pick_realloc_flag
					)
					values
					( 	l_mvt_key -- New generated key
					, 	l_mvt_key -- new generated key
					, 	l_new_consol_link -- new generated consol link
					, 	r_containers_on_pallet.container_id
					, 	r_containers_on_pallet.container_type
					, 	'Single container pallet'
					, 	r_containers_on_pallet.final_loc_id
					, 	r_containers_on_pallet.work_group
					, 	r_containers_on_pallet.consignment
					, 	3 --It will use level 5 by default if not specified
					, 	r_containers_on_pallet.stage_route_id
					, 	r_mvt.old_qty_to_move,r_mvt.status,r_mvt.print_label_id,r_mvt.qty_to_move,r_mvt.shipment_number,r_mvt.stage_route_sequence,r_mvt.labelling,r_mvt.pf_consol_link,r_mvt.inv_key,r_mvt.first_pick,r_mvt.serial_number
					, 	r_mvt.label_exceptioned,r_mvt.shipment_group,r_mvt.deconsolidate,r_mvt.kit_plan_id,r_mvt.plan_sequence,r_mvt.to_container_config,r_mvt.container_config,r_mvt.ce_rotation_id,r_mvt.ce_avail_status,r_mvt.rdt_user_mode
					, 	r_mvt.consol_run_num,r_mvt.labor_assignment,r_mvt.list_to_pallet_id,r_mvt.list_to_container_id,r_mvt.labor_grid_sequence,r_mvt.trolley_slot_id,r_mvt.processing,r_mvt.move_whole,r_mvt.user_def_match_blank_1
					, 	r_mvt.user_def_match_blank_2,r_mvt.user_def_match_blank_3,r_mvt.user_def_match_blank_4,r_mvt.user_def_match_blank_5,r_mvt.user_def_match_blank_6,r_mvt.user_def_match_blank_7
					, 	r_mvt.user_def_match_blank_8,r_mvt.user_def_match_chk_1,r_mvt.user_def_match_chk_2,r_mvt.user_def_match_chk_3,r_mvt.user_def_match_chk_4,r_mvt.last_held_user
					, 	r_mvt.last_released_user,r_mvt.last_held_workstation,r_mvt.last_released_workstation,r_mvt.last_held_reason_code,r_mvt.last_released_reason_code,r_mvt.last_held_date,r_mvt.last_released_date
					, 	r_mvt.spec_code,r_mvt.full_pallet_cluster,r_mvt.shipping_unit,r_mvt.task_type,r_mvt.task_id,r_mvt.line_id,r_mvt.client_id,r_mvt.sku_id,r_mvt.config_id,r_mvt.tag_id,r_mvt.old_tag_id,r_mvt.customer_id,r_mvt.origin_id
					, 	r_mvt.condition_id,r_mvt.site_id,r_mvt.from_loc_id,r_mvt.old_from_loc_id,r_mvt.to_loc_id,r_mvt.old_to_loc_id,r_mvt.owner_id,r_mvt.sequence,r_mvt.list_id,r_mvt.dstamp,r_mvt.start_dstamp,r_mvt.finish_dstamp
					, 	r_mvt.original_dstamp,r_mvt.priority,r_mvt.face_type,r_mvt.face_key,r_mvt.work_zone,r_mvt.bol_id,r_mvt.reason_code,r_mvt.container_id,r_mvt.to_container_id,r_mvt.to_pallet_id,r_mvt.to_pallet_config
					, 	r_mvt.to_pallet_volume,r_mvt.to_pallet_height,r_mvt.to_pallet_depth,r_mvt.to_pallet_width,r_mvt.to_pallet_weight,r_mvt.pallet_grouped,r_mvt.pallet_volume,r_mvt.pallet_height,r_mvt.pallet_depth,r_mvt.pallet_width
					, 	r_mvt.pallet_weight,r_mvt.user_id,r_mvt.station_id,r_mvt.session_type,r_mvt.summary_record,r_mvt.repack,r_mvt.kit_sku_id,r_mvt.kit_line_id,r_mvt.kit_ratio,r_mvt.kit_link,r_mvt.status_link,r_mvt.due_type,r_mvt.due_task_id,r_mvt.due_line_id
					, 	r_mvt.trailer_position,r_mvt.consolidated_task,r_mvt.disallow_tag_swap,r_mvt.ce_under_bond,r_mvt.increment_time,r_mvt.estimated_time,r_mvt.uploaded_labor,r_mvt.print_label,r_mvt.old_status,r_mvt.repack_qc_done
					, 	r_mvt.old_task_id,r_mvt.catch_weight,r_mvt.moved_lock_status,r_mvt.pick_realloc_flag
					);

					-- update order container
					update  dcsdba.order_container
					set     pallet_id     = r_containers_on_pallet.container_id
					,       config_id     = null
					,       pallet_weight = null
					,       pallet_volume = null
					,       pallet_height = null
					,       pallet_width  = null  
					,       pallet_depth  = null
					where   pallet_id     = p_pallet_i
					and     container_id  = r_containers_on_pallet.container_id
					;

					-- update consol tasks 
					-- Will fail when consol is still at status Complete
					update  dcsdba.move_task
					set     pallet_id       = r_containers_on_pallet.container_id
					,       consol_link     = l_new_consol_link
					,       pallet_config   = r_containers_on_pallet.container_type
					,       list_id         = null
					,       trolley_slot_id = null
					where   pallet_id     = p_pallet_i
					and     container_id  = r_containers_on_pallet.container_id
					and     site_id       = p_site_i
					and     status        = 'Consol'
					;

					-- update inventory
					-- Will fail when consol is still at status Complete
					update  dcsdba.inventory
					set     pallet_id     = r_containers_on_pallet.container_id
					,       pallet_config = r_containers_on_pallet.container_type
					where   pallet_id     = p_pallet_i
					and     container_id  = r_containers_on_pallet.container_id
					and     site_id       = p_site_i
					;

					-- Update serial number
					update  dcsdba.serial_number
					set	pallet_id     = r_containers_on_pallet.container_id
					where	pallet_id     = p_pallet_i
					and     container_id  = r_containers_on_pallet.container_id
					and     site_id       = p_site_i
					and	client_id     = r_containers_on_pallet.client_id;

					commit;

					-- Check if container already has VAS actovity QC assigned to it
					select	count(*)
					into 	l_qc
					from	cnl_sys.cnl_container_vas_activity v
					where	(	v.container_id = r_containers_on_pallet.container_id 
						or 	v.container_id is null
						)
					and	v.client_id 	= r_containers_on_pallet.client_id
					and	v.order_id 	= r_containers_on_pallet.order_id
					and	upper(v.activity_name) = 'QC'
					;		   

					if	l_qc = 0
					then
						-- All containers default get the VAS activity QC assigned
						cnl_sys.cnl_as_pck.add_vas_activity( p_container_id_i           => r_containers_on_pallet.container_id
										   , p_client_id_i              => r_containers_on_pallet.client_id
										   , p_order_id_i               => r_containers_on_pallet.order_id
										   , p_activity_name_i          => 'QC'
										   , p_activity_sequence_i      => 0
										   , p_activity_instruction_i   => 'Default check the content for any container that enters any VAS station'
										   );

						-- Container requires QC settings to exist for the order.
						select	count(*)
						into	l_qc
						from	cnl_sys.cnl_wms_qc_order q
						where	q.order_id 	= r_containers_on_pallet.order_id
						and	q.client_id 	= r_containers_on_pallet.client_id
						and	q.site_id 	= p_site_i
						;
						if	l_qc = 0
						then
							insert 
							into 	cnl_sys.cnl_wms_qc_order
							(	order_id
							,	client_id
							,	site_id
							,	qc_req_yn
							,	qc_batch_yn
							,	qc_qty_def_yn
							,	qc_sku_select_yn
							,	qc_qty_upd_yn
							)
							values
							(	r_containers_on_pallet.order_id
							, 	r_containers_on_pallet.client_id
							,	p_site_i
							,	'Y'
							,	'N'
							, 	'Y'
							,	'Y'
							,	'Y'
							);
						end if;
					end if;

					-- Fetch any other VAS activity for this container.
					cnl_sys.cnl_as_pck.fetch_client_vas_activity( p_client_id_i => r_containers_on_pallet.client_id
										    , p_container_i => r_containers_on_pallet.container_id
										    , p_order_id_i  => r_containers_on_pallet.order_id
										    );
				end loop; -- container_split_loop

				-- Check if original pallet task can be deleted.
				-- Not needed when seperate containers = Y
				select  count(*)
				into	l_del_pal
				from    dcsdba.move_task
				where   pallet_id   = p_pallet_i
				and     site_id     = p_site_i
				and     status      = 'Consol'
				;
				if	l_del_pal = 0
				then
					delete  dcsdba.move_task
					where   pallet_id 	= p_pallet_i
					and     task_id   	= 'PALLET'
					and     key 		= r_mvt.key
					;
					commit;
				end if;
			end if; -- seperate containers
		end if; -- Marshal task found Y/N	

		commit;
	exception
		when others
		then
			cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_outbound_pck.manual_pick_finished'
							    , substr('Exception handling: SQLERRM = ' 
							    ||sqlerrm 
							    ||' and SQLCODE = ' 
							    ||sqlcode 
							    ||'Pallet id ' || p_pallet_i,1,4000)
							    || dbms_utility.format_error_backtrace);
			commit;
	end manual_pick_finished;
/***************************************************************************************************************
* split pick task before cubing. If trakcing levels exist that don't require cubing the task is split into shippable and not shippable units.
***************************************************************************************************************/
    procedure wms_split_pick_to_cube ( p_mt_key_i in number, --Original move task key
                                       p_mt_qty_i in number, --Qty for new task to create.
                                       p_mt_key_o out number) 
    is
    l_new_key number;
    l_new_print_label number;
    begin
        p_mt_key_o        := cnl_sys.cnl_as_pck.get_move_task_key;
        l_new_print_label := cnl_sys.cnl_as_pck.get_pick_label_id;
        --
        insert into dcsdba.move_task( key 
                                    , qty_to_move
                                    , old_qty_to_move
                                    , status
                                    , print_label_id      
				    , logging_level
                                    , shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,kit_plan_id
                                    , plan_sequence,to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,list_to_pallet_id
                                    , list_to_container_id,labor_grid_sequence,trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,user_def_match_blank_3
                                    , user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,user_def_match_blank_7,user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2
                                    , user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,last_held_workstation,last_released_workstation,last_held_reason_code
                                    , last_released_reason_code,last_held_date,last_released_date,spec_code,full_pallet_cluster,shipping_unit,first_key,task_type,task_id,line_id,client_id,sku_id
                                    , config_id,description,tag_id,old_tag_id,customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,old_to_loc_id,final_loc_id,owner_id
                                    , sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,bol_id,reason_code
                                    , container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,to_pallet_weight
                                    , pallet_grouped,pallet_config,pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,user_id,station_id,session_type,summary_record,repack
                                    , kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond
                                    , increment_time,estimated_time,uploaded_labor,print_label,old_status,repack_qc_done,old_task_id,catch_weight,moved_lock_status,pick_realloc_flag,stage_route_id
                                    )
                              select  p_mt_key_o  -- Get unique key
                                    , p_mt_qty_i  -- new qty to move    
                                    , p_mt_qty_i  -- new old qty to move
                                    , 'Released' -- status, Will be updated when cubing results are returned
                                    , l_new_print_label -- A new generated print_label_id, 
				    , 3 -- logging_level
                                    , shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,kit_plan_id
                                    , plan_sequence,to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,list_to_pallet_id
                                    , list_to_container_id,labor_grid_sequence,trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,user_def_match_blank_3
                                    , user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,user_def_match_blank_7,user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2
                                    , user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,last_held_workstation,last_released_workstation,last_held_reason_code
                                    , last_released_reason_code,last_held_date,last_released_date,spec_code,full_pallet_cluster,shipping_unit,first_key,task_type,task_id,line_id,client_id,sku_id
                                    , config_id,description,tag_id,old_tag_id,customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,old_to_loc_id,final_loc_id,owner_id
                                    , sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,bol_id,reason_code
                                    , container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,to_pallet_weight
                                    , pallet_grouped,pallet_config,pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,user_id,station_id,session_type,summary_record,repack
                                    , kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond
                                    , increment_time,estimated_time,uploaded_labor,print_label,old_status,repack_qc_done,old_task_id,catch_weight,moved_lock_status,pick_realloc_flag,stage_route_id
                              from    dcsdba.move_task
                              where   key = p_mt_key_i
        ;
        --
        update  dcsdba.move_task 
        set     qty_to_move = qty_to_move - p_mt_qty_i 
        ,       old_qty_to_move = old_qty_to_move - p_mt_qty_i 
        where   key = p_mt_key_i;
        --
    exception
        when others
        then
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.wms_split_pick_to_cube',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end wms_split_pick_to_cube;

/***************************************************************************************************************
* Autostore Insert serial numbers
***************************************************************************************************************/
    procedure insert_serial ( p_serial_number_i     in  varchar2
                            , p_client_id_i         in  varchar2
                            , p_sku_id_i            in  varchar2
                            , p_order_id_i          in  varchar2
                            , p_line_id_i           in  varchar2
                            , p_tag_id_i            in  varchar2
                            , p_pick_key_i          in  number
                            , p_site_id_i           in  varchar2
                            , p_picked_dstamp_i     in  varchar2
                            , p_pallet_id_i         in  varchar2
                            , p_container_id_i      in  varchar2
                            , p_ok_yn_o             out varchar2
                            )
    is
	l_exists	integer;
	l_sn_trans_key	integer;
	l_reused	varchar2(1) := 'N';
    begin
	-- When serial is found the serial is returned and must be reused e.g. updated.
	select	count(*)
	into	l_exists
	from	dcsdba.serial_number s
	where	s.serial_number 	= p_serial_number_i
	and	s.client_id		= p_client_id_i
	and	s.sku_id		= p_sku_id_i
	and	s.status 		= 'S'
	and	s.order_id		!= p_order_id_i
	and	s.order_id 		is not null
	;
	if	l_exists > 0
	then
		l_reused := 'Y';

		update	dcsdba.serial_number s
		set	s.order_id 		= p_order_id_i
		,	s.line_id		= p_line_id_i
		,	s.tag_id		= p_tag_id_i
		,	s.pick_key		= p_pick_key_i
		,	s.old_pick_key 		= null
		,	s.manifest_key		= null
		,	s.old_manifest_key 	= null
		,	s.status 		= 'I'
		,	s.picked_dstamp 	= p_picked_dstamp_i
		,	s.shipped_dstamp	= null
		,	s.uploaded		= 'N'
		,	s.uploaded_filename	= null
		,	s.uploaded_dstamp	= null
		,	s.repacked		= 'N'
		,	s.created		= 'N'
		,	s.screen_mode		= 'P'
		,	s.pallet_id		= upper(p_pallet_id_i)
		,	s.container_id		= upper(p_container_id_i)
		,	s.old_pallet_id		= null
		,	s.old_container_id	= null
		,	s.reused		= 'Y'
		where	s.serial_number		= p_serial_number_i
		and	s.client_id		= p_client_id_i
		and	s.sku_id		= p_sku_id_i
		and	s.status		= 'S'
		and	s.order_id		is not null
		;
	else
		insert into dcsdba.serial_number( serial_number
						, client_id
						, sku_id
						, order_id
						, line_id
						, tag_id
						, pick_key
						, status
						, site_id
						, picked_dstamp
						, uploaded
						, repacked
						, created
						, screen_mode
						, pallet_id
						, container_id)
					 values(  upper(p_serial_number_i)
						, p_client_id_i
						, p_sku_id_i
						, p_order_id_i
						, p_line_id_i
						, p_tag_id_i
						, p_pick_key_i
						, 'I'
						, p_site_id_i
						, p_picked_dstamp_i
						, 'N'
						, 'N'
						, 'Y'
						, 'P'
						, upper(p_pallet_id_i)
						, upper(p_container_id_i)
						);
	end if;

	-- Create serial transaction
	l_sn_trans_key := dcsdba.sn_transaction_pk_seq.nextval;

	insert into dcsdba.sn_transaction( key
					 , code
					 , dstamp
					 , user_id
					 , serial_number
					 , client_id
					 , sku_id
					 , order_id
					 , line_id
					 , tag_id
					 , status
					 , site_id
					 , picked_dstamp
					 , uploaded
					 , repacked
					 , screen_mode
					 , station_id
					 , pallet_id
					 , container_id
					 , reused
					 )
	values( l_sn_trans_key --key
	      , 'Pick'--code
	      , sysdate--dstamp
	      , 'AUTOSTORE'--user_id
	      , p_serial_number_i--serial_number
	      , p_client_id_i--client_id
	      , p_sku_id_i--sku_id
	      , p_order_id_i--order_id
	      , p_line_id_i--line_id
	      , p_tag_id_i--tag_id
	      , 'I'--status
	      , p_site_id_i--site_id
	      , p_picked_dstamp_i--picked_dstamp
	      , 'N'--uploaded
	      , 'N'--repacked
	      , 'P'--screen_mode
	      , 'AUTOSTORE'--station_id
	      , upper(p_pallet_id_i)--pallet_id
	      , upper(p_container_id_i)--container_id
	      , l_reused --reused
	      )
	;
        --
	p_ok_yn_o := 'Y';
    exception
        when others
        then
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.insert_serial',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            p_ok_yn_o := 'N';
            cnl_sys.cnl_as_pck.log_container_suspect( p_container_id_i  => p_container_id_i
                                                    , p_client_id_i     => p_client_id_i
                                                    , p_order_id_i      => p_order_id_i
                                                    , p_description_i   => 'Insert of Serial number ' 
                                                                        || p_serial_number_i 
									|| ' for SKU '
									|| p_sku_id_i
                                                                        || ' and order id '
                                                                        || p_order_id_i
                                                                        || ' and client id '
                                                                        || p_client_id_i
                                                                        || ' failed'
                                                    );
            cnl_sys.cnl_as_pck.add_vas_activity( p_container_id_i           => p_container_id_i
                                               , p_client_id_i              => p_client_id_i
                                               , p_order_id_i               => p_order_id_i
                                               , p_activity_name_i          => 'SERIAL-CHECK'
                                               , p_activity_sequence_i      => 0
                                               , p_activity_instruction_i   => 'Something went wrong during serial number validation at the Autostore pick port. Check all serial numbers and comapare with WMS data'
                                               );


    end insert_serial;                        
/***************************************************************************************************************
* Autostore Pick completion 
***************************************************************************************************************/
    procedure pick_confirmation( p_hme_tbl_key_i    in number
                               , p_hme_key_i        in number
                               , p_error_yn_o       in out varchar2
                               , p_error_code_o     in out varchar2
                               , p_error_text_o     in out varchar2
                               , p_container_id_o   in out varchar2
                               )
    is
        --
        cursor  c_otp( b_key    number)
        is
            select  p.user_id
            ,       p.workstation_id
            ,       p.order_id
            ,       p.owner_id
            from    rhenus_synq.host_order_tu_pick@as_synq.rhenus.de p
            where   p.order_tu_pick_key = b_key
        ;
        --
        cursor  c_hlu( b_key    number)
        is
            select  u.order_line_number
            ,       u.product_id
            ,       u.quantity
            ,       u.tu_type
            ,       u.tu_id
            ,       u.lu_key
            ,       u.suspect
            from    rhenus_synq.host_load_unit@as_synq.rhenus.de u
            where   u.order_tu_pick_key = b_key
        ;
        --
        cursor  c_apt( b_odl    number)
        is
            select  w.qty_to_move
            ,       w.client_id
            ,       w.task_id
            ,       w.cubing_req
            ,       w.category
            ,       w.cubed_container_type
            ,       w.cubed_container_id
            ,       w.creation_date
            ,       w.cubed_date
            ,       w.ord_master_host_message_key
            ,       w.cub_result_host_message_key
            ,       w.wms_mt_key
            ,       w.line_id
            from    cnl_sys.cnl_as_pick_task w
            where   w.wms_mt_key        = b_odl
        ;
        --
        cursor  c_atu( b_container_id   varchar2
                     , b_client_id      varchar2
                     , b_order_id       varchar2
                     )
        is
            select  wms_container_type
            from    cnl_as_tu
            where   wms_container_id    = b_container_id
            and     wms_client_id       = b_client_id
            and     wms_order_id        = b_order_id
            and     rownum              = 1
        ;
        --
        cursor  c_atr( b_key    number
                     , b_type   varchar2
                     )
        is
            select  attribute_value_key
            ,       class_type
            ,       upper(attribute_name) attribute_name
            ,       upper(attribute_value) attribute_value
            ,       attribute_role
            from    rhenus_synq.host_attribute@as_synq.rhenus.de 
            where   lu_key = b_key
            and     attribute_name = b_type
        ;
        --
        cursor c_pallet_type( b_pallet_type varchar2
                            , b_client_id   varchar2
                            )
        is
                select  count(*)
                from    dcsdba.pallet_config
                where   config_id = b_pallet_type
                and     (client_id = b_client_id or client_id is null)
        ;
        --
        r_otp       c_otp%rowtype;
        r_apt       c_apt%rowtype;
        r_atu       varchar2(30);
        r_atr       c_atr%rowtype;
        r_pallet_type   number;
        --
        l_new_key   number;
        l_con_type  varchar2(30);
        l_serial    varchar2(100);
        l_tag       varchar2(100);
        l_batch     varchar2(100);
        l_suspect   varchar2(1);
        l_descr     varchar2(4000);
        l_container varchar2(50);
        l_pick_type varchar2(30);
	l_qc	    number;
	l_site	    varchar2(30);

        --
    begin
        --Get host_order_tu_pick
        open    c_otp( p_hme_tbl_key_i);
        fetch   c_otp into r_otp;
        close   c_otp;

        -- Get host_load_unit(s) Always from a single container
	cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.pick_confirmation','start looping Host_load_units with table key' || p_hme_tbl_key_i);

        l_suspect := 'N';
        for     r_hlu in c_hlu( p_hme_tbl_key_i)
        loop
                l_container      := r_hlu.tu_id;
                -- Set box type to an in WMS existing box type.
                if      r_hlu.tu_type = 'NO_TRANSPORT_UNIT'
                then
                        l_pick_type := 'NOTU';
                else
                        open    c_pallet_type(nvl(r_hlu.tu_type,'NOTU'), r_otp.owner_id);
                        fetch   c_pallet_type into r_pallet_type;
                        close   c_pallet_type;
                        if      r_pallet_type = 0
                        then
                                l_pick_type := 'NOTU';
                        else
                                l_pick_type := r_hlu.tu_type;
                        end if;
                end if;

                -- Set suspect container
                if      r_hlu.suspect = 1
                then
                        l_suspect := 'Y';
                        l_descr   := 'Weight deviation during picking from Autostore for SKU id '|| r_hlu.product_id;  
                end if;

                -- check if pick task can be found. cnl_as_pick_task
                open    c_apt( r_hlu.order_line_number);
                fetch   c_apt into    r_apt;
                if      c_apt%notfound
                then
                        p_error_yn_o    := 'Y';
                        p_error_code_o  := 9000;
                        p_error_text_o  := 'Pick task with line number ' 
                                        || r_hlu.order_line_number
                                        || ' does not exist and can not be completed in Rhenus WMS';
                        l_suspect := 'Y';
                        l_descr   := 'Inventory was picked which did not had a task in WMS. SKU id '|| r_hlu.product_id;  
                else                                        
                        -- Check if Rhenus WMS pick task must be split.
                        if      r_apt.qty_to_move != r_hlu.quantity
                        then    -- Pick task must be split in WMS.
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.pick_confirmation','Pick task must be split into smaller QTY due to pick exception. Order id = ' || r_apt.task_id ||' line = '||r_apt.line_id);

                                wms_split_pick_to_cube ( r_hlu.order_line_number
                                                       , r_hlu.quantity
                                                       , l_new_key
                                                       );
                                --
                                insert into cnl_sys.cnl_as_pick_task( wms_mt_key
                                                                    , client_id
                                                                    , task_id
                                                                    , qty_to_move
                                                                    , cubing_req
                                                                    , category
                                                                    , cnl_if_status
                                                                    , cubed_container_type
                                                                    , cubed_container_id
                                                                    , picked_container_type
                                                                    , picked_container_id
                                                                    , creation_date
                                                                    , cubed_date
                                                                    , picked_date
                                                                    , ord_master_host_message_key
                                                                    , cub_result_host_message_key
                                                                    , picked_host_message_key
                                                                    , split_pick_key
                                                                    , user_id
                                                                    , station_id
                                                                    , line_id
                                                                    )
                                                              values( l_new_key
                                                                    , r_apt.client_id
                                                                    , r_apt.task_id
                                                                    , r_hlu.quantity
                                                                    , r_apt.cubing_req
                                                                    , r_apt.category
                                                                    , 'Picked'
                                                                    , r_apt.cubed_container_type
                                                                    , r_apt.cubed_container_id
                                                                    , l_pick_type
                                                                    , r_hlu.tu_id
                                                                    , r_apt.creation_date
                                                                    , r_apt.cubed_date
                                                                    , sysdate
                                                                    , r_apt.ord_master_host_message_key
                                                                    , r_apt.cub_result_host_message_key
                                                                    , p_hme_key_i
                                                                    , r_apt.wms_mt_key
                                                                    , r_otp.user_id
                                                                    , r_otp.workstation_id
                                                                    , r_apt.line_id
                                                                    );
                                --
                                update  cnl_sys.cnl_as_pick_task p
                                set     p.qty_to_move = p.qty_to_move - r_hlu.quantity
                                where   wms_mt_key    = r_hlu.order_line_number;

                                -- save picked attributes. A load_unit always contains one tag and can only contain one tag.
                                open    c_atr( r_hlu.lu_key, 'TAG');
                                fetch   c_atr into r_atr;
                                if      c_atr%notfound
                                then
                                        l_tag := 'ERROR';
                                else
                                        l_tag := r_atr.attribute_value;
                                end if;
                                close   c_atr;
                                --
                                for r_atr in c_atr( r_hlu.lu_key, 'SERIAL')
                                loop
                                        insert into cnl_sys.cnl_as_picked_attributes( attribute_type, client_id, order_id, wms_line_id, wms_mt_key, sku_id, tag_id, serial_number, batch_id, dstamp, tu_id)
                                            values('SERIAL',r_apt.client_id, r_apt.task_id, r_apt.line_id, l_new_key, r_hlu.product_id, l_tag, r_atr.attribute_value, null, sysdate, r_hlu.tu_id);
                                end loop c_atr;
                        else    
                                -- Get possible cnl_as_tu.
                                open    c_atu( r_hlu.tu_id
                                             , r_apt.client_id
                                             , r_apt.task_id
                                             );
                                fetch   c_atu
                                into    r_atu;
                                if      c_atu%notfound
                                then
                                        l_con_type := l_pick_type;
                                else
                                        l_con_type := r_atu;
                                end if;
                                close   c_atu;
                                --
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.pick_confirmation','update cnl_as_pick with cnl_if_status = Picked. in container ' ||r_hlu.tu_id ||' for line '||r_hlu.order_line_number);
                                update  cnl_sys.cnl_as_pick_task p
                                set     p.picked_container_id       = r_hlu.tu_id
                                ,       p.picked_container_type     = l_con_type--r_hlu.tu_type
                                ,       p.picked_host_message_key   = p_hme_key_i
                                ,       p.picked_date               = sysdate
                                ,       p.cnl_if_status             = 'Picked'
                                ,       p.user_id                   = r_otp.user_id
                                ,       p.station_id                = r_otp.workstation_id
                                where   p.wms_mt_key                = r_hlu.order_line_number
                                and     p.category                  = 'AUTOSTORE';

                                -- save picked attributes
                                open    c_atr( r_hlu.lu_key, 'TAG');
                                fetch   c_atr into r_atr;
                                if      c_atr%notfound
                                then
                                        l_tag := 'ERROR';
                                else
                                        l_tag := r_atr.attribute_value;
                                end if;
                                close   c_atr;
                                --
                                for r_atr in c_atr( r_hlu.lu_key, 'SERIAL')
                                loop
                                        insert into cnl_sys.cnl_as_picked_attributes( attribute_type, client_id, order_id, wms_line_id, wms_mt_key, sku_id, tag_id, serial_number, batch_id, dstamp, tu_id)
                                            values('SERIAL',r_apt.client_id, r_apt.task_id, r_apt.line_id, r_hlu.order_line_number, r_hlu.product_id, l_tag, r_atr.attribute_value, null, sysdate, r_hlu.tu_id);
                                end loop c_atr;
                        end if; -- qty mismatch
                end if; --c_apt%notfound
                close   c_apt;
                commit; -- Required due to possible insert in WMS.
                p_container_id_o := r_hlu.tu_id;
		-- Add standard VAS activities if VAS station is reached.
		cnl_sys.cnl_as_pck.fetch_client_vas_activity( p_client_id_i => r_apt.client_id
							    , p_container_i => l_container
                                                            , p_order_id_i  => r_otp.order_id
                                                            );
		cnl_sys.cnl_as_pck.add_vas_activity( p_container_id_i           => l_container
	                                           , p_client_id_i              => r_apt.client_id
						   , p_order_id_i               => r_otp.order_id
					           , p_activity_name_i          => 'QC'
				                   , p_activity_sequence_i      => 0
			                           , p_activity_instruction_i   => 'Default check the content for any container that enters any VAS station'
		                                   );
		-- Container requires QC settings to exist for the order.
		select  o.from_site_id
		into	l_site
		from	dcsdba.order_header o
		where	o.client_id = r_apt.client_id
		and	o.order_id  = r_otp.order_id
		;
		select	count(*)
		into	l_qc
		from	cnl_sys.cnl_wms_qc_order q
		where	q.order_id 	= r_otp.order_id
		and	q.client_id 	= r_apt.client_id
		and	q.site_id       = l_site
		;
		if	l_qc = 0
		then
			insert into cnl_sys.cnl_wms_qc_order(	order_id
							    ,	client_id
							    ,	site_id
							    ,	qc_req_yn
							    ,	qc_batch_yn
							    ,	qc_qty_def_yn
							    ,	qc_sku_select_yn
							    ,	qc_qty_upd_yn
							    )
			values(	r_otp.order_id
			      , r_apt.client_id
			      ,	l_site
			      ,	'Y'
			      ,	'N'
			      , 'Y'
			      ,	'Y'
			      ,	'Y'
			      );
		end if;


        end loop c_hlu;               
        if      l_suspect = 'Y'
        then
                cnl_sys.cnl_as_pck.log_container_suspect( p_container_id_i  => l_container
                                                        , p_client_id_i     => r_otp.owner_id
                                                        , p_order_id_i      => r_otp.order_id
                                                        , p_description_i   => l_descr
                                                        );
                commit;
        end if;
    exception
        when others
        then
            p_error_yn_o        := 'Y';
            p_error_code_o      := sqlcode;
            p_error_text_o      := sqlerrm;  
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.pick_confirmation',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end pick_confirmation;
    --
/***************************************************************************************************************
* complete pick tasks WMS
***************************************************************************************************************/
    procedure complete_pick_tasks( p_container_id_i in varchar2)
    is
        -- Fetch all pick tasks from saved pick tasks in CNL SYS
        cursor  c_tsk( b_container  varchar2)
        is
            select  upper(p.user_id)    user_id
            ,       upper(p.station_id) station_id
            ,       p.wms_mt_key
            ,       upper(p.task_id)    task_id
            ,       upper(p.client_id)  client_id
            ,       upper(p.picked_container_id) picked_container_id
            ,       upper(p.picked_container_type) picked_container_type
            from    cnl_sys.cnl_as_pick_task p
            where   upper(p.picked_container_id) = upper(b_container)
        ;
        --
        cursor  c_mvt( b_key    number)
        is
            select  upper(m.to_loc_id) to_loc_id
            ,       upper(m.site_id)    site_id
            ,       upper(m.final_loc_id) final_loc_id
            ,       upper(m.stage_route_id) stage_route_id
            ,       m.stage_route_sequence
            from    dcsdba.move_task m
            where   m.key   = b_key
        ;
        --
        cursor  c_csl( b_pallet_id  varchar2
                     , b_client_id  varchar2
                     )
        is
            select  m.consol_link
            ,       m.key
            from    dcsdba.move_task m
            where   m.task_id   = 'PALLET'
	    and	    m.pallet_id in (select m2.pallet_id from dcsdba.move_task m2 where m2.container_id = upper(b_pallet_id))	
	    and	    rownum = 1
--            and     m.pallet_id = upper(b_pallet_id)
--            and     m.client_id = upper(b_client_id)
        ;

        cursor  c_csll( b_pallet_id  varchar2
                     , b_client_id  varchar2
                     )
        is
            select  m.consol_link
            ,       m.key
            from    dcsdba.move_task m
            where   m.task_id   = 'PALLET'
            and     m.pallet_id in (select m2.pallet_id from dcsdba.move_task m2 where m2.container_id = upper(b_pallet_id)/* and m.client_id = upper(b_client_id)*/)
	    and     rownum = 1

        ;
        --
        cursor  c_ord( b_order_id   varchar2
                     , b_client_id  varchar2
                     )
        is
            select  o.carrier_id
            ,       o.service_level
            ,       o.consignment
            ,       o.work_group
            from    dcsdba.order_header o
            where   o.order_id  = upper(b_order_id)
            and     o.client_id = upper(b_client_id)
        ;
        --
        cursor  c_ocr( b_container_id   varchar2
                     , b_client_id      varchar2
                     , b_order_id       varchar2
                     )
        is
            select  o.*
            from    dcsdba.order_container o
            where   o.container_id  = b_container_id
            and     o.order_id      = b_order_id
            and     o.client_id     = b_client_id
        ;
        --
        cursor  c_ser( b_container  varchar2)
        is
            select  serial_number
            ,       client_id
            ,       sku_id
            ,       order_id
            ,       wms_line_id
            ,       tag_id
            ,       wms_mt_key
            ,       dstamp
            ,       tu_id
            from    cnl_as_picked_attributes
            where   tu_id = b_container
            and     attribute_type = 'SERIAL'
        ;
        --
        cursor c_sn (b_sn varchar2) is
            select      distinct regexp_substr(b_sn,'[^,]+', 1, level) as serial from dual
            connect by  regexp_substr(b_sn, '[^,]+', 1, level) is not null;
        --
	cursor c_ord_cont( b_container_id varchar2
			 , b_order_id varchar2
			 , b_client_id varchar2
			 )
	is
		select	*
		from	dcsdba.order_container 
		where	container_id = b_container_id
		union
		select 	*
		from	dcsdba.order_container
		where	order_id = b_order_id
	;

        r_mvt               c_mvt%rowtype;
        r_csl               c_csl%rowtype;
        r_ord               c_ord%rowtype;
        r_ocr               c_ocr%rowtype;
        --
        l_marshal_key       number;
        l_consol_link       number;
        l_status            integer;
        l_result            integer;
        l_user              varchar2(50);
        l_station           varchar2(50);
        l_to_loc            varchar2(50);
        l_final_loc         varchar2(50);
        l_serial_ok         varchar2(1);
	l_ord_cont	    dcsdba.order_container%rowtype;
        --
    begin
	cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Start processing Completed pick tasks in WMS picked by Autostore inside container ' ||p_container_id_i);

	-- Set WMS debug level to defalut 3. If no value level will be 5.
	dcsdba.libmqsdebug.setsessionid(USERENV('SESSIONID'),'sql','AUTOSTORE');
	dcsdba.libmqsdebug.setdebuglevel(3);

        -- Loop true each saved pick task
        for     r_tsk in c_tsk( p_container_id_i)
        loop
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Fetched the following task from cnl_as_pick_task: user'
							||r_tsk.user_id
							|| ', station id '
							||r_tsk.station_id
							|| ', wms_mt_key '
							||r_tsk.wms_mt_key
							|| ', task_id '
							||r_tsk.task_id
							|| ', client id '
							||r_tsk.client_id
							|| ', picked container id '
							||r_tsk.picked_container_id
							|| ', picked container type '
							||r_tsk.picked_container_type
							||'.'
							);

		-- check user and station are valid
                l_user      := cnl_sys.cnl_as_pck.check_user_id(r_tsk.user_id);
                l_station   := cnl_sys.cnl_as_pck.check_station_id(r_tsk.station_id);

                -- Fetch pick task in WMS.
                open        c_mvt( r_tsk.wms_mt_key);
                fetch       c_mvt
                into        r_mvt;
                if          c_mvt%notfound
                then
			    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Could not find pick task in WMS with key '||r_tsk.wms_mt_key||' inside container '||p_container_id_i);	
                            update      cnl_sys.cnl_as_pick_task
                            set         cnl_if_status       = 'ErrorKeyNotFound'
                            where       wms_mt_key          = r_tsk.wms_mt_key;
                            close       c_mvt;
                            continue;
		end if;
                close       c_mvt;

                -- Fetch task locations
                l_to_loc    := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(r_mvt.site_id) || '_DROP-LOCATION_LOCATION');
                l_final_loc := r_mvt.final_loc_id;

                -- get order header
                open        c_ord( r_tsk.task_id
                                 , r_tsk.client_id
                                 );
                fetch       c_ord
                into        r_ord;
                close       c_ord;

                -- check for order container record
                open        c_ocr( r_tsk.picked_container_id
                                 , r_tsk.client_id
                                 , r_tsk.task_id
                                 );
                fetch       c_ocr
                into        r_ocr;
                if          c_ocr%notfound
                then
			cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Container '||r_tsk.picked_container_id||' from client '||r_tsk.client_id||' and order '||r_tsk.task_id||' does not yet exist. Adding container '||r_tsk.picked_container_id||' to order container');	

			dcsdba.libcontainerpicking.addordercontainers( status          => l_status
                                                                     , palletid        => r_tsk.picked_container_id
								     , containerid     => r_tsk.picked_container_id
								     , clientid        => r_tsk.client_id
								     , taskid          => r_tsk.task_id
								     , containertype   => r_tsk.picked_container_type
								     , carrierid       => r_ord.carrier_id
								     , servicelevel    => r_ord.service_level
								     );

                end if; --c_ocr not found
                close       c_ocr;

		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Start fetching consol id using this query "select m.consol_link, m.key from dcsdba.move_task m where m.task_id = ''PALLET'' and m.pallet_id = upper('''||r_tsk.picked_container_id||''') and m.client_id = upper('''||r_tsk.client_id||'''');

                -- get consol link from pallet task
               if 	l_consol_link is null
	       then
			open        c_csl( r_tsk.picked_container_id
					 , r_tsk.client_id
					 );
			fetch       c_csl
			into        r_csl;
			if          c_csl%found
			then
				    l_consol_link := r_csl.consol_link;
				    l_marshal_key := r_csl.key;
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Found consol id '||to_char(l_consol_link)||' and pallet task key '||to_char(l_marshal_key)||' from pallet '||r_tsk.picked_container_id||'.');

			else
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','No task was found using the query "select m.consol_link, m.key from dcsdba.move_task m where m.task_id = ''PALLET'' and m.pallet_id = upper('''||r_tsk.picked_container_id||''') and m.client_id = upper('''||r_tsk.client_id||'''');

				for i in c_csll( r_tsk.picked_container_id
					 , r_tsk.client_id
					 )
				loop
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','First attempts of fetching consol link and march task key failed but using a different query did return a value. consol id '||to_char(i.consol_link)||' move task key '||to_char(i.key)||'.');	
				end loop;

				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Create marshal task for container '||r_tsk.picked_container_id);	
				    -- Create pallet task
				    dcsdba.libcontainerpicking.createpalletheader( marshalkey       => l_marshal_key
										 , consollink       => l_consol_link
										 , palletid         => r_tsk.picked_container_id
										 , containerid      => r_tsk.picked_container_id
										 , tolocid          => 'CONTAINER'
										 , finallocid       => r_mvt.final_loc_id
										 , workgroup        => r_ord.work_group
										 , clientid         => r_tsk.client_id
										 , taskid           => r_tsk.task_id
										 , consignment      => r_ord.consignment
										 , siteid           => r_mvt.site_id
										 , userid           => l_user
										 , stationid        => l_station
										 , stagerouteid     => r_mvt.stage_route_id
										 , stagerouteseq    => r_mvt.stage_route_sequence
										 , fromlocid        => null
										 );
			end if; -- c_csl not found
			close c_csl;
		end if;
                -- Update move task in WMS
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Update pick task in WMS with to_container_id '||r_tsk.picked_container_id||' etc. etc.');
                update  dcsdba.move_task m
                set     m.to_container_id         = r_tsk.picked_container_id
                ,       m.to_pallet_id            = r_tsk.picked_container_id
                ,       m.to_container_config     = r_tsk.picked_container_type
                ,       m.to_pallet_config        = r_tsk.picked_container_type
                ,       m.to_loc_id               = l_to_loc
                ,       m.consol_link             = l_consol_link
                where   m.key                     = r_tsk.wms_mt_key;                                          

                -- Update saved task CNL SYS
                update  cnl_sys.cnl_as_pick_task
                set     consol_link     = l_consol_link
                where   wms_mt_key      = r_tsk.wms_mt_key;

        end loop c_tsk;

        -- Commit all changes and updates.
        commit;

        -- Complete marshal task
	cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','Complete move_task marsal task in WMS for key '||l_marshal_key);
        dcsdba.libmovetask.completemovetask( result      => l_result
                                           , keyfield    => l_marshal_key
                                           , tolocid     => l_to_loc
                                           , finallocid  => l_final_loc
                                           , sessiontype => 'M'
                                           , stationid   => l_station
                                           , userid      => l_user
                                           );
        --
        -- insert all serials for container
        for r_ser in c_ser( p_container_id_i)
        loop
            for r_sn in c_sn(r_ser.serial_number) 
            loop
		cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.Complete_pick_tasks','insert serials for container '||r_ser.tu_id);
                insert_serial(  p_serial_number_i   => r_sn.serial
                             ,  p_client_id_i       => r_ser.client_id
                             ,  p_sku_id_i          => r_ser.sku_id
                             ,  p_order_id_i        => r_ser.order_id
                             ,  p_line_id_i         => r_ser.wms_line_id
                             ,  p_tag_id_i          => r_ser.tag_id
                             ,  p_pick_key_i        => r_ser.wms_mt_key
                             ,  p_site_id_i         => r_mvt.site_id
                             ,  p_picked_dstamp_i   => r_ser.dstamp
                             ,  p_pallet_id_i       => r_ser.tu_id
                             ,  p_container_id_i    => r_ser.tu_id
                             ,  p_ok_yn_o           => l_serial_ok
                             );
            end loop;
        end loop c_ser;
        commit;
    exception
        when others
        then
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.complete_pick_tasks',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end complete_pick_tasks;
    --
/***************************************************************************************************************
* complete pick tasks WMS
***************************************************************************************************************/
    procedure order_status_change( p_order_id_i     in varchar2
                                 , p_client_id_i    in varchar2
                                 , p_status_i       in varchar2
                                 )
    is
        --
        cursor  c_pen( b_order_id   varchar2
                     , b_client_id  varchar2
                     , b_to_loc     varchar2
                     , b_from_loc   varchar2
                     )
        is
            select  m.key
            ,       m.qty_to_move
            from    dcsdba.move_task m
            where   m.task_id       = b_order_id
            and     m.client_id     = b_client_id
            and     m.to_loc_id     = b_to_loc
            and     m.from_loc_id   = b_from_loc
            and     m.task_type     = 'O'
	    and	    m.status 	    != 'Complete'
        ;
        --
        cursor  c_sit( b_order_id   varchar2
                     , b_client_id  varchar2
                     )
        is
            select  o.from_site_id
            from    dcsdba.order_header o
            where   o.order_id  = b_order_id
            and     o.client_id = b_client_id
        ;
        --
        cursor c_all_cnt( b_order_id    varchar2
                        , b_client_id   varchar2
                        )
        is
            select  distinct container_id
            from    dcsdba.order_container
            where   order_id    = b_order_id
            and     client_id   = b_client_id
        ;
        --
        cursor c_can( b_order_id   varchar2
                    , b_client_id  varchar2
                    )
        is
            select  key
            ,       qty_to_move
            from    dcsdba.move_task
            where   task_id   = b_order_id
            and     client_id = b_client_id
            and     task_type = 'O'
            and     (   status in ('Cubing','Autostore') or
                        (   status in ('Hold','Released') and
                            to_container_id like '#C%'));

        r_pen           number;
        r_sit           varchar2(30);
        --
        l_to_loc        varchar2(30);
        l_from_loc      varchar2(30);
        l_result        integer;
        l_new_status    varchar2(30);
        l_short         varchar2(1) := 'N';
        l_ok_yn_o       varchar2(1);
        --

    begin
	-- Set WMS debug level to defalut 3. If no value level will be 5.
	dcsdba.libmqsdebug.setsessionid(USERENV('SESSIONID'),'sql','AUTOSTORE');
	dcsdba.libmqsdebug.setdebuglevel(3);


        if  upper(p_status_i) = 'CANCELLED'
        then
            -- delete order from cnl_orders table.
            delete  cnl_sys.cnl_as_orders
            where   client_id   = p_client_id_i
            and     order_id    = p_order_id_i
            and     cnl_if_status != 'Completed';

	    update 	cnl_as_manual_order_started
	    set		processed = 'N'
	    ,		cnl_if_status = 'OrderStarted'
	    where 	client_id   = p_client_id_i
            and     	order_id    = p_order_id_i
		;	    
            -- delete any pending and cubed tasks.
            dcsdba.libsession.InitialiseSession( UserID       => 'AUTOSTORE'
                                               , GroupID      => null
                                               , StationID    => 'AUTOSTORE'
                                               , WksGroupID   => null
                                               );
            for r_can in c_can( p_order_id_i
                              , p_client_id_i
                              )
            loop
                    Update  dcsdba.move_task
                    set     status = 'Hold'
                    where   key = r_can.key;
                    commit;
                    dcsdba.libdeallocate.deallocatestock( result            => l_result
                                                        , taskkey           => r_can.key
                                                        , qtydeallocate     => r_can.qty_to_move
                                                        );
                    delete  cnl_as_pick_task
                    where   task_id = p_order_id_i;
            end loop;
        end if;
        --
        if  upper(p_status_i) = 'COMPLETED'
        then
            -- Fetch site id
            open    c_sit( p_order_id_i
                         , p_client_id_i
                         );
            fetch   c_sit
            into    r_sit;
            close   c_sit;
            -- set session settings
            dcsdba.libsession.InitialiseSession( UserID       => 'AUTOSTORE'
                                               , GroupID      => null
                                               , StationID    => 'AUTOSTORE'
                                               , WksGroupID   => null
                                               );
            -- Fetch autostore drop location
            l_to_loc    := cnl_sys.cnl_as_pck.get_drop_location(r_sit);
            l_from_loc  := cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_'
                                                                                || upper(r_sit)
                                                                                || '_STORAGE-LOCATION_LOCATION');
            -- Check for open pick tasks in WMS and deallocate when from autostore.
            for     r_pen in c_pen( p_order_id_i
                                  , p_client_id_i
                                  , l_to_loc
                                  , l_from_loc
                                  )
            loop            
                    l_short := 'Y'; -- Tasks from Autostore have been found while Synq says it is completed.
                    -- Set status to Hold because DCSDBA procedures do not work for status Cubing and Autostore.
                    cnl_sys.cnl_as_pck.insert_itl( p_mt_key_i      => r_pen.key
                                                 , p_to_status_i   => 'Hold'
                                                 , p_ok_yn_o       => l_ok_yn_o
                                                 );
                    Update  dcsdba.move_task
                    set     status = 'Hold'
                    where   key = r_pen.key;
                    commit;
                    -- Deallocate move task
                    dcsdba.libdeallocate.deallocatestock( result            => l_result
                                                        , taskkey           => r_pen.key
                                                        , qtydeallocate     => r_pen.qty_to_move
                                                        );
                    update  cnl_sys.cnl_as_pick_task
                    set     cnl_if_status   = 'DeAllocated'
                    where   wms_mt_key      = r_pen.key;

            end loop c_pen;

            -- Update cnl_sys_tables
            update  cnl_as_orders
            set     cnl_if_status   = 'Completed'
            ,       update_date     = sysdate
            where   client_id       = p_client_id_i
            and     order_id        = p_order_id_i;

	    update 	cnl_as_manual_order_started	    
	    set		cnl_if_status = 'Completed'
	    where 	client_id   = p_client_id_i
            and     	order_id    = p_order_id_i
		;	    
            -- Set all container from this order as suspect to ensure they are send to error chute
            if      l_short = 'Y' 
            then
                    for     r_all_cnt in c_all_cnt(p_order_id_i, p_client_id_i)
                    loop
                            cnl_sys.cnl_as_pck.log_container_suspect( p_container_id_i  => r_all_cnt.container_id
                                                                    , p_client_id_i     => p_client_id_i
                                                                    , p_order_id_i      => p_order_id_i
                                                                    , p_description_i   => 'Order has shortages. First perform corrective actions'
                                                                    );
                    end loop;
            end if;
            --
        end if;
    exception
        when others
        then
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.order_status_change',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end order_status_change;
    --
/***************************************************************************************************************
* Process deallocations in WMS
***************************************************************************************************************/
    procedure task_deallocation( p_site_id_i        in varchar2
                               , p_owner_id_i       in varchar2
                               , p_client_id_i      in varchar2
                               , p_tag_id_i         in varchar2
                               , p_sku_id_i         in varchar2
                               , p_from_loc_id      in varchar2
                               , p_to_loc_id_i      in varchar2
                               , p_final_loc_id     in varchar2
                               , p_qty_i            in number
                               , p_task_id_i        in varchar2
                               , p_line_id_i        in number
                               , p_work_group_i     in varchar2
                               , p_consignment_i    in varchar2
                               )
    is
        cursor  c_aor( b_site       varchar2
                     , b_client     varchar2
                     , b_order      varchar2
                     )
        is
            select  a.*
            from    cnl_sys.cnl_as_orders a
            where   a.from_site     = b_site
            and     a.client_id     = b_client
            and     a.order_id      = b_order
        ;
        --
        cursor  c_tsk( b_client varchar2
                     , b_task   varchar2
                     )
        is
            select  a.wms_mt_key
            from    cnl_as_pick_task a
            where   a.client_id   = b_client
            and     a.task_id     = b_task
            and     a.wms_mt_key  not in (  select  m.key
                                            from    dcsdba.move_task m
                                            where   m.key = a.wms_mt_key
                                         )
        ;
        --
        r_aor       c_aor%rowtype;
        --
    begin
        open        c_aor( p_site_id_i
                         , p_client_id_i
                         , p_task_id_i
                         );
        fetch       c_aor
        into        r_aor;
        if          c_aor%notfound
        then
                    null; --ignore deallocation
        else
                    for     r_tsk in c_tsk( p_client_id_i
                                          , p_task_id_i
                                          )
                    loop
                            update  cnl_sys.cnl_as_pick_task
                            set     cnl_if_status   = 'Deallocated'
                            where   wms_mt_key      = r_tsk.wms_mt_key;
			    commit;
                    end loop c_tsk;
        end if;
        close c_aor;
    exception
        when others
        then
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_outbound_pck.task_deallocation',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end task_deallocation;
/***************************************************************************************************************
* Fix pick task with trolley slot but no to_container_id
***************************************************************************************************************/
    procedure fix_task( p_site_id_i     in varchar2
                      )
    is
        cursor c_fix
        is
            select  m.key
            ,       m.to_container_config
            ,       m.container_config
            from    dcsdba.move_task m
            where   trolley_slot_id is not null
            and     to_container_id is null
            and     container_id is null
            and     list_id is null
            and     site_id = p_site_id_i
            and     status in ('Released','Hold')
        ;
        --
        l_container_id  varchar2(50);
        l_config        varchar2(50);
    begin
        for     r_fix in c_fix 
        loop
                l_container_id := '#C1' ||cnl_sys.cnl_as_container_id_seq1.nextval;
                if      r_fix.to_container_config is null
                then    
                        l_config := 'NOTU';
                else
                        l_config := r_fix.to_container_config;
                end if;
                update  dcsdba.move_task
                set     trolley_slot_id = null
                ,       to_container_id = l_container_id
                ,       list_id = l_container_id
                ,       to_container_config = l_config
                where   key = r_fix.key;
        end loop;
        commit;
    end fix_task;
/***************************************************************************************************************
* Run cluster configuration
***************************************************************************************************************/
     procedure run_cluster_config( p_cluster_group_id_i varchar2
                                 , p_site_id_i          varchar2
                                 , p_client_id_i        varchar2 default null
                                 )
    is
            cursor c_cluster_config( b_cluster_group_id     varchar2
                                   , b_site_id              varchar2
                                   , b_client_id            varchar2
                                   )
            is
                    select      distinct cc.cluster_config_id
                    from        dcsdba.cluster_group_config cc
                    where       cc.cluster_group_id = b_cluster_group_id
                    and         nvl(cc.site_id,'NVL') = nvl(b_site_id,'NVL')
                    and         nvl(cc.client_id,'NVL') = nvl(b_client_id,'NVL')
            ;
            --
            l_taskcount     integer := 0;
            l_tskcount      integer := 0;
            l_listcount     integer := 0;
            l_lstcount      integer := 0;
            --
    begin
	-- Set WMS debug level to default 3. If no value level will be 5.
	dcsdba.libmqsdebug.setsessionid(USERENV('SESSIONID'),'sql','AUTOSTORE');
	dcsdba.libmqsdebug.setdebuglevel(3);
	dcsdba.libsession.initialisesession( userid	=> 'AUTOSTORE'
					   , groupid	=> null
					   , stationid 	=> 'AUTOSTORE'
					   , wksgroupid => null
					);

            fix_task(p_site_id_i);
            for r in c_cluster_config( p_cluster_group_id_i, p_site_id_i, p_client_id_i)
            loop
                    dcsdba.libmovetaskclustering.runconfigclustering( p_taskcount       => l_taskcount
                                                                    , p_listcount       => l_listcount
                                                                    , p_clusterconfigid => r.cluster_config_id
                                                                    , p_siteid          => p_site_id_i
                                                                    , p_clientid        => p_client_id_i
                                                                    , p_maxpickers      => null
                                                                    , p_clustertaskkey  => null
                                                                    );
                    l_tskcount := l_tskcount + l_taskcount;
                    l_lstcount := l_lstcount + l_listcount;
            end loop;
    end run_cluster_config;
/***************************************************************************************************************
* Enveloped need to be marshalled to the outstage location automatically.
***************************************************************************************************************/
    procedure marshal_envelope( p_from_loc_id_i     in varchar2
                              , p_to_loc_id_i       in varchar2
                              , p_container_type_i  in varchar2
                              , p_site_id_i         in varchar2
                              )
    is
        cursor  c_envelope
        is
            select  m.key
            ,       m.pallet_id
            from    dcsdba.move_task m
            where   m.site_id     = p_site_id_i
            and     m.from_loc_id = p_from_loc_id_i
            and     m.to_loc_id   = p_to_loc_id_i
            and     m.task_id     = 'PALLET'
            and     m.pallet_id   like '#C%'
            and     (   select  count(*)
                        from    dcsdba.move_task m2
                        where   m2.status = 'Consol'
                        and     m2.pallet_config   = p_container_type_i
                        and     m2.from_loc_id     = m.from_loc_id
                        and     m2.to_loc_id       = m.to_loc_id
                        and     m2.site_id         = m.site_id
                        and     m2.consol_link     = m.consol_link) > 0
            for update of status
        ;
        --
    begin
    -- This will automatically marshal all envelopes to the sortation area of the coveyor. 
    -- Envelopes are not scanned by any scanner on the conveyor due to the fact they are in a crate.
        for     r in c_envelope
        loop
            update  dcsdba.move_task
            set     status  = 'Complete'
            ,       pallet_config = 'ENVELOPE'
            ,       container_config = 'ENVELOPE'
	    ,	    logging_level = 3
            where   key = r.key;
            commit;
        end loop;
    end marshal_envelope;
    --
    begin
    -- Initialization
    null;   
    --
end cnl_as_outbound_pck;