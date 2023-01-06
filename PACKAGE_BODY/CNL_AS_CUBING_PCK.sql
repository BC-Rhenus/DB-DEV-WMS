CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_CUBING_PCK" is
/********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 04-05-2018
**********************************************************************************
*
* Description: 
* Package to initiate and start process cubing tasks.
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
* function to get order header key
***************************************************************************************************************/                   
    function key_f(p_tbl number)
        return number
    is
        cursor c_ord
        is
            select rhenus_synq.host_order_header_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_orl
        is
            select rhenus_synq.host_order_line_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_att
        is
            select rhenus_synq.host_attribute_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        l_retval number;
    begin
        if      p_tbl = 1
        then
                open  c_ord;
                fetch c_ord into l_retval;
                close c_ord;
        elsif   p_tbl = 2
        then
                open  c_orl;
                fetch c_orl into l_retval;
                close c_orl;
        elsif   p_tbl = 3
        then
                open  c_att;
                fetch c_att into l_retval;
                close c_att;
        end if;
        return l_retval;
    end key_f;

/***************************************************************************************************************
* get QTY to cube
* If a tracking level is shippable it will not be taken into account during cubing. 
* In WMS the pick task will be split into 2 taks. One for shippable and one for remaining QTY.
***************************************************************************************************************/
    procedure get_qty_to_cube( p_sku_i              in  varchar2
                             , p_config_i           in  varchar2
                             , p_client_i           in  varchar2
                             , p_qty_i              in  number
                             , p_qty_left_o         out number
                             , p_nbr_shp_units_o    out number
                             , p_qty_in_shp_un_o    out number
                             , p_shippable_o        out varchar2
                             )
    is
        cursor c_cnf ( b_config     varchar2
                     , b_client     varchar2
                     )
        is
            select  shipping_unit_lev_1 sul_1
            ,       shipping_unit_lev_2 sul_2
            ,       shipping_unit_lev_3 sul_3
            ,       shipping_unit_lev_4 sul_4
            ,       shipping_unit_lev_5 sul_5
            ,       shipping_unit_lev_6 sul_6
            ,       shipping_unit_lev_7 sul_7
            ,       shipping_unit_lev_8 sul_8
            ,       1                           rat_1 -- each in first
            ,       nvl(ratio_1_to_2,0)         rat_2 -- each in second
            ,       nvl(ratio_2_to_3,0) * 
                    nvl(ratio_1_to_2,1)         rat_3 -- each in third
            ,       nvl(ratio_3_to_4,0) * 
                    nvl(ratio_2_to_3,1) * 
                    nvl(ratio_1_to_2,1)         rat_4 -- each in fourth
            ,       nvl(ratio_4_to_5,0) *
                    nvl(ratio_3_to_4,1) * 
                    nvl(ratio_2_to_3,1) * 
                    nvl(ratio_1_to_2,1)         rat_5 -- each in fifth
            ,       nvl(ratio_5_to_6,0) * 
                    nvl(ratio_4_to_5,1) *
                    nvl(ratio_3_to_4,1) * 
                    nvl(ratio_2_to_3,1) * 
                    nvl(ratio_1_to_2,1)         rat_6 -- each in sixth
            ,       nvl(ratio_6_to_7,0) *
                    nvl(ratio_5_to_6,1) *
                    nvl(ratio_4_to_5,1) *
                    nvl(ratio_3_to_4,1) * 
                    nvl(ratio_2_to_3,1) * 
                    nvl(ratio_1_to_2,1)         rat_7 -- each in seven
            ,       nvl(ratio_7_to_8,0) *
                    nvl(ratio_6_to_7,1) *
                    nvl(ratio_5_to_6,1) *
                    nvl(ratio_4_to_5,1) *
                    nvl(ratio_3_to_4,1) * 
                    nvl(ratio_2_to_3,1) * 
                    nvl(ratio_1_to_2,1)         rat_8 -- each in eight
            from    dcsdba.sku_config
            where   config_id = b_config
            and     client_id = b_client;
        --
        r_cnf       c_cnf%rowtype;
        l_su_qty    number;    --Shipping unit qty
        l_continue  varchar2(1);
        --
    begin
        open    c_cnf( p_config_i
                     , p_client_i
                     );
        fetch   c_cnf 
        into    r_cnf;
        if      c_cnf%notfound 
        then 
                -- QTY in move task must be cubed as is.
                close   c_cnf;
                p_qty_left_o            := p_qty_i;
                p_nbr_shp_units_o       := 0;
                p_qty_in_shp_un_o       := 0;
                p_shippable_o           := 'N';
                l_continue              := 'N';
        else
                close   c_cnf;
                l_continue              := 'Y';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_8,'N') = 'Y'
        and     p_qty_i >= r_cnf.rat_8
        then
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_8) *  r_cnf.rat_8);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_8);
                p_qty_in_shp_un_o   := r_cnf.rat_8;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_7,'N') = 'Y' 
        and     p_qty_i >= r_cnf.rat_7
        then
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_7) *  r_cnf.rat_7);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_7);
                p_qty_in_shp_un_o   := r_cnf.rat_7;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_6,'N') = 'Y' 
        and     p_qty_i >= r_cnf.rat_6
        then
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_6) *  r_cnf.rat_6);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_6);
                p_qty_in_shp_un_o   := r_cnf.rat_6;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_5,'N') = 'Y' 
        and     p_qty_i >= r_cnf.rat_5
        then
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_5) *  r_cnf.rat_5);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_5);
                p_qty_in_shp_un_o   := r_cnf.rat_5;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_4,'N') = 'Y'
        and     p_qty_i >= r_cnf.rat_4
        then
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_4) *  r_cnf.rat_4);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_4);
                p_qty_in_shp_un_o   := r_cnf.rat_4;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_3,'N') = 'Y' 
        and     p_qty_i >= r_cnf.rat_3
        then
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_3) *  r_cnf.rat_3);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_3);
                p_qty_in_shp_un_o   := r_cnf.rat_3;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_2,'N') = 'Y' 
        and     p_qty_i >= r_cnf.rat_2
        then    
                p_qty_left_o        := p_qty_i - (floor(p_qty_i / r_cnf.rat_2) *  r_cnf.rat_2);
                p_nbr_shp_units_o   := floor(p_qty_i / r_cnf.rat_2);
                p_qty_in_shp_un_o   := r_cnf.rat_2;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        and     nvl(r_cnf.sul_1,'N') = 'Y' 
        then    -- 1 each is already shippable.
                p_qty_left_o        := 0;
                p_nbr_shp_units_o   := p_qty_i;
                p_qty_in_shp_un_o   := 1;
                p_shippable_o       := 'Y';
                l_continue          := 'N';
        end if;
        --
        if      l_continue = 'Y'
        then
                p_qty_left_o        := p_qty_i;
                p_nbr_shp_units_o   := 0;
                p_qty_in_shp_un_o   := 0;
                p_shippable_o       := 'N';
        end if;
        --dbms_output.put_line('p_qty_left_o = '||p_qty_left_o);
        --dbms_output.put_line('p_nbr_shp_units_o = '||p_nbr_shp_units_o);
        --dbms_output.put_line('p_qty_in_shp_un_o = '||p_qty_in_shp_un_o);
        --dbms_output.put_line('p_shippable_o = '||p_shippable_o);
    exception
        when others
        then    
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_qty_to_cube',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end get_qty_to_cube;

/***************************************************************************************************************
* split pick task before cubing. If trakcing levels exist that don't require cubing the task is split into shippable and not shippable units.
***************************************************************************************************************/
    procedure wms_split_pick_to_cube ( p_mt_key_i   in number --Original move task key
                                     , p_mt_qty_i   in number --Qty for new task to create.
                                     , p_mt_key_o   out number
                                     ) 
    is
        --
        l_new_key           number;
        l_new_print_label   number;
        l_ok                varchar2(1);
        --
    begin
            p_mt_key_o        := cnl_sys.cnl_as_pck.get_move_task_key;
            l_new_print_label := cnl_sys.cnl_as_pck.get_pick_label_id;
            --
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.wms_split_pick_to_cube','Splitting pick tasks, Inserting new task. QTY for new task = ' || p_mt_qty_i || ' and must be taken of the QTY from original task');
            --
            begin
                    l_ok := 'Y';
                    insert into dcsdba.move_task(   key,
                                                    qty_to_move,
                                                    old_qty_to_move,
                                                    status,
                                                    print_label_id,
						    logging_level,
                                                    shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,
                                                    label_exceptioned,shipment_group,deconsolidate,kit_plan_id,plan_sequence,to_container_config,container_config,ce_rotation_id,ce_avail_status,
                                                    rdt_user_mode,consol_run_num,labor_assignment,list_to_pallet_id,list_to_container_id,labor_grid_sequence,trolley_slot_id,
                                                    processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,user_def_match_blank_3,user_def_match_blank_4,
                                                    user_def_match_blank_5,user_def_match_blank_6,user_def_match_blank_7,user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,
                                                    user_def_match_chk_4,last_held_user,last_released_user,last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,
                                                    last_held_date,last_released_date,spec_code,full_pallet_cluster,shipping_unit,first_key,task_type,task_id,line_id,client_id,sku_id,config_id,description,
                                                    tag_id,old_tag_id,customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,old_to_loc_id,final_loc_id,
                                                    owner_id,sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,   
                                                    bol_id,reason_code,container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,    
                                                    to_pallet_weight,pallet_grouped,pallet_config,pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,user_id,station_id,session_type,summary_record,  
                                                    repack,kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond,      
                                                    increment_time,estimated_time,uploaded_labor,print_label,old_status,repack_qc_done,old_task_id,catch_weight,moved_lock_status,pick_realloc_flag,stage_route_id
                                                 ) 
                                            select  p_mt_key_o,         -- Get unique key
                                                    p_mt_qty_i,         -- new qty to move    
                                                    p_mt_qty_i,         -- new old qty to move
                                                    'Cubing',           -- status, Will be updated when cubing results are returned
                                                    l_new_print_label,  -- A new generated print_label_id, 
						    3,			-- logging_level should be set to 3 by default.
                                                    shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,kit_plan_id,plan_sequence,
                                                    to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,list_to_pallet_id,list_to_container_id,labor_grid_sequence,
                                                    trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,user_def_match_blank_3,user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,
                                                    user_def_match_blank_7,user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,
                                                    last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,last_held_date,last_released_date,spec_code,full_pallet_cluster,shipping_unit,
                                                    first_key,task_type,task_id,line_id,client_id,sku_id,config_id,description,tag_id,old_tag_id,customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,
                                                    old_to_loc_id,final_loc_id,owner_id,sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,
                                                    bol_id,reason_code,container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,to_pallet_weight,
                                                    pallet_grouped,pallet_config,pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,user_id,station_id,session_type,summary_record,repack,kit_sku_id,kit_line_id,
                                                    kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond,increment_time,estimated_time,uploaded_labor,
                                                    print_label,old_status,repack_qc_done,old_task_id,catch_weight,moved_lock_status,pick_realloc_flag,stage_route_id        
                                            from    dcsdba.move_task
                                            where   key = p_mt_key_i
                    ;
                    commit;
                    --
            exception
                    when others
                    then
                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.wms_split_pick_to_cube',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
                            l_ok := 'N';
                            p_mt_key_o := p_mt_key_i;
            end;
            --
            if      l_ok = 'Y'
            then
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.wms_split_pick_to_cube','Updating original task. QTY for new task = ' || p_mt_qty_i || ' and must be taken of the QTY from original task');
                    --
                    update  dcsdba.move_task 
                    set     qty_to_move = qty_to_move - p_mt_qty_i 
                    ,       old_qty_to_move = qty_to_move - p_mt_qty_i
                    where   key = p_mt_key_i
                    ;
                    --
                    commit;
                    --
            end if;
    exception
            when others
            then    
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.wms_split_pick_to_cube',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end wms_split_pick_to_cube;

/***************************************************************************************************************
* Fetch cubing tasks from WMS
***************************************************************************************************************/ 
    procedure get_cubing_tasks( p_site_id_i         varchar2
                              , p_client_group_i    varchar2
                              )
    is
        --
        cursor  c_clt( b_client_group   varchar2)
        is
            select  clt.client_id
            from    dcsdba.client_group_clients clt
            where   clt.client_group = b_client_group
        ;

        --
        cursor  c_ord( b_site           varchar2
                     , b_client         varchar2
                     )
        is
            select  o.order_id
            from    dcsdba.order_header o
            where   o.from_site_id      = b_site
            and     o.client_id         = b_client
            and     o.tax_rate_5 = (    select  v.vas_codes 
                                        from    cnl_sys.cnl_as_vas_codes v
                                        where   v.vas_codes = o.tax_rate_5
                                   )
            and     o.status            not in ('Shipped','Ready to Load','Complete','Picked','Packed','Delivered')
            and     o.order_id          not in (    select  order_id 
                                                    from    cnl_sys.cnl_as_orders
                                                    where   order_id = o.order_id
                                                    and     cnl_if_status != 'Completed'
                                                )
        ;
        --
        cursor  c_tsk( b_site           varchar2
                     , b_client         varchar2
                     , b_order_id       varchar2
                     , b_from_loc_id    varchar2
                     , b_drop_loc_id    varchar2
                     )
        is
            select  m.site_id
            ,       m.client_id
            ,       m.task_id
            ,       m.key
            ,       m.from_loc_id
            ,       m.config_id
            ,       m.sku_id
            ,       m.qty_to_move
            ,       m.tag_id
            ,       m.line_id
            ,       m.stage_route_id
            from    dcsdba.move_task m
            where   m.site_id               = b_site
            and     m.client_id             = b_client
            and     m.task_id               = b_order_id
            and     m.task_type             = 'O'
            and     m.status                in ('Hold','Released')
            and     m.list_id               is null
            -- SKU of task is not ugly
            and     (   select nvl(s.ugly,'N') from dcsdba.sku s where s.sku_id = m.sku_id and s.client_id = m.client_id) = 'N'
            -- Only tasks older then 5 minutes. This to ensure allocation has finished!
            and     (   select  max(dstamp) from dcsdba.move_task where task_id = m.task_id) < (sysdate - interval '5' minute)
            -- Does not exist in cnl_as_pick_task
            and     (   select count(wmk.wms_mt_key) from cnl_sys.cnl_as_pick_task wmk where wmk.wms_mt_key = m.key) = 0
            -- Task from Autostore or task that must go to conveyor
            and     (   m.from_loc_id = b_from_loc_id or
                        (   m.stage_route_id is not null and ( select count(*) from dcsdba.location_zone_staging l where l.stage_route_id = m.stage_route_id and next_stage = b_drop_loc_id) > 0) or
                        (   m.stage_route_id is null and ( m.to_loc_id = b_drop_loc_id or m.final_loc_id = b_drop_loc_id)))
            -- No kits to build for entire order
            and     (   select count(*) from dcsdba.move_task t where t.task_id = m.task_id and t.client_id = m.client_id and t.site_id = m.site_id 
                        and (   (   select count(l.sku_id) from dcsdba.order_line l where l.order_id = t.task_id and l.client_id = m.client_id and l.sku_id = t.sku_id) = 0 and                 
                                (   select count(c.location_id) from dcsdba.location c where c.loc_type = 'Kitting' and c.site_id = m.site_id and c.location_id = t.to_loc_id) = 0))= 0             
            -- No pending relocates, replenishments or putaway tasks from same location as pick tasks for order only from storage locations.
            and     (   select count(*) from dcsdba.move_task k where k.task_id = m.task_id and k.client_id = m.client_id and k.site_id = m.site_id and k.task_type = 'O' and k.status != 'Consol' and k.from_loc_id not in (b_from_loc_id, b_drop_loc_id, (select nvl(s.out_stage,'NOSTAGE') from dcsdba.location s where s.location_id = b_drop_loc_id and s.site_id = k.site_id))
                        and (   k.from_loc_id = ( select distinct l.from_loc_id from dcsdba.move_task l where l.task_type in ('M','P','R') and l.site_id = m.site_id and l.client_id = m.client_id and l.from_loc_id = k.from_loc_id) or
                                k.from_loc_id = ( select  distinct l.location_id from dcsdba.location l where l.location_id = k.from_loc_id and loc_type in ('Receive Dock','Sampling','Trailer','Suspense','Repack','ShipDock')))) = 0
	    -- No pick tasks without from location. (Waiting for pick face to get a location assigned!!
	    and     (   select count(*) from dcsdba.move_task s where s.task_id = m.task_id and s.client_id = m.client_id and s.site_id = m.site_id and s.from_loc_id is null) = 0
	    -- no tasks from *BULK* locations for PARCEL shipments allowed (relocate could not be created yet)
	    and	    (   select count(*) from dcsdba.move_task m2 
			where  m2.from_loc_id = (select l2.location_id from dcsdba.location l2 where l2.location_id = m2.from_loc_id and l2.zone_1 like '%BULK%' and l2.site_id = m.site_id)
			and    m2.task_id = m.task_id
			and    m2.site_id = m.site_id
			and    m2.client_id = m.client_id
			and    ( 	m2.shipment_group = 'PARCEL' or 
					(	m2.shipment_group = 'PALLET' and 
						m2.qty_to_move < (select sum(i.qty_on_hand) from dcsdba.inventory i where i.client_id = m2.client_id and i.location_id = m2.from_loc_id and i.sku_id = m2.sku_id and (i.tag_id = m2.tag_id or m2.tag_id is null))
					)
				)
		    ) = 0
        ;        
        --
        cursor c_lin( b_client_id   varchar2
                    , b_order_id    varchar2
                    , b_sku_id      varchar2
                    , b_line_id     varchar2
                    )
        is
            select  nvl(l.user_def_chk_3,'N') disallow_cubing
            from    dcsdba.order_line l
            where   l.client_id     = b_client_id
            and     l.order_id      = b_order_id
            and     l.sku_id        = b_sku_id
            and     ( l.line_id       = b_line_id or 
                      b_line_id is null)
            and     rownum          = 1
        ;
        --
        cursor c_cnf( b_location_id     varchar2
                    , b_sku_id          varchar2
                    , b_client_id       varchar2
                    , b_site_id         varchar2
                    )
        is
            select  i.config_id
            from    dcsdba.inventory i
            where   (i.location_id      = b_location_id or b_location_id is null)
            and     i.sku_id            = b_sku_id
            and     i.client_id         = b_client_id
            and     i.site_id           = b_site_id
            and     rownum              = 1
        ;
        --
        cursor c_snf( b_sku_id          varchar2
                    , b_client_id       varchar2
                    )
        is
            select  i.config_id
            from    dcsdba.sku_sku_config i
            where   i.sku_id            = b_sku_id
            and     i.client_id         = b_client_id
            and     rownum              = 1
        ;
	--
        cursor c_aso( b_task    varchar2)
        is
            select  count(*)
            from    cnl_as_orders
            where   order_id = b_task
            and     cnl_if_status != 'Completed'
        ;
        --
        r_aso           number;
        r_cnf           varchar2(30);
        r_lin           varchar2(1);
        --
        l_new_key       number;
        l_qty_to_cube   number; -- Qty left over from task after removing all shippable units.
        l_ship_units    number; -- Number of ship units from the task.
        l_qty_ship_un   number; -- qty in one ship unit.
        l_continue      varchar2(1) := 'N';
        l_qty_to_move   number;
        l_location      varchar2(30);
        l_drop_loc      varchar2(30);
        l_config        varchar2(30);
        l_shippable     varchar2(1) := 'Y';
        l_original_task_qty number;
        l_start         varchar2(1) := 'Y';
        l_stop          varchar2(1) := 'N';
        l_ok_yn_o       varchar2(1);
        l_dis_cubing    varchar2(1);
        l_cubing_req    varchar2(1);
        --
    begin

        for     r_clt in c_clt( p_client_group_i) -- Client group holds all clienst for which we want to cube.
        loop    
                --get drop location 
                l_location := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_id_i || '_STORAGE-LOCATION_LOCATION');
                l_drop_loc := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_id_i || '_DROP-LOCATION_LOCATION');
                --
                for     r_ord in c_ord( p_site_id_i, r_clt.client_id)
                loop
                        -- Loop true all move tasks
                        for     r_tsk in c_tsk( p_site_id_i, r_clt.client_id, r_ord.order_id, l_location, l_drop_loc)
                        loop
                                -- Check if order already created in CNL_AS_ORDERS
                                open    c_aso( r_tsk.task_id); 
                                fetch   c_aso 
                                into    r_aso;
                                close   c_aso;
                                if      r_aso > 0
                                then
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Order ' || r_ord.order_id || ' was already created in CNL_AS_ORDERS');
                                        null;
                                else
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Order ' || r_ord.order_id || ' Created in CNL_AS_ORDERS');
					insert into cnl_as_orders( from_site
                                                                 , client_id
                                                                 , order_id
                                                                 , cnl_if_status
                                                                 , creation_date
                                                                 , update_date 
                                                                 )
                                                           values( p_site_id_i
                                                                 , r_clt.client_id
                                                                 , r_ord.order_id
                                                                 , 'PendingOrderMaster'
                                                                 , sysdate
                                                                 , sysdate
                                                                 );
                                end if;
                                --
                                if l_start = 'Y'
                                then
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Start fetching tasks');
                                        l_start := 'N';
                                        l_stop := 'Y';
                                end if;
                                --              
                                -- get a config if not in this task
                                if      r_tsk.config_id is null
                                then
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Move task did not contain config. Fetching config from WMS.');
                                        open    c_cnf( r_tsk.from_loc_id, r_tsk.sku_id, r_tsk.client_id, r_tsk.site_id);
                                        fetch   c_cnf
                                        into    r_cnf;
					if	c_cnf%notfound
					then
						close 	c_cnf;
						open    c_cnf( null, r_tsk.sku_id, r_tsk.client_id, r_tsk.site_id);
						fetch   c_cnf
						into    r_cnf;
						if	c_cnf%notfound 
						then
							close 	c_cnf;
							open 	c_snf(r_tsk.sku_id, r_tsk.client_id);
							fetch 	c_snf
							into 	r_cnf;
							close	c_snf;
							cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Config '||r_cnf||' fetched from sku sku config.');
							l_config := r_cnf;
						else
							close 	c_cnf;
							cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Config '||r_cnf||' fetched from random inventory record for SKU.');
							l_config := r_cnf;
						end if;
					else
						close 	c_cnf;
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Config '||r_cnf||' fetched from pick location.');
						l_config := r_cnf;
					end if;
                                else
                                        l_config := r_tsk.config_id;
                                end if;
                                --
                                -- Save pick task(s)
                                l_original_task_qty := r_tsk.qty_to_move;
                                l_shippable := 'Y';
                                l_cubing_req := 'Y';
                                while   l_shippable = 'Y'
                                loop
                                        -- Set values for manual pick tasks with disallow cubing flag.
                                        open    c_lin( r_tsk.client_id
                                                     , r_tsk.task_id
                                                     , r_tsk.sku_id
                                                     , r_tsk.line_id
                                                     );
                                        fetch   c_lin into r_lin;
                                        close   c_lin;
                                        if      nvl(r_lin,'N') = 'Y' --Cubing disallowed
					and	r_tsk.from_loc_id != l_location
                                        then
                                                l_dis_cubing   	:= 'Y';
                                                l_cubing_req    := 'N';
                                                l_shippable     := 'N';
                                                l_qty_to_cube   := r_tsk.qty_to_move;
                                                l_ship_units    := 1;
                                                l_qty_ship_un   := r_tsk.qty_to_move;
                                        else
                                                l_dis_cubing   := 'N';
                                        end if;

                                        -- Set values for Autostore picks or fetch details about shippable unit.
                                        if      l_shippable = 'Y'
                                        then
                                                if  r_tsk.from_loc_id  != l_location
                                                then
                                                        -- First check if pick task in WMS containes shippable units
                                                        get_qty_to_cube( p_sku_i            => r_tsk.sku_id
                                                                       , p_config_i         => l_config
                                                                       , p_client_i         => r_clt.client_id
                                                                       , p_qty_i            => l_original_task_qty
                                                                       , p_qty_left_o       => l_qty_to_cube -- Qty left after 1 shippable unit has been removed.
                                                                       , p_nbr_shp_units_o  => l_ship_units  -- 
                                                                       , p_qty_in_shp_un_o  => l_qty_ship_un
                                                                       , p_shippable_o      => l_shippable
                                                                       );
                                                        --
                                                else
                                                        l_qty_to_cube   := r_tsk.qty_to_move;
                                                        l_ship_units    := 1;
                                                        l_qty_ship_un   := r_tsk.qty_to_move;
                                                        l_shippable     := 'N';
                                                        l_cubing_req    := 'Y';
                                                end if;
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','QTY to cube = '||l_qty_to_cube||' l_ship_units_left = '||l_ship_units||' qty in ship unit = ' ||l_qty_ship_un);
                                        end if;
                                        --

                                        if      l_shippable     = 'Y'  -- Task contains shippable units
                                        then
                                                l_cubing_req    := 'N';
                                                cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','Task contains ' || 
										      l_ship_units ||
										      ' shippable units. ' ||
										      l_qty_ship_un ||
										      ' pieces per shippable unit.'
										      );
                                                -- Split all shippable units away from original task
                                                l_qty_to_move := l_qty_ship_un;
                                                while   l_ship_units > 1
                                                loop
                                                        cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_cubing_pck.get_cubing_tasks'
											    , 'Task must be split. More then one ship unit'
											    );

                                                        -- Loop true shippable units and split task until one is left
                                                        wms_split_pick_to_cube( p_mt_key_i => r_tsk.key     -- Original move task key of task that must be split.
                                                                              , p_mt_qty_i => l_qty_ship_un -- QTY in ship unit
                                                                              , p_mt_key_o => l_new_key     -- Key from new move task
                                                                              );    

                                                        -- Insert new task into cnl_as_pick tasks.
                                                        insert into cnl_as_pick_task( wms_mt_key
                                                                                    , client_id
                                                                                    , task_id
                                                                                    , qty_to_move
                                                                                    , cubing_req
                                                                                    , cnl_if_status
                                                                                    , creation_date
                                                                                    , line_id
                                                                                    , disallow_cubing
                                                                                    )
                                                                              values( l_new_key
                                                                                    , r_clt.client_id
                                                                                    , r_tsk.task_id
                                                                                    , l_qty_to_move
                                                                                    , l_cubing_req
                                                                                    , 'PendingShippable'
                                                                                    , sysdate
                                                                                    , r_tsk.line_id
                                                                                    , l_dis_cubing
                                                                                    );
                                                        l_ship_units := l_ship_units -1;
                                                end loop;
                                                --
                                                -- Now there is still 1 shippable unit left and it can be that it still mst be split or it 
						-- is the remaining QTY of the orginal task.
                                                if      l_qty_to_cube   = 0    -- complete task is shippable
                                                then    
                                                        cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_cubing_pck.get_cubing_tasks'
											    , 'Remainin QTY of task is also shippable no need to split anymore.'
											    );

                                                        l_new_key   := r_tsk.key;
                                                        l_shippable := 'N';
                                                        --
                                                        cnl_sys.cnl_as_pck.insert_itl( p_mt_key_i      => r_tsk.key
                                                                                     , p_to_status_i   => 'Cubing'
                                                                                     , p_ok_yn_o       => l_ok_yn_o
                                                                                     );
                                                        --
                                                        update  dcsdba.move_task 
                                                        set     status = 'Cubing'
                                                        where   key = r_tsk.key;
                                                else
                                                        cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_cubing_pck.get_cubing_tasks'
											    , 'Task must be split. Remaining qty could contain shippable units of another level'
											    );

                                                        wms_split_pick_to_cube( p_mt_key_i => r_tsk.key     -- Original move task key of task that must be split.
                                                                              , p_mt_qty_i => l_qty_ship_un -- QTY in ship unit
                                                                              , p_mt_key_o => l_new_key     -- Key from new move task
                                                                              );    
                                                end if;        
                                                --
                                                insert into cnl_as_pick_task( wms_mt_key
                                                                            , client_id
                                                                            , task_id
                                                                            , qty_to_move
                                                                            , cubing_req
                                                                            , cnl_if_status
                                                                            , creation_date
                                                                            , line_id
                                                                            , disallow_cubing
                                                                            )
                                                                      values( l_new_key
                                                                            , r_clt.client_id
                                                                            , r_tsk.task_id
                                                                            , l_qty_to_move
                                                                            , 'N'
                                                                            , 'PendingShippable'
                                                                            , sysdate
                                                                            , r_tsk.line_id
                                                                            , l_dis_cubing
                                                                            );
                                        else -- Remaining QTY is not a shippable unit.
                                                if      nvl(r_lin,'N') = 'N'
                                                then
                                                        l_cubing_req    := 'Y';
                                                end if;
                                                cnl_sys.cnl_as_pck.create_log_record( 'cnl_sys.cnl_as_cubing_pck.get_cubing_tasks'
										    , 'Task ' || 
										      r_tsk.key || 
										      ' does not contain shippable units.'
										    );
                                                --
                                                insert into cnl_as_pick_task( wms_mt_key
                                                                            , client_id
                                                                            , task_id
                                                                            , qty_to_move
                                                                            , cubing_req
                                                                            , cnl_if_status
                                                                            , creation_date
                                                                            , line_id
                                                                            , disallow_cubing
                                                                            )
                                                                      values( r_tsk.key
                                                                            , r_clt.client_id
                                                                            , r_tsk.task_id
                                                                            , l_qty_to_cube
                                                                            , l_cubing_req
                                                                            , 'PendingCubeTask'
                                                                            , sysdate
                                                                            , r_tsk.line_id
                                                                            , l_dis_cubing
                                                                            );
                                                --
                                                cnl_sys.cnl_as_pck.insert_itl( p_mt_key_i      => r_tsk.key
                                                                             , p_to_status_i   => 'Cubing'
                                                                             , p_ok_yn_o       => l_ok_yn_o
                                                                             );
                                                --
                                                update  dcsdba.move_task 
                                                set     status = 'Cubing'
                                                where   key = r_tsk.key;
                                                commit;
                                                --
                                        end if; -- if shippable= Y
                                        l_original_task_qty := l_qty_to_cube;
                                        l_qty_to_cube       := 0;
                                        l_ship_units        := 0;
                                end loop;--l_shippable = 'Y'
                        end loop c_tsk;
                end loop c_ord;
        end loop c_clt;
        commit;
        --
        if      l_stop = 'Y'
        then
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks','fetching tasks finished.');
                l_stop := 'N';
        end if;
        synq_order_master( p_site_id_i 
                         , p_client_group_i
                         );
        --
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.get_cubing_tasks',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end get_cubing_tasks;
/***************************************************************************************************************
* translate WMS priority to Synq Priority
* In Synq priority 1 to 9 exists. In WMS 1 to 9999 exists. In Synq 1 is highest in WMS 9999.
* Priority 1 must not be used. This is reserved by the release scanner on the conveyor.
***************************************************************************************************************/  
    function translate_priority( p_priority_i number)
        return number
    is
        l_wms_priority  number;
        l_retval        number;
    begin
        l_wms_priority := p_priority_i;
        case 
        -- Priorty 1 may not be used as it is reserved by Synq / release scanner of conveyor.
        when l_wms_priority >= 8000 then l_retval := 2;
        when l_wms_priority >= 7000 then l_retval := 3;
        when l_wms_priority >= 6000 then l_retval := 4;
        when l_wms_priority >= 5000 then l_retval := 5;
        when l_wms_priority >= 4000 then l_retval := 6;
        when l_wms_priority >= 3000 then l_retval := 7;
        when l_wms_priority >= 2000 then l_retval := 8;
        else l_retval := 9;
        end case;
        return l_retval;
    end translate_priority;
/***************************************************************************************************************
* send data to synq needed for cubing
***************************************************************************************************************/                   
    procedure synq_order_master( p_site_id_i        in varchar2
                               , p_client_group_i   in varchar2
                               )
    is
        --
        cursor  c_clt( b_client_group   varchar2)
        is
            select  clt.client_id
            from    dcsdba.client_group_clients clt
            where   clt.client_group = b_client_group
        ;
        --
        cursor c_aso( b_site    varchar2
                    , b_client  varchar2
                    )
        is
            select  order_id
            ,       client_id
            from    cnl_sys.cnl_as_orders
            where   cnl_if_status   = 'PendingOrderMaster'
            and     from_site       = b_site
            and     client_id       = b_client
        ;
        --
        cursor  c_asc( b_task   varchar2
                     , b_client varchar2
                     )
        is
            select  wms_mt_key
            ,       cubing_req
            from    cnl_sys.cnl_as_pick_task
            where   (cnl_if_status   = 'PendingCubeTask' or
                     cnl_if_status   = 'PendingShippable')
            and     task_id         = b_task
            and     client_id       = b_client
        ;
        --
        cursor c_ord( b_order_id    varchar2
                    , b_client_id   varchar2
                    )
        is
            select  oh.order_id
            ,       oh.client_id
            ,       oh.ship_by_date
            ,       oh.priority
            ,       oh.creation_date
            ,       oh.tax_rate_5
            from    dcsdba.order_header oh
            where   oh.client_id    = b_client_id
            and     oh.order_id     = b_order_id
        ;
        --
        cursor      c_tsk( b_key number)
        is
            select  mt.tag_id
            ,       mt.key
            ,       mt.config_id
            ,       mt.sku_id
            ,       mt.qty_to_move
            ,       mt.from_loc_id
            ,       mt.site_id
            ,       mt.line_id
            from    dcsdba.move_task mt
            where   mt.key = b_key
        ;
        --
        cursor c_chk( b_client      varchar2
                    , b_order       varchar2  
                    , b_location    varchar2
                    )
        is
            select  (   select  count(key) 
                        from    dcsdba.move_task 
                        where   task_type = 'O' 
                        and     client_id = b_client 
                        and     task_id = b_order
                        and     (   (   select  shipment_group 
                                        from    dcsdba.order_header 
                                        where   order_id = task_id
					and	client_id = b_client
                                    )   = 'PARCEL'  
                                    or 
                                    (   (   select  shipment_group 
                                            from    dcsdba.order_header 
                                            where   order_id = task_id
					    and		client_id = b_client
                                        )   = 'PALLET' 
                                        and from_loc_id = b_location
                                    )
                                )
                    )   nbr_wms_tasks
            ,       (   select  count(wms_mt_key) 
                        from    cnl_sys.cnl_as_pick_task 
                        where   client_id = b_client 
                        and     task_id = b_order
                     )       nbr_cub_tasks
            from    dual
        ;
        --
        r_ord       c_ord%rowtype;
        r_tsk       c_tsk%rowtype;
        r_chk       c_chk%rowtype;
        --
        l_ord_key   number;
        l_orl_key   number;
        l_att_key   number;
        l_hos_key   number;
        l_category  varchar2(10);
        l_ord_type  varchar2(10);
        l_cubing    number;
        l_location  varchar2(30);
        l_priority  number;
        --
    begin
        l_location := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_id_i || '_STORAGE-LOCATION_LOCATION');
        for     r_clt in c_clt( p_client_group_i) -- For each client in client group
        loop
                for     r_aso in c_aso( p_site_id_i
                                      , r_clt.client_id
                                      ) -- For each order in cnl_as_orders
                loop
                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master','Found order ' || r_aso.order_id || ' for client_id ' || r_clt.client_id);
                        -- Check if more tasks exist in WMS than in CNL_SYS
                        open    c_chk( r_aso.client_id
                                     , r_aso.order_id
                                     , l_location
                                     );
                        fetch   c_chk
                        into    r_chk;
                        close   c_chk;
                        if      r_chk.nbr_wms_tasks = 0 
                        then
                                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master','Check tasks results in no move tasks found in WMS for order ' || r_aso.order_id);
                                continue; -- Move to next order in loop
                        end if;

                        -- Fetch order details from WMS
                        open    c_ord( r_aso.order_id
                                     , r_clt.client_id
                                     );
                        fetch   c_ord
                        into    r_ord;
                        close   c_ord;

                        -- Manual, Autostore ,Mixed
                        l_ord_type := null;
                        for     r_asc in c_asc( r_aso.order_id -- for each task of the order
                                              , r_clt.client_id
                                              )                 
                        loop
				if      l_ord_type = 'MIXED'
				then 
					continue;
				else
					open    c_tsk( r_asc.wms_mt_key);
					fetch   c_tsk
					into    r_tsk;
					close   c_tsk;

					-- check if pick is from autostore
					if      r_tsk.from_loc_id = cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' 
															 || r_tsk.site_id
															 || '_STORAGE-LOCATION_LOCATION')
					then
						if      l_ord_type = 'MANUAL'
						then    
							l_ord_type := 'MIXED';
						else
							l_ord_type := 'AUTOSTORE';
						end if;--l_ord_type = 'MANUAL'
					else
						if      l_ord_type = 'AUTOSTORE'
						then
							l_ord_type := 'MIXED';
						else
							l_ord_type := 'MANUAL';
						end if;--l_ord_type = 'AUTOSTORE'
					end if;--Autostore pick
				end if; -- Order type
                        end loop;

                        -- Priority
                        l_priority := translate_priority(r_ord.priority);
                        if      l_priority is null
                        then
                                l_priority := 9;
                        end if;

                        -- Insert order header master
			begin -- Create internal routine to capture exceptions and continue loop when exception is handled.        
				l_ord_key := key_f(1);
				insert into rhenus_synq.host_order_header@as_synq.rhenus.de( order_key 
											   , owner_id
											   , order_id
											   , order_type
											   , dispatch_date
											   , order_date
											   , priority
											   , short_allocation_allowed
											   , short_releasing_allowed
											   , short_shipping_allowed
											   , auto_alloc_allowed--auto_allocation_allowed
											   , capability
											   , multiple_vas_allowed
											   ) 
										     values( l_ord_key 
											   , r_ord.client_id
											   , r_ord.order_id
											   , l_ord_type
											   , to_timestamp(to_char(nvl(r_ord.ship_by_date,sysdate),'DD-MON-YY')||' '||'06.00.00.000000000 AM')
											   , r_ord.creation_date
											   , l_priority
											   , 0
											   , 0
											   , 0
											   , 1
											   , to_char(r_ord.tax_rate_5) -- Automation route id
											   , 1
											   );
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master','Inserting order header in SynQ for client_id ' || r_clt.client_id || ', order_id ' || r_ord.order_id || ', Order key = ' || l_ord_key);
			exception
				when others
				then
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master',substr('Exception handling order '||r_ord.order_id||': SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
			end;
                        --
                        for     r_asc in c_asc( r_aso.order_id -- for each task of the order
                                              , r_clt.client_id
                                              )                 
                        loop
                                if      r_asc.cubing_req = 'Y'
                                then
                                        l_cubing := 0;
                                else
                                        l_cubing := 1;
                                end if;
                                --
                                open    c_tsk( r_asc.wms_mt_key);
                                fetch   c_tsk
                                into    r_tsk;
                                close   c_tsk;

                                -- check if pick is from autostore
                                if      r_tsk.from_loc_id = cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' 
                                                                                                                 || r_tsk.site_id
                                                                                                                 || '_STORAGE-LOCATION_LOCATION')
                                then
                                        l_category := 'AUTOSTORE';
                                else
                                        l_category := 'MANUAL';
                                end if;

                                --Save original move task key
                                begin
					insert into cnl_as_manual_lines( client_id
								       , order_id
								       , line_id
								       , sku_id
								       , mt_key
								       , mt_qty
								       , qty_picked
								       , finished
								       , dstamp
								       , category
								       )
								 values( r_ord.client_id
								       , r_ord.order_id
								       , r_tsk.line_id
								       , r_tsk.sku_id
								       , r_tsk.key
								       , r_tsk.qty_to_move
								       , 0
								       , 'N'
								       , sysdate
								       , l_category
								       );
					--
				exception
					when others 
					then
						null;
				end;						
                                -- Insert wms task as master order line
				begin
					l_orl_key := key_f(2);
					insert into rhenus_synq.host_order_line@as_synq.rhenus.de( order_key
												 , order_line_number
												 , product_id
												 , uom_tree
												 , min_uom
												 , max_uom
												 , quantity
												 , category
												 , expiration_window
												 , allocation_tolerance_window
												 , inventory_sorting
												 , relevant_date_for_allocation
												 , order_line_key
												 , excluded_from_allocation
												 ) 
											   values( l_ord_key
												 , r_tsk.key
												 , r_tsk.sku_id
												 , 'DEFAULT'--r_tsk.config_id
												 , null
												 , null
												 , r_tsk.qty_to_move
												 , l_category
												 , 0
												 , 0
												 , null
												 , 'CHECK_IN_DATE'
												 , l_orl_key
												 , l_cubing
												 );
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master','Inserting order_line for client_id ' || r_clt.client_id || ', order_id ' || r_ord.order_id || ', wms move task key = ' || r_tsk.key);
					--
				exception 
					when others
					then
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master',substr('Exception handling order '||r_ord.order_id||' and task '||r_tsk.key||': SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
				end;
				--
                                if      l_category = 'AUTOSTORE'
                                and     r_tsk.tag_id is not null
                                then
					begin
						l_att_key := key_f(3);
						insert into rhenus_synq.host_attribute@as_synq.rhenus.de( order_line_key
													, asn_line_key
													, class_type
													, attribute_name
													, attribute_value
													, attribute_role
													, attribute_value_key
													)
												  values( l_orl_key
													, null
													, 'ORDERLINE_ATTRIBUTE'
													, 'TAG'
													, r_tsk.tag_id
													, null
													, l_att_key
													);
						cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master','Inserting order_line attribute for client_id ' || r_clt.client_id || ', order_id ' || r_ord.order_id || ', wms move task key = ' || r_tsk.key);
					exception
						when others
						then
							cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master',substr('Exception handling attribute of order '||r_ord.order_id||' and task '||r_tsk.key||': SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));							
					end;
                                end if;
				begin
					update  cnl_as_pick_task
					set     cnl_if_status = 'Shared'
					,       category      = l_category  
					where   wms_mt_key = r_asc.wms_mt_key;
				exception
					when others
					then
						null;
				end;

                        end loop c_asc; -- End looping all tasks.

			-- Create Host message for each order processed.
                        cnl_sys.cnl_as_pck.create_message_exchange( p_message_id_i              => r_aso.order_id||r_clt.client_id
                                                                  , p_message_status_i          => 'UNPROCESSED'
                                                                  , p_message_type_i            => 'OrderMaster'
                                                                  , p_trans_code_i              => 'NEW'
                                                                  , p_host_message_table_key_i  => l_ord_key
                                                                  , P_key_o                     => l_hos_key
                                                                  );
                        -- Update shared cubing tasks with host message key.                                                                  
                        begin
				update  cnl_as_pick_task
				set     ord_master_host_message_key    = l_ord_key
				where   task_id                        = r_aso.order_id
				and     cnl_if_status                  = 'Shared';
			exception
				when others
				then
					null;
			end;

			-- update shared orders
                        begin
				update  cnl_as_orders
				set     cnl_if_status                  = 'Shared'
				,       ord_master_host_message_key    = l_ord_key
				,       ord_type                       = l_ord_type
				,       update_date                    = sysdate
				where   order_id                       = r_aso.order_id;
			exception
				when others
				then
					null;
			end;
                end loop c_aso; -- End looping all orders.
                commit;            
        end loop c_clt;
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.synq_order_master',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end synq_order_master;

/***************************************************************************************************************
* update pick tasks
***************************************************************************************************************/
    Procedure wms_update_pick_tasks
    is
	--
        cursor c_aso
        is
            select  order_id
            ,       client_id
            from    cnl_sys.cnl_as_orders o
            where   (   select  count(distinct cnl_if_status)
                        from    cnl_sys.cnl_as_pick_task t
                        where   t.task_id  	= o.order_id  
			and	t.client_id	= o.client_id
                    ) 	= 1
            and     (   select  distinct 1
                        from    cnl_sys.cnl_as_pick_task t
                        where   t.task_id  	= o.order_id  
			and	t.client_id	= o.client_id
                        and     t.cnl_if_status = 'CubingReturned'
                    ) = 1
        ;
        --
        cursor c_asc( b_task    varchar2
                    , b_client  varchar2
                    )
        is
            select  category
            ,       cubing_req
            ,       cubed_container_id
            ,       cubed_container_type
            ,       wms_mt_key
            ,       client_id
            ,       task_id
            ,       disallow_cubing
            from    cnl_sys.cnl_as_pick_task
            where   cnl_if_status   = 'CubingReturned'
            and     task_id         = b_task
            and     client_id       = b_client
        ;
        --
        cursor  c_config_chk( b_pallet_type varchar2
                            , b_client_id   varchar2
                            )
        is
            select  count(*)
            from    dcsdba.pallet_config
            where   config_id = b_pallet_type
            and     (client_id = b_client_id or client_id is null)
        ;
        --
        r_config_chk        number;
        l_container_id      varchar2(50);
        l_container_type    varchar2(50);
        l_status            varchar2(20);
        l_list              varchar2(50);
        l_ok_yn_o           varchar2(1);
        --
    begin
        for     r_aso in c_aso
        loop
                -- Loop true all tasks from manual pick
                for     r_asc in c_asc( r_aso.order_id
                                      , r_aso.client_id
                                      )
                loop
                    if      r_asc.category = 'MANUAL'
                    and     r_asc.cubing_req = 'Y' 
                    then
                            l_container_id      := r_asc.cubed_container_id;
                            if  r_asc.cubed_container_type = 'NO_TRANPORT_UNIT'
                            then
                                l_container_type := 'NOTU';
                            else
                                open    c_config_chk(nvl(r_asc.cubed_container_type,'NOTU'),r_aso.client_id);
                                fetch   c_config_chk into r_config_chk;
                                close   c_config_chk;
                                if      r_config_chk = 0
                                then
                                        l_container_type    := 'NOTU';
                                else
                                        l_container_type    := nvl(r_asc.cubed_container_type,'NOTU');
                                end if;
                            end if;
                            l_status            := 'Hold';
                            l_list              := r_asc.cubed_container_id;
                    elsif   r_asc.category = 'MANUAL'  
                    and     r_asc.cubing_req = 'N' 
                    then
                            if      nvl(r_asc.disallow_cubing,'N') = 'Y'
                            then
                                    l_container_id      := null;
                                    l_container_type    := null;
                                    l_status            := 'Hold';
                                    l_list              := '#NCUB'||cnl_sys.cnl_as_container_id_seq1.nextval;
                            else
                                    l_container_id      := null;
                                    l_container_type    := 'NOOUTERCARTON';
                                    l_status            := 'Hold';
                                    l_list              := '#N1' || cnl_sys.cnl_as_container_id_seq1.nextval;
                            end if;
                    elsif   r_asc.category = 'AUTOSTORE'    or 
                            r_asc.category is null          
                    then
                            l_container_id      := null;
                            l_container_type    := null;
                            l_status            := 'Autostore';
                            l_list              := null;
                    end if;
                    --
                    cnl_sys.cnl_as_pck.insert_itl( p_mt_key_i      => r_asc.wms_mt_key
                                                 , p_to_status_i   => l_status
                                                 , p_ok_yn_o       => l_ok_yn_o
                                                 );
                    --
                    Update  dcsdba.move_task m
                    set     m.to_container_id       = l_container_id
                    ,       m.to_container_config   = l_container_type
                    ,       m.list_id               = l_list
                    ,       m.status                = l_status
                    where   m.key                   = r_asc.wms_mt_key;

                    update  cnl_sys.cnl_as_pick_task p
                    set     p.cnl_if_status         = 'CubingProcessed'
                    where   p.wms_mt_key            = r_asc.wms_mt_key;
                    --
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.wms_update_pick_tasks','Update all tasks in WMS completing cubing for client ' || r_asc.client_id 
                                                                                                            || ', task_id ' || r_asc.task_id 
                                                                                                            || ', container_id ' || r_asc.cubed_container_id
                                                                                                            || ', container_type ' || r_asc.cubed_container_type
                                                                                                            );
                end loop c_asc;
        end loop c_aso;
    exception
        when others
        then    
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.wms_update_pick_tasks',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end wms_update_pick_tasks;

/***************************************************************************************************************
* Check containers move_task to_location
***************************************************************************************************************/                   
    function check_to_location ( p_container_id_i   varchar2
                               , p_key_i            number
                               )
        return varchar2
    is
        cursor c_cnt
        is
            select  count(distinct to_loc_id)
            from    dcsdba.move_task
            where   to_container_id = p_container_id_i
        ;
        --
        cursor c_loc
        is
            select  distinct to_loc_id
            from    dcsdba.move_task
            where   to_container_id = p_container_id_i
        ;
        --
        cursor  c_klc
        is
            select  to_loc_id
            from    dcsdba.move_task
            where   key = p_key_i
        ;
        r_cnt       number;
        r_loc       varchar2(30);
        r_klc       varchar2(30);
        l_retval    varchar2(50);
    begin
        open    c_cnt;
        fetch   c_cnt into r_cnt;
        close   c_cnt;
        if      r_cnt = 0 
        then 
                l_retval := p_container_id_i;
        elsif   r_cnt > 1
        then
                l_retval := '#C1' || cnl_sys.cnl_as_container_id_seq1.nextval;
        else
                open    c_loc;
                fetch   c_loc into r_loc;
                close   c_loc;
                open    c_klc;
                fetch   c_klc into r_klc;
                close   c_klc;
                if      r_loc = r_klc
                then
                        l_retval := p_container_id_i;
                else
                        l_retval := '#C1' || cnl_sys.cnl_as_container_id_seq1.nextval;
                end if;
        end if;
        return l_retval;
    end check_to_location;
/***************************************************************************************************************
* process the Cubing result to WMS
***************************************************************************************************************/                   
    procedure cubing_result( p_hme_tbl_key_i in number
                           , p_hme_key_i     in number
                           )
    is
        -- Get transport unit
        cursor  c_otu( b_cub_key number)
        is
            select  order_tu_key
            ,       tu_type
            from    rhenus_synq.host_order_tu@as_synq.rhenus.de
            where   cubing_result_key = b_cub_key
        ;
        -- Get lines inside transport unit
        cursor  c_orl( b_otu_key    number)
        is
            select  order_line_number
            ,       quantity
            from    rhenus_synq.host_order_line@as_synq.rhenus.de
            where   order_line_key in ( select  order_line_key
                                        from    rhenus_synq.host_order_tu_orderline@as_synq.rhenus.de
                                        where   order_tu_key = b_otu_key
                                      )
        ;
        -- Get tasks from cnl_sys.
        cursor  c_asc( b_line_id    number)
        is
            select  cubing_req
            ,       wms_mt_key
            ,       client_id
            ,       qty_to_move
            ,       task_id
            ,       cubed_container_type
            ,       cubed_container_id
            ,       category
            ,       disallow_cubing
            from    cnl_sys.cnl_as_pick_task
            where   wms_mt_key = b_line_id
        ;
        --
        cursor  c_tsk( b_mt_key number)
        is
            select  m.qty_to_move
            from    dcsdba.move_task m
            where   m.key = b_mt_key
        ;
        --
        cursor  c_shp( b_task   varchar2
                     , b_client varchar2
                     )
        is
            select  wms_mt_key
            from    cnl_as_pick_task
            where   task_id     = b_task
            and     client_id   = b_client
            and     nvl(cubing_req,'N')  = 'N'
        ;
        --
        r_tsk       c_tsk%rowtype;
        r_asc       c_asc%rowtype;
        --
        l_cid       varchar2(30);
        l_new_key   number;
        --
    begin
        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Start looping TU lines.');
        for     r_otu in c_otu(p_hme_tbl_key_i) -- For each transport unit linked to result key
        loop
                -- generate container id
                l_cid := '#C1' || cnl_sys.cnl_as_container_id_seq1.nextval;
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Create WMS container id' || l_cid);
                --
                for     r_orl in c_orl( r_otu.order_tu_key) -- for each WMS task (or part of a task) in transport unit. !! can result in splittng tasks in WMS!!
                loop
                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Starting with order line ' || r_orl.order_line_number || ' in container ' || l_cid || 'Start fetching matching pick task from CNL_SYS');
                        --
                        open    c_asc( r_orl.order_line_number);
                        fetch   c_asc
                        into    r_asc;
                        close   c_asc;
                        --
                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Start looping all shipable units and Start fetching matching pick task from CNL_SYS');
                        if      r_asc.cubing_req = 'N'
                        then
                                if      nvl(r_asc.disallow_cubing,'N') = 'Y'
                                then
                                        update  cnl_sys.cnl_as_pick_task
                                        set     cnl_if_status           = 'CubingReturned'
                                        ,       cubed_container_type    = null
                                        ,       cubed_date              = sysdate
                                        where   wms_mt_key              = r_asc.wms_mt_key;
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Updating cnl_sys disallow cubing pick record for client_id ' || r_asc.client_id);
                                else
                                        update  cnl_sys.cnl_as_pick_task
                                        set     cnl_if_status           = 'CubingReturned'
                                        ,       cubed_container_type    = 'NOOUTERCARTON'
                                        ,       cubed_date              = sysdate
                                        where   wms_mt_key              = r_asc.wms_mt_key;
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Updating cnl_sys shippable unit pick record for client_id ' || r_asc.client_id);
                                end if;
                        else
                            if      r_asc.category != 'AUTOSTORE'
                            then
                                    -- First chekc to location of task. Tasks going to different locations can not be cubed together.
                                    l_cid := check_to_location( p_container_id_i => l_cid
                                                              , p_key_i          => r_asc.wms_mt_key
                                                              );

                                    if      r_orl.quantity = r_asc.qty_to_move  -- WMS task does not require any splitting.
                                    then
                                            update  cnl_as_pick_task
                                            set     cubed_container_type = r_otu.tu_type
                                            ,       cubed_container_id   = l_cid
                                            ,       cubed_date           = sysdate
                                            ,       cnl_if_status        = 'CubingReturned'
                                            where   wms_mt_key           = r_orl.order_line_number;
                                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Updating cnl_sys pick tasks. Task can be placed inside box completely.');
                                    else
                                            wms_split_pick_to_cube( p_mt_key_i => r_orl.order_line_number
                                                                  , p_mt_qty_i => r_orl.quantity
                                                                  , p_mt_key_o => l_new_key
                                                                  );
                                            insert into cnl_as_pick_task( wms_mt_key
                                                                        , client_id
                                                                        , task_id
                                                                        , qty_to_move
                                                                        , cubing_req
                                                                        , category
                                                                        , cnl_if_status
                                                                        , cubed_container_type
                                                                        , cubed_container_id
                                                                        , creation_date
                                                                        , cubed_date
                                                                        , cub_result_host_message_key
                                                                        )
                                                                  values( l_new_key
                                                                        , r_asc.client_id
                                                                        , r_asc.task_id
                                                                        , r_orl.quantity
                                                                        , 'Y'
                                                                        , r_asc.category
                                                                        , 'CubingReturned'
                                                                        , r_otu.tu_type
                                                                        , l_cid
                                                                        , sysdate
                                                                        , sysdate
                                                                        , p_hme_key_i
                                                                        );
                                            update  cnl_as_pick_task
                                            set     qty_to_move = qty_to_move - r_orl.quantity
                                            where   wms_mt_key  = r_orl.order_line_number;

                                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_results','Split pick after cubing. task is split into more then one container type');
                                    end if; -- split y/n
                            else
                                    update  cnl_as_pick_task
                                    set     cnl_if_status   = 'CubingReturned'
                                    ,       cubed_date      = sysdate
                                    where   wms_mt_key      = r_orl.order_line_number;
                            end if; -- Category Autostore Y/N
                        end if; -- Cubing req
                end loop c_orl;
        end loop c_out;
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.cubing_result',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end cubing_result;
    --
    begin
    -- Initialization
    null;   
    --
end cnl_as_cubing_pck;