CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_INBOUND_PCK" is
/********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 07-05-2018
**********************************************************************************
*
* Description: 
* Share and process putaway and relocate tasks WMS and Synq
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
* function to get table  key
***************************************************************************************************************/                   
    function key_f(p_tbl number)
        return number
    is
        cursor c_asn
        is
            select rhenus_synq.host_asn_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_asl
        is
            select rhenus_synq.host_asn_line_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_atr
        is
            select rhenus_synq.host_attribute_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        l_retval number;
    begin
        if      p_tbl = 1
        then
                open  c_asn;
                fetch c_asn into l_retval;
                close c_asn;
        elsif   p_tbl = 2
        then
                open  c_asl;
                fetch c_asl into l_retval;
                close c_asl;
        elsif   p_tbl = 3
        then
                open  c_atr;
                fetch c_atr into l_retval;
                close c_atr;
        end if;
        return l_retval;
    end key_f;

/***************************************************************************************************************
* Fetch pending putaway, relocate tasks for SynQ.
***************************************************************************************************************/
    procedure wms_get_put_rel_tsk( p_final_loc_i in varchar2
                                 , p_site_i      in varchar2
                                 ) 
    is
        cursor c_pr_tsk ( b_final_loc_id varchar2
                        , b_site         varchar2
                        )
        is
            select  m.key
            ,       m.qty_to_move
            ,       m.tag_id
            ,       m.task_type
            ,       m.client_id
            from    dcsdba.move_task m
            where  (m.task_type = 'M' or 
                    m.task_type = 'P') -- M = relocate, P = Putaway
            and     m.final_loc_id  = b_final_loc_id
            and     m.to_loc_id     = b_final_loc_id
            and     m.site_id = b_site
            and     m.status in ('Released','Hold')
            and  (  (m.key not in (select wms_mt_key     from cnl_as_inb_tasks) and 
                     m.key not in (select wms_mt_new_key from cnl_as_inb_tasks)) or
                    (m.key in (select wms_mt_new_key from cnl_as_inb_tasks where cnl_if_status = 'Complete' and
                     m.key not in (select wms_mt_new_key from cnl_as_inb_tasks where cnl_if_status != 'Complete' and wms_mt_new_key = m.key)))
                 )
            ;
            --

            --
            l_batch_key number;
            --
    begin
            l_batch_key := cnl_as_batch_tasks_seq1.nextval;
            --
            for r in c_pr_tsk( p_final_loc_i
                             , p_site_i
                             ) 
            loop
                    if      cnl_sys.cnl_as_pck.chk_client( p_site_i
                                                         , r.client_id
                                                         ) = 0
                    then
                            --
                            continue;
                            --
                    end if; --cnl_sys.cnl_as_pck.chk_client( p_site_i, r.client_id)
                    --
                    insert into cnl_as_inb_tasks( cnl_batch_key
                                                , wms_mt_key
                                                , wms_mt_new_key
                                                , wms_mt_qty_to_move
                                                , wms_mt_tag_id
                                                , wms_mt_task_type
                                                , cnl_if_status
                                                , as_site_id
                                                , dstamp
                                                ) 
                                          values( l_batch_key
                                                , r.key-- old key
                                                , r.key-- new key
                                                , r.qty_to_move
                                                , r.tag_id
                                                , r.task_type
                                                , 'Pending'
                                                , p_site_i
                                                , sysdate
                                                ); 
                    --
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_get_put_rel_tsk','Saving putaway or relocate for tag_id ' || r.tag_id);
            end loop; -- c_pr_tsk
            --
            commit;
            --
            asn_receiving_notification;
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound.pck.wms_get_put_rel_tsk',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end wms_get_put_rel_tsk;
/***************************************************************************************************************
* ASN receiving Notification. Sends putaway and relocate information to SynQ
***************************************************************************************************************/
    procedure asn_receiving_notification
    is
        cursor  c_inb
        is
            select  as_site_id
            ,       wms_mt_tag_id
            ,       wms_mt_task_type
            ,       wms_mt_qty_to_move
            ,       wms_mt_key
            from    cnl_sys.cnl_as_inb_tasks
            where   cnl_if_status = 'Pending'
        ;
        --

        cursor c_tsk( b_key_i number)
        is
            select  mt.sku_id
            ,       mt.client_id
            ,       mt.config_id
            from    dcsdba.move_task mt
            where   mt.key = b_key_i
        ;
        --
        cursor c_skc( b_config_id_i varchar2
                    , b_client_id_i varchar2
                    )
        is
            select  sc.track_level_1
            from    dcsdba.sku_config sc
            where   sc.config_id = b_config_id_i
            and     sc.client_id = b_client_id_i
        ;
        --
        cursor c_sku( b_sku_id_i    varchar2
                    , b_client_id_i varchar2
                    )
        is
            select  count(*)
            from    dcsdba.sku s
            where   s.sku_id    = b_sku_id_i
            and     s.client_id = b_client_id_i
            and     s.qc_status is not null
        ;
        --
        cursor c_inv( b_sku_id_i    varchar2
                    , b_client_id_i varchar2
                    , b_tag_id_i    varchar2
                    )
        is
            select  i.batch_id
            from    dcsdba.inventory i
            where   i.sku_id    = b_sku_id_i
            and     i.client_id = b_client_id_i
            and     i.tag_id    = b_tag_id_i
        ;
        --
        r_inb       c_inb%rowtype;
        r_tsk       c_tsk%rowtype;
        r_skc       c_skc%rowtype;
        r_sku       number;
        r_inv       c_inv%rowtype;

        l_asn_key   integer;
        l_asl_key   integer;
        l_atr_key   integer;
        l_hme_key   integer;
        l_trk_lvl   varchar2(15);
    begin
        -- Get record from CNL_SYS put and rel tasks 
        for     r_inb in c_inb
        loop
                if      r_inb.as_site_id = 'NLTLG01'
                then
                        -- Get WMS move task
                        open    c_tsk( r_inb.wms_mt_key);
                        fetch   c_tsk
                        into    r_tsk;
                        if      c_tsk%found
                        then
                                -- Get Lowest tracking level
                                open    c_skc( r_tsk.config_id
                                             , r_tsk.client_id
                                             );
                                fetch   c_skc
                                into    r_skc;
                                if      c_skc%notfound
                                then
                                        l_trk_lvl := 'EACH';
                                else
                                        l_trk_lvl := r_skc.track_level_1;
                                end if; --c_skc%notfound
                                close c_skc;
                                -- Insert records
                                if r_inb.wms_mt_tag_id is not null
                                then
                                        -- Insert ASN
                                        l_asn_key := key_f(1);
                                        insert into rhenus_synq.host_asn@as_synq.rhenus.de( asn_type
                                                                                          , keep_tu
                                                                                          , owner_id
                                                                                          , tu_id
                                                                                          , tu_type
                                                                                          , asn_key
                                                                                          ) 
                                                                                   values ( decode(r_inb.wms_mt_task_type,'M','Relocate','Putaway')
                                                                                          , 1
                                                                                          , r_tsk.client_id
                                                                                          , r_inb.wms_mt_tag_id--to_char(p_key_i)
                                                                                          , 'PALLET'
                                                                                          , l_asn_key
                                                                                          );
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Inserting ASN ' || r_inb.wms_mt_tag_id);
                                        -- insert ASN line
                                        l_asl_key := key_f(2);
                                        insert into rhenus_synq.host_asn_line@as_synq.rhenus.de( asn_line_number
                                                                                               , product_id
                                                                                               , product_uom
                                                                                               , product_uom_tree
                                                                                               , quantity_expected
                                                                                               , quantity_received
                                                                                               , tu_id
                                                                                               , asn_key
                                                                                               , asn_line_key
                                                                                               ) 
                                                                                        values ( 1
                                                                                               , r_tsk.sku_id
                                                                                               , 'EACH'--l_trk_lvl--r_skc.track_level_1'
                                                                                               , 'DEFAULT'--r_tsk.config_id
                                                                                               , r_inb.wms_mt_qty_to_move
                                                                                               , 0
                                                                                               , r_inb.wms_mt_tag_id--to_char(p_key_i)
                                                                                               , l_asn_key
                                                                                               , l_asl_key
                                                                                               );
                                        --
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Inserting ASN line' || r_inb.wms_mt_tag_id);
                                        -- Insert attributes Tag and Batch
                                        l_atr_key := key_f(3);
                                        insert into rhenus_synq.host_attribute@as_synq.rhenus.de( class_type
                                                                                                , attribute_name
                                                                                                , attribute_value
                                                                                                , attribute_role
                                                                                                , lu_key
                                                                                                , asn_line_key
                                                                                                , order_line_key
                                                                                                , attribute_value_key
                                                                                                ) 
                                                                                         values ( 'ASNLINE_ATTRIBUTE'
                                                                                                , 'TAG'
                                                                                                , r_inb.wms_mt_tag_id
                                                                                                , null
                                                                                                , null
                                                                                                , l_asl_key
                                                                                                , null
                                                                                                , l_atr_key
                                                                                                );
                                        --
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Inserting ASN attribute ' || r_inb.wms_mt_tag_id);
                                        --
                                        open    c_sku( r_tsk.sku_id
                                                     , r_tsk.client_id
                                                     );
                                        fetch   c_sku
                                        into    r_sku;
                                        close   c_sku;
                                        --
                                        if      r_sku = 0
                                        then
                                                null;
                                        else
                                                open    c_inv( r_tsk.sku_id
                                                         , r_tsk.client_id
                                                         , r_inb.wms_mt_tag_id
                                                         );
                                                fetch   c_inv
                                                into    r_inv;
                                                if      c_inv%notfound
                                                then    
                                                        r_inv.batch_id := 'NOBATCH';
                                                end if; -- c_inv%notfound
                                                close c_inv;
                                                --
                                                l_atr_key := key_f(3);
                                                insert into rhenus_synq.host_attribute@as_synq.rhenus.de( class_type
                                                                                                        , attribute_name
                                                                                                        , attribute_value
                                                                                                        , attribute_role
                                                                                                        , lu_key
                                                                                                        , asn_line_key
                                                                                                        , order_line_key
                                                                                                        , attribute_value_key
                                                                                                        ) 
                                                                                                 values ( 'ASNLINE_ATTRIBUTE'
                                                                                                        , 'BATCH'
                                                                                                        , r_inv.batch_id
                                                                                                        , null
                                                                                                        , null
                                                                                                        , l_asl_key
                                                                                                        , null
                                                                                                        , l_atr_key
                                                                                                        );
                                                --
                                                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Inserting ASN attribute ' || r_inb.wms_mt_tag_id);
                                        end if; -- r_sku = 0
                                        -- Insert exchange message
                                        cnl_sys.cnl_as_pck.create_message_exchange( p_message_id_i              => r_inb.wms_mt_key
                                                                                  , p_message_status_i          => 'UNPROCESSED'
                                                                                  , p_message_type_i            => 'AsnReceivingNotification'
                                                                                  , p_trans_code_i              => 'NEW'
                                                                                  , p_host_message_table_key_i  => l_asn_key
                                                                                  , P_key_o                     => l_hme_key
                                                                                  );
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Inserting ASN ' || r_inb.wms_mt_tag_id);
                                else -- r_inb.wms_mt_tag_id is null
                                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification', 'No tag id found. ASN not created ' || r_inb.wms_mt_tag_id);
                                end if; -- r_inb.wms_mt_tag_id is not null
                                -- When a record is processed update it so it won't be re-processed. Even at failure.
                                update  cnl_sys.cnl_as_inb_tasks
                                set     cnl_if_status = 'Shared'
                                ,       synq_key      = l_hme_key
                                where   wms_mt_key    = r_inb.wms_mt_key
                                and     cnl_if_status = 'Pending';
                                --
                                commit;
                                close c_tsk;
                        else
                                null;
                                close c_tsk;
                        end if; -- c_tsk%found
                end if; -- r_inb.as_site_id = 'NLTLG01'
        end loop;
        commit;
    exception
        when others
        then    
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end asn_receiving_notification;

/***************************************************************************************************************
* Complete putaway or relocate. Complete existing putaway
***************************************************************************************************************/
    procedure wms_complete_put_rel( p_mt_key_i in number,
                                    p_user_i in varchar2,
                                    p_station_i in varchar2)
    is
    begin
        update dcsdba.move_task
        set status        = 'Complete'
        ,   user_id       = cnl_sys.cnl_as_pck.check_user_id(nvl(p_user_i,'NOUSER'))
        ,   station_id    = cnl_sys.cnl_as_pck.check_station_id(nvl(p_station_i,'NOSTATION'))
	,   logging_level = 3
        where key = p_mt_key_i;
        -- delete record from cnl_as_inb_tasks
        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_complete_put_rel','Completing move tasks in WMS. Move task key = ' || p_mt_key_i || ' User = ' || p_user_i || ' station = ' || p_station_i);
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_complete_put_rel',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end wms_complete_put_rel;
/***************************************************************************************************************
* split relocate. Create a new move task for the QTY that is relocated and update the old task.
***************************************************************************************************************/
    procedure wms_split_relocate ( p_mt_key_i   in number
                                 , p_mt_qty_i   in number
                                 , p_user_i     in varchar2
                                 , p_station_i  in varchar2
                                 , p_client_i   in varchar2
                                 , p_tag_id_i   in varchar2
                                 , p_site_id_i  in varchar2
                                 , p_ok_yn_o    out varchar2
                                 , p_key_o      out number
                                 )
    is
        cursor c_tsk( b_key number)
        is
            select  count(*)
            from    dcsdba.move_task m
            where   m.key = b_key
        ;
        --
        cursor  c_mtk( b_key number)
        is
            select  *
            from    dcsdba.move_task
            where   key = b_key
        ;
        --
        cursor c_key( b_tag_id_i    varchar2
                    , b_site_id_i   varchar2
                    , b_loc_id_i    varchar2
                    , b_client_id_i varchar2
                    )
        is
            select  m.key
            from    dcsdba.move_task m
            where   m.tag_id        = b_tag_id_i
            and     m.from_loc_id   = b_loc_id_i
            and     m.site_id       = b_site_id_i
            and     m.task_type     = 'M'
            and     m.client_id     = b_client_id_i
            and     rownum          = 1
        ;
        --
        cursor  c_rel( b_tag_id varchar2)
        is
            select  count(*)
            from    dcsdba.move_task
            where   task_type   = 'M'
            and     tag_id      = b_tag_id
            and     status      = 'Complete'
        ;
        --
        r_tsk       number;
        r_mtk       c_mtk%rowtype;
        r_rel       number;
        r_key       number;
        --
        l_new_key   number;
        l_ok        varchar2(1);
        l_key       number;
        l_timer     number := 10;
        l_location  varchar2(50);
        --
    begin
            l_location := cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_id_i) || '_STORAGE-LOCATION_LOCATION');
            open    c_tsk(p_mt_key_i);
            fetch   c_tsk
            into    r_tsk;
            close   c_tsk;
            if      r_tsk > 0 -- Relocate found
            then
                    l_ok    := 'Y';
                    l_key   := p_mt_key_i;
            else
                    open    c_key(p_tag_id_i, p_site_id_i, l_location, p_client_i);
                    fetch   c_key
                    into    r_key;
                    if      c_key%notfound
                    then
                            l_ok := 'N';
                    else
                            l_ok := 'Y';
                            l_key := r_key;
                    end if;
                    close   c_key;
            end if;
            --
            if      l_ok = 'Y'
            then
                    open    c_mtk(l_key);
                    fetch   c_mtk
                    into    r_mtk;
                    close   c_mtk;

                    delete  dcsdba.move_task
                    where   key = l_key;
                    --
                    commit;
                    --
                    insert into dcsdba.move_task( key,
                                                  qty_to_move,
                                                  old_qty_to_move,
                                                  status,
                                                  user_id,
                                                  station_id,
						  logging_level,
                                                  shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,
                                                  kit_plan_id,plan_sequence,to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,
                                                  list_to_pallet_id,list_to_container_id,labor_grid_sequence,trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,
                                                  user_def_match_blank_3,user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,user_def_match_blank_7,user_def_match_blank_8,
                                                  user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,
                                                  last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,last_held_date,last_released_date,
                                                  spec_code,full_pallet_cluster,shipping_unit,first_key,task_type,task_id,line_id,client_id,sku_id,config_id,description,tag_id,old_tag_id,
                                                  customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,old_to_loc_id,final_loc_id,
                                                  owner_id,sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,
                                                  consignment,bol_id,reason_code,container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,
                                                  to_pallet_depth,to_pallet_width,to_pallet_weight,pallet_grouped,pallet_config,pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,
                                                  session_type,summary_record,repack,kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,
                                                  trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond,increment_time,estimated_time,uploaded_labor,print_label_id,print_label,
                                                  old_status,repack_qc_done,old_task_id,catch_weight,moved_lock_status,pick_realloc_flag,stage_route_id
                                                )
                                          values( cnl_sys.cnl_as_pck.get_move_task_key,         -- Get unique key
                                                  p_mt_qty_i,                                   -- new qty to move    
                                                  p_mt_qty_i,--old_qty_to_move,                 -- Original qty to move
                                                  'Complete',
                                                  cnl_sys.cnl_as_pck.check_user_id(p_user_i),   -- User who processed task   
                                                  cnl_sys.cnl_as_pck.check_user_id(p_station_i),-- Station used by user to execute putaway 
						  3,						-- fixed level 3. r_mtk.logging_level,
                                                  r_mtk.shipment_number,r_mtk.stage_route_sequence,r_mtk.labelling,r_mtk.pf_consol_link,r_mtk.inv_key,r_mtk.first_pick,r_mtk.serial_number,
                                                  r_mtk.label_exceptioned,r_mtk.shipment_group,r_mtk.deconsolidate,r_mtk.kit_plan_id,r_mtk.plan_sequence,r_mtk.to_container_config,r_mtk.container_config,
                                                  r_mtk.ce_rotation_id,r_mtk.ce_avail_status,r_mtk.rdt_user_mode,r_mtk.consol_run_num,r_mtk.labor_assignment,r_mtk.list_to_pallet_id,r_mtk.list_to_container_id,
                                                  r_mtk.labor_grid_sequence,r_mtk.trolley_slot_id,r_mtk.processing,r_mtk.move_whole,r_mtk.user_def_match_blank_1,
                                                  r_mtk.user_def_match_blank_2,r_mtk.user_def_match_blank_3,r_mtk.user_def_match_blank_4,r_mtk.user_def_match_blank_5,r_mtk.user_def_match_blank_6,
                                                  r_mtk.user_def_match_blank_7,r_mtk.user_def_match_blank_8,r_mtk.user_def_match_chk_1,r_mtk.user_def_match_chk_2,r_mtk.user_def_match_chk_3,
                                                  r_mtk.user_def_match_chk_4,r_mtk.last_held_user,r_mtk.last_released_user,r_mtk.last_held_workstation,r_mtk.last_released_workstation,
                                                  r_mtk.last_held_reason_code,r_mtk.last_released_reason_code,r_mtk.last_held_date,r_mtk.last_released_date,r_mtk.spec_code,r_mtk.full_pallet_cluster,
                                                  r_mtk.shipping_unit,r_mtk.first_key,r_mtk.task_type,r_mtk.task_id,r_mtk.line_id,r_mtk.client_id,r_mtk.sku_id,r_mtk.config_id,r_mtk.description,
                                                  r_mtk.tag_id,r_mtk.old_tag_id,r_mtk.customer_id,r_mtk.origin_id,r_mtk.condition_id,r_mtk.site_id,r_mtk.from_loc_id,r_mtk.old_from_loc_id,r_mtk.to_loc_id,r_mtk.old_to_loc_id,
                                                  r_mtk.final_loc_id,r_mtk.owner_id,r_mtk.sequence,r_mtk.list_id,r_mtk.dstamp,r_mtk.start_dstamp,r_mtk.finish_dstamp,r_mtk.original_dstamp,r_mtk.priority,r_mtk.consol_link,
                                                  r_mtk.face_type,r_mtk.face_key,r_mtk.work_zone,r_mtk.work_group,r_mtk.consignment,r_mtk.bol_id,r_mtk.reason_code,r_mtk.container_id,r_mtk.to_container_id,r_mtk.pallet_id,
                                                  r_mtk.to_pallet_id,r_mtk.to_pallet_config,r_mtk.to_pallet_volume,r_mtk.to_pallet_height,r_mtk.to_pallet_depth,r_mtk.to_pallet_width,r_mtk.to_pallet_weight,r_mtk.pallet_grouped,
                                                  r_mtk.pallet_config,r_mtk.pallet_volume,r_mtk.pallet_height,r_mtk.pallet_depth,r_mtk.pallet_width,r_mtk.pallet_weight,r_mtk.session_type,r_mtk.summary_record,r_mtk.repack,
                                                  r_mtk.kit_sku_id,r_mtk.kit_line_id,r_mtk.kit_ratio,r_mtk.kit_link,r_mtk.status_link,r_mtk.due_type,r_mtk.due_task_id,r_mtk.due_line_id,r_mtk.trailer_position,
                                                  r_mtk.consolidated_task,r_mtk.disallow_tag_swap,r_mtk.ce_under_bond,r_mtk.increment_time,r_mtk.estimated_time,r_mtk.uploaded_labor,r_mtk.print_label_id,r_mtk.print_label,
                                                  r_mtk.old_status,r_mtk.repack_qc_done,r_mtk.old_task_id,r_mtk.catch_weight,r_mtk.moved_lock_status,r_mtk.pick_realloc_flag,r_mtk.stage_route_id
                                                );
                    --
                    commit;
                    --
                    l_new_key := cnl_sys.cnl_as_pck.get_move_task_key;
                    --
                    while l_timer != 0
                    loop
                            open    c_rel( r_mtk.tag_id);
                            fetch   c_rel
                            into    r_rel;
                            close   c_rel;
                            if      r_rel = 0
                            then
                                    l_timer := 0;
                            else
                                    l_timer := l_timer -1;
                                    dbms_lock.sleep(2);
                            end if;
                    end loop;
                    --
                    insert into dcsdba.move_task( key,
                                                  qty_to_move,
                                                  old_qty_to_move,
                                                  status,
                                                  user_id,
                                                  station_id,
						  logging_level,
                                                  shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,
                                                  kit_plan_id,plan_sequence,to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,
                                                  list_to_pallet_id,list_to_container_id,labor_grid_sequence,trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,
                                                  user_def_match_blank_3,user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,user_def_match_blank_7,user_def_match_blank_8,
                                                  user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,
                                                  last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,last_held_date,last_released_date,spec_code,
                                                  full_pallet_cluster,shipping_unit,first_key,task_type,task_id,line_id,client_id,sku_id,config_id,description,tag_id,old_tag_id,customer_id,
                                                  origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,old_to_loc_id,final_loc_id,owner_id,sequence,
                                                  list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,bol_id,
                                                  reason_code,container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,
                                                  to_pallet_weight,pallet_grouped,pallet_config,pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,session_type,
                                                  summary_record,repack,kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,trailer_position,consolidated_task,
                                                  disallow_tag_swap,ce_under_bond,increment_time,estimated_time,uploaded_labor,print_label_id,print_label,old_status,repack_qc_done,old_task_id,
                                                  catch_weight,moved_lock_status,pick_realloc_flag,stage_route_id
                                                ) 
                                          values( l_new_key,  -- Get unique key
                                                  nvl(r_mtk.qty_to_move,0) - p_mt_qty_i, -- new qty to move    
                                                  nvl(r_mtk.qty_to_move,0) - p_mt_qty_i, -- new old qty to move
                                                  'Released',
                                                  cnl_sys.cnl_as_pck.check_user_id(p_user_i),
                                                  cnl_sys.cnl_as_pck.check_station_id(p_station_i),
						  3, -- Fixed level 3 r_mtk.logging_level,
                                                  r_mtk.shipment_number,r_mtk.stage_route_sequence,r_mtk.labelling,r_mtk.pf_consol_link,r_mtk.inv_key,r_mtk.first_pick,r_mtk.serial_number,r_mtk.label_exceptioned,r_mtk.shipment_group,
                                                  r_mtk.deconsolidate,r_mtk.kit_plan_id,r_mtk.plan_sequence,r_mtk.to_container_config,r_mtk.container_config,r_mtk.ce_rotation_id,r_mtk.ce_avail_status,r_mtk.rdt_user_mode,r_mtk.consol_run_num,
                                                  r_mtk.labor_assignment,r_mtk.list_to_pallet_id,r_mtk.list_to_container_id,r_mtk.labor_grid_sequence,r_mtk.trolley_slot_id,r_mtk.processing,r_mtk.move_whole,r_mtk.user_def_match_blank_1,
                                                  r_mtk.user_def_match_blank_2,r_mtk.user_def_match_blank_3,r_mtk.user_def_match_blank_4,r_mtk.user_def_match_blank_5,r_mtk.user_def_match_blank_6,r_mtk.user_def_match_blank_7,
                                                  r_mtk.user_def_match_blank_8,r_mtk.user_def_match_chk_1,r_mtk.user_def_match_chk_2,r_mtk.user_def_match_chk_3,r_mtk.user_def_match_chk_4,r_mtk.last_held_user,
                                                  r_mtk.last_released_user,r_mtk.last_held_workstation,r_mtk.last_released_workstation,r_mtk.last_held_reason_code,r_mtk.last_released_reason_code,r_mtk.last_held_date,
                                                  r_mtk.last_released_date,r_mtk.spec_code,r_mtk.full_pallet_cluster,r_mtk.shipping_unit,r_mtk.first_key,r_mtk.task_type,r_mtk.task_id,r_mtk.line_id,r_mtk.client_id,r_mtk.sku_id,r_mtk.config_id,
                                                  r_mtk.description,r_mtk.tag_id,r_mtk.old_tag_id,r_mtk.customer_id,r_mtk.origin_id,r_mtk.condition_id,r_mtk.site_id,r_mtk.from_loc_id,r_mtk.old_from_loc_id,r_mtk.to_loc_id,r_mtk.old_to_loc_id,
                                                  r_mtk.final_loc_id,r_mtk.owner_id,r_mtk.sequence,r_mtk.list_id,r_mtk.dstamp,r_mtk.start_dstamp,r_mtk.finish_dstamp,r_mtk.original_dstamp,r_mtk.priority,r_mtk.consol_link,r_mtk.face_type,
                                                  r_mtk.face_key,r_mtk.work_zone,r_mtk.work_group,r_mtk.consignment,r_mtk.bol_id,r_mtk.reason_code,r_mtk.container_id,r_mtk.to_container_id,r_mtk.pallet_id,r_mtk.to_pallet_id,r_mtk.to_pallet_config,
                                                  r_mtk.to_pallet_volume,r_mtk.to_pallet_height,r_mtk.to_pallet_depth,r_mtk.to_pallet_width,r_mtk.to_pallet_weight,r_mtk.pallet_grouped,r_mtk.pallet_config,r_mtk.pallet_volume,r_mtk.pallet_height,
                                                  r_mtk.pallet_depth,r_mtk.pallet_width,r_mtk.pallet_weight,r_mtk.session_type,r_mtk.summary_record,r_mtk.repack,r_mtk.kit_sku_id,r_mtk.kit_line_id,r_mtk.kit_ratio,r_mtk.kit_link,r_mtk.status_link,
                                                  r_mtk.due_type,r_mtk.due_task_id,r_mtk.due_line_id,r_mtk.trailer_position,r_mtk.consolidated_task,r_mtk.disallow_tag_swap,r_mtk.ce_under_bond,r_mtk.increment_time,r_mtk.estimated_time,
                                                  r_mtk.uploaded_labor,r_mtk.print_label_id,r_mtk.print_label,r_mtk.old_status,r_mtk.repack_qc_done,r_mtk.old_task_id,r_mtk.catch_weight,r_mtk.moved_lock_status,r_mtk.pick_realloc_flag,r_mtk.stage_route_id
                                                );
                    --
                    commit;
                    --
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_split_relocate','Adding relocate for remaining QTY with key l_new_key = ' || l_new_key);
                    --
                    update  cnl_as_inb_tasks 
                    set     wms_mt_new_key  = l_new_key 
                    where   wms_mt_new_key  = p_mt_key_i;
                    --     
                    commit;
                    --
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_split_relocate','Splitting a relocate task in WMS. Old Move task key = ' || p_mt_key_i || ' new key for remaining QTY = ' || l_new_key);
                    p_ok_yn_o := 'Y';
                    p_key_o := l_new_key;
            else
                    p_ok_yn_o := 'N';
            end if;
            commit;
    exception
        when others
        then    
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_split_relocate',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));             p_ok_yn_o := 'N';
             commit;
    end wms_split_relocate;

/***************************************************************************************************************
* split putaway. Create a new move task for the QTY that is putawayed and update the existing task with new QTY.
***************************************************************************************************************/ 
    procedure wms_split_putaway ( p_mt_key_i    in number
                                , P_mt_qty_i    in number
                                , p_user_i      in varchar2
                                , p_station_i   in varchar2
                                , p_client_i    in varchar2
                                , p_tag_id_i    in varchar2
                                , p_site_id_i   in varchar2
                                , p_ok_yn_o     out varchar2
                                , p_key_o       out number
                                )
    is
        -- Check if task exist in WMS.
        cursor c_tsk( b_key number)
        is
            select  count(*)
            from    dcsdba.move_task m
            where   m.key = b_key
        ;
        -- Get task details from WMS.
        cursor  c_mtk( b_key number)
        is
            select  *
            from    dcsdba.move_task
            where   key = b_key
        ;
        --

        cursor c_key( b_tag_id_i    varchar2
                    , b_site_id_i   varchar2
                    , b_loc_id_i    varchar2
                    , b_client_id_i varchar2
                    )
        is
            select  m.key
            from    dcsdba.move_task m
            where   m.tag_id    = b_tag_id_i
            and     m.site_id   = b_site_id_i
            and     m.to_loc_id = b_loc_id_i
            and     m.task_type = 'P'
            and     m.client_id = b_client_id_i
            and     rownum = 1
        ;
        --
        cursor  c_put( b_tag_id varchar2)
        is
            select  count(*)
            from    dcsdba.move_task
            where   task_type   = 'P'
            and     tag_id      = b_tag_id
            and     status      = 'Complete'
        ;
        --
        r_tsk       number;
        r_key       number;
        r_mtk       c_mtk%rowtype;
        r_put       number;
        --
        l_key       number;  -- Key used to get task data from WMS
        l_ok        varchar2(1);
        l_new_key   number;
        l_timer     number := 10;
        l_location  varchar2(50);
        --
    begin
        l_location := cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(p_site_id_i) || '_STORAGE-LOCATION_LOCATION');
        --
        open    c_tsk(p_mt_key_i);
        fetch   c_tsk
        into    r_tsk;
        close   c_tsk;
        if      r_tsk > 0 -- Task has been found
        then    
                l_key   := p_mt_key_i;
                l_ok    := 'Y';
        else
                open    c_key(p_tag_id_i, p_site_id_i, l_location, p_client_i);
                fetch   c_key
                into    r_key;
                if      c_key%notfound --No task could be found for original tag.
                then    
                        l_ok    := 'N';
                else
                        l_key   := r_key;
                        l_ok    := 'Y';
                end if; -- c_key%notfound
                close   c_key;
        end if; -- r_tsk > 0 

        -- Key to use has been identified now get task details.
        if      l_ok = 'Y'
        then
                open    c_mtk(l_key);
                fetch   c_mtk into r_mtk;
                if      c_mtk%notfound 
                then
                        l_ok :='N';
                else
                        delete  dcsdba.move_task
                        where   key = l_key;
                end if;
                close   c_mtk;
        end if;

        --  Insert putaway task that has been executed in SynQ.      
        if l_ok = 'Y'
        then
                insert into dcsdba.move_task(  key,
                                               qty_to_move,
                                               old_qty_to_move,
                                               status,
                                               user_id,      
                                               station_id,   
					       logging_level,
                                               shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,kit_plan_id,plan_sequence,
                                               to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,list_to_pallet_id,list_to_container_id,labor_grid_sequence,
                                               trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,user_def_match_blank_3,user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,
                                               user_def_match_blank_7,user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,
                                               last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,last_held_date,last_released_date,spec_code,full_pallet_cluster,shipping_unit,first_key,
                                               task_type,task_id,line_id,client_id,sku_id,config_id,description,tag_id,old_tag_id,customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,
                                               old_to_loc_id,final_loc_id,owner_id,sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,bol_id,
                                               reason_code,container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,to_pallet_weight,pallet_grouped,pallet_config,
                                               pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,session_type,summary_record,repack,kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,
                                               trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond,increment_time,estimated_time,uploaded_labor,print_label_id,print_label,old_status,repack_qc_done,old_task_id,catch_weight,
                                               moved_lock_status,pick_realloc_flag,stage_route_id
                                            ) 
                                        values(cnl_sys.cnl_as_pck.get_move_task_key, -- Get unique key
                                               p_mt_qty_i, -- new qty to move    
                                               p_mt_qty_i, -- old_qty_to_move,
                                               'Complete', -- Is executed
                                               cnl_sys.cnl_as_pck.check_user_id(p_user_i), -- User who processed task   
                                               cnl_sys.cnl_as_pck.check_user_id(p_station_i), -- Station used by user to execute putaway 
					       3, --r_mtk.logging_level,
                                               r_mtk.shipment_number,r_mtk.stage_route_sequence,r_mtk.labelling,r_mtk.pf_consol_link,r_mtk.inv_key,r_mtk.first_pick,r_mtk.serial_number,r_mtk.label_exceptioned,r_mtk.shipment_group,r_mtk.deconsolidate,r_mtk.kit_plan_id,r_mtk.plan_sequence,
                                               r_mtk.to_container_config,r_mtk.container_config,r_mtk.ce_rotation_id,r_mtk.ce_avail_status,r_mtk.rdt_user_mode,r_mtk.consol_run_num,r_mtk.labor_assignment,r_mtk.list_to_pallet_id,r_mtk.list_to_container_id,r_mtk.labor_grid_sequence,
                                               r_mtk.trolley_slot_id,r_mtk.processing,r_mtk.move_whole,r_mtk.user_def_match_blank_1,r_mtk.user_def_match_blank_2,r_mtk.user_def_match_blank_3,r_mtk.user_def_match_blank_4,r_mtk.user_def_match_blank_5,r_mtk.user_def_match_blank_6,
                                               r_mtk.user_def_match_blank_7,r_mtk.user_def_match_blank_8,r_mtk.user_def_match_chk_1,r_mtk.user_def_match_chk_2,r_mtk.user_def_match_chk_3,r_mtk.user_def_match_chk_4,r_mtk.last_held_user,r_mtk.last_released_user,
                                               r_mtk.last_held_workstation,r_mtk.last_released_workstation,r_mtk.last_held_reason_code,r_mtk.last_released_reason_code,r_mtk.last_held_date,r_mtk.last_released_date,r_mtk.spec_code,r_mtk.full_pallet_cluster,r_mtk.shipping_unit,r_mtk.first_key,
                                               r_mtk.task_type,r_mtk.task_id,r_mtk.line_id,r_mtk.client_id,r_mtk.sku_id,r_mtk.config_id,r_mtk.description,r_mtk.tag_id,r_mtk.old_tag_id,r_mtk.customer_id,r_mtk.origin_id,r_mtk.condition_id,r_mtk.site_id,r_mtk.from_loc_id,r_mtk.old_from_loc_id,r_mtk.to_loc_id,
                                               r_mtk.old_to_loc_id,r_mtk.final_loc_id,r_mtk.owner_id,r_mtk.sequence,r_mtk.list_id,r_mtk.dstamp,r_mtk.start_dstamp,r_mtk.finish_dstamp,r_mtk.original_dstamp,r_mtk.priority,r_mtk.consol_link,r_mtk.face_type,r_mtk.face_key,r_mtk.work_zone,r_mtk.work_group,r_mtk.consignment,r_mtk.bol_id,
                                               r_mtk.reason_code,r_mtk.container_id,r_mtk.to_container_id,r_mtk.pallet_id,r_mtk.to_pallet_id,r_mtk.to_pallet_config,r_mtk.to_pallet_volume,r_mtk.to_pallet_height,r_mtk.to_pallet_depth,r_mtk.to_pallet_width,r_mtk.to_pallet_weight,r_mtk.pallet_grouped,r_mtk.pallet_config,
                                               r_mtk.pallet_volume,r_mtk.pallet_height,r_mtk.pallet_depth,r_mtk.pallet_width,r_mtk.pallet_weight,r_mtk.session_type,r_mtk.summary_record,r_mtk.repack,r_mtk.kit_sku_id,r_mtk.kit_line_id,r_mtk.kit_ratio,r_mtk.kit_link,r_mtk.status_link,r_mtk.due_type,r_mtk.due_task_id,r_mtk.due_line_id,
                                               r_mtk.trailer_position,r_mtk.consolidated_task,r_mtk.disallow_tag_swap,r_mtk.ce_under_bond,r_mtk.increment_time,r_mtk.estimated_time,r_mtk.uploaded_labor,r_mtk.print_label_id,r_mtk.print_label,r_mtk.old_status,r_mtk.repack_qc_done,r_mtk.old_task_id,r_mtk.catch_weight,
                                               r_mtk.moved_lock_status,r_mtk.pick_realloc_flag,r_mtk.stage_route_id
                                              );
            commit;                                              
        end if;

        -- Wait until task has been completed.
        while l_timer != 0
        loop
                open    c_put( r_mtk.tag_id);
                fetch   c_put
                into    r_put;
                close   c_put;
                if      nvl(r_put,0) = 0
                then
                        l_timer := 0;
                else
                        l_timer := l_timer -1;
                        dbms_lock.sleep(1);
                end if;
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Timer. Searching for task with to_loc = ' || r_mtk.to_loc_id ||
                                                                                   ' tag_id = ' || r_mtk.tag_id || 
                                                                                   ' loop timer is now set to ' || l_timer || ' cursor returned ' || r_put);
        end loop;

        -- Add move task for remaining inventory
        if l_ok = 'Y'
        then
                l_new_key := cnl_sys.cnl_as_pck.get_move_task_key;
                insert into dcsdba.move_task(  key,
                                               qty_to_move,
                                               old_qty_to_move,
                                               status,
                                               user_id,      
                                               station_id,   
					       logging_level,
                                               shipment_number,stage_route_sequence,labelling,pf_consol_link,inv_key,first_pick,serial_number,label_exceptioned,shipment_group,deconsolidate,kit_plan_id,plan_sequence,
                                               to_container_config,container_config,ce_rotation_id,ce_avail_status,rdt_user_mode,consol_run_num,labor_assignment,list_to_pallet_id,list_to_container_id,labor_grid_sequence,
                                               trolley_slot_id,processing,move_whole,user_def_match_blank_1,user_def_match_blank_2,user_def_match_blank_3,user_def_match_blank_4,user_def_match_blank_5,user_def_match_blank_6,
                                               user_def_match_blank_7,user_def_match_blank_8,user_def_match_chk_1,user_def_match_chk_2,user_def_match_chk_3,user_def_match_chk_4,last_held_user,last_released_user,
                                               last_held_workstation,last_released_workstation,last_held_reason_code,last_released_reason_code,last_held_date,last_released_date,spec_code,full_pallet_cluster,shipping_unit,first_key,
                                               task_type,task_id,line_id,client_id,sku_id,config_id,description,tag_id,old_tag_id,customer_id,origin_id,condition_id,site_id,from_loc_id,old_from_loc_id,to_loc_id,
                                               old_to_loc_id,final_loc_id,owner_id,sequence,list_id,dstamp,start_dstamp,finish_dstamp,original_dstamp,priority,consol_link,face_type,face_key,work_zone,work_group,consignment,bol_id,
                                               reason_code,container_id,to_container_id,pallet_id,to_pallet_id,to_pallet_config,to_pallet_volume,to_pallet_height,to_pallet_depth,to_pallet_width,to_pallet_weight,pallet_grouped,pallet_config,
                                               pallet_volume,pallet_height,pallet_depth,pallet_width,pallet_weight,session_type,summary_record,repack,kit_sku_id,kit_line_id,kit_ratio,kit_link,status_link,due_type,due_task_id,due_line_id,
                                               trailer_position,consolidated_task,disallow_tag_swap,ce_under_bond,increment_time,estimated_time,uploaded_labor,print_label_id,print_label,old_status,repack_qc_done,old_task_id,catch_weight,
                                               moved_lock_status,pick_realloc_flag,stage_route_id
                                               ) 
                                      values(  l_new_key, -- Get unique key
                                               r_mtk.qty_to_move - p_mt_qty_i, -- new qty to move    
                                               r_mtk.qty_to_move - p_mt_qty_i, -- old_qty_to_move,
                                               'Released', -- Is executed
                                               'AUTOSTORE', -- User who processed task   
                                               'AUTOSTORE', -- Station used by user to execute putaway 
					       3, --r_mtk.logging_level,
                                               r_mtk.shipment_number,r_mtk.stage_route_sequence,r_mtk.labelling,r_mtk.pf_consol_link,r_mtk.inv_key,r_mtk.first_pick,r_mtk.serial_number,r_mtk.label_exceptioned,r_mtk.shipment_group,r_mtk.deconsolidate,r_mtk.kit_plan_id,r_mtk.plan_sequence,
                                               r_mtk.to_container_config,r_mtk.container_config,r_mtk.ce_rotation_id,r_mtk.ce_avail_status,r_mtk.rdt_user_mode,r_mtk.consol_run_num,r_mtk.labor_assignment,r_mtk.list_to_pallet_id,r_mtk.list_to_container_id,r_mtk.labor_grid_sequence,
                                               r_mtk.trolley_slot_id,r_mtk.processing,r_mtk.move_whole,r_mtk.user_def_match_blank_1,r_mtk.user_def_match_blank_2,r_mtk.user_def_match_blank_3,r_mtk.user_def_match_blank_4,r_mtk.user_def_match_blank_5,r_mtk.user_def_match_blank_6,
                                               r_mtk.user_def_match_blank_7,r_mtk.user_def_match_blank_8,r_mtk.user_def_match_chk_1,r_mtk.user_def_match_chk_2,r_mtk.user_def_match_chk_3,r_mtk.user_def_match_chk_4,r_mtk.last_held_user,r_mtk.last_released_user,
                                               r_mtk.last_held_workstation,r_mtk.last_released_workstation,r_mtk.last_held_reason_code,r_mtk.last_released_reason_code,r_mtk.last_held_date,r_mtk.last_released_date,r_mtk.spec_code,r_mtk.full_pallet_cluster,r_mtk.shipping_unit,r_mtk.first_key,
                                               r_mtk.task_type,r_mtk.task_id,r_mtk.line_id,r_mtk.client_id,r_mtk.sku_id,r_mtk.config_id,r_mtk.description,r_mtk.tag_id,r_mtk.old_tag_id,r_mtk.customer_id,r_mtk.origin_id,r_mtk.condition_id,r_mtk.site_id,r_mtk.from_loc_id,r_mtk.old_from_loc_id,r_mtk.to_loc_id,
                                               r_mtk.old_to_loc_id,r_mtk.final_loc_id,r_mtk.owner_id,r_mtk.sequence,r_mtk.list_id,r_mtk.dstamp,r_mtk.start_dstamp,r_mtk.finish_dstamp,r_mtk.original_dstamp,r_mtk.priority,r_mtk.consol_link,r_mtk.face_type,r_mtk.face_key,r_mtk.work_zone,r_mtk.work_group,r_mtk.consignment,r_mtk.bol_id,
                                               r_mtk.reason_code,r_mtk.container_id,r_mtk.to_container_id,r_mtk.pallet_id,r_mtk.to_pallet_id,r_mtk.to_pallet_config,r_mtk.to_pallet_volume,r_mtk.to_pallet_height,r_mtk.to_pallet_depth,r_mtk.to_pallet_width,r_mtk.to_pallet_weight,r_mtk.pallet_grouped,r_mtk.pallet_config,
                                               r_mtk.pallet_volume,r_mtk.pallet_height,r_mtk.pallet_depth,r_mtk.pallet_width,r_mtk.pallet_weight,r_mtk.session_type,r_mtk.summary_record,r_mtk.repack,r_mtk.kit_sku_id,r_mtk.kit_line_id,r_mtk.kit_ratio,r_mtk.kit_link,r_mtk.status_link,r_mtk.due_type,r_mtk.due_task_id,r_mtk.due_line_id,
                                               r_mtk.trailer_position,r_mtk.consolidated_task,r_mtk.disallow_tag_swap,r_mtk.ce_under_bond,r_mtk.increment_time,r_mtk.estimated_time,r_mtk.uploaded_labor,r_mtk.print_label_id,r_mtk.print_label,r_mtk.old_status,r_mtk.repack_qc_done,r_mtk.old_task_id,r_mtk.catch_weight,
                                               r_mtk.moved_lock_status,r_mtk.pick_realloc_flag,r_mtk.stage_route_id
                                             );
        end if;
        --
        if      l_ok = 'Y'
        then
                p_ok_yn_o   := 'Y';
                p_key_o     := l_new_key;
                --
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','Putaway has been succesfully split. Move task key = ' || l_key || ' The key for the remaining QTY = ' || l_new_key);
        else
                p_ok_yn_o := 'N';
                --
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_receiving_notification','No task could be found for this tag to split. No processing done in WMS');
        end if; --l_ok = 'Y'
        --
        commit;
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.wms_split_putaway',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            p_ok_yn_o   := 'N';
            commit;
    end wms_split_putaway;

/***************************************************************************************************************
* ASN Check in Confirmation
***************************************************************************************************************/ 
    procedure asn_check_in_confirmation( p_hme_tbl_key_i    in number
                                       , p_client_i         in varchar2 
                                       , p_ok_yn_o          out varchar2
                                       )
    is
        cursor  c_hlu( b_tbl_key    number)
        is
            select  quantity
            ,       asn_tu_id
            from    rhenus_synq.host_load_unit@as_synq.rhenus.de
            where   inventory_status_key    = b_tbl_key
        ;
        --
        cursor  c_usr( b_asn_id varchar2)
        is
            select  upper(from_workstation_id)  station_id
            ,       upper(updated_by)           user_id
            from    rhenus_synq.trans@as_synq.rhenus.de
            where   transaction_type = 'ASN_CHECKIN' -- Operator opens ASN.
            and     from_asn = b_asn_id
            and     trans_key = (   select  max(trans_key)
                                    from    rhenus_synq.trans@as_synq.rhenus.de
                                    where   transaction_type = 'ASN_CHECKIN' -- Operator opens ASN.
                                    and     from_asn = b_asn_id)
        ;
        --
        cursor  c_inb( b_tag_id varchar2)
        is
            select  cnl_split_tasks
            ,       wms_mt_qty_to_move
            ,       as_qty_putawayed
            ,       wms_mt_key
            ,       wms_mt_new_key
            ,       wms_mt_task_type
            ,       as_site_id
            from    cnl_sys.cnl_as_inb_tasks
            where   wms_mt_tag_id = b_tag_id
            and     dstamp = (  select  max(dstamp) 
                                from    cnl_sys.cnl_as_inb_tasks
                                where   wms_mt_tag_id = b_tag_id
                             )
            and     cnl_if_status != 'Complete'
        ;
        cursor  c_tsk ( b_tag_id    varchar2)
        is
                select  count(*)
                from    dcsdba.move_task
                where   status      = 'Complete'
                and     tag_id      = b_tag_id
        ;
        --
        r_inb       c_inb%rowtype;
        r_usr       c_usr%rowtype;
        r_tsk       number;
        --
        l_cnt       number;
        l_message   varchar2(1000);
        l_status    varchar2(20);
        l_user      varchar2(30);
        l_station   varchar2(30);
        l_ok        varchar2(1);
        l_key       number;
        --
    begin
        for     r_hlu in c_hlu( p_hme_tbl_key_i)
        loop    
                -- get user and workstation
                open    c_usr( r_hlu.asn_tu_id);
                fetch   c_usr
                into    r_usr;
                if      c_usr%found
                then
                        l_user      := cnl_sys.cnl_as_pck.check_user_id(nvl(r_usr.user_id,'N'));
                        l_station   := cnl_sys.cnl_as_pck.check_user_id(nvl(r_usr.station_id,'N'));
                else
                        l_user      := cnl_sys.cnl_as_pck.check_user_id('N');
                        l_station   := cnl_sys.cnl_as_pck.check_user_id('N');
                end if;
                close   c_usr;

                -- Search for tasks with status complete for this tag.
                open    c_tsk( r_hlu.asn_tu_id);
                fetch   c_tsk
                into    r_tsk;
                close   c_tsk;
                if      r_tsk > 0 --Found tasks with status complete. Move to next r_hlu record
                then    
                        p_ok_yn_o := 'N';
                        continue;
                else
                        open    c_inb( r_hlu.asn_tu_id);
                        fetch   c_inb
                        into    r_inb;
                        close   c_inb;
                        --
                        if      r_inb.cnl_split_tasks is null
                        then    
                                l_cnt := 0;
                        else    
                                l_cnt := r_inb.cnl_split_tasks;
                        end if;
                        --
                        if      r_hlu.quantity = r_inb.wms_mt_qty_to_move - nvl(r_inb.as_qty_putawayed,0)
                        then    -- Putaway completed
                                wms_complete_put_rel( p_mt_key_i    => r_inb.wms_mt_new_key
                                                    , p_user_i      => l_user
                                                    , p_station_i   => l_station
                                                    );
                                commit;
                                --
                                update  cnl_sys.cnl_as_inb_tasks
                                set     cnl_if_status       = 'Complete'
                                ,       cnl_split_tasks     = l_cnt
                                ,       as_qty_putawayed    = nvl(r_inb.as_qty_putawayed,0) + r_hlu.quantity
                                where   wms_mt_new_key      = r_inb.wms_mt_new_key;
                                commit;
                                --
                        else    -- must split
                                if      r_inb.wms_mt_task_type = 'P'
                                then -- WMS putaway
                                        wms_split_putaway ( p_mt_key_i      => r_inb.wms_mt_new_key
                                                          , p_mt_qty_i      => r_hlu.quantity
                                                          , p_user_i        => l_user
                                                          , p_station_i     => l_station
                                                          , p_client_i      => p_client_i
                                                          , p_tag_id_i      => r_hlu.asn_tu_id
                                                          , p_site_id_i     => r_inb.as_site_id
                                                          , p_ok_yn_o       => l_ok
                                                          , p_key_o         => l_key
                                                          );
                                        --
                                        if      l_ok = 'Y'
                                        then
                                                l_cnt := l_cnt + 1;
                                                update  cnl_sys.cnl_as_inb_tasks
                                                set     cnl_split_tasks     = l_cnt
                                                ,       as_qty_putawayed    = nvl(r_inb.as_qty_putawayed,0) + r_hlu.quantity
                                                ,       wms_mt_new_key      = l_key
                                                where   wms_mt_new_key      = r_inb.wms_mt_new_key; -- New key can change thats why update must be done now.
                                                commit;
                                                --
                                        else
                                                p_ok_yn_o := 'N';
                                                continue;
                                        end if;
                                else -- WMS relocate
                                        wms_split_relocate( p_mt_key_i      => r_inb.wms_mt_new_key
                                                          , p_mt_qty_i      => r_hlu.quantity
                                                          , p_user_i        => l_user
                                                          , p_station_i     => l_station
                                                          , p_client_i      => p_client_i
                                                          , p_tag_id_i      => r_hlu.asn_tu_id
                                                          , p_site_id_i     => r_inb.as_site_id
                                                          , p_ok_yn_o       => l_ok
                                                          , p_key_o         => l_key
                                                          );
                                        --
                                        if      l_ok = 'Y'
                                        then
                                                l_cnt := l_cnt + 1;
                                                update  cnl_sys.cnl_as_inb_tasks
                                                set     cnl_split_tasks     = l_cnt
                                                ,       as_qty_putawayed    = nvl(as_qty_putawayed,0) + r_hlu.quantity
                                                where   wms_mt_new_key      = l_key; -- New key can change thats why update must be done now.
                                                commit;
                                                --
                                        else
                                                p_ok_yn_o := 'N';
                                                continue;
                                        end if;
                                end if; --r_inb.wms_mt_task_type = 'P'
                        end if;--r_hlu.quantity = r_inb.wms_mt_qty_to_move - nvl(r_inb.as_qty_putawayed,0)                
                        --
                        p_ok_yn_o := 'Y';
                end if;--r_tsk > 0
        end loop c_hlu;
         --
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_inbound_pck.asn_check in _confirmation',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end asn_check_in_confirmation;
    --
    begin
    -- Initialization
    null;   
    --
end cnl_as_inbound_pck;
--Show errors;