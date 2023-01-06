CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_INVENTORY_PCK" is
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
* SynQ Inventory Status. Process the inventory status messages from Synq.
***************************************************************************************************************/
    procedure autostore_adjustment( p_key_i     in  number
                                  , p_site_id_i in  varchar2
                                  , p_ok_yn_o   out varchar2
                                  )
    is
        cursor c_hlu(b_key number)
        is
            select  *
            from    rhenus_synq.host_load_unit@as_synq
            where   inventory_status_key = b_key
        ;
        --
        cursor  c_inv( b_client_id  varchar2
                     , b_site_id    varchar2
                     , b_sku_id     varchar2
                     , b_tag_id     varchar2
                     , b_location   varchar2
                     )
        is
            select  location_id
            ,       site_id
            ,       client_id
            ,       owner_id
            ,       sku_id
            ,       tag_id
            ,       qty_on_hand
            from    dcsdba.inventory
            where   location_id = b_location
            and     client_id   = b_client_id
            and     site_id     = b_site_id
            and     sku_id      = b_sku_id
            and     tag_id      = b_tag_id
            and     rownum      = 1
        ;
        r_hlu       c_hlu%rowtype;
        r_inv       c_inv%rowtype;
        --
        l_adj_qty        number;
        l_reason         varchar2(30); 
        l_ok_yn          varchar2(1);


    begin
        open    c_hlu(p_key_i);
        fetch   c_hlu into r_hlu;
        if      c_hlu%found
        then
                close   c_hlu;
                l_adj_qty := nvl(r_hlu.prev_quantity,0)-r_hlu.quantity;
                if      l_adj_qty < 0 -- More then should be
                then    -- Search for missing inventory
                        open    c_inv( r_hlu.owner_id
                                     , p_site_id_i
                                     , r_hlu.product_id
                                     , r_hlu.asn_tu_id
                                     , 'ASMISSING'
                                     );
                        fetch   c_inv into r_inv;
                        if      c_inv%notfound
                        then
                                l_reason := 'EXTRASTOCK';
                        close   c_inv;
                        else
                                l_reason := 'EXTRASTOCKRESOLVED';
                                close   c_inv;
                        end if;
                        --
                elsif   l_adj_qty > 0 -- less inventory then should be
                then
                        open    c_inv( r_hlu.owner_id
                                     , p_site_id_i
                                     , r_hlu.product_id
                                     , r_hlu.asn_tu_id
                                     , 'ASFOUND'
                                     );
                        fetch   c_inv into r_inv;
                        if      c_inv%notfound
                        then
                                l_reason := 'MISSINGSTOCK';
                                close   c_inv;
                        else
                                l_reason := 'MISSINGSTOCKRESOLVED';
                                close   c_inv;
                        end if;
                        --
                else
                        l_reason    := 'NOADJUSTMENT';
                end if;
                -- make positive number from negative number
                if      l_adj_qty < 0
                then
                        l_adj_qty := to_number(substr(to_char(l_adj_qty),2));
                end if;
                --
                insert into cnl_sys.cnl_as_adjustment( site_id, client_id, sku_id, tag_id, quantity, prev_quantity, adjusted_quantity, adjustment_reason, processed_quantity, load_unit_key, inventory_status_key, creation_dstamp)
                    values( p_site_id_i, r_hlu.owner_id, r_hlu.product_id, r_hlu.asn_tu_id, r_hlu.quantity, r_hlu.prev_quantity, l_adj_qty, l_reason, 0, r_hlu.lu_key, r_hlu.inventory_status_key, sysdate);
                commit;                            
        else
                close   c_hlu;
        end if;
        p_ok_yn_o := 'Y';
    exception
        when others
        then
            p_ok_yn_o := 'N';
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_cubing_pck.autostore_adjustment',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end autostore_adjustment;
/***************************************************************************************************************
* SynQ Inventory reconciliation. Process and save inventory reconciliation messages
***************************************************************************************************************/
    procedure create_relocate( p_from_loc_id_i  in  varchar2
                             , p_to_loc_id_i    in  varchar2
                             , p_tag_id_i       in  varchar2
                             , p_sku_id_i       in  varchar2
                             , p_client_id_i    in  varchar2
                             , p_site_id_i      in  varchar2
                             , p_qty_to_move_i  in  number
                             , p_config_id_i    in  varchar2
                             , p_description_i  in  varchar2
                             , p_owner_id_i     in  varchar2
                             , p_work_zone_i    in  varchar2
                             , p_origin_id_i    in  varchar2
                             , p_condition_id_i in  varchar2
                             , p_user_id_i      in  varchar2
                             , p_station_id_i   in  varchar2
                             , p_ok_yn_o        out varchar2
                             )
    is
            l_key number := cnl_sys.cnl_as_pck.get_move_task_key;
    begin
            insert into dcsdba.move_task( key
                                        , first_key
                                        , client_id
                                        , sku_id
                                        , config_id
                                        , description
                                        , tag_id
                                        , old_tag_id
                                        , origin_id
                                        , condition_id
                                        , qty_to_move
                                        , old_qty_to_move
                                        , site_id
                                        , from_loc_id
                                        , old_from_loc_id
                                        , to_loc_id
                                        , old_to_loc_id
                                        , final_loc_id
                                        , owner_id
                                        , work_zone
                                        , user_id
                                        , station_id
                                        , last_held_user
                                        , last_held_workstation
                                        , logging_level, task_id, task_type, sequence, status, dstamp, start_dstamp, finish_dstamp, original_dstamp, priority, session_type, summary_record, repack, kit_ratio, ce_under_bond, uploaded_labor, print_label, old_status, repack_qc_done, stage_route_sequence, labelling, first_pick, ce_rotation_id, last_held_reason_code, last_held_date, shipping_unit)
            values(     l_key
            ,           l_key                        
            ,           p_client_id_i
            ,           p_sku_id_i
            ,           p_config_id_i
            ,           p_description_i
            ,           p_tag_id_i
            ,           p_tag_id_i
            ,           p_origin_id_i
            ,           p_condition_id_i
            ,           p_qty_to_move_i
            ,           p_qty_to_move_i
            ,           p_site_id_i
            ,           p_from_loc_id_i
            ,           p_from_loc_id_i
            ,           p_to_loc_id_i
            ,           p_to_loc_id_i
            ,           p_to_loc_id_i
            ,           p_owner_id_i
            ,           p_work_zone_i
            ,           p_user_id_i
            ,           p_station_id_i
            ,           p_user_id_i
            ,           p_station_id_i
            ,           3,'ASADJRELOCATE','M',0,'Complete',sysdate,sysdate,sysdate,sysdate,50,'W','Y','N',1,'N','N','N','Released','N',0,'N','N','XX/D00000001','STATUSUPDATE',sysdate,'N'
            );
            commit;
            p_ok_yn_o := 'Y';
    exception
            when others
            then
                    p_ok_yn_o   := 'N';
    end create_relocate;
/***************************************************************************************************************
* SynQ Inventory reconciliation. Process and save inventory reconciliation messages
***************************************************************************************************************/
    procedure process_as_adjustment
    is
            cursor  c_adj
            is
                    select  *
                    from    cnl_sys.cnl_as_adjustment
                    where   nvl(processed,'N') = 'N'
                    order by adjustment_key asc
            ;
            --
            cursor  c_inv( b_site_id    varchar2
                         , b_client_id  varchar2
                         , b_tag_id     varchar2
                         , b_location   varchar2
                         , b_sku_id     varchar2
                         )
            is
                    select  nvl(i.qty_on_hand,0) qty_on_hand
                    ,       nvl(i.qty_allocated,0) qty_allocated
                    ,       i.config_id
                    ,       s.description
                    ,       i.owner_id
                    ,       l.work_zone
                    ,       i.origin_id
                    ,       i.condition_id
                    from    dcsdba.inventory i
                    ,       dcsdba.sku       s
                    ,       dcsdba.location  l
                    where   i.sku_id        = b_sku_id
                    and     s.sku_id        = b_sku_id
                    and     i.site_id       = b_site_id
                    and     l.site_id       = b_site_id
                    and     i.client_id     = b_client_id
                    and     s.client_id     = b_client_id
                    and     i.tag_id        = b_tag_id
                    and     i.location_id   = b_location
                    and     l.location_id   = b_location
            ;
            --
            cursor  c_chk( b_site_id    varchar2
                         , b_client_id  varchar2
                         , b_tag_id     varchar2
                         , b_location   varchar2
                         , b_sku_id     varchar2
                         )
            is
                    select count(*)
                    from    dcsdba.inventory i
                    where   i.sku_id        = b_sku_id
                    and     i.site_id       = b_site_id
                    and     i.client_id     = b_client_id
                    and     i.tag_id        = b_tag_id
                    and     i.location_id   = b_location
            ;
            --
            r_inv           c_inv%rowtype;
            r_chk           number;
            --
            l_reason        varchar2(50);
            l_ok_yn         varchar2(1);
            l_from_loc      varchar2(30);
            l_to_loc        varchar2(30);
            l_free_qty      number;
            l_qty_to_move   number;
            l_qty_to_adjust number;
            l_processed     varchar2(1);
    begin
            -- Loop true all adjustment records
            for     r_adj in c_adj
            loop
                    l_reason := r_adj.adjustment_reason;
                    --
                    case l_reason
                            when    'NOADJUSTMENT'
                            then    -- Wait untill stock reconsiliation.
                                    update  cnl_sys.cnl_as_adjustment
                                    set     processed = 'Y'
                                    where   adjustment_key = r_adj.adjustment_key;
                                    continue;
                            --
                            when    'EXTRASTOCK'
                            then    -- Wait untill stock reconsiliation.
                                    update  cnl_sys.cnl_as_adjustment
                                    set     processed = 'Y'
                                    where   adjustment_key = r_adj.adjustment_key;
                                    continue;
                            --
                            when    'MISSINGSTOCK'
                            then    -- Relocate stock to ASMISSING.
                                    l_qty_to_adjust := r_adj.adjusted_quantity-r_adj.processed_quantity;
                                    l_from_loc      := cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(r_adj.site_id) || '_STORAGE-LOCATION_LOCATION');
                                    l_to_loc        := 'ASMISSING';
                                    l_processed     := 'N';
                                    -- get available inventory.
                                    for     r_inv in c_inv( r_adj.site_id
                                                          , r_adj.client_id
                                                          , r_adj.tag_id
                                                          , l_from_loc
                                                          , r_adj.sku_id
                                                          )
                                    loop
                                            if    l_processed = 'N'
                                            then
                                                    --  QTY to relocate available
                                                    l_free_qty := r_inv.qty_on_hand - r_inv.qty_allocated;
                                                    if      l_free_qty <= 0
                                                    then
                                                            continue;
                                                    end if;
                                                    -- Set qty to move
                                                    if      l_free_qty <= l_qty_to_adjust
                                                    then
                                                            l_qty_to_move := l_free_qty;
                                                    elsif   l_free_qty > l_qty_to_adjust
                                                    then
                                                            l_qty_to_move := l_qty_to_adjust;
                                                    end if;
                                                    --        
                                                    create_relocate( p_from_loc_id_i  => l_from_loc
                                                                   , p_to_loc_id_i    => l_to_loc
                                                                   , p_tag_id_i       => r_adj.tag_id
                                                                   , p_sku_id_i       => r_adj.sku_id
                                                                   , p_client_id_i    => r_adj.client_id
                                                                   , p_site_id_i      => r_adj.site_id
                                                                   , p_qty_to_move_i  => l_qty_to_move
                                                                   , p_config_id_i    => r_inv.config_id
                                                                   , p_description_i  => r_inv.description
                                                                   , p_owner_id_i     => r_inv.owner_id
                                                                   , p_work_zone_i    => r_inv.work_zone
                                                                   , p_origin_id_i    => r_inv.origin_id
                                                                   , p_condition_id_i => r_inv.condition_id
                                                                   , p_user_id_i      => 'AUTOSTORE'
                                                                   , p_station_id_i   => 'AUTOSTORE'
                                                                   , p_ok_yn_o        => l_ok_yn
                                                                   );
                                                    --
                                                    if      l_ok_yn = 'N'
                                                    then    -- An error occured
                                                            l_processed := 'Y';
                                                            update  cnl_sys.cnl_as_adjustment
                                                            set     processed       = 'Y'
                                                            ,       proces_error    = 'Y'
                                                            where   adjustment_key = r_adj.adjustment_key;
                                                            continue;
                                                    end if;
                                                    -- set processed flag
                                                    if      l_qty_to_adjust - l_qty_to_move <= 0
                                                    then
                                                            l_processed := 'Y';
                                                    end if;
                                                    --
                                                    update  cnl_sys.cnl_as_adjustment
                                                    set     processed_quantity  = nvl(processed_quantity,0) + l_qty_to_move
                                                    ,       processed           = l_processed
                                                    where   adjustment_key      = r_adj.adjustment_key;
                                                    commit;
                                            end if;
                                    end loop c_inv;
                            --
                            when    'MISSINGSTOCKRESOLVED'
                            then    -- Cancels missing adjustment because same stock was found earlier.
                                    l_qty_to_adjust := r_adj.adjusted_quantity-r_adj.processed_quantity;
                                    l_from_loc      := 'ASFOUND';
                                    l_to_loc        := cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(r_adj.site_id) || '_STORAGE-LOCATION_LOCATION');
                                    l_processed     := 'N';
                                    -- get available inventory.
                                    for     r_inv in c_inv( r_adj.site_id
                                                          , r_adj.client_id
                                                          , r_adj.tag_id
                                                          , l_from_loc
                                                          , r_adj.sku_id
                                                          )
                                    loop
                                            if      l_processed = 'N'
                                            then
                                                    --  QTY to relocate available
                                                    l_free_qty := r_inv.qty_on_hand - r_inv.qty_allocated;
                                                    if      l_free_qty <= 0
                                                    then
                                                            continue;
                                                    end if;
                                                    -- Set qty to move
                                                    if      l_free_qty <= l_qty_to_adjust
                                                    then
                                                            l_qty_to_move := l_free_qty;
                                                    elsif   l_free_qty > l_qty_to_adjust
                                                    then
                                                            l_qty_to_move := l_qty_to_adjust;
                                                    end if;
                                                    --        
                                                    create_relocate( p_from_loc_id_i  => l_from_loc
                                                                   , p_to_loc_id_i    => l_to_loc
                                                                   , p_tag_id_i       => r_adj.tag_id
                                                                   , p_sku_id_i       => r_adj.sku_id
                                                                   , p_client_id_i    => r_adj.client_id
                                                                   , p_site_id_i      => r_adj.site_id
                                                                   , p_qty_to_move_i  => l_qty_to_move
                                                                   , p_config_id_i    => r_inv.config_id
                                                                   , p_description_i  => r_inv.description
                                                                   , p_owner_id_i     => r_inv.owner_id
                                                                   , p_work_zone_i    => r_inv.work_zone
                                                                   , p_origin_id_i    => r_inv.origin_id
                                                                   , p_condition_id_i => r_inv.condition_id
                                                                   , p_user_id_i      => 'AUTOSTORE'
                                                                   , p_station_id_i   => 'AUTOSTORE'
                                                                   , p_ok_yn_o        => l_ok_yn
                                                                   );
                                                    --
                                                    if      l_ok_yn = 'N'
                                                    then
                                                            l_processed := 'Y';
                                                            update  cnl_sys.cnl_as_adjustment
                                                            set     processed       = 'Y'
                                                            ,       proces_error    = 'Y'
                                                            where   adjustment_key = r_adj.adjustment_key;
                                                            continue;
                                                    end if;
                                                    -- set processed flag
                                                    if      l_qty_to_adjust - l_qty_to_move <= 0
                                                    then
                                                            l_processed := 'Y';
                                                    end if;
                                                    --
                                                    update  cnl_sys.cnl_as_adjustment
                                                    set     processed_quantity  = nvl(processed_quantity,0) + l_qty_to_move
                                                    ,       processed           = l_processed
                                                    where   adjustment_key      = r_adj.adjustment_key;
                                                    commit;
                                                    -- 
                                                    open   c_chk( r_adj.site_id
                                                                , r_adj.client_id
                                                                , r_adj.tag_id
                                                                , l_from_loc
                                                                , r_adj.sku_id
                                                                );
                                                    fetch   c_chk into r_chk;
                                                    close   c_chk;
                                                    if      r_chk = 0
                                                    and     l_processed = 'N'
                                                    then
                                                            update  cnl_sys.cnl_as_adjustment
                                                            set     adjustment_reason   = 'MISSINGSTOCK'
                                                            where   adjustment_key      = r_adj.adjustment_key;
                                                            commit;
                                                    end if;
                                            end if;
                                    end loop c_inv;
                            --
                            when    'EXTRASTOCKRESOLVED'
                            then    -- Cancels any extra stock found because it was lost earlier
                                    l_qty_to_adjust := r_adj.adjusted_quantity-r_adj.processed_quantity;
                                    l_from_loc      := 'ASMISSING';
                                    l_to_loc        := cnl_sys.cnl_as_pck.get_system_profile( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || upper(r_adj.site_id) || '_STORAGE-LOCATION_LOCATION');
                                    l_processed     := 'N';
                                    -- get available inventory.
                                    for     r_inv in c_inv( r_adj.site_id
                                                          , r_adj.client_id
                                                          , r_adj.tag_id
                                                          , l_from_loc
                                                          , r_adj.sku_id
                                                          )
                                    loop
                                            if      l_processed = 'N'
                                            then
                                                    --  QTY to relocate available
                                                    l_free_qty := r_inv.qty_on_hand - r_inv.qty_allocated;
                                                    if      l_free_qty <= 0
                                                    then
                                                            continue;
                                                    end if;
                                                    -- Set qty to move
                                                    if      l_free_qty <= l_qty_to_adjust
                                                    then
                                                            l_qty_to_move := l_free_qty;
                                                    elsif   l_free_qty > l_qty_to_adjust
                                                    then
                                                            l_qty_to_move := l_qty_to_adjust;
                                                    end if;
                                                    --        
                                                    create_relocate( p_from_loc_id_i  => l_from_loc
                                                                   , p_to_loc_id_i    => l_to_loc
                                                                   , p_tag_id_i       => r_adj.tag_id
                                                                   , p_sku_id_i       => r_adj.sku_id
                                                                   , p_client_id_i    => r_adj.client_id
                                                                   , p_site_id_i      => r_adj.site_id
                                                                   , p_qty_to_move_i  => l_qty_to_move
                                                                   , p_config_id_i    => r_inv.config_id
                                                                   , p_description_i  => r_inv.description
                                                                   , p_owner_id_i     => r_inv.owner_id
                                                                   , p_work_zone_i    => r_inv.work_zone
                                                                   , p_origin_id_i    => r_inv.origin_id
                                                                   , p_condition_id_i => r_inv.condition_id
                                                                   , p_user_id_i      => 'AUTOSTORE'
                                                                   , p_station_id_i   => 'AUTOSTORE'
                                                                   , p_ok_yn_o        => l_ok_yn
                                                                   );
                                                    --
                                                    if      l_ok_yn = 'N'
                                                    then
                                                            l_processed := 'Y';
                                                            update  cnl_sys.cnl_as_adjustment
                                                            set     processed = 'Y'
                                                            where   adjustment_key = r_adj.adjustment_key;
                                                            continue;
                                                    end if;
                                                    -- set processed flag
                                                    if      l_qty_to_adjust - l_qty_to_move <= 0
                                                    then
                                                            l_processed := 'Y';
                                                    end if;
                                                    --
                                                    update  cnl_sys.cnl_as_adjustment
                                                    set     processed_quantity  = nvl(processed_quantity,0) + l_qty_to_move
                                                    ,       processed           = l_processed
                                                    where   adjustment_key      = r_adj.adjustment_key;
                                                    commit;
                                                    --
                                                    open   c_chk( r_adj.site_id
                                                                , r_adj.client_id
                                                                , r_adj.tag_id
                                                                , l_from_loc
                                                                , r_adj.sku_id
                                                                );
                                                    fetch   c_chk into r_chk;
                                                    close   c_chk;
                                                    if      r_chk = 0
                                                    and     l_processed = 'N'
                                                    then
                                                            update  cnl_sys.cnl_as_adjustment
                                                            set     adjustment_reason   = 'EXTRASTOCK'
                                                            where   adjustment_key      = r_adj.adjustment_key;
                                                            commit;
                                                    end if;
                                            end if;
                                    end loop c_inv;
                            --
                            else
                                    update  cnl_sys.cnl_as_adjustment
                                    set     processed = 'Y'
                                    where   adjustment_key = r_adj.adjustment_key;
                                    continue;
                            --
                    end case l_reason;

            end loop c_adj;
    exception
            when others
            then
                null;
    end process_as_adjustment;
/***************************************************************************************************************
* Insert inventory records in recosile table
***************************************************************************************************************/
    procedure insert_reconsile_records( p_dstamp_i      in timestamp with local time zone
                                      , p_comments_i    in varchar2
                                      , p_action_i      in varchar2
                                      , p_client_id_i   in varchar2
                                      , p_sku_id_i      in varchar2
                                      , p_tag_id_i      in varchar2
                                      , p_wms_qty_i     in number
                                      , p_wms_mis_qty_i in number
                                      , p_wms_fnd_qty_i in number
                                      , p_as_qty_i      in number
                                      , p_as_sus_qty_i  in number
                                      , p_difference_i  in number
                                      , p_recon_key_i   in number
                                      , p_new_yn_i      in varchar2
                                      , p_ok_yn_o       out varchar2
                                      )
    is
        l_ok_yn     varchar2(1);
    begin
        if  p_new_yn_i = 'Y'
        then
            insert into cnl_as_reconsiliation( dstamp
                                             , comments
                                             , action
                                             , client_id
                                             , sku_id
                                             , tag_id
                                             , wms_qty_on_hand
                                             , wms_loc_asmissing
                                             , wms_loc_asfound
                                             , as_qty_on_hand
                                             , as_suspect_qty
                                             , difference
                                             )
                                       values( p_dstamp_i
                                             , p_comments_i
                                             , p_action_i
                                             , p_client_id_i
                                             , p_sku_id_i
                                             , p_tag_id_i
                                             , p_wms_qty_i
                                             , p_wms_mis_qty_i
                                             , p_wms_fnd_qty_i
                                             , p_as_qty_i
                                             , p_as_sus_qty_i
                                             , p_difference_i
                                             );
            commit;
        else
            update  cnl_sys.cnl_as_reconsiliation
            set     comments            = p_comments_i 
            ,       action              = p_action_i
            ,       client_id           = p_client_id_i
            ,       sku_id              = p_sku_id_i
            ,       tag_id              = p_tag_id_i
            ,       wms_qty_on_hand     = p_wms_qty_i
            ,       wms_loc_asmissing   = p_wms_mis_qty_i
            ,       wms_loc_asfound     = p_wms_fnd_qty_i
            ,       as_qty_on_hand      = p_as_qty_i
            ,       as_suspect_qty      = p_as_sus_qty_i
            ,       difference          = p_difference_i
            where   reconsile_key       = p_recon_key_i;
            commit;
            --
        end if;
        p_ok_yn_o := 'Y';
    exception
        when others
        then
            p_ok_yn_o := 'N';
    end insert_reconsile_records;

/***************************************************************************************************************
* SynQ Inventory reconciliation. Process and save inventory reconciliation messages
***************************************************************************************************************/
    procedure inventory_reconciliation( p_site_id_i in varchar2)
    is
        cursor  c_wms_inv( b_site_id        varchar2
                         , b_storage_loc    varchar2
                         , b_missing_loc    varchar2
                         , b_found_loc      varchar2
                         )
        is
            select  client_id
            ,       sku_id
            ,       tag_id
            ,       qty_on_hand
            ,       location_id
            from    dcsdba.inventory
            where   location_id in (b_storage_loc, b_missing_loc, b_found_loc)
            and     site_id = b_site_id
        ;
        --
        cursor  c_as_inv
        is
            select  ow.owner_id             as client_id
            ,       pr.product_id           as sku_id
            ,       lu.asn_tu_id            as tag_id
            ,       lu.quantity_on_hand
            ,       (   select  lo.location_id
                        from    rhenus_synq.location@as_synq        lo
                        where   lu.root_location_key    = lo.location_key) as location_id
            ,       lu.suspect
            ,       (   select  tu.tu_id
                        from    rhenus_synq.transport_unit@as_synq  tu
                        where   lu.tu_key = tu.tu_key) as tu_id
            from    rhenus_synq.load_unit@as_synq       lu
            ,       rhenus_synq.product@as_synq         pr
            ,       rhenus_synq.owner@as_synq           ow
            where   lu.product_key          = pr.product_key
            and     pr.owner_key            = ow.owner_key
            and     (   select  lo.location_id
                        from    rhenus_synq.location@as_synq        lo
                        where   lu.root_location_key    = lo.location_key) in ( 'FOUND'
                                                                               , 'GRID1'
                                                                               , 'MISSING'
                                                                               , 'PORT1'
                                                                               , 'PORT10'
                                                                               , 'PORT11'
                                                                               , 'PORT12'
                                                                               , 'PORT2'
                                                                               , 'WS10_DESKTOP'
                                                                               , 'WS10_ORDER'
                                                                               , 'WS10_PUTAWAY'
                                                                               , 'WS11_DESKTOP'
                                                                               , 'WS11_ORDER'
                                                                               , 'WS11_PUTAWAY'
                                                                               , 'WS12_DESKTOP'
                                                                               , 'WS12_ORDER'
                                                                               , 'WS12_PUTAWAY'
                                                                               , 'WS1_DESKTOP'
                                                                               , 'WS1_ORDER'
                                                                               , 'WS1_PUTAWAY'
                                                                               , 'WS2_DESKTOP'
                                                                               , 'WS2_ORDER'
                                                                               , 'WS2_PUTAWAY')
            ;
        --
        cursor  c_recon( b_sku_id       varchar2
                       , b_client_id    varchar2
                       , b_tag_id       varchar2
                       )
        is
            select  *
            from    cnl_sys.cnl_as_reconsiliation
            where   sku_id              = b_sku_id
            and     client_id           = b_client_id
            and     nvl(tag_id,'NOTAG') = nvl(b_tag_id,'NOTAG')
            and     rownum = 1
        ;
        --
        cursor  c_rec
        is
            select  *
            from    cnl_sys.cnl_as_reconsiliation
        ;
        --
        cursor  c_bin( b_owner       varchar2
                     , b_product     varchar2
                     , b_asn_tu_id   varchar2
                     )
        is
            select  distinct to_char(tu.tu_id) tu_id
            from    rhenus_synq.load_unit@as_synq       lu
            ,       rhenus_synq.transport_unit@as_synq  tu
            ,       rhenus_synq.owner@as_synq           ow
            ,       rhenus_synq.product@as_synq         pr            
            where   lu.suspect = 1
            and     lu.tu_key       = tu.tu_key
            and     nvl(lu.asn_tu_id,'NOTAG')    = nvl(b_asn_tu_id,'NOTAG')
            and     lu.product_key  = pr.product_key
            and     pr.product_id   = b_product
            and     pr.owner_key    = ow.owner_key
            and     ow.owner_id     = b_owner
        ;
        --    
        r_recon             c_recon%rowtype;
        --
        l_dstamp            timestamp with local time zone;
        l_comments          varchar2(4000);
        l_action            varchar2(4000);
        l_client_id         varchar2(30);
        l_sku_id            varchar2(50);
        l_tag_id            varchar2(50);
        l_wms_qty_on_hand   number;
        l_wms_loc_asmissing number;
        l_wms_loc_asfound   number;
        l_as_qty_on_hand    number;
        l_as_suspect_qty    number;
        l_difference        number;
        l_reconsile_key     number;
        --
        l_new_yn        varchar2(1);
        l_ok_yn         varchar2(1);
        l_storage_loc   varchar2(50);
        l_missing_loc   varchar2(50);
        l_found_loc     varchar2(50);
        l_bin           varchar2(400);
        --
    begin
            -- Clear table
            execute immediate 'truncate table cnl_sys.cnl_as_reconsiliation';

            -- get Autostore storage location
            l_storage_loc   := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_id_i || '_STORAGE-LOCATION_LOCATION');
            l_missing_loc   := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_id_i || '_MISSING-LOCATION_LOCATION');
            l_found_loc     := cnl_sys.cnl_as_pck.get_system_profile(p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_' || p_site_id_i || '_FOUND-LOCATION_LOCATION');

            -- Loop true all wms inventory records
            For r_wms_inv  in c_wms_inv( p_site_id_i
                                       , l_storage_loc
                                       , l_missing_loc
                                       , l_found_loc
                                       )
            loop
                    -- Check for existing records in reconsile table
                    open    c_recon( r_wms_inv.sku_id
                                   , r_wms_inv.client_id
                                   , r_wms_inv.tag_id
                                   );
                    fetch   c_recon into r_recon;
                    if      c_recon%found
                    then
                            close   c_recon;
                            l_new_yn            := 'N';
                            --
                            l_dstamp            := r_recon.dstamp;
                            l_comments          := r_recon.comments;
                            l_action            := r_recon.action;
                            l_client_id         := r_recon.client_id;
                            l_sku_id            := r_recon.sku_id;
                            l_tag_id            := r_recon.tag_id;
                            case    r_wms_inv.location_id
                                    when    l_storage_loc
                                    then
                                            l_wms_qty_on_hand   := nvl(r_wms_inv.qty_on_hand,0) + nvl(r_recon.wms_qty_on_hand,0);
                                            l_wms_loc_asmissing := nvl(r_recon.wms_loc_asmissing,0);
                                            l_wms_loc_asfound   := nvl(r_recon.wms_loc_asfound,0);
                                    when    l_missing_loc
                                    then
                                            l_wms_qty_on_hand   := nvl(r_recon.wms_qty_on_hand,0);
                                            l_wms_loc_asmissing := nvl(r_wms_inv.qty_on_hand,0) + nvl(r_recon.wms_loc_asmissing,0);
                                            l_wms_loc_asfound   := nvl(r_recon.wms_loc_asfound,0);
                                    when    l_found_loc
                                    then
                                            l_wms_qty_on_hand   := nvl(r_recon.wms_qty_on_hand,0);
                                            l_wms_loc_asmissing := nvl(r_recon.wms_loc_asmissing,0);
                                            l_wms_loc_asfound   := nvl(r_wms_inv.qty_on_hand,0) + nvl(r_recon.wms_loc_asfound,0);
                                    else
                                            continue;
                            end case;
                            l_as_qty_on_hand    := nvl(r_recon.as_qty_on_hand,0);
                            l_as_suspect_qty    := nvl(r_recon.as_suspect_qty,0);
                            l_difference        := nvl(r_recon.difference,0);
                            l_reconsile_key     := nvl(r_recon.reconsile_key,0);
                    else    --Not found
                            close   c_recon;
                            l_new_yn        := 'Y';
                            --
                            l_dstamp        := sysdate;
                            l_comments      := null;
                            l_action        := null;
                            l_client_id     := r_wms_inv.client_id;
                            l_sku_id        := r_wms_inv.sku_id;
                            l_tag_id        := r_wms_inv.tag_id;
                            case    r_wms_inv.location_id
                                    when    l_storage_loc
                                    then
                                            l_wms_qty_on_hand   := nvl(r_wms_inv.qty_on_hand,0);
                                            l_wms_loc_asmissing := 0;
                                            l_wms_loc_asfound   := 0;
                                    when    l_missing_loc
                                    then
                                            l_wms_qty_on_hand   := 0;
                                            l_wms_loc_asmissing := nvl(r_wms_inv.qty_on_hand,0);
                                            l_wms_loc_asfound   := 0;
                                    when    l_found_loc
                                    then
                                            l_wms_qty_on_hand   := 0;
                                            l_wms_loc_asmissing := 0;
                                            l_wms_loc_asfound   := nvl(r_wms_inv.qty_on_hand,0);
                                    else
                                            continue;
                            end case;
                            l_as_qty_on_hand    := 0;
                            l_as_suspect_qty    := 0;
                            l_difference        := 0;
                            l_reconsile_key     := 0;
                    end if; -- c_recon%found
                    --
                    insert_reconsile_records( p_dstamp_i        => l_dstamp
                                            , p_comments_i      => l_comments
                                            , p_action_i        => l_action
                                            , p_client_id_i     => l_client_id
                                            , p_sku_id_i        => l_sku_id
                                            , p_tag_id_i        => l_tag_id
                                            , p_wms_qty_i       => l_wms_qty_on_hand
                                            , p_wms_mis_qty_i   => l_wms_loc_asmissing
                                            , p_wms_fnd_qty_i   => l_wms_loc_asfound
                                            , p_as_qty_i        => l_as_qty_on_hand
                                            , p_as_sus_qty_i    => l_as_suspect_qty
                                            , p_difference_i    => l_difference
                                            , p_recon_key_i     => l_reconsile_key
                                            , p_new_yn_i        => l_new_yn
                                            , p_ok_yn_o         => l_ok_yn
                                            );
                    --
            end loop c_wms_inv;

            -- loop true all autostore inventory records
            for     r_as_inv in c_as_inv
            loop
                    open    c_recon( r_as_inv.sku_id
                                   , r_as_inv.client_id
                                   , r_as_inv.tag_id
                                   );
                    fetch   c_recon into r_recon;
                    if      c_recon%found
                    then
                            close   c_recon;
                            l_new_yn            := 'N';
                            --
                            l_dstamp            := r_recon.dstamp;
                            l_comments          := r_recon.comments;
                            l_action            := r_recon.action;
                            l_client_id         := r_recon.client_id;
                            l_sku_id            := r_recon.sku_id;
                            l_tag_id            := r_recon.tag_id;
                            l_wms_qty_on_hand   := nvl(r_recon.wms_qty_on_hand,0);
                            l_wms_loc_asmissing := nvl(r_recon.wms_loc_asmissing,0);
                            l_wms_loc_asfound   := nvl(r_recon.wms_loc_asfound,0);
                            case    r_as_inv.suspect
                            when    1
                            then
                                    l_as_qty_on_hand    := nvl(r_recon.as_qty_on_hand,0);
                                    l_as_suspect_qty    := nvl(r_as_inv.quantity_on_hand,0) + nvl(r_recon.as_suspect_qty,0);
                            when    0
                            then
                                    l_as_qty_on_hand    := nvl(r_as_inv.quantity_on_hand,0) + nvl(r_recon.as_qty_on_hand,0);
                                    l_as_suspect_qty    := nvl(r_recon.as_suspect_qty,0);
                            else
                                    continue;
                            end case;
                            l_difference        := nvl(r_recon.difference,0);
                            l_reconsile_key     := nvl(r_recon.reconsile_key,0);
                    else -- not found
                            close   c_recon;
                            l_new_yn            := 'Y';
                            --
                            l_dstamp            := sysdate;
                            l_comments          := null;
                            l_action            := null;
                            l_client_id         := r_as_inv.client_id;
                            l_sku_id            := r_as_inv.sku_id;
                            l_tag_id            := r_as_inv.tag_id;
                            l_wms_qty_on_hand   := 0;
                            l_wms_loc_asmissing := 0;
                            l_wms_loc_asfound   := 0;
                            case    r_as_inv.suspect
                                    when    1
                                    then
                                            l_as_qty_on_hand    := nvl(r_as_inv.quantity_on_hand,0);
                                            l_as_suspect_qty    := 0;
                                    when    0
                                    then
                                            l_as_qty_on_hand    := 0;
                                            l_as_suspect_qty    := nvl(r_as_inv.quantity_on_hand,0);
                                    else
                                            continue;
                            end case;
                            l_difference        := 0;
                            l_reconsile_key     := 0;
                    end if;  -- c_recon%found
                    --
                    insert_reconsile_records( p_dstamp_i        => l_dstamp
                                            , p_comments_i      => l_comments
                                            , p_action_i        => l_action
                                            , p_client_id_i     => l_client_id
                                            , p_sku_id_i        => l_sku_id
                                            , p_tag_id_i        => l_tag_id
                                            , p_wms_qty_i       => l_wms_qty_on_hand
                                            , p_wms_mis_qty_i   => l_wms_loc_asmissing
                                            , p_wms_fnd_qty_i   => l_wms_loc_asfound
                                            , p_as_qty_i        => l_as_qty_on_hand
                                            , p_as_sus_qty_i    => l_as_suspect_qty
                                            , p_difference_i    => l_difference
                                            , p_recon_key_i     => l_reconsile_key
                                            , p_new_yn_i        => l_new_yn
                                            , p_ok_yn_o         => l_ok_yn
                                            );
                    --
            end loop c_as_inv;

            -- loop true all reconsile records and set comments and status.
            for     r_rec in c_rec
            loop
                    l_difference    := nvl(r_rec.wms_qty_on_hand,0) - (nvl(r_rec.as_qty_on_hand,0) + nvl(r_rec.as_suspect_qty,0));
                    --
                    if      nvl(l_difference,0) = 0
                    then    -- no difference in QTY found.
                            if      nvl(r_rec.wms_loc_asmissing,0) = 0 and
                                    nvl(r_rec.wms_loc_asfound,0)   = 0
                            then    -- In WMS no QTY is stored on the missing and found locations.
                                    l_comments  := 'No differences.';
                                    l_action    := null;
                            else    -- In WMS QTY is stored on the missing and found locations.
                                    l_comments  := 'No differences. But found QTY on locations ' || l_missing_loc || ' and ' || l_found_loc || '.';
                                    l_action    := 'Any inventory on the missing or found location can be stock checked to suspense in WMS.';
                            end if;
                    else    -- Difference in QTY found.
                            if      r_rec.as_suspect_qty > 0
                            then    -- Some bins are marked as suspect.
                                    l_comments  := 'Some bins in Autostore are marked as suspect.';
                                    l_bin := null;
                                    -- Get all bins marked as suspect
                                    for     r_bin in c_bin( r_rec.client_id
                                                          , r_rec.sku_id
                                                          , r_rec.tag_id
                                                          )
                                    loop
                                            if      l_bin is null
                                            then    
                                                    l_bin := to_char(r_bin.tu_id);
                                            else
                                                    l_bin := substr(l_bin,1,380) || ', ' || substr(to_char(r_bin.tu_id),1,20);
                                            end if; -- l_bin is null
                                    end loop;
                                    l_action    := 'Execute the cycle count task or maintenance task for the Autostore bins (' || l_bin || ' ) before processing this difference.';
                            else    -- No bins marked as suspect
                                    if      r_rec.wms_qty_on_hand > r_rec.as_qty_on_hand
                                    then    -- More QTY in WMS than in Autostore.
                                            l_comments      := 'More inventory found in WMS then in Autostore.';
                                            l_action        := 'Relocate '|| l_difference || ' pieces from location ' || l_storage_loc || ' to location ' || l_missing_loc || '.' ;
                                    else    -- less QTY in WMS then in Autostore.
                                            if      r_rec.wms_loc_asmissing > 0
                                            then    -- Found matching inventory on missing location.
                                                    l_comments      := 'Less inventory found in WMS then in Autostore but found inventory on location ' || l_missing_loc || '.';
                                                    l_action        := 'Relocate the missing QTY from location ' || l_missing_loc || ' back to location ' || l_storage_loc || 
                                                                       '. Be aware to relocate directly to location '|| l_storage_loc || ' using stock relocation in WMS to prevent ASN''s to be created.';
                                            elsif   r_rec.wms_loc_asfound > 0
                                            then    -- Found matching inventory on found location.
                                                    l_comments      := 'Less inventory found in WMS then in Autostore but found inventory on location ' || l_found_loc || '.';
                                                    l_action        := 'Relocate the missing QTY from location ' || l_found_loc || ' back to location ' || l_storage_loc || 
                                                                       '. Be aware to relocate directly to location '|| l_storage_loc || ' using stock relocation in WMS to prevent ASN''s to be created.';
                                            else    -- Found no matching inventory
                                                    l_comments      := 'Less inventory found in WMS then in Autostore and no inventory on locations ' || l_missing_loc || ' and ' || l_found_loc || '.';
                                                    l_action        := 'Book inventory from Autostore by creating and executing a cycle count for this SKU and TAG. This will generate a relocate for missing QTY from location ' || l_storage_loc || 
                                                                       ' to location ' || l_missing_loc || '. Don''t forget to relocate this QTY back to ' || l_storage_loc || ' to even the levels again. Be aware to relocate directly to location '|| l_storage_loc || 
                                                                       ' using stock relocation in WMS to prevent ASN''s to be created.';
                                            end if;
                                    end if;
                            end if;
                    end if;

                    --  set other variable
                    l_new_yn            := 'N';
                    l_dstamp            := r_rec.dstamp;
                    l_client_id         := r_rec.client_id;
                    l_sku_id            := r_rec.sku_id;
                    l_tag_id            := r_rec.tag_id;
                    l_wms_qty_on_hand   := r_rec.wms_qty_on_hand;
                    l_wms_loc_asmissing := r_rec.wms_loc_asmissing;
                    l_wms_loc_asfound   := r_rec.wms_loc_asfound;
                    l_as_qty_on_hand    := r_rec.as_qty_on_hand;
                    l_as_suspect_qty    := r_rec.as_suspect_qty;
                    l_reconsile_key     := r_rec.reconsile_key;
                    --
                    insert_reconsile_records( p_dstamp_i        => l_dstamp
                                            , p_comments_i      => l_comments
                                            , p_action_i        => l_action
                                            , p_client_id_i     => l_client_id
                                            , p_sku_id_i        => l_sku_id
                                            , p_tag_id_i        => l_tag_id
                                            , p_wms_qty_i       => l_wms_qty_on_hand
                                            , p_wms_mis_qty_i   => l_wms_loc_asmissing
                                            , p_wms_fnd_qty_i   => l_wms_loc_asfound
                                            , p_as_qty_i        => l_as_qty_on_hand
                                            , p_as_sus_qty_i    => l_as_suspect_qty
                                            , p_difference_i    => l_difference
                                            , p_recon_key_i     => l_reconsile_key
                                            , p_new_yn_i        => l_new_yn
                                            , p_ok_yn_o         => l_ok_yn
                                            );
                    --
            end loop;                
    end inventory_reconciliation;
    --
    begin
    -- Initialization
    null;   
    --
end cnl_as_inventory_pck;