CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_MASTERDATA_PCK" is
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
* function to get table  key
***************************************************************************************************************/                   
    function key_f(p_tbl number)
        return number
    is
        cursor c_uom
        is
            select rhenus_synq.host_product_uom_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_bar
        is
            select rhenus_synq.host_barcode_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_sku
        is
            select rhenus_synq.host_product_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_atr
        is
            select rhenus_synq.host_product_attribute_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        cursor c_int
        is
            select rhenus_synq.host_instruction_seq.nextval@as_synq.rhenus.de 
            from    dual
        ;
        --    
        l_retval number;
    begin
        if      p_tbl = 1
        then
                open  c_uom;
                fetch c_uom into l_retval;
                close c_uom;
        elsif   p_tbl = 2
        then
                open  c_bar;
                fetch c_bar into l_retval;
                close c_bar;
        elsif   p_tbl = 3
        then
                open  c_sku;
                fetch c_sku into l_retval;
                close c_sku;
        elsif   p_tbl = 4
        then
                open  c_atr;
                fetch c_atr into l_retval;
                close c_atr;
        elsif   p_tbl = 5
        then
                open  c_int;
                fetch c_int into l_retval;
                close c_int;
        end if;
        return l_retval;
    end key_f;

/***************************************************************************************************************
* Processes the triggers from WMS.
* Add a record for any insert , update or delete on SKU, SUP, TUC, SSC, SKC in the 'To process' table.
***************************************************************************************************************/
    procedure save_wms_iud_record (p_data_type_i     in varchar2,
                                   p_action_i        in varchar2,
                                   p_client_id_i     in varchar2 default null,
                                   p_sku_id_i        in varchar2 default null,
                                   p_config_id_i     in varchar2 default null,
                                   p_tuc_i           in varchar2 default null,
                                   p_supplier_sku_i  in varchar2 default null
                                  )
    is        
        -- Fetch all sites with an Autostore system
        cursor  c_sit
        is
            select  substr(profile_id,-7) site
            from    dcsdba.system_profile
            where   parent_profile_id = '-ROOT-_USER_AUTOSTORE_SITE';

        --    
        cursor  c_sku( b_client varchar2
                     , b_config varchar2 
                     )
        is
            select  ssc.sku_id
            ,       ssc.client_id
            from    dcsdba.sku_sku_config ssc
            where   ssc.config_id = b_config
            and     (ssc.client_id = b_client or b_client is null)
        ;
        --
    begin
            -- Loop trough all sites.                                               
            for r_sit in c_sit
            loop
                    -- For future situations we can create constants where we link a site to a system id. Now only NLTLG01 is used.
                    if      r_sit.site = 'NLTLG01' 
                    then
                            if      cnl_sys.cnl_as_pck.chk_client( r_sit.site
                                                                 , p_client_id_i
                                                                 ) = 1 -- When p_client_id_i is null a 1 is returned. Pack configs can be created without a client!
                            then
                                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.save_wms_iud_record','Start saving SKU Masterdata in CNL_SYS table for SKU ' || p_sku_id_i || ', client_id ' || p_client_id_i);
                                    if      p_data_type_i = 'SKC' -- A config can be lined to multiple SKU. For every SKU an update must be created
                                    then 
                                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.save_wms_iud_record','saving SKC record Looping true all clients linked to Config');
                                            for r in        c_sku( p_client_id_i
                                                                 , p_config_id_i
                                                                 )
                                            loop
                                                    insert into cnl_sys.cnl_as_masterdata( cnl_key
                                                                                         , wms_data_tbl
                                                                                         , wms_action
                                                                                         , cnl_if_status
                                                                                         , wms_client_id
                                                                                         , wms_sku_id
                                                                                         , wms_config_id
                                                                                         , wms_tuc
                                                                                         , wms_supplier_sku_id
                                                                                         , synq_key
                                                                                         , synq_action
                                                                                         , dstamp
                                                                                         , as_site_id
                                                                                         ) 
                                                                                   values( cnl_sys.cnl_as_masterdata_seq1.nextval
                                                                                         , p_data_type_i
                                                                                         , p_action_i
                                                                                         , 'Pending'
                                                                                         , r.client_id --P_client_id_i can be null
                                                                                         , r.sku_id
                                                                                         , p_config_id_i
                                                                                         , p_tuc_i
                                                                                         , p_supplier_sku_i
                                                                                         , null
                                                                                         , null
                                                                                         , sysdate
                                                                                         , r_sit.site
                                                                                         );
                                            end loop c_sku;                                                        
                                    else
                                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.save_wms_iud_record','saving record');
                                            insert into cnl_sys.cnl_as_masterdata( cnl_key
                                                                                 , wms_data_tbl
                                                                                 , wms_action
                                                                                 , cnl_if_status
                                                                                 , wms_client_id
                                                                                 , wms_sku_id
                                                                                 , wms_config_id
                                                                                 , wms_tuc
                                                                                 , wms_supplier_sku_id
                                                                                 , synq_key
                                                                                 , synq_action
                                                                                 , dstamp
                                                                                 , as_site_id
                                                                                 ) 
                                                                           values( cnl_sys.cnl_as_masterdata_seq1.nextval
                                                                                 , p_data_type_i
                                                                                 , p_action_i
                                                                                 , 'Pending'
                                                                                 , p_client_id_i
                                                                                 , p_sku_id_i
                                                                                 , p_config_id_i
                                                                                 , p_tuc_i
                                                                                 , p_supplier_sku_i
                                                                                 , null
                                                                                 , null
                                                                                 , sysdate
                                                                                 , r_sit.site
                                                                                 );
                                    end if; -- p_data_type_i = 'SKC'
                            else
                                    null;
                            end if; --cnl_sys.cnl_as_pck.chk_client( r_sit.site, p_client_id_i) = 1
                            commit;
                    else
                            null;
                    end if; --r_sit.site = 'NLTLG01'
            end loop c_sit;
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.save_wms_iud_record',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end save_wms_iud_record;

/***************************************************************************************************************
* Insert product UOM 
***************************************************************************************************************/
    Procedure ins_product_uom( p_product_key_i      in number
                             , p_uom_id_i           in varchar2 -- Tracking level
                             , p_product_uom_tree_i in varchar2 -- Config id
                             , p_ratio_i            in number
                             , p_base_uom_i         in number
                             , p_pick_uom_i         in number
                             , p_putaway_uom_i      in number
                             , p_length_i           in number
                             , p_height_i           in number
                             , p_width_i            in number
                             , p_weight_i           in number
                             , p_volume_i           in number
                             , p_weight_tolerance_i in number
                             , p_ok_yn_o            in out number
                             , p_ret_key_o          in out number
                             )
    is
    l_ret_key   number;
    l_ok_yn     number;
    begin
        l_ret_key := key_f(1);
        insert into rhenus_synq.host_product_uom@as_synq.rhenus.de( base_uom             --number(1,0)null
                                                                  , pick_uom             --number(1,0) null
                                                                  , product_uom_tree     --Varchar2(64)null
                                                                  , putaway_uom          --number(1,0) null
                                                                  , ratio                --number(10,0)
                                                                  , uom_id               --varchar2(255)
                                                                  , weight_tolerance     --number(19,4)
                                                                  , height               --number(19,4)
                                                                  , length               --number(19,4)
                                                                  , volume               --number(19,4)
                                                                  , weight               --number(19,4)
                                                                  , width                --number(19,4)
                                                                  , product_key          --number(19,4)
                                                                  , product_uom_key
                                                                  )
                                                            values( p_base_uom_i
                                                                  , p_pick_uom_i
                                                                  , p_product_uom_tree_i
                                                                  , p_putaway_uom_i
                                                                  , p_ratio_i
                                                                  , p_uom_id_i
                                                                  , p_weight_tolerance_i
                                                                  , p_height_i
                                                                  , p_length_i
                                                                  , p_volume_i
                                                                  , p_weight_i
                                                                  , p_width_i
                                                                  , p_product_key_i
                                                                  , l_ret_key
                                                                  );
        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.ins_product_uom','Inserting product UOM in SynQ' || l_ret_key || ' ' || p_uom_id_i );
        p_ret_key_o := l_ret_key;
        l_ok_yn := 1;
        p_ok_yn_o := l_ok_yn;
    exception
        when others
        then    
             l_ok_yn := 0;
             p_ok_yn_o := l_ok_yn;
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.ins_product_uom',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end ins_product_uom;

/***************************************************************************************************************
* Insert product_barcode
***************************************************************************************************************/    
    procedure ins_product_barcode( p_dtype_i                in  varchar2
                                 , p_product_uom_key_i      in  number
                                 , p_product_barcode_id_i   in  varchar2
                                 , p_description_i          in  varchar2
                                 , p_product_key_i          in  number
                                 , p_ok_yn_o                in out number
                                 , p_ret_key_o              in out number
                                 )
    is
    l_ret_key   number;
    l_ok_yn     number;
    begin
        l_ret_key := key_f(2);
        insert into rhenus_synq.host_barcode@as_synq.rhenus.de( class_type
                                                              , product_uom_key
                                                              , product_barcode_id
                                                              , description
                                                              , product_key
                                                              , product_barcode_key
                                                              )
                                                        values( p_dtype_i
                                                              , p_product_uom_key_i
                                                              , p_product_barcode_id_i
                                                              , p_description_i
                                                              , p_product_key_i
                                                              , l_ret_key
                                                              );
        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.ins_product_barcode','Inserting barcode in SynQ ' || l_ret_key || ' ' || p_product_barcode_id_i || ' ' || P_description_i);
        p_ret_key_o := l_ret_key;
        l_ok_yn := 1;
        p_ok_yn_o := l_ok_yn;
    exception
        when others
        then    
             l_ok_yn := 0;
             p_ok_yn_o := l_ok_yn;
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.ins_product_barcode',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end ins_product_barcode;
/***************************************************************************************************************
* Get velocity for SKU
***************************************************************************************************************/
    function velocity( p_client varchar2
                     , p_sku    varchar2
                     )
        return varchar2
    is
        cursor c_vel( b_client   varchar2
                    , b_sku      varchar2
                    )
        is
            select  a.velocity
            from    rhenus_synq.product@as_synq.rhenus.de a
            where   a.class_type = 'RHENUS_PRODUCT'
            and     a.owner_key = (   select  b.owner_key
                                      from    rhenus_synq.owner@as_synq.rhenus.de b
                                      where   b.owner_id = b_client
                                  )
            and     a.product_id = b_sku
        ;
        --
        r_vel       varchar2(50);
        --
        l_retval    varchar2(50);
        --
    begin
            open    c_vel(p_client, p_sku);
            fetch   c_vel into r_vel;
            if      c_vel%notfound
            then    
                    l_retval := 'Medium';
            else
                    l_retval := r_vel;
            end if;
            close   c_vel;
            return l_retval;
    end velocity;
/***************************************************************************************************************
* product Master. Process any insert, update or delete on SKU related data.
***************************************************************************************************************/
    procedure get_largest_box( p_client_id_i    in  varchar2
                             , p_vol_tu_type_o  out varchar2    -- biggest box
                             , p_vol_volume_o   out number      -- volume of biggest box
                             , p_vol_weight_o   out number      -- max weight of biggest box.
                             , p_wht_tu_type_o  out varchar2    -- Strongest box
                             , p_wht_volume_o   out number      -- Max volume of strongest box
                             , p_wht_weight_o   out number      -- Max weight of strongest box
                             )
    is
        cursor  c_box(b_owner_id    varchar2)
        is
            select  nvl(tu.volume,0)                                     max_volume
            ,       (nvl(tu.height,0)*nvl(tu.width,0)*nvl(tu.length,0))  calc_volume
            ,       nvl(tu.weight,0)                                     max_weight
            ,       tu_type_id
            from rhenus_synq.tu_type@as_synq tu
            where   tu.tu_type_id in (  select  tm.tu_type_id
                                        from    rhenus_synq.owner_tu_type_mapping@as_synq tm
                                        where   tm.owner_key = (    select  ow.owner_key
                                                                    from    rhenus_synq.owner@as_synq ow
                                                                    where   ow.owner_id = b_owner_id))
        ;
        --
        l_vol_volume    number := 0;
        l_vol_weight    number := 0;
        l_vol_tu_type   varchar2(20);
        l_wht_volume    number := 0;
        l_wht_weight    number := 0;
        l_wht_tu_type   varchar2(20);
        --
    begin
        for r in c_box(p_client_id_i)
        loop
            if  r.max_volume = 0
            then
                if  r.calc_volume > l_vol_volume
                then
                    l_vol_volume    := r.calc_volume;
                    l_vol_weight    := r.max_weight;
                    l_vol_tu_type   := r.tu_type_id;
                end if;
            else
                if r.max_volume > l_vol_volume
                then
                    l_vol_volume    := r.max_volume;
                    l_vol_weight    := r.max_weight;
                    l_vol_tu_type   := r.tu_type_id;
                end if;
            end if;
            if  r.max_weight > l_wht_weight
            then
                    if r.max_volume = 0
                    then
                        l_wht_volume    := r.calc_volume;
                    else
                        l_wht_volume    := r.max_volume;
                    end if;
                    l_wht_weight    := r.max_weight;
                    l_wht_tu_type   := r.tu_type_id;
            end if;
        end loop;  
        p_vol_tu_type_o  := l_vol_tu_type;
        p_vol_volume_o   := l_vol_volume;
        p_vol_weight_o   := l_vol_weight;
        p_wht_tu_type_o  := l_wht_tu_type;
        p_wht_volume_o   := l_wht_volume;
        p_wht_weight_o   := l_wht_weight;
    end get_largest_box;
/***************************************************************************************************************
* product Master. Process any insert, update or delete on SKU related data.
***************************************************************************************************************/
    procedure synq_product_master
    is
        -- get data to share with SynQ
        cursor  c_dat
        is
            select  m.as_site_id
            ,       m.wms_client_id
            ,       m.wms_sku_id
            ,       m.cnl_key
            ,       m.wms_data_tbl
            ,       m.wms_action
            ,       m.wms_config_id
            from    cnl_sys.cnl_as_masterdata m
            where   cnl_if_status = 'Pending'
            order by cnl_key asc
        ;

        --check if product is new or update
        cursor c_prd( b_product_id  varchar2
                    , b_owner_id    varchar2
                    )
        is
                select  count(*)
                from    rhenus_synq.product@as_synq.rhenus.de a
                where   a.product_id = b_product_id
                and     a.owner_key = ( select  b.owner_key
                                        from    rhenus_synq.owner@as_synq.rhenus.de b
                                        where   owner_id = b_owner_id
                                      )
        ;
        -- get WMS sku data
        cursor c_sku( b_client_id   varchar2
                    , b_sku_id      varchar2
                    )
        is
            select  s.sku_id
            ,       s.upc
            ,       s.ean
            ,       s.qc_status
            ,       round(s.each_weight*1000,4)        each_weight
            ,       round(s.each_volume*1000000000,4)  each_volume
            ,       round(s.each_depth*1000,4)         each_depth
            ,       round(s.each_width*1000,4)         each_width
            ,       round(s.each_height*1000,4)        each_height
            ,       s.serial_at_pick
            ,       s.fragile
            ,       s.description
            ,       s.user_def_chk_1                    not_scannable
            from    dcsdba.sku s
            where   s.client_id = b_client_id
            and     s.sku_id    = b_sku_id
        ;

        -- Get WMS supplier SKU data
        cursor c_sup( b_client_id   varchar2
                    , b_sku_id      varchar2
                    )
        is
            select  s.supplier_sku_id
            from    dcsdba.supplier_sku s
            where   s.client_id = b_client_id
            and     s.sku_id    = b_sku_id

        ;        
        --
        r_prd       number;
        r_sku       c_sku%rowtype;
        --
        l_weight_chk            number;  -- Weight check
        l_tolerance             number;  -- weight tolerance
        l_tolerance_percent     number; 
        l_serial                varchar2(20);
        l_velocity              varchar2(50);
        l_action                varchar2(3);  -- Interface action
        l_product_key           number;
        l_width_1               number    := 0;
        l_depth_1               number    := 0;
        l_height_1              number    := 0;
        l_volume_1              number    := 0;
        l_weight_1              number    := 0;    
        l_uom_ins_ok            number;
        l_product_uom_key       number;
        l_barcode_id            varchar2(40);
        l_description           varchar2(80);
        l_bar_ins_ok            number;        
        l_product_barcode_key   number;
        l_product_attribute_key number;
        l_instruction_key       number;
        l_message_key           number;
        l_largest_box           number;
        l_cubing_required       number;
        --
        l_next                  number;
        l_level                 number;
        l_vol_volume            number := 0;
        l_vol_weight            number := 0;
        l_vol_tu_type           varchar2(20);
        l_wht_volume            number := 0;
        l_wht_weight            number := 0;
        l_wht_tu_type           varchar2(20);
        l_scan_validation_mode  varchar2(10);
        --
    begin
            -- Process all pending masterdata records
            for r_dat in c_dat
            loop
                    -- It is possible that a SKU that is inserted already exist due to a delete action earlier
                    open    c_prd( r_dat.wms_sku_id, r_dat.wms_client_id);
                    fetch   c_prd into r_prd;
                    close   c_prd;

                    -- Get volume of largest box.
                    get_largest_box( p_client_id_i    => r_dat.wms_client_id
                                   , p_vol_tu_type_o  => l_vol_tu_type
                                   , p_vol_volume_o   => l_vol_volume
                                   , p_vol_weight_o   => l_vol_weight
                                   , p_wht_tu_type_o  => l_wht_tu_type
                                   , p_wht_volume_o   => l_wht_volume
                                   , p_wht_weight_o   => l_wht_weight
                                   );

                    -- Only NLTLG01 currently has an Autostore
                    if      r_dat.as_site_id != 'NLTLG01'
                    then
                            update  cnl_sys.cnl_as_masterdata
                            set     cnl_if_status   = 'Cancelled'
                            where   cnl_key         = r_dat.cnl_key;
                            commit;
                            continue;               -- Move to next iteration in loop
                    end if;

                    -- Fetch sku data.
                    open    c_sku( r_dat.wms_client_id, r_dat.wms_sku_id);
                    fetch   c_sku into r_sku;
                    if      c_sku%notfound 
                    then
                            close   c_sku;
                            update  cnl_sys.cnl_as_masterdata
                            set     cnl_if_status   = 'Cancelled'
                            where   cnl_key         = r_dat.cnl_key;
                            commit;
                            continue;
                    else
                            close   c_sku;
                    end if; -- c_sku%notfound 

                    -- Cubing possible y/n
                    if      r_sku.each_volume > l_vol_volume
                    then
                            l_cubing_required := 0; -- No box exists that can hold this product.
                    else
                            l_cubing_required := 1;
                    end if;

                    -- Get maximum weight for weight check. When more then one site is returned the first site found detirmines the criteria
                    if      l_weight_chk is null
                    then
                            l_weight_chk := nvl(cnl_sys.cnl_as_pck.wht_chk_req( nvl(r_sku.each_weight,0),r_dat.as_site_id),0);
                    end if; --l_weight_chk is null

                    -- Get weigh tolerance
                    if      l_tolerance_percent is null
                    then
                            l_tolerance_percent := 100-nvl(cnl_sys.cnl_as_pck.wht_tolerance(r_dat.as_site_id),0);
                    end if;--l_tolerance is null

                    -- Serial controlled y/n
                    if      nvl(r_sku.serial_at_pick,'N') = 'Y'
                    then    
                            l_serial := 'SERIAL';
                    else    
                            l_serial := null;
                    end if; --nvl(r_sku.serial_at_pick,'N') = 'Y'

                    -- Get velocity from SynQ if product already exist.
                    l_velocity := velocity(r_dat.wms_client_id, r_dat.wms_sku_id);

                    -- Define Action for SynQ.
                    if      r_dat.wms_data_tbl = 'SKU' and r_dat.wms_action = 'I' and r_prd > 0 
                    then
                            l_action := 'UPD';
                    elsif   r_dat.wms_data_tbl = 'SKU' and r_dat.wms_action = 'I' and r_prd = 0 
                    then
                            l_action := 'NEW';
                    elsif   r_dat.wms_data_tbl = 'SKU' and r_dat.wms_action = 'U' and r_prd = 0 
                    then
                            l_action := 'NEW';
                    elsif   r_dat.wms_data_tbl = 'SKU' and r_dat.wms_action = 'U' and r_prd > 0 
                    then    
                            l_action := 'UPD';
                    elsif   r_dat.wms_data_tbl = 'SUP'
                    then
                            l_action := 'UPD';
                    else    
                            l_action := 'UPD';
                    end if;
                    --

                    -- Set scan validation mode
                    if      r_sku.not_scannable = 'N'
                    then
                            l_scan_validation_mode := 'ONCE';
                    else
                            l_scan_validation_mode := 'NONE';
                    end if;

                    -- Insert product 
                    l_product_key := key_f(3);
                    insert into rhenus_synq.host_product@as_synq.rhenus.de( product_id
                                                                          , owner_id
                                                                          , avoid_envelope
                                                                          , cycle_count_window
                                                                          , tracked_attribute_name
                                                                          , cubing_required
                                                                          , description
                                                                          , product_category_id
                                                                          , velocity
                                                                          , shelf_life
                                                                          , shelf_life_controlled
                                                                          , expiration_window
                                                                          , allocation_strategy
                                                                          , allocation_tolerance_window
                                                                          , inventory_sorting
                                                                          , relevant_date_for_allocation
                                                                          , scan_validation_mode
                                                                          , weight_validation
                                                                          , default_product_uom_tree
                                                                          , uom_id
                                                                          , host_uom_id
                                                                          , image_path
                                                                          , product_family_id
                                                                          , product_key
                                                                          ) 
                                                                    values( r_dat.wms_sku_id
                                                                          , r_dat.wms_client_id
                                                                          , decode(nvl(r_sku.fragile,'N'),'N',0,1)
                                                                          , null             -- cycle_count_window
                                                                          , l_serial         -- tracked_attribute_name
                                                                          , l_cubing_required
                                                                          , r_sku.description
                                                                          , null             -- product_catagory_id
                                                                          , l_velocity
                                                                          , 0                -- shelf_life
                                                                          , 0                -- Shelf life controlled               
                                                                          , 0                -- Epiration window
                                                                          , 0                -- allocation strategy
                                                                          , 0                -- allocation tolerance window
                                                                          , null             -- inventory sorting
                                                                          , null             -- relevant date for allocation
                                                                          , l_scan_validation_mode  
                                                                          , l_weight_chk     -- 0 no check, 1 check
                                                                          , 'DEFAULT'        -- default_product_uom_tree
                                                                          , null             -- uom_id
                                                                          , null             -- host_uom_id
                                                                          , null             -- image_path
                                                                          , null             -- product family id
                                                                          , l_product_key
                                                                          );
                    --
                    l_width_1   := nvl(r_sku.each_width,0);
                    l_depth_1   := nvl(r_sku.each_depth,0);
                    l_height_1  := nvl(r_sku.each_height,0);
                    l_volume_1  := nvl(r_sku.each_volume,0);
                    l_weight_1  := nvl(r_sku.each_weight,0);
                    l_tolerance := l_weight_1/100*l_tolerance_percent;
                    -- create UOM tree default with max values per tracking level
                    ins_product_uom ( p_product_key_i           => l_product_key
                                    , p_uom_id_i                => 'EACH'
                                    , p_product_uom_tree_i      => 'DEFAULT'
                                    , p_ratio_i                 => 1
                                    , p_base_uom_i              => 1
                                    , p_pick_uom_i              => null
                                    , p_putaway_uom_i           => null
                                    , p_length_i                => l_depth_1
                                    , p_height_i                => l_height_1
                                    , p_width_i                 => l_width_1
                                    , p_weight_i                => l_weight_1
                                    , p_volume_i                => l_volume_1
                                    , p_weight_tolerance_i      => l_tolerance
                                    , p_ok_yn_o                 => l_uom_ins_ok
                                    , p_ret_key_o               => l_product_uom_key
                                    );

                    -- Insert barcodes
                    l_next  := 1;
                    l_level := 1;
                    -- insert SKU, EAN, UPC barcodes
                    while   l_next = 1 
                    loop                                    
                            if      l_level = 1 -- SKU
                            then
                                    l_barcode_id  := r_sku.sku_id;
                                    l_description := 'Barcode for SKU ID ' || r_sku.sku_id;
                            end if; --l_level = 1
                            --
                            if      l_level = 2 -- EAN
                            then
                                    l_barcode_id  := r_sku.ean;
                                    l_description := 'EAN number for SKU ' || r_sku.sku_id;
                            end if; -- l_level = 2
                            --
                            if      l_level = 3 -- UPC
                            then
                                    l_barcode_id  := r_sku.upc;
                                    l_description := 'UPC number for SKU ' || r_sku.sku_id;
                            end if; -- l_level = 3
                            --
                            ins_product_barcode( p_dtype_i                => 'PRODUCT_BARCODE'
                                               , p_product_uom_key_i      => null
                                               , p_product_barcode_id_i   => l_barcode_id
                                               , p_description_i          => l_description
                                               , p_product_key_i          => l_product_key
                                               , p_ok_yn_o                => l_bar_ins_ok
                                               , p_ret_key_o              => l_product_barcode_key
                                               );
                            --
                            case
                                    when    l_level = 1 
                                    and     r_sku.ean is not null
                                    then    
                                            l_level := 2; -- EAN must be added as barcode
                                    when    l_level = 1 
                                    and     r_sku.upc is not null 
                                    then    
                                            l_level := 3; -- UPC must be added as barcode
                                    when    l_level = 1 
                                    and     r_sku.ean is null 
                                    and     r_sku.upc is null
                                    then    
                                            l_next := 0; -- No more barcodes from SKU table
                                    when    l_level = 2 
                                    and     r_sku.upc is not null
                                    then    
                                            l_level := 3; -- UPC must be added as barcode
                                    when    l_level = 2 
                                    and     r_sku.upc is null
                                    then    
                                            l_next := 0; -- No more barcodes from SKU table
                                    when    l_level = 3
                                    then    
                                            l_next := 0;  -- No more barcodes from SKU table
                            end case;
                            --
                    end loop; -- l_next = 1 

                    -- insert supplier sku barcodes
                    For r_sup in c_sup( r_dat.wms_client_id
                                      , r_dat.wms_sku_id
                                      )
                    loop
                            l_product_barcode_key := key_f(2);
                            insert into rhenus_synq.host_barcode@as_synq.rhenus.de( class_type
                                                                                  , product_uom_key
                                                                                  , product_barcode_id
                                                                                  , description
                                                                                  , product_key
                                                                                  , product_barcode_key
                                                                                  )
                                                                            values( 'PRODUCT_BARCODE' -- class_type
                                                                                  , null
                                                                                  , r_sup.supplier_sku_id
                                                                                  , 'Supplier SKU id for SKU ' || r_dat.wms_sku_id
                                                                                  , l_product_key
                                                                                  , l_product_barcode_key
                                                                                  );
                    end loop c_sup;

                    -- insert SKU attribute BATCH
                    if      r_sku.qc_status is not null
                    then 
                            l_product_attribute_key := key_f(4);
                            insert into rhenus_synq.host_product_attribute@as_synq.rhenus.de( attribute_id
                                                                                            , check_in_behavior
                                                                                            , default_value
                                                                                            , picking_behavior
                                                                                            , put_away_behavior
                                                                                            , product_key
                                                                                            , product_attribute_key
                                                                                            )
                                                                                      values( 'BATCH'
                                                                                            , 'NONE'
                                                                                            , null
                                                                                            , 'NONE'--'VALIDATE'
                                                                                            , 'NONE'
                                                                                            , l_product_key
                                                                                            , l_product_attribute_key
                                                                                            );
                    end if; --r_sku.qc_status is not null

                    -- insert SKU attribute TAG
                    l_product_attribute_key := key_f(4);
                    insert into rhenus_synq.host_product_attribute@as_synq.rhenus.de( attribute_id
                                                                                    , check_in_behavior
                                                                                    , default_value
                                                                                    , picking_behavior
                                                                                    , put_away_behavior
                                                                                    , product_key
                                                                                    , product_attribute_key
                                                                                    ) 
                                                                              values( 'TAG'
                                                                                    , 'NONE'
                                                                                    , null
                                                                                    , 'NONE'
                                                                                    , 'NONE'
                                                                                    , l_product_key
                                                                                    , l_product_attribute_key
                                                                                    );
                    -- insert SKU instructions
                    if      nvl(r_sku.fragile,'N') = 'Y'
                    then
                            l_instruction_key := key_f(5);
                            insert into rhenus_synq.host_instruction@as_synq.rhenus.de( product_key
                                                                                      , instruction_key
                                                                                      , instruction_text
                                                                                      , role
                                                                                      , class_type
                                                                                      )
                                                                                values( l_product_key
                                                                                      , l_instruction_key
                                                                                      , 'Not allowed to use an envelope for packing'
                                                                                      , 'PICK'
                                                                                      , 'PRODUCT_INSTRUCTION'
                                                                                      );
                    end if; --nvl(r_sku.fragile,'N') = 'Y'
                    --
/* Removed to reduce steps at pick port. This part creates a flag at picking that an operator must check to confirm the action was done
		    if      nvl(r_sku.serial_at_pick,'N') = 'Y'
                    then    
                            l_instruction_key := key_f(5);
                            insert into rhenus_synq.host_instruction@as_synq.rhenus.de( product_key
                                                                                      , instruction_key
                                                                                      , instruction_text
                                                                                      , role
                                                                                      , class_type
                                                                                      )
                                                                                values( l_product_key
                                                                                      , l_instruction_key  
                                                                                      , 'Serial number must be captured.'
                                                                                      , 'PICK'
                                                                                      , 'PRODUCT_INSTRUCTION'
                                                                                      );
                    end if; -- nvl(r_sku.serial_at_pick,'N') = 'Y'
*/                    --
                    if      r_sku.qc_status is not null
                    then
                            l_instruction_key := key_f(5);
                            insert into rhenus_synq.host_instruction@as_synq.rhenus.de( product_key
                                                                                      , instruction_key
                                                                                      , instruction_text
                                                                                      , role
                                                                                      , class_type
                                                                                      )
                                                                                values( l_product_key
                                                                                      , l_instruction_key
                                                                                      , 'Batch id must be validated.'
                                                                                      , 'PICK'
                                                                                      , 'PRODUCT_INSTRUCTION'
                                                                                      );
                    end if; -- r_sku.qc_status is not null
                    --
                    commit;

                    -- Create message exchange
                    cnl_sys.cnl_as_pck.create_message_exchange( p_message_id_i              => to_char(r_dat.cnl_key)
                                                              , p_message_status_i          => 'UNPROCESSED'
                                                              , p_message_type_i            => 'ProductMaster'
                                                              , p_trans_code_i              => l_action
                                                              , p_host_message_table_key_i  => l_product_key
                                                              , P_key_o                     => l_message_key
                                                              );
                    --
                    -- Update processed record
                    update  cnl_sys.cnl_as_masterdata
                    set     synq_key        = l_message_key
                    ,       cnl_if_status   = 'Shared'
                    ,       synq_action     = l_action
                    where   cnl_key         = r_dat.cnl_key;
                    --
                    commit;
                    --
            end loop c_dat;
            --
    exception
        when others
        then    
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_masterdata_pck.synq_product_master',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
                --
    end synq_product_master;    --
    begin
    -- Initialization
    null;   
end cnl_as_masterdata_pck;
-- show errors;