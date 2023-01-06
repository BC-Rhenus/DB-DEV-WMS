CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: WMS functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
  type ref_cur is ref cursor;
--
-- Private constant declarations
--
  g_yes                      constant varchar2(1)               := 'Y';
  g_no                       constant varchar2(1)               := 'N';
  g_true                     constant varchar2(20)              := 'TRUE';
  g_false                    constant varchar2(20)              := 'FALSE';
  g_cs_error                 constant varchar2(10)              := 'CSERROR';
  g_cs_pending               constant varchar2(10)              := 'CSPENDING';
  g_cs_required              constant varchar2(10)              := 'CSREQUIRED';
  g_cs_succes                constant varchar2(10)              := 'CSSUCCES';
  g_hold                     constant varchar2(10)              := 'Hold';
  g_picked                   constant varchar2(15)              := 'Picked';
  g_packed                   constant varchar2(15)              := 'Packed';
  g_ready_to_load            constant varchar2(15)              := 'Ready to Load';
  g_complete                 constant varchar2(15)              := 'Complete';
  g_shipped                  constant varchar2(10)              := 'Shipped';
  g_delivered                constant varchar2(10)              := 'Delivered';
  g_jr_urepctopackparcel     constant varchar2(30)              := 'UREPCTOPACKPARCEL';
  g_jr_urepctocancelparcel   constant varchar2(30)              := 'UREPCTOCANCELPARCEL';
  g_jr_urepparcelpacking     constant varchar2(30)              := 'UREPPARCELPACKING';
  g_jr_urepssvplt            constant varchar2(30)              := 'UREPSSVPLT';
  g_jr_urepssvtrl            constant varchar2(30)              := 'UREPSSVTRL';
  g_jr_urepssvpltcon         constant varchar2(30)              := 'UREPSSVPLTCON';
  g_jr_urepssvpltpal         constant varchar2(30)              := 'UREPSSVPLTPAL';
  g_jr_urepstreamserve       constant varchar2(30)              := 'UREPSTREAMSERVE';
  g_rtk_cmd_cto_packparcel   constant varchar2(30)              := 'CTO_PACKPARCEL';
  g_rtk_cmd_cto_cancelparcel constant varchar2(30)              := 'CTO_CANCELPARCEL';
  g_rtk_cmd_pallet_closing   constant varchar2(30)              := 'PALLET_CLOSING';
  g_rtk_cmd_parcel_packing   constant varchar2(30)              := 'PARCEL_PACKING';
  g_rtk_cmd_repacking        constant varchar2(30)              := 'REPACKING';
  g_rtk_cmd_ssv_plt_all      constant varchar2(30)              := 'SSV_PLT_ALL';
  g_rtk_cmd_ssv_trl_all      constant varchar2(30)              := 'SSV_TRL_ALL';
  g_rtk_cmd_ssv_plt_con      constant varchar2(30)              := 'SSV_PLT_CON';
  g_rtk_cmd_ssv_plt_pal      constant varchar2(30)              := 'SSV_PLT_PAL';
  g_rtk_par_site_id          constant varchar2(20)              := 'site_id';
  g_rtk_par_list_id          constant varchar2(30)              := 'list_id';
  g_rtk_par_client_id        constant varchar2(20)              := 'client_id';
  g_rtk_par_owner_id         constant varchar2(20)              := 'owner_id';
  g_rtk_par_order_id         constant varchar2(20)              := 'order_id';
  g_rtk_par_pallet_id        constant varchar2(20)              := 'pallet_id';
  g_rtk_par_container_id     constant varchar2(20)              := 'container_id';
  g_rtk_par_unit_id          constant varchar2(20)              := 'unit_id';
  g_rtk_par_locality         constant varchar2(20)              := 'locality';
  g_rtk_par_rdtlocality      constant varchar2(20)              := 'rdtlocality';
  g_rtk_par_linked_to_dws    constant varchar2(20)              := 'linked_to_dws';
  g_rtk_par_pdf_link         constant varchar2(20)              := 'pdf_link';
  g_rtk_par_pdf_autostore    constant varchar2(20)              := 'pdf_autostore';
  g_pae                      constant varchar2(30)              := 'PreAdv Status';
  g_odr                      constant varchar2(30)              := 'Order Status';
  g_ajt                      constant varchar2(30)              := 'Adjustment';
  g_dws                      constant varchar2(20)              := 'DWS';
  g_wms_wait_sec_rpk_mtk     constant cnl_constants.value%type  := cnl_util_pck.get_constant( p_name_i => 'WMS_WAIT_SEC_RPK_MTK');
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 17-Jun-2016
-- Purpose : Function to get the parameters from the command in RUN_TASK
------------------------------------------------------------------------------------------------
	function get_rtk_params ( p_key_i       in number
                                , p_parameter_i in varchar2
				)
		return varchar2
	is
		cursor c_rtk( b_key       in number
			    , b_parameter in varchar2
			    )
		is
			select substr( rtk.command
				     , (instr( rtk.command, '"', instr( rtk.command, b_parameter), 2) + 1) 
				     , (instr( rtk.command, '"', instr( rtk.command, b_parameter), 3)) - (instr( rtk.command, '"', instr( rtk.command, b_parameter), 2) + 1)
				     ) param_value
			from   cnl_tmp_run_task rtk--dcsdba.run_task rtk
			where  rtk.key         = b_key
		;
		--
		l_param_value	varchar2(50);
	begin
		open  	c_rtk( b_key       => p_key_i
			     , b_parameter => p_parameter_i
			     );
		fetch 	c_rtk
		into  	l_param_value;
		close 	c_rtk;
		--
		return l_param_value;

	end get_rtk_params;  
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 17-Jun-2016
-- Purpose : Function to check if the restriction (extra_parameters) in the Java_Report_map is valid
------------------------------------------------------------------------------------------------
  function is_ohr_restriction_valid( p_client_id_i   in varchar2
                                   , p_order_id_i    in varchar2
                                   , p_where_i       in varchar2
                                   )
    return integer
  is
    c_print  ref_cur;
    l_retval integer := 0;  -- 0 = not valid, 1 = valid
    l_query  varchar2(4000);
  begin
    l_query := 'select 1 from dcsdba.order_header where'
            || ' client_id = :client_id'
            || ' and order_id  = :order_id'
            || ' and ('
            || p_where_i
            || ')'
            ;
    begin
      open  c_print 
      for   l_query using p_client_id_i, p_order_id_i;
      fetch c_print 
      into  l_retval;
      close c_print;
    end;

    return nvl(l_retval, 0);

  end is_ohr_restriction_valid;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Jan-2017
-- Purpose : Check if Customs Streamliner is used for a Client for Bonded Warehousing (Entrepot)
------------------------------------------------------------------------------------------------
  function is_csl_enabled ( p_client_id_i in  varchar2)
    return integer
  is
    cursor c_clt ( b_client_id in varchar2)
    is
    select 1
    from   dcsdba.client_group_clients
    where  client_id    = b_client_id
    and    client_group = 'CSLENT'    -- Client Visibility Group with Bonded Warehouse Customers
    ;

    l_retval integer := 0;  -- 0 = not valid, 1 = valid
  begin
    open  c_clt ( b_client_id => p_client_id_i);
    fetch c_clt
    into  l_retval;
    close c_clt;

    return l_retval;

  end is_csl_enabled;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Jan-2017
-- Purpose : Check if Customs Streamliner is used for a Client for Export declarations
------------------------------------------------------------------------------------------------
  function is_exp_enabled ( p_client_id_i in  varchar2)
    return integer
  is
    cursor c_clt ( b_client_id in varchar2)
    is
    select 1
    from   dcsdba.client_group_clients
    where  client_id    = b_client_id
    and    client_group = 'CSLEXP'    -- Client Visibility Group with Clients which require Export declarations
    ;

    l_retval integer := 0;  -- 0 = not valid, 1 = valid
  begin
    open  c_clt ( b_client_id => p_client_id_i);
    fetch c_clt
    into  l_retval;
    close c_clt;

    return l_retval;

  end is_exp_enabled;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Jun-2016
-- Purpose : Process SaveOrder requests from WMS
------------------------------------------------------------------------------------------------
  procedure process_saveorder
  is
    cursor c_ohr
    is
      select ohr.from_site_id
      ,      ohr.client_id
      ,      ohr.order_id
      from   dcsdba.order_header    ohr
      where  ohr.status             = g_hold
      and    ohr.status_reason_code = g_cs_required
      and    nvl(ohr.tm_stop_seq,0)    < 1000 -- Process failes when this value is 9999 and due to accidental loops in merge rules this happens
      and    ohr.client_id not in (	select 	cc.client_id
					from	dcsdba.client_group_clients cc
					where	cc.client_group = 'CTOSAAS'
					and	cc.client_id = ohr.client_id
				)
      order  by ohr.uploaded_ws2pc_id
      --for    update of ohr.status_reason_code
      ;

    l_ok         integer;
    l_mergeerror varchar2(10);
  begin
    for r_ohr in c_ohr
    loop
      if r_ohr.from_site_id is null
      then
        -- update order to CSERROR
        update dcsdba.order_header ohr
        set    ohr.status_reason_code = g_cs_error
        ,      ohr.instructions       = 'Error - From Site ID not filled in Order Header'
        where  ohr.client_id          = r_ohr.client_id
        and    ohr.order_id           = r_ohr.order_id
        ;                                                         
      else
        cnl_centiro_pck.create_saveorder ( p_site_id_i   => r_ohr.from_site_id
                                         , p_client_id_i => r_ohr.client_id
                                         , p_order_id    => r_ohr.order_id
                                         );
        -- update order to CSPENDING
        update dcsdba.order_header ohr
        set    ohr.status_reason_code = g_cs_pending
        where  ohr.from_site_id       = r_ohr.from_site_id
        and    ohr.client_id          = r_ohr.client_id
        and    ohr.order_id           = r_ohr.order_id
        ;
        commit;
      end if;                                                         
    end loop;
  exception
    when others
    then
      if c_ohr%isopen
      then
        close c_ohr;
      end if;
    raise;
  end process_saveorder;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 16-Jun-2016
-- Purpose : Process PackParcelResponse (Centiro) from Seeburger (cnl_api_pck.cto_pp_response) 
------------------------------------------------------------------------------------------------
  procedure process_packparcelresponse ( p_errorcode_i     in  varchar2
                                       , p_errormessage_i  in  varchar2
                                       , p_clientid_i      in  varchar2
                                       , p_carrier_i       in  varchar2
                                       , p_service_i       in  varchar2
                                       , p_orderno_i       in  varchar2
                                       , p_shipmentid_i    in  varchar2
                                       , p_sequenceno_i    in  varchar2
                                       , p_parcelid_i      in  varchar2
                                       , p_trackingno_i    in  varchar2
                                       , p_trackingurl_i   in  varchar2 := null
                                       , p_error_o         out varchar2
                                       , p_errortext_o     out varchar2
                                       )
  is
    cursor c_clt ( b_cto_client in varchar2)
    is
      select substr( b_cto_client, 1, decode( instr( b_cto_client, '@'), 0, length( b_cto_client)
                                                                          , instr( b_cto_client, '@') - 1
                                            )
                   ) client_id
      ,      substr( b_cto_client, decode( instr( b_cto_client, '@'), 0, null
                                                                       , instr( b_cto_client, '@') + 1
                                         )                                    
                   ) site_id
      from   dual      
      ;
    cursor c_ocr_con ( b_container_id in varchar2)
    is
      select ocr.container_id
      ,      ocr.container_type
      ,      ocr.pallet_id
      ,      ocr.config_id                                 pallet_type
      ,      ocr.container_n_of_n
      ,      ohr.from_site_id                              site_id
      ,      ocr.client_id
      ,      ohr.owner_id
      ,      ocr.order_id
      ,      ohr.customer_id
      ,      ohr.carrier_id
      ,      ohr.service_level
      ,      nvl( ocr.container_weight, ocr.pallet_weight) weight
      ,      nvl( ocr.container_height, ocr.pallet_height) height
      ,      nvl( ocr.container_width,  ocr.pallet_width)  width
      ,      nvl( ocr.container_depth,  ocr.pallet_depth)  depth
      ,      'JDA2016'                                     wms_database
      from   dcsdba.order_header    ohr
      ,      dcsdba.order_container ocr
      where  ohr.client_id          = ocr.client_id
      and    ohr.order_id           = ocr.order_id
      and    ocr.container_id       = b_container_id
      ;
    cursor c_ocr_pal ( b_pallet_id in varchar2)
    is
      select distinct -- 1 record only
             '0000000000'                      container_id -- By Pallet ID so default Container ID for CDA table = '0000000000'
      ,      null                              container_type
      ,      ocr.pallet_id
      ,      ocr.config_id                     pallet_type
      ,      null                              container_n_of_n
      ,      ohr.from_site_id                  site_id
      ,      ocr.client_id
      ,      ohr.owner_id
      ,      ocr.order_id
      ,      ohr.customer_id
      ,      ohr.carrier_id
      ,      ohr.service_level
      ,      max( nvl( ocr.pallet_weight, 1))  weight
      ,      max( nvl( ocr.pallet_height, 1))  height
      ,      max( nvl( ocr.pallet_width,  1))  width
      ,      max( nvl( ocr.pallet_depth,  1))  depth
      ,      'JDA2016'                         wms_database
      from   dcsdba.order_header               ohr
      ,      dcsdba.order_container            ocr
      where  ohr.client_id                     = ocr.client_id
      and    ohr.order_id                      = ocr.order_id
      and    ocr.pallet_id                     = b_pallet_id
      group  
      by     '0000000000'
      ,      null
      ,      ocr.pallet_id
      ,      ocr.config_id
      ,      null
      ,      ohr.from_site_id
      ,      ocr.client_id
      ,      ohr.owner_id
      ,      ocr.order_id
      ,      ohr.customer_id
      ,      ohr.carrier_id
      ,      ohr.service_level      
      ,      'JDA2016'
      ;
    cursor c_cda_con ( b_client_id    in varchar2
                     , b_order_id     in varchar2
                     , b_container_id in varchar2
                     )
    is
      select 'C'                parcel_type
      from   cnl_container_data cda
      where  cda.client_id      = b_client_id
      and    cda.order_id       = b_order_id
      and    cda.container_id   = b_container_id
      ;
    cursor c_cda_pal ( b_client_id in varchar2
                     , b_order_id  in varchar2
                     , b_pallet_id in varchar2
                     )
    is
      select 'P'                parcel_type
      from   cnl_container_data cda
      where  cda.client_id      = b_client_id
      and    cda.order_id       = b_order_id
      and    cda.pallet_id      = b_pallet_id
      and    cda.container_id   = '0000000000'
      ;
    cursor c_cda ( b_client_id    in varchar2
                 , b_order_id     in varchar2
                 , b_pallet_id    in varchar2
                 , b_container_id in varchar2
                 , b_wms_database in varchar2
                 )
    is
      select cda.*
      from   cnl_container_data cda
      where  cda.client_id      = b_client_id
      and    cda.order_id       = b_order_id
      and    cda.pallet_id      = nvl(b_pallet_id, cda.pallet_id)
      and    cda.container_id   = nvl(b_container_id, cda.container_id)
      and    cda.wms_database   = b_wms_database
      ;
    cursor c_smt ( b_client_id in varchar2
                 , b_order_id  in varchar2
                 , b_parcel_id in varchar2
                 )
    is
      select 1
      from   dcsdba.shipping_manifest smt
      where  smt.client_id = b_client_id
      and    smt.order_id  = b_order_id
      and    (
             smt.container_id = b_parcel_id
             or
             smt.pallet_id    = b_parcel_id
             )
      ;


    r_clt              c_clt%rowtype;
    r_ocr_con          c_ocr_con%rowtype;
    r_ocr_pal          c_ocr_pal%rowtype;
    r_cda_con          c_cda_con%rowtype;
    r_cda_pal          c_cda_pal%rowtype;
    r_cda              c_cda%rowtype;

    l_err              integer := 1; -- 1 = OK, 0 = Error
    l_err_txt          varchar2(500);
    l_wms_database     varchar2(20);
    l_client_id        varchar2(10);
    l_old_client_id    varchar2(10);
    l_site_id          varchar2(10);
    l_container_id     varchar2(20) := null;
    l_container_type   varchar2(15);   
    l_pallet_id        varchar2(20) := null;
    l_pallet_type      varchar2(15);
    l_container_n_of_n number;
    l_owner_id         varchar2(20);
    l_order_id         varchar2(20);
    l_customer_id      varchar2(15);
    l_carrier_id       varchar2(25);
    l_service_level    varchar2(40);
    l_weight           number;
    l_height           number;
    l_width            number;
    l_depth            number;
    l_parcel_type      varchar2(1);
    l_tracking_no      varchar(30);
    l_sequence_no      varchar(30);
    l_integer          integer;
  begin
    -- Get client/site from parameter
    open  c_clt ( b_cto_client => p_clientid_i);
    fetch c_clt
    into  r_clt;
    close c_clt;
    l_client_id := r_clt.client_id;
    l_site_id   := r_clt.site_id;

    -- check if Parcel ID is a Pallet or Container in WMS2016
    open  c_cda_con ( b_client_id    => l_client_id
                    , b_order_id     => p_orderno_i
                    , b_container_id => p_parcelid_i
                    );
    fetch c_cda_con
    into  l_parcel_type;
    --
    if c_cda_con%notfound
    then
      open  c_cda_pal ( b_client_id => l_client_id
                      , b_order_id  => p_orderno_i
                      , b_pallet_id => p_parcelid_i
                      );
      fetch c_cda_pal
      into  l_parcel_type;
      --
      if c_cda_pal%notfound
      then
        l_err     := 0;
        l_err_txt := 'Parcel ID unknown: p_parcelid_i [' 
                  || p_parcelid_i
                  || '] for Client-Order ['
                  || l_client_id || '-' || p_orderno_i
                  || '] does not exist in WMS system(s) as container or pallet, check WMS, source file or mapping.'
                  ;
      end if;
      close c_cda_pal;
    end if;
    close c_cda_con;

    -- Parcel Type defined now, continue with the correct cursor
    if  l_err = 1 -- no errors
    and l_parcel_type = 'C'
    then
      open  c_ocr_con ( b_container_id => p_parcelid_i);
      fetch c_ocr_con
      into  r_ocr_con;

      if c_ocr_con%notfound
      then
        -- Check if parcel exists in Shipping Manifest, else send CancelPackParcel to delete the parcel in Centiro
        open  c_smt ( b_client_id => l_client_id
                    , b_order_id  => p_orderno_i
                    , b_parcel_id => p_parcelid_i
                    );
        fetch c_smt
        into  l_integer;
        if c_smt%notfound
        then       
          cnl_centiro_pck.create_cancelparcel ( p_site_id_i   => l_site_id
                                              , p_client_id_i => l_client_id
                                              , p_order_id_i  => p_orderno_i
                                              , p_parcel_id   => p_parcelid_i
                                              );
          l_err     := 0;
          l_err_txt := 'Parcel ID unknown: p_parcelid_i [' 
                    || p_parcelid_i
                    || '] for Client-Order ['
                    || l_client_id || '-' || p_orderno_i
                    || '] does not exist in WMS system(s) anymore as container or pallet. A CancelParcel has been sent to Centiro'
                    ;
        end if;
        close c_smt;
      --
      else
        l_container_id     := r_ocr_con.container_id;
        l_container_type   := r_ocr_con.container_type;
        l_pallet_id        := r_ocr_con.pallet_id;
        l_pallet_type      := r_ocr_con.pallet_type;
        l_container_n_of_n := r_ocr_con.container_n_of_n;
        l_owner_id         := r_ocr_con.owner_id;
        l_order_id         := r_ocr_con.order_id;
        l_customer_id      := r_ocr_con.customer_id;
        l_carrier_id       := r_ocr_con.carrier_id;
        l_service_level    := r_ocr_con.service_level;
        l_weight           := r_ocr_con.weight;
        l_height           := r_ocr_con.height;
        l_width            := r_ocr_con.width;
        l_depth            := r_ocr_con.depth;
        l_wms_database     := r_ocr_con.wms_database; 
      end if;
      close c_ocr_con;       
    end if;
    --
    if  l_err = 1 -- no errors
    and l_parcel_type = 'P'
    then
      open  c_ocr_pal ( b_pallet_id => p_parcelid_i);
      fetch c_ocr_pal
      into  r_ocr_pal;

      if c_ocr_pal%notfound
      then
        -- Check if parcel exists in Shipping Manifest, else send CancelPackParcel to delete the parcel in Centiro
        open  c_smt ( b_client_id => l_client_id
                    , b_order_id  => p_orderno_i
                    , b_parcel_id => p_parcelid_i
                    );
        fetch c_smt
        into  l_integer;
        if c_smt%notfound
        then       
          cnl_centiro_pck.create_cancelparcel ( p_site_id_i   => l_site_id
                                              , p_client_id_i => l_client_id
                                              , p_order_id_i  => p_orderno_i
                                              , p_parcel_id   => p_parcelid_i
                                              );
          l_err     := 0;
          l_err_txt := 'Parcel ID unknown: p_parcelid_i [' 
                    || p_parcelid_i
                    || '] for Client-Order ['
                    || l_client_id || '-' || p_orderno_i
                    || '] does not exist in WMS system(s) anymore as container or pallet. A CancelParcel has been sent to Centiro'
                    ;
        end if;
        close c_smt;
      --
      else
        l_container_id     := r_ocr_pal.container_id;     -- By Pallet ID so default Container ID for CDA table = '0000000000'
        l_container_type   := r_ocr_pal.container_type;
        l_pallet_id        := r_ocr_pal.pallet_id;
        l_pallet_type      := r_ocr_pal.pallet_type;
        l_container_n_of_n := r_ocr_pal.container_n_of_n;
        l_owner_id         := r_ocr_pal.owner_id;
        l_order_id         := r_ocr_pal.order_id;
        l_customer_id      := r_ocr_pal.customer_id;
        l_carrier_id       := r_ocr_pal.carrier_id;
        l_service_level    := r_ocr_pal.service_level;
        l_weight           := r_ocr_pal.weight;
        l_height           := r_ocr_pal.height;
        l_width            := r_ocr_pal.width;
        l_depth            := r_ocr_pal.depth;
        l_wms_database     := r_ocr_pal.wms_database;
      end if;
      close c_ocr_pal;
    end if;
    --
    if  l_err = 1 -- no errors
    and l_wms_database is not null
    then            
      open  c_cda ( b_client_id    => l_client_id
                  , b_order_id     => l_order_id
                  , b_pallet_id    => l_pallet_id
                  , b_container_id => l_container_id
                  , b_wms_database => l_wms_database
                  );
      fetch c_cda
      into  r_cda;
      --
      if c_cda%notfound
      then
        -- insert cda record
        insert into 
        cnl_container_data ( container_id
                           , container_type
                           , pallet_id
                           , pallet_type
                           , container_n_of_n
                           , site_id
                           , client_id
                           , owner_id
                           , order_id
                           , customer_id
                           , carrier_id
                           , service_level
                           , wms_weight
                           , wms_height
                           , wms_width
                           , wms_depth
                           , wms_database
                           , cto_carrier
                           , cto_service
                           , cto_sequence_nr
                           , cto_tracking_nr
                           , cto_tracking_url
                           , cto_error_code
                           , cto_error_message
                           , cto_ppr_dstamp
                           )
        values             ( l_container_id
                           , l_container_type
                           , l_pallet_id
                           , l_pallet_type
                           , l_container_n_of_n
                           , l_site_id
                           , l_client_id
                           , l_owner_id
                           , l_order_id
                           , l_customer_id
                           , l_carrier_id
                           , l_service_level
                           , l_weight
                           , l_height
                           , l_width
                           , l_depth
                           , l_wms_database
                           , p_carrier_i
                           , p_service_i
                           , p_sequenceno_i
                           , p_trackingno_i
                           , p_trackingurl_i
                           , p_errorcode_i
                           , p_errormessage_i
                           , current_timestamp
                           );
      else
        -- update cda record with cto data
        update cnl_container_data      cda
        set    cto_carrier             = p_carrier_i
        ,      cto_service             = p_service_i
        ,      cto_sequence_nr         = p_sequenceno_i
        ,      cto_tracking_nr         = p_trackingno_i
        ,      cto_tracking_url        = p_trackingurl_i
        ,      cto_error_code          = p_errorcode_i
        ,      cto_error_message       = p_errormessage_i
        ,      cto_ppr_dstamp          = current_timestamp
        where  cda.client_id           = l_client_id
        and    cda.order_id            = l_order_id
        and    cda.pallet_id           = l_pallet_id
        and    cda.container_id        = l_container_id
        ;
      end if;
      commit;
      close c_cda;
    end if;

    -- Update Order_Container with Tracking_Nr and Order Header with Sequence_No
    if p_trackingno_i is not null
    then
      if length(p_trackingno_i) > 30
      then
        l_tracking_no := substr(p_trackingno_i, -30);
      else
        l_tracking_no := p_trackingno_i;
      end if;
      --
      if length(p_sequenceno_i) > 30
      then
        l_sequence_no := substr(p_sequenceno_i, -30);
      else
        l_sequence_no := p_sequenceno_i;
      end if;

      case l_wms_database
      -- Update WMS2016
      when 'JDA2016'
      then
        update dcsdba.order_container ocr
        set    ocr.carrier_consignment_id = l_tracking_no
        where  ocr.client_id    = l_client_id
        and    ocr.order_id     = l_order_id
        and    ocr.pallet_id    = l_pallet_id
        and    ocr.container_id = decode( l_parcel_type, 'C', l_container_id    -- For Parcel Type 'P' (Pallet) all containers with Pallet ID need to be updated
                                                            , ocr.container_id
                                        );
        --
        update dcsdba.order_header ohr
        set    ohr.trax_id   = l_sequence_no   
        where  ohr.client_id = l_client_id
        and    ohr.order_id  = l_order_id
        and    (
               ohr.trax_id   != l_sequence_no 
               or
               ohr.trax_id   is null
               );
        commit;       
      else
        if l_err = 1  -- No errors
        then
          l_err     := 0;
          l_err_txt := 'WMS Database unknown: [' 
                    || nvl( l_wms_database, 'NO VALUE')
                    || '] .'
                    ;
        end if;            

      end case;
    end if;

    p_error_o     := l_err;
    p_errortext_o := l_err_txt;

  exception
    when others
    then
      case
      when c_clt%isopen
      then
        close c_clt;
      when c_cda_con%isopen
      then
        close c_cda_con;
      when c_cda_pal%isopen
      then
        close c_cda_pal;
      when c_cda%isopen
      then
        close c_cda;
      when c_ocr_con%isopen
      then
        close c_ocr_con;
      when c_ocr_pal%isopen
      then
        close c_ocr_pal;
      when c_smt%isopen
      then
        close c_smt;
      else
        null;
      end case;

      l_err         := 0;
      l_err_txt     := substr( sqlerrm, 1, 500);

      p_error_o     := l_err;
      p_errortext_o := l_err_txt;

      raise;

  end process_packparcelresponse;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 15-Jun-2016
-- Purpose : Process TrackingEventUpdate (Centiro) from Seeburger (cnl_api_pck.cto_te_update) 
------------------------------------------------------------------------------------------------
  procedure process_trackingeventupdate ( p_clientid_i       in  varchar2
                                        , p_orderno_i        in  varchar2
                                        , p_eventtime_i      in  varchar2
                                        , p_eventsignature_i in  varchar2 := null
                                        , p_error_o          out integer
                                        , p_errortext_o      out varchar2
                                        )
  is
    cursor c_ohr ( b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is 
      select ohr.client_id
      ,      ohr.order_id
      ,      ohr.status
      ,      'JDA2016'           wms_database
      from   dcsdba.order_header ohr
      where  ohr.client_id       = b_client_id
      and    ohr.order_id        = b_order_id
      ;
    cursor c_clt ( b_cto_client in varchar2)
    is
      select substr( b_cto_client, 1, decode( instr( b_cto_client, '@'), 0, length(b_cto_client)
                                                                          , instr( b_cto_client, '@') - 1
                                            )
                   ) client_id
      ,      substr( b_cto_client, instr( b_cto_client, '@') + 1 
                   ) site_id
      from   dual
      ;
    cursor c_ltt (b_mergeerror in varchar2)
    is
      select b_mergeerror
      ||     ' - '
      ||     text
      from   dcsdba.language_text
      where  language = 'EN_GB'
      and    label    = b_mergeerror
      ;

    r_clt           c_clt%rowtype;
    r_ohr           c_ohr%rowtype;

    l_err           integer := 1; -- 1 = OK, 0 = Error
    l_err_txt       varchar2(500);
    l_mergeerror    varchar2(40);
    l_client_id     varchar2(10);
    l_old_client_id varchar2(10);
    l_site_id       varchar2(10);
    l_wms_database  varchar2(20);
    l_status        varchar2(20);
  begin
    open  c_clt ( b_cto_client => p_clientid_i);
    fetch c_clt
    into  r_clt;
    close c_clt;
    l_client_id := r_clt.client_id;
    l_site_id   := r_clt.site_id;
    --
    open  c_ohr ( b_client_id => l_client_id
                , b_order_id  => p_orderno_i
                );
    fetch c_ohr
    into  r_ohr;
    close c_ohr;
    --
    l_wms_database := r_ohr.wms_database;
    l_status       := r_ohr.status;
    --
    if  l_err = 1 -- no errors
    and l_wms_database is not null
    then
      case l_wms_database
      -- Update WMS2016
      when 'JDA2016'
      then
        if l_status = g_shipped
        then
          dcsdba.libsession.setsessionuserid (userid => 'SEEBURGER');
          dcsdba.libsession.setsessionworkstation (stationid => 'SEEBURGER');
          l_err := dcsdba.libmergedeliveryconfirm.directdeliveryconfirm ( p_mergeerror      => l_mergeerror
                                                                        , p_toupdatecols    => null
                                                                        , p_mergeaction     => 'U'  -- Update
                                                                        , p_confirmmode     => 'O'  -- Order
                                                                        , p_clientid        => r_ohr.client_id
                                                                        , p_orderid         => r_ohr.order_id
                                                                        , p_lineid          => null
                                                                        , p_containerid     => null
                                                                        , p_palletid        => null
                                                                        , p_skuid           => null
                                                                        , p_qtydelivered    => null
                                                                        , p_delivereddstamp => to_date( p_eventtime_i, 'YYYYMMDDHH24MISS')
                                                                        , p_signatory       => p_eventsignature_i
                                                                        , p_reasonid        => null
                                                                        , p_timezonename    => 'Europe/Amsterdam'
                                                                        );
        end if;
      else
        l_err     := 0;
        l_err_txt := 'WMS Database unknown: [' 
                  || nvl( r_ohr.wms_database, 'NO VALUE')
                  || '] .'
                  ;

      end case;
    end if;

    if l_err = 0
    then
      if l_mergeerror is not null
      then
        open  c_ltt (b_mergeerror => l_mergeerror);
        fetch c_ltt
        into  l_err_txt;
        close c_ltt;
      end if;
    end if;
    --
    p_error_o     := l_err;
    p_errortext_o := l_err_txt;

  exception
    when others
    then
      case
      when c_clt%isopen
      then
        close c_clt;
      when c_ohr%isopen
      then
        close c_ohr;
      when c_ltt%isopen
      then
        close c_ltt;
      end case;

      l_err         := 0;
      l_err_txt     := substr( sqlerrm, 1, 500);

      p_error_o     := l_err;
      p_errortext_o := l_err_txt;

    raise;
  end process_trackingeventupdate;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 17-Jun-2016
-- Purpose : Process the CTO and SSV Run Tasks from WMS 
------------------------------------------------------------------------------------------------
	procedure process_runtask( p_key_i			in dcsdba.run_task.key%type
				 , p_report_i			in varchar2
				 , p_user_id_i  		in dcsdba.run_task.user_id%type
				 , p_station_id_i 		in dcsdba.run_task.station_id%type
				 , p_site_id_i			in dcsdba.run_task.site_id%type
				 , p_status_i			in dcsdba.run_task.status%type
				 , p_command_i			in dcsdba.run_task.command%type
				 , p_pid_i			in dcsdba.run_task.pid%type
				 , p_old_dstamp_i		in dcsdba.run_task.old_dstamp%type
				 , p_dstamp_i			in dcsdba.run_task.dstamp%type
				 , p_language_i			in dcsdba.run_task.language%type
				 , p_name_i			in dcsdba.run_task.name%type
				 , p_time_zone_name_i		in dcsdba.run_task.time_zone_name%type
				 , p_nls_calendar_i		in dcsdba.run_task.nls_calendar%type
				 , p_print_label_i		in dcsdba.run_task.print_label%type
				 , p_java_report_i		in dcsdba.run_task.java_report%type
				 , p_run_light_i		in dcsdba.run_task.run_light%type
				 , p_server_instance_i		in dcsdba.run_task.server_instance%type
				 , p_priority_i			in dcsdba.run_task.priority%type
				 , p_archive_i			in dcsdba.run_task.archive%type
				 , p_archive_ignore_screen_i	in dcsdba.run_task.archive_ignore_screen%type
				 , p_archive_restrict_user_i	in dcsdba.run_task.archive_restrict_user%type
				 , p_client_id_i		in dcsdba.run_task.client_id%type
				 , p_email_recipients_i		in dcsdba.run_task.email_recipients%type
				 , p_email_attachment_i		in dcsdba.run_task.email_attachment%type
				 , p_email_subject_i		in dcsdba.run_task.email_subject%type
				 , p_email_message_i		in dcsdba.run_task.email_message%type
				 , p_master_key_i		in dcsdba.run_task.master_key%type
				 , p_use_db_time_zone_i		in dcsdba.run_task.use_db_time_zone%type
				 )
	is
		--
		cursor c_occ( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    )
		is
			select	(	select	count(distinct order_id)
					from   	dcsdba.order_container   ocr
					where  	ocr.client_id		in nvl( b_client_id, ocr.client_id)
					and    	ocr.order_id		in nvl( b_order_id, ocr.order_id)
					and    	ocr.pallet_id		in nvl( b_pallet_id, ocr.pallet_id)
					and    	ocr.container_id	in nvl( b_container_id, ocr.container_id)
				)
				+
				(	select count(distinct order_id)
					from   	dcsdba.shipping_manifest ocr
					where  	ocr.client_id           in nvl( b_client_id, ocr.client_id)
					and    	ocr.order_id            in nvl( b_order_id, ocr.order_id)
					and    	ocr.pallet_id           in nvl( b_pallet_id, ocr.pallet_id)
					and    	ocr.container_id        in nvl( b_container_id, ocr.container_id)
				)
			from 	dual      
		;
		--
		cursor c_ocr2( b_client_id    	in varchar2
			     , b_order_id     	in varchar2
			     , b_pallet_id    	in varchar2
			     , b_container_id 	in varchar2
			     )
		is
			select	ocr.client_id
			,      	ocr.order_id
			,      	ocr.pallet_id
			,      	ocr.container_id
			,      	nvl( ocr.labelled, g_no) is_cont_yn
			from   	dcsdba.order_container   ocr
			where  	ocr.client_id            in nvl( b_client_id, ocr.client_id)
			and    	ocr.order_id             in nvl( b_order_id, ocr.order_id)
			and    	ocr.pallet_id            in nvl( b_pallet_id, ocr.pallet_id)
			and    	ocr.container_id         in nvl( b_container_id, ocr.container_id)
			union
			select 	ocr.client_id
			,      	ocr.order_id
			,      	ocr.pallet_id
			,      	ocr.container_id
			,      	nvl( ocr.labelled, g_no) is_cont_yn
			from   	dcsdba.shipping_manifest ocr
			where  	ocr.client_id            in nvl( b_client_id, ocr.client_id)
			and    	ocr.order_id             in nvl( b_order_id, ocr.order_id)
			and    	ocr.pallet_id            in nvl( b_pallet_id, ocr.pallet_id)
			and    	ocr.container_id         in nvl( b_container_id, ocr.container_id)
		;
		--
		cursor c_ocr( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    )
		is
			select	ocr.client_id
			,      	ocr.order_id
			,      	ocr.pallet_id
			,      	ocr.container_id
			,      	nvl( ocr.labelled, g_no) is_cont_yn
			from   	dcsdba.order_container   ocr
			where  	ocr.client_id            in nvl( b_client_id, ocr.client_id)
			and    	ocr.order_id             in nvl( b_order_id, ocr.order_id)
			and    	ocr.pallet_id            in nvl( b_pallet_id, ocr.pallet_id)
			and    	ocr.container_id         in nvl( b_container_id, ocr.container_id)
		;
		--
		cursor c_ocr_unt( b_client_id in varchar2
				, b_order_id  in varchar2
				, b_unit_id   in varchar2
				)
		is
			select	ocr.client_id
			,      	ocr.order_id
			,      	ocr.pallet_id
			,      	ocr.container_id
			,      	nvl( ocr.labelled, g_no) is_cont_yn
			from   	dcsdba.order_container   ocr
			where  	ocr.client_id            = b_client_id
			and    	ocr.order_id             = b_order_id
			and    	(
				ocr.container_id         = b_unit_id
				or
				ocr.pallet_id            = b_unit_id
				)
		;
		--
		cursor c_jrp( b_report_name in varchar2
			    , b_site_id     in varchar2
			    , b_client_id   in varchar2
			    , b_owner_id    in varchar2
			    , b_order_id    in varchar2
			    , b_carrier_id  in varchar2
			    , b_user_id     in varchar2
			    , b_station_id  in varchar2
			    , b_locality    in varchar2
			    )
		is
			select	jrp.key
			,      	jrp.print_mode
			,      	jrp.report_name
			,      	jrp.template_name
			,      	jrp.site_id
			,      	jrp.client_id
			,      	jrp.owner_id
			,      	jrp.carrier_id
			,      	jrp.user_id
			,      	jrp.station_id
			,      	jrp.customer_id
			,      	jrp.extra_parameters
			,      	jrp.email_enabled
			,      	jrp.email_export_type
			,      	jrp.email_attachment
			,      	jrp.email_subject
			,      	jrp.email_message
			,      	jrp.copies
			,      	jrp.locality
			from   	dcsdba.java_report_map jrp
			where  	jrp.report_name        = b_report_name
			and    	(
				jrp.site_id            = nvl(b_site_id, jrp.site_id)
				or
				jrp.site_id            is null
				)
			and    	(
				jrp.client_id          = nvl(b_client_id, jrp.client_id)
				or
				jrp.client_id          is null
				)
			and    	(
				jrp.owner_id           = nvl(b_owner_id, jrp.owner_id)
				or
				jrp.owner_id           is null
				)
			and    	(
				jrp.carrier_id         = nvl(b_carrier_id, jrp.carrier_id)
				or
				jrp.carrier_id         is null
				)
			and    	(
				jrp.user_id            = nvl(b_user_id, jrp.user_id)
				or
				jrp.user_id            is null
				)
			and    	(
				jrp.station_id         = nvl(b_station_id, jrp.station_id)
				or
				jrp.station_id         is null
				)
			and    	(      
				jrp.locality           = b_locality
				or
				jrp.locality           is null
				)
			and    	1                      = cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i => b_client_id
											     , p_order_id_i  => b_order_id
											     , p_where_i     => nvl( jrp.extra_parameters, '1=1')
											     )
			order  
			by 	jrp.locality         	nulls last
			,      	jrp.station_id          nulls last
			,      	jrp.site_id             nulls last
			,      	jrp.client_id           nulls last
			,      	jrp.owner_id            nulls last
			,      	jrp.carrier_id          nulls last
			,      	jrp.user_id             nulls last
			,      	jrp.extra_parameters    nulls last
		;
		--
		cursor c_jrt( b_key in number)
		is
			select jrt.key
			,      jrt.export_target
			,      jrt.copies
			from   dcsdba.java_report_export  jrt
			where  jrt.key = b_key
		;
		--
		cursor c_jrl( b_key in number)
		is
			select jrl.key
			,      jrl.email_address
			,      jrl.email_select
			from   dcsdba.java_report_email jrl
			where  jrl.key = b_key
		;
		--
		cursor c_ohr( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select	ohr.from_site_id    site_id
			,      	ohr.client_id
			,      	ohr.owner_id
			,      	ohr.order_id
			,      	ohr.carrier_id
			,      	decode( nvl( crr.user_def_type_7, g_false), g_false, g_no, g_yes) use_dws_yn
			from   	dcsdba.order_header ohr
			,      	dcsdba.carriers     crr
			where  	ohr.from_site_id    = crr.site_id
			and    	ohr.client_id       = crr.client_id
			and    	ohr.carrier_id      = crr.carrier_id
			and    	ohr.service_level   = crr.service_level
			and    	ohr.client_id       = b_client_id
			and    	ohr.order_id        = b_order_id
		;
		--
		cursor c_wsn( b_station_id in varchar2)
		is
			select 1
			from   dcsdba.workstation  wsn
			where  wsn.station_id      = b_station_id
			and    wsn.equipment_class = g_dws
		;
		--
		cursor c_wsn_dws( b_station_id in varchar2)
		is
			select nvl( user_def_num_1, 0)
			from   dcsdba.workstation wsn
			where  wsn.user_def_chk_1 = g_yes
			and    wsn.station_id     = b_station_id
		;
		--
		cursor c_dws( b_site_id in varchar2
			    , b_dws_nr  in number
			    )
		is
			select 	wsn.station_id
			,      	decode( nvl(wsn.disabled, g_no), g_no, g_yes, g_no) dws_enabled_yn
			from   	dcsdba.workstation wsn
			where  	wsn.site_id = b_site_id
			and    	substr(wsn.station_id, -3) = to_char(lpad(b_dws_nr,3,0))
			and    	wsn.equipment_class = g_dws
		;
		--
		cursor 	c_mtk( b_client_id in varchar2
			     , b_order_id  in varchar2
			     )
		is
			select	count(*) qty_recs
			from   	dcsdba.move_task
			where  	task_type = 'B'
			and    	client_id = b_client_id
			and    	task_id   = b_order_id
		;
		--
		cursor	c_jsp( b_report_name in varchar2
			     , b_site_id     in varchar2
			     , b_client_id   in varchar2
			     , b_owner_id    in varchar2
			     , b_order_id    in varchar2
			     , b_carrier_id  in varchar2
			     , b_user_id     in varchar2
			     , b_station_id  in varchar2
			     , b_locality    in varchar2
			     )
		is
			select	count(*)
			from   	dcsdba.java_report_map jrp
			where  	jrp.report_name        = b_report_name
			and    	(	jrp.site_id            = nvl(b_site_id, jrp.site_id)
				or	jrp.site_id            is null
				)
			and    	(	jrp.client_id          = nvl(b_client_id, jrp.client_id)
				or	jrp.client_id          is null
				)
			and    	(	jrp.owner_id           = nvl(b_owner_id, jrp.owner_id)
				or	jrp.owner_id           is null
				)
			and    	(	jrp.carrier_id         = nvl(b_carrier_id, jrp.carrier_id)
				or	jrp.carrier_id         is null
				)
			and    	(	jrp.user_id            = nvl(b_user_id, jrp.user_id)
				or	jrp.user_id            is null
				)
			and    	(	jrp.station_id         = nvl(b_station_id, jrp.station_id)
				or	jrp.station_id         is null
				)
			and    	(	jrp.locality           = b_locality
				or	jrp.locality           is null
				)
			and    	1 = cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i => b_client_id
                                                                        , p_order_id_i  => b_order_id
                                                                        , p_where_i     => nvl( jrp.extra_parameters, '1=1')
                                                                        )
			and	instr(lower(jrp.extra_parameters),'jaspersoft') > 0

		;
		--
		cursor	c_ssv( b_report_name in varchar2
			     , b_site_id     in varchar2
			     , b_client_id   in varchar2
			     , b_owner_id    in varchar2
			     , b_order_id    in varchar2
			     , b_carrier_id  in varchar2
			     , b_user_id     in varchar2
			     , b_station_id  in varchar2
			     , b_locality    in varchar2
			     )
		is
			select	count(*)
			from   	dcsdba.java_report_map jrp
			where  	jrp.report_name        = b_report_name
			and    	(	jrp.site_id            = nvl(b_site_id, jrp.site_id)
				or	jrp.site_id            is null
				)
			and    	(	jrp.client_id          = nvl(b_client_id, jrp.client_id)
				or	jrp.client_id          is null
				)
			and    	(	jrp.owner_id           = nvl(b_owner_id, jrp.owner_id)
				or	jrp.owner_id           is null
				)
			and    	(	jrp.carrier_id         = nvl(b_carrier_id, jrp.carrier_id)
				or	jrp.carrier_id         is null
				)
			and    	(	jrp.user_id            = nvl(b_user_id, jrp.user_id)
				or	jrp.user_id            is null
				)
			and    	(	jrp.station_id         = nvl(b_station_id, jrp.station_id)
				or	jrp.station_id         is null
				)
			and    	(	jrp.locality           = b_locality
				or	jrp.locality           is null
				)
			and    	1 = cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i => b_client_id
									, p_order_id_i  => b_order_id
									, p_where_i     => nvl( jrp.extra_parameters, '1=1')
									)
			and	instr(lower(nvl(jrp.extra_parameters,'X')),'jaspersoft') = 0
		;
		--
		r_rtk              	dcsdba.run_task%rowtype;
		r_ocr              	c_ocr%rowtype;
		r_ocr2             	c_ocr2%rowtype;
		r_occ	       	   	number;
		r_ocr_unt          	c_ocr_unt%rowtype;
		r_jrp              	c_jrp%rowtype;
		r_jsp              	number;
		r_ssv              	number;
		r_jrt              	c_jrt%rowtype;
		r_jrl              	c_jrl%rowtype;
		r_ohr              	c_ohr%rowtype;
		r_dws              	c_dws%rowtype;

		l_rtk_key          	number;
		l_jrp_key          	number;
		l_report_name      	varchar2(20);
		l_report           	varchar2(20);
		l_user_id          	varchar2(20);
		l_station_id       	varchar2(256);
		l_site_id          	varchar2(10);
		l_list_id  	   	varchar2(30);
		l_client_id        	varchar2(10);
		l_owner_id         	varchar2(10);
		l_carrier_id       	varchar2(25);
		l_order_id         	varchar2(20);
		l_pallet_id        	varchar2(20);
		l_container_id     	varchar2(20);
		l_unit_id          	varchar2(20);
		l_locality         	varchar2(20);
		l_rdtlocality      	varchar2(20);
		l_linked_to_dws    	varchar2(1);
		l_pdf_link         	varchar2(256);
		l_pdf_autostore    	varchar2(256);
		l_dws_enabled_yn   	varchar2(1) := g_yes;
		l_use_dws_yn       	varchar2(1) := g_no;
		l_print_at_dws_yn  	varchar2(1) := g_no;
		l_dws_nr           	integer := 0;
		l_printer          	varchar2(250);
		l_copies           	number;
		l_eml_address      	varchar2(256);
		l_eml_select       	varchar2(4000);
		l_result           	integer;
		l_client_chk       	dcsdba.java_report_map.client_id%type;
		l_print2file       	varchar2(1);
		l_integer          	integer;
		l_mtk_recs         	integer;
		l_mtk_cnt          	integer;
		l_mtk_cnt_max      	integer;
		l_rtk_err_yn       	varchar2(1) := g_no;
		l_failed	   	varchar2(1);
		l_rtk_cnt	   	integer;
		l_log		   	varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_PRE-PRINT-LOG_ENABLE');
		l_pck		   	varchar2(30) := 'cnl_wms_pck';
		l_rtn		   	varchar2(30) := 'process_runtask';
		l_fail_reason		varchar2(1000);
		l_exists		number;
	begin
		-- Rebuild run task record
		r_rtk.key 			:= p_key_i;
		r_rtk.site_id			:= p_site_id_i;
		r_rtk.station_id		:= p_station_id_i;
		r_rtk.user_id			:= p_user_id_i;
		r_rtk.status			:= p_status_i;
		r_rtk.command			:= p_command_i;
		r_rtk.pid			:= p_pid_i;
		r_rtk.old_dstamp		:= p_old_dstamp_i;
		r_rtk.dstamp			:= p_dstamp_i;
		r_rtk.language			:= p_language_i;
		r_rtk.name			:= p_name_i;
		r_rtk.time_zone_name		:= p_time_zone_name_i;
		r_rtk.nls_calendar		:= p_nls_calendar_i;
		r_rtk.print_label		:= p_print_label_i;
		r_rtk.java_report		:= p_java_report_i;
		r_rtk.run_light			:= p_run_light_i;
		r_rtk.server_instance		:= p_server_instance_i;
		r_rtk.priority			:= p_priority_i;
		r_rtk.archive			:= p_archive_i;
		r_rtk.archive_ignore_screen	:= p_archive_ignore_screen_i;
		r_rtk.archive_restrict_user	:= p_archive_restrict_user_i;
		r_rtk.client_id			:= p_client_id_i;
		r_rtk.email_recipients		:= p_email_recipients_i;
		r_rtk.email_attachment		:= p_email_attachment_i;
		r_rtk.email_subject		:= p_email_subject_i;
		r_rtk.email_message		:= p_email_message_i;
		r_rtk.master_key		:= p_master_key_i;
		r_rtk.use_db_time_zone		:= p_use_db_time_zone_i;

		-- Save run task as tmp run task
		-- A temp record is saved that will be updated instead of original run task
		-- There is a timing problem where run tasks have not yet been commited
		-- and this procedure catches up and tries to update the run task that has not yet been commited by WMS
		cnl_sys.cnl_wms_pck.save_tmp_run_task_p(r_rtk);

		--
		if	l_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
								   , p_file_name_i		=> null
								   , p_source_package_i		=> l_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Process run task started'
								   , p_code_parameters_i 	=> '"Report name" "'||p_report_i||'" "user_id" "'||p_user_id_i||'" "station_id" "'||p_station_id_i
								   , p_order_id_i		=> null
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
		--
		l_rtk_key       := p_key_i;
		l_report        := p_report_i;
		l_user_id       := p_user_id_i;
		l_station_id    := p_station_id_i;
		l_report_name   := p_name_i;

		-- Check if the Station ID is a DWS, if so print2file
		open 	c_wsn ( b_station_id => l_station_id);
		fetch 	c_wsn
		into  	l_integer;
		if 	c_wsn%found
		then
			l_print2file      := g_yes;
		else
			l_print2file      := g_no;
		end if;
		close 	c_wsn; 

		-- Add log record
		if	l_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
								   , p_file_name_i		=> null
								   , p_source_package_i		=> l_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Check if print to file is required'
								   , p_code_parameters_i 	=> '"Print2file" "'||l_print2file||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		-- Set variables parameters
		l_site_id       := nvl( get_rtk_params( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_site_id), r_rtk.site_id);              
		--
		l_client_id     := nvl( get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_client_id), r_rtk.client_id);
		--
		l_list_id       := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_list_id);
		--
		l_owner_id      := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_owner_id);
		--
		l_order_id      := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_order_id);
		--
		l_pallet_id     := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_pallet_id);
		--
		l_container_id  := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_container_id);
		--
		l_unit_id       := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_unit_id);
		--
		l_locality      := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_locality);
		--
		l_rdtlocality   := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_rdtlocality);
		--
		l_linked_to_dws := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_linked_to_dws);
		--
		l_pdf_link      := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_pdf_link);
		--
		l_pdf_autostore := get_rtk_params ( p_key_i => l_rtk_key, p_parameter_i => g_rtk_par_pdf_autostore);

		-- Add log record
		if	l_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
								   , p_file_name_i		=> null
								   , p_source_package_i		=> l_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Set variable parameters'
								   , p_code_parameters_i 	=> '"Print2file" "'||l_print2file||'" '
												|| '"list_id" "'||l_list_id||'" '
												|| '"unit_id" "'||l_unit_id||'" '
												|| '"locality" "'||l_locality||'" '
												|| '"rdtlocality" "'||l_rdtlocality||'" '
												|| '"Linked_to_dws" "'||l_linked_to_dws||'" '
												|| '"Pdf_link" "'||l_pdf_link||'" '
												|| '"pdf_autostore" "'||l_pdf_autostore||'" '
								   , p_order_id_i		=> l_order_id
								   , p_client_id_i		=> l_client_id
								   , p_pallet_id_i		=> l_pallet_id
								   , p_container_id_i		=> l_container_id
								   , p_site_id_i		=> l_site_id
								   );
		end if;
		--
		if	 r_rtk.command not like '%SSV_TRL_ALL%'
		then
			-- check if order exists in order container or shipping manifest
			open 	c_occ( b_client_id    => l_client_id
				     , b_order_id     => l_order_id
				     , b_pallet_id    => l_pallet_id
				     , b_container_id => l_container_id);
			fetch 	c_occ 
			into 	r_occ;
			close 	c_occ;
			if	r_occ = 0 --or r_occ > 1
			then
				l_failed 	:= 'Y';
				l_fail_reason	:= 'Order container record does not exist yet during first check';
				-- Add log record
				if	l_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
										   , p_file_name_i		=> null
										   , p_source_package_i		=> l_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Check if order exists in order container or shipping manifest failed'
										   , p_code_parameters_i 	=> '"failed" "'||l_failed||'" '
										   , p_order_id_i		=> l_order_id
										   , p_client_id_i		=> l_client_id
										   , p_pallet_id_i		=> l_pallet_id
										   , p_container_id_i		=> l_container_id
										   , p_site_id_i		=> l_site_id
										   );
				end if;
			else
				open	c_ocr2( b_client_id    => l_client_id
					      , b_order_id     => l_order_id
					      , b_pallet_id    => l_pallet_id
					      , b_container_id => l_container_id
					      );
				fetch 	c_ocr2
				into  	r_ocr2 ;
				if	c_ocr2%notfound
				then
					l_failed := 'Y';
					l_fail_reason	:= 'Order container record could not be found during second check';
					-- Add log record
					if	l_log = 'ON'
					then
						cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
											   , p_file_name_i		=> null
											   , p_source_package_i		=> l_pck
											   , p_source_routine_i		=> l_rtn
											   , p_routine_step_i		=> 'Check if order exists in order cotainer or shipping manifest failed'
											   , p_code_parameters_i 	=> '"failed" "'||l_failed||'" '
											   , p_order_id_i		=> l_order_id
											   , p_client_id_i		=> l_client_id
											   , p_pallet_id_i		=> l_pallet_id
											   , p_container_id_i		=> l_container_id
											   , p_site_id_i		=> l_site_id
											   );
					end if;
				end if;
				close 	c_ocr2;
				l_client_id := r_ocr2.client_id;
				l_order_id  := r_ocr2.order_id;
			end if;
		end if;

		--
		if	nvl(l_failed,'N') = 'N'
		then
			-- Fetch order header details
			open	c_ohr( b_client_id => l_client_id
				     , b_order_id  => l_order_id
				     );
			fetch 	c_ohr
			into  	r_ohr;
			close 	c_ohr;

			l_site_id    := nvl( l_site_id,    r_ohr.site_id);
			l_client_id  := nvl( l_client_id,  r_ohr.client_id);
			l_owner_id   := nvl( l_owner_id,   r_ohr.owner_id);
			l_order_id   := nvl( l_order_id,   r_ohr.order_id);
			l_carrier_id := nvl( l_carrier_id, r_ohr.carrier_id);
			l_use_dws_yn := r_ohr.use_dws_yn;

			-- Check if locality/workstation is linked to DWS system and if label/shipments needs to triggered at PackStation or at DWS system and if DWS is enabled
			open	c_wsn_dws ( b_station_id => l_station_id);
			fetch 	c_wsn_dws
			into  	l_dws_nr;
			if 	c_wsn_dws%notfound
			then
				l_dws_nr := 0;
			end if;
			close 	c_wsn_dws;
			--
			open	c_dws( b_site_id => l_site_id
				     , b_dws_nr  => l_dws_nr
				     );
			fetch 	c_dws
			into  	r_dws;
			if 	c_dws%found
			then
				l_dws_enabled_yn   := r_dws.dws_enabled_yn;
			else
				l_dws_enabled_yn   := g_no;
			end if; 
			--
			if  	l_use_dws_yn     = g_yes  -- DWS enabled for Site/Client/Carrier/Service ?
			and 	l_linked_to_dws  = g_yes  -- Workstation linked to DWS
			and 	l_dws_enabled_yn = g_yes  -- DWS station enabled which is linked to repack workstation
			then
				l_print_at_dws_yn := g_yes;
			else
				l_print_at_dws_yn := g_no;
			end if;
			-- Add log record
			if	l_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
									   , p_file_name_i		=> null
									   , p_source_package_i		=> l_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Check if DWS is enabled and which DWS it is and if it must print at DWS'
									   , p_code_parameters_i 	=> '"Print2file" "'||l_print2file||'" '
													|| '"list_id" "'||l_list_id||'" '
													|| '"unit_id" "'||l_unit_id||'" '
													|| '"locality" "'||l_locality||'" '
													|| '"rdtlocality" "'||l_rdtlocality||'" '
													|| '"Linked_to_dws" "'||l_linked_to_dws||'" '
													|| '"Pdf_link" "'||l_pdf_link||'" '
													|| '"pdf_autostore" "'||l_pdf_autostore||'" '
													|| '"dws_enabled" "'||l_dws_enabled_yn||'" '
													|| 'print_at_dws" "'||l_print_at_dws_yn||'" '
													|| 'DWS number" "'||l_dws_nr||'" '
									   , p_order_id_i		=> l_order_id
									   , p_client_id_i		=> l_client_id
									   , p_pallet_id_i		=> l_pallet_id
									   , p_container_id_i		=> l_container_id
									   , p_site_id_i		=> l_site_id
									   );
			end if;

			-- Check which report type needs to be processed
			case	l_report
			-- Trigger Centiro PackParcel interface from ITL Code Report or User report (UREPCTOPACKPARCEL)
			when 	g_rtk_cmd_cto_packparcel
			then
				-- Call Centiro PackParcel interface procedure for best match Java_Report_Map
				open	c_jrp( b_report_name => g_jr_urepctopackparcel -- always with UREPCTOPACKPARCEL report name
					     , b_site_id     => l_site_id
					     , b_client_id   => l_client_id
					     , b_owner_id    => l_owner_id
					     , b_order_id    => l_order_id
					     , b_carrier_id  => l_carrier_id
					     , b_user_id     => l_user_id
					     , b_station_id  => l_station_id
					     , b_locality    => nvl( l_locality, l_rdtlocality)
					     );
				fetch 	c_jrp
				into  	r_jrp;
				close 	c_jrp;
				l_jrp_key := r_jrp.key;

				-- get the printer and copies
				open  	c_jrt ( b_key => l_jrp_key);
				fetch 	c_jrt
				into  	r_jrt;
				close 	c_jrt;
				l_printer := r_jrt.export_target;
				l_copies  := r_jrt.copies;

				-- now call the Create PackParcel procedure by container_id
				if	l_container_id is null
				then
					for	r_ocr in c_ocr( b_client_id    => l_client_id
							      , b_order_id     => l_order_id
							      , b_pallet_id    => l_pallet_id
							      , b_container_id => l_container_id
							      )
					loop
						cnl_centiro_pck.create_packparcel( p_site_id_i		=> l_site_id
										 , p_client_id_i    	=> l_client_id
										 , p_order_id_i     	=> l_order_id
										 , p_pallet_id_i    	=> null
										 , p_container_id_i 	=> r_ocr.container_id
										 , p_printer_i      	=> l_printer
										 , p_copies_i       	=> l_copies
										 , p_print2file_i   	=> l_print2file
										 , p_rtk_key_i	    	=> P_key_i
										 , p_run_task_i		=> r_rtk
										 );
						-- Add log record
						if	l_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
												   , p_file_name_i		=> null
												   , p_source_package_i		=> l_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Trigger Centiro PackParcel interface from ITL Code Report or User report (UREPCTOPACKPARCEL) for every container'
												   , p_code_parameters_i 	=> '"Printer" "'||l_printer||'" '
																|| '"copies" "'||l_copies||'" '
																|| '"print2file" "'||l_print2file||'" '
												   , p_order_id_i		=> l_order_id
												   , p_client_id_i		=> l_client_id
												   , p_pallet_id_i		=> null
												   , p_container_id_i		=> r_ocr.container_id
												   , p_site_id_i		=> l_site_id
												   );
						end if;
					end loop;
				else
					cnl_centiro_pck.create_packparcel( p_site_id_i      	=> l_site_id
									 , p_client_id_i    	=> l_client_id
									 , p_order_id_i     	=> l_order_id
									 , p_pallet_id_i    	=> null
									 , p_container_id_i 	=> l_container_id
									 , p_printer_i      	=> l_printer
									 , p_copies_i       	=> l_copies
									 , p_print2file_i   	=> l_print2file
									 , p_rtk_key_i	    	=> P_key_i
									 , p_run_task_i		=> r_rtk
									 );        
					-- Add log record
					if	l_log = 'ON'
					then
						cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
											   , p_file_name_i		=> null
											   , p_source_package_i		=> l_pck
											   , p_source_routine_i		=> l_rtn
											   , p_routine_step_i		=> 'Trigger Centiro PackParcel interface from ITL Code Report or User report (UREPCTOPACKPARCEL)'
											   , p_code_parameters_i 	=> '"Printer" "'||l_printer||'" '
															|| '"copies" "'||l_copies||'" '
															|| '"print2file" "'||l_print2file||'" '
											   , p_order_id_i		=> l_order_id
											   , p_client_id_i		=> l_client_id
											   , p_pallet_id_i		=> null
											   , p_container_id_i		=> l_container_id
											   , p_site_id_i		=> l_site_id
											   );
					end if;
				end if;

			-- Trigger Centiro PackParcel interface from Parcel Packing (Web Client) (UREPPARCELPACKING)
			when g_rtk_cmd_parcel_packing
			then
				-- Skip PackParcel process if Workstation is linked to a DWS system (l_print_at_dws_yn = 'Y') and Carrier is enabled for DWS and DWS is enabled, PackParcel will be triggered from DWS
				if 	nvl(l_print_at_dws_yn, 'N') = g_no
				then 
					-- Call Centiro PackParcel interface procedure for best match Java_Report_Map
					open  	c_jrp( b_report_name => g_jr_urepctopackparcel -- always with UREPCTOPACKPARCEL report name
						     , b_site_id     => l_site_id
						     , b_client_id   => l_client_id
						     , b_owner_id    => l_owner_id
						     , b_order_id    => l_order_id
						     , b_carrier_id  => l_carrier_id
						     , b_user_id     => l_user_id
						     , b_station_id  => l_station_id
						     , b_locality    => nvl( l_locality, l_rdtlocality)
						     );
					fetch 	c_jrp
					into  	r_jrp;
					close 	c_jrp;
					l_jrp_key := r_jrp.key;
					-- get the printer and copies
					open  	c_jrt ( b_key => l_jrp_key);
					fetch 	c_jrt
					into  	r_jrt;
					close 	c_jrt;
					l_printer := r_jrt.export_target;
					l_copies  := r_jrt.copies;

					-- now call the Create PackParcel procedure by container_id
					if 	l_container_id is not null
					then
						cnl_centiro_pck.create_packparcel( p_site_id_i      	=> l_site_id
										 , p_client_id_i    	=> l_client_id
										 , p_order_id_i     	=> l_order_id
										 , p_pallet_id_i    	=> null
										 , p_container_id_i 	=> l_container_id
										 , p_printer_i      	=> l_printer
										 , p_copies_i       	=> l_copies
										 , p_print2file_i   	=> l_print2file
										 , p_rtk_key_i	    	=> P_key_i
										 , p_run_task_i		=> r_rtk
										 );        
						-- Add log record
						if	l_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
												   , p_file_name_i		=> null
												   , p_source_package_i		=> l_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Trigger Centiro PackParcel interface from Parcel Packing (Web Client) (UREPPARCELPACKING)'
												   , p_code_parameters_i 	=> '"Printer" "'||l_printer||'" '
																|| '"copies" "'||l_copies||'" '
																|| '"print2file" "'||l_print2file||'" '
												   , p_order_id_i		=> l_order_id
												   , p_client_id_i		=> l_client_id
												   , p_pallet_id_i		=> null
												   , p_container_id_i		=> l_container_id
												   , p_site_id_i		=> l_site_id
												   );
						end if;
					end if;
				end if;
				-- Always add Run Task for Container Label
				l_result := dcsdba.libruntask.createruntask( stationid             => l_station_id
									   , userid                => l_user_id
									   , commandtext           => '"'
												   || g_rtk_cmd_ssv_plt_con 
												   || '" "lp" "J" "1" '
												   || '"site_id" "'        || l_site_id
												   || '" "client_id" "'    || l_client_id
												   || '" "order_id" "'     || l_order_id
												   || '" "container_id" "' || l_container_id
												   || '"'
									   , nametext              => g_jr_urepssvpltcon
									   , siteid                => l_site_id
									   , tmplanguage           => 'EN_GB'
									   , p_javareport          => g_yes
									   , p_archive             => g_no
									   , p_runlight            => null
									   , p_serverinstance      => null
									   , p_priority            => null
									   , p_timezonename        => 'Europe/Amsterdam'
									   , p_archiveignorescreen => null
									   , p_archiverestrictuser => null
									   , p_clientid            => l_client_id
									   , p_emailrecipients     => null
									   , p_masterkey           => null
									   , p_usedbtimezone       => g_no
									   , p_nlscalendar         => 'Gregorian'
									   , p_emailattachment     => null
									   , p_emailsubject        => null
									   , p_emailmessage        => null
									   );
				-- Add log record
				if	l_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
										   , p_file_name_i		=> null
										   , p_source_package_i		=> l_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Create run task for container label generation during parcel packing'
										   , p_code_parameters_i 	=> '"station" "'||l_station_id||'" '
														|| '"user_id" "'||l_user_id||'" '
														|| '"print2file" "'||l_print2file||'" '
														|| '"command_text" "('|| '"'
																     || g_rtk_cmd_ssv_plt_con 
																     || '" "lp" "J" "1" '
																     || '"site_id" "'        || l_site_id
																     || '" "client_id" "'    || l_client_id
																     || '" "order_id" "'     || l_order_id
																     || '" "container_id" "' || l_container_id
																     || '"'||')" '
														|| '"nametext" "'||g_jr_urepssvpltcon||'" '
														|| '"javareport" "'||g_yes||'" '
														|| '"archive" "'||g_no||'" '
														|| '"timezonename" "'||'"Europe/Amsterdam" '
														|| '"usedbtimezone" "'||g_no
														|| '"nlscalendar" "'||'"Gregorian" '
										   , p_order_id_i		=> l_order_id
										   , p_client_id_i		=> l_client_id
										   , p_pallet_id_i		=> null
										   , p_container_id_i		=> l_container_id
										   , p_site_id_i		=> l_site_id
										   );
				end if;
				commit;

			-- Trigger Centiro PackParcel interface from Web Repacking (Web Client) (UREPREPACKING)
			when g_rtk_cmd_repacking
			then
				-- Skip PackParcel process if Workstation is linked to a DWS system (l_print_at_dws_yn = 'Y') and Carrier is enabled for DWS and DWS is enabled, PackParcel will be triggered from DWS
				if nvl(l_print_at_dws_yn, 'N') = g_no
				then 
					-- Call Centiro PackParcel interface procedure for best match Java_Report_Map
					open	c_jrp( b_report_name => g_jr_urepctopackparcel -- always with UREPCTOPACKPARCEL report name
						     , b_site_id     => l_site_id
						     , b_client_id   => l_client_id
						     , b_owner_id    => l_owner_id
						     , b_order_id    => l_order_id
						     , b_carrier_id  => l_carrier_id
						     , b_user_id     => l_user_id
						     , b_station_id  => l_station_id
						     , b_locality    => nvl( l_locality, l_rdtlocality)
						     );
					fetch 	c_jrp
					into  	r_jrp;
					close 	c_jrp;
					l_jrp_key := r_jrp.key;

					-- get the printer and copies
					open  	c_jrt ( b_key => l_jrp_key);
					fetch 	c_jrt
					into  	r_jrt;
					close 	c_jrt;
					l_printer := r_jrt.export_target;
					l_copies  := r_jrt.copies;

					-- now call the Create PackParcel procedure by pallet_id
					if 	l_pallet_id is not null
					then
						cnl_centiro_pck.create_packparcel( p_site_id_i      	=> l_site_id
										 , p_client_id_i    	=> l_client_id
										 , p_order_id_i     	=> l_order_id
										 , p_pallet_id_i    	=> l_pallet_id
										 , p_container_id_i 	=> null
										 , p_printer_i      	=> l_printer
										 , p_copies_i       	=> l_copies
										 , p_print2file_i   	=> l_print2file
										 , p_rtk_key_i	    	=> P_key_i
										 , p_run_task_i		=> r_rtk
										 );        
						-- Add log record
						if	l_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
												   , p_file_name_i		=> null
												   , p_source_package_i		=> l_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Trigger Centiro PackParcel interface from Web Repacking (Web Client) (UREPREPACKING)'
												   , p_code_parameters_i 	=> '"Printer" "'||l_printer||'" '
																|| '"copies" "'||l_copies||'" '
																|| '"print2file" "'||l_print2file||'" '
												   , p_order_id_i		=> l_order_id
												   , p_client_id_i		=> l_client_id
												   , p_pallet_id_i		=> l_pallet_id
												   , p_container_id_i		=> null
												   , p_site_id_i		=> l_site_id
												   );
						end if;
					end if;
				end if;

			-- Trigger Centiro PackParcel interface from Pallet Closing (RDT) (UREPPALLETCLOSING or advshippinglabel)
			when g_rtk_cmd_pallet_closing
			then
				-- Check if SSV_PLT_CON for container label is necessary
				begin
					select 	distinct
						m.client_id
					into	l_client_chk
					from 	dcsdba.move_task m 
					inner 
					join 	dcsdba.java_report_map r 
					on 	r.client_id 	= m.client_id 
					and 	r.station_id 	= l_station_id
					and 	r.template_name in ('UREPJSTLBCONLAB','TLB_CONLAB')
					and 	nvl(r.site_id,l_site_id) = l_site_id
					and 	m.pallet_id = l_pallet_id
					;
				exception
					when NO_DATA_FOUND
					then 
						null;
				end;

				if	l_client_chk is not null
				then
					if	l_container_id is null
					then
						select 	container_id 
						into 	l_container_id 
						from 	dcsdba.order_container 
						where 	client_id = l_client_id 
						and 	pallet_id = l_pallet_id 
						and 	rownum=1
						;
					end if;
					l_result := dcsdba.libruntask.createruntask( stationid             => l_station_id
										   , userid                => l_user_id
										   , commandtext           => '"'
													   || g_rtk_cmd_ssv_plt_con 
													   || '" "lp" "J" "1" '
													   || '"site_id" "'        || l_site_id
													   || '" "client_id" "'    || l_client_id
													   || '" "order_id" "'     || l_order_id
													   || '" "container_id" "' || l_container_id
													   || '"'
										   , nametext              => g_jr_urepssvpltcon
										   , siteid                => l_site_id
										   , tmplanguage           => 'EN_GB'
										   , p_javareport          => g_yes
										   , p_archive             => g_no
										   , p_runlight            => null
										   , p_serverinstance      => null
										   , p_priority            => null
										   , p_timezonename        => 'Europe/Amsterdam'
										   , p_archiveignorescreen => null
										   , p_archiverestrictuser => null
										   , p_clientid            => l_client_id
										   , p_emailrecipients     => null
										   , p_masterkey           => null
										   , p_usedbtimezone       => g_no
										   , p_nlscalendar         => 'Gregorian'
										   , p_emailattachment     => null
										   , p_emailsubject        => null
										   , p_emailmessage        => null
										   );
					commit;
				end if;
				-- Add log record
				if	l_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
										   , p_file_name_i		=> null
										   , p_source_package_i		=> l_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Create run task for container label durng pallet closing'
										   , p_code_parameters_i 	=> '"Station" "'||l_station_id||'" '
														|| '"User_id" "'||l_user_id||'" '
														|| '"nametext" "'||g_jr_urepssvpltcon||'" '
										   , p_order_id_i		=> l_order_id
										   , p_client_id_i		=> l_client_id
										   , p_pallet_id_i		=> l_pallet_id
										   , p_container_id_i		=> l_container_id
										   , p_site_id_i		=> l_site_id
										   );
				end if;

				-- Call Centiro PackParcel interface procedure for best match Java_Report_Map
				open	c_jrp( b_report_name => g_jr_urepctopackparcel -- always with UREPCTOPACKPARCEL report name
					     , b_site_id     => l_site_id
					     , b_client_id   => l_client_id
					     , b_owner_id    => l_owner_id
					     , b_order_id    => l_order_id
					     , b_carrier_id  => l_carrier_id
					     , b_user_id     => l_user_id
					     , b_station_id  => l_station_id
					     , b_locality    => nvl( l_locality, l_rdtlocality)
					     );
				fetch 	c_jrp
				into  	r_jrp;
				close 	c_jrp;
				l_jrp_key := r_jrp.key;
				-- get the printer and copies
				open 	c_jrt ( b_key => l_jrp_key);
				fetch 	c_jrt
				into  	r_jrt;
				close 	c_jrt;
				l_printer := r_jrt.export_target;
				l_copies  := r_jrt.copies;

				-- now call the Create PackParcel procedure by pallet_id
				if 	l_pallet_id is not null
				then
					cnl_centiro_pck.create_packparcel( p_site_id_i      	=> l_site_id
									 , p_client_id_i    	=> l_client_id
									 , p_order_id_i     	=> l_order_id
									 , p_pallet_id_i    	=> l_pallet_id
									 , p_container_id_i 	=> null
									 , p_printer_i      	=> l_printer
									 , p_copies_i       	=> l_copies
									 , p_print2file_i   	=> l_print2file
									 , p_rtk_key_i	    	=> P_key_i
									 , p_run_task_i		=> r_rtk
									 );        
				end if;

			-- Trigger Centiro CancelParcel interface from ITL Code Report or User report (UREPCTOCANCELPARCEL)
			when g_rtk_cmd_cto_cancelparcel
			then
				-- Call Centiro PackParcel interface procedure for best match Java_Report_Map
				-- now call the Create PackParcel procedure by container_id
				if l_unit_id is null
				then
					for	r_ocr in c_ocr( b_client_id    => l_client_id
							      , b_order_id     => l_order_id
							      , b_pallet_id    => null
							      , b_container_id => null
							      )
					loop
						if	r_ocr.is_cont_yn = g_yes
						then
							cnl_centiro_pck.create_cancelparcel( p_site_id_i   => l_site_id
											   , p_client_id_i => l_client_id
											   , p_order_id_i  => l_order_id
											   , p_parcel_id   => r_ocr.container_id
											   );
							-- Add log record
							if	l_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
													   , p_file_name_i		=> null
													   , p_source_package_i		=> l_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Cancel parcel for container'
													   , p_code_parameters_i 	=> null
													   , p_order_id_i		=> l_order_id
													   , p_client_id_i		=> l_client_id
													   , p_pallet_id_i		=> null
													   , p_container_id_i		=> r_ocr.container_id
													   , p_site_id_i		=> l_site_id
													   );
							end if;

						else
							cnl_centiro_pck.create_cancelparcel( p_site_id_i   => l_site_id
											   , p_client_id_i => l_client_id
											   , p_order_id_i  => l_order_id
											   , p_parcel_id   => r_ocr.pallet_id
											   );
							-- Add log record
							if	l_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
													   , p_file_name_i		=> null
													   , p_source_package_i		=> l_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Cancel parcel for pallet'
													   , p_code_parameters_i 	=> null
													   , p_order_id_i		=> l_order_id
													   , p_client_id_i		=> l_client_id
													   , p_pallet_id_i		=> r_ocr.pallet_id
													   , p_container_id_i		=> null
													   , p_site_id_i		=> l_site_id
													   );
							end if;
						end if;
					end loop;
				else
					open	c_ocr_unt( b_client_id => l_client_id
							 , b_order_id  => l_order_id
							 , b_unit_id   => l_unit_id
							 );
					fetch 	c_ocr_unt
					into 	r_ocr_unt;
					if 	c_ocr_unt%found
					then
						if	r_ocr_unt.is_cont_yn = g_yes
						then
							cnl_centiro_pck.create_cancelparcel( p_site_id_i   => l_site_id
											   , p_client_id_i => l_client_id
											   , p_order_id_i  => l_order_id
											   , p_parcel_id   => r_ocr_unt.container_id
											   );
							-- Add log record
							if	l_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
													   , p_file_name_i		=> null
													   , p_source_package_i		=> l_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Cancel parcel for unit container'
													   , p_code_parameters_i 	=> null
													   , p_order_id_i		=> l_order_id
													   , p_client_id_i		=> l_client_id
													   , p_pallet_id_i		=> r_ocr.pallet_id
													   , p_container_id_i		=> null
													   , p_site_id_i		=> l_site_id
													   );
							end if;
						else
							if	r_ocr_unt.container_id = l_unit_id
							then
								cnl_centiro_pck.create_cancelparcel( p_site_id_i   => l_site_id
												   , p_client_id_i => l_client_id
												   , p_order_id_i  => l_order_id
												   , p_parcel_id   => r_ocr_unt.container_id
												   );
								-- Add log record
								if	l_log = 'ON'
								then
									cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
														   , p_file_name_i		=> null
														   , p_source_package_i		=> l_pck
														   , p_source_routine_i		=> l_rtn
														   , p_routine_step_i		=> 'Cancel parcel for unit container'
														   , p_code_parameters_i 	=> null
														   , p_order_id_i		=> l_order_id
														   , p_client_id_i		=> l_client_id
														   , p_pallet_id_i		=> null
														   , p_container_id_i		=> r_ocr_unt.container_id
														   , p_site_id_i		=> l_site_id
														   );
								end if;
							else
								cnl_centiro_pck.create_cancelparcel( p_site_id_i   => l_site_id
												   , p_client_id_i => l_client_id
												   , p_order_id_i  => l_order_id
												   , p_parcel_id   => r_ocr_unt.pallet_id
												   );
								-- Add log record
								if	l_log = 'ON'
								then
									cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
														   , p_file_name_i		=> null
														   , p_source_package_i		=> l_pck
														   , p_source_routine_i		=> l_rtn
														   , p_routine_step_i		=> 'Cancel parcel for unit pallet'
														   , p_code_parameters_i 	=> null
														   , p_order_id_i		=> l_order_id
														   , p_client_id_i		=> l_client_id
														   , p_pallet_id_i		=> r_ocr.pallet_id
														   , p_container_id_i		=> null
														   , p_site_id_i		=> l_site_id
														   );
								end if;
							end if;
						end if;
					else
						cnl_centiro_pck.create_cancelparcel( p_site_id_i   => l_site_id
										   , p_client_id_i => l_client_id
										   , p_order_id_i  => l_order_id
										   , p_parcel_id   => l_unit_id
										   );
						-- Add log record
						if	l_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
												   , p_file_name_i		=> null
												   , p_source_package_i		=> l_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Cancel parcel for unit'
												   , p_code_parameters_i 	=> '"unit" "'||l_unit_id||'" '
												   , p_order_id_i		=> l_order_id
												   , p_client_id_i		=> l_client_id
												   , p_pallet_id_i		=> null
												   , p_container_id_i		=> null
												   , p_site_id_i		=> l_site_id
												   );
						end if;
					end if;
					close 	c_ocr_unt;
				end if;
	--
	-- STREAMSERVE/JASPERSOFT
	--    
			-- Call StreamServe Packlist file procedure for Container Label only
			when g_rtk_cmd_ssv_plt_con
			then
				open	c_jsp( b_report_name => g_jr_urepssvpltcon 
					     , b_site_id     => l_site_id
					     , b_client_id   => l_client_id
					     , b_owner_id    => l_owner_id
					     , b_order_id    => l_order_id
					     , b_carrier_id  => l_carrier_id
					     , b_user_id     => l_user_id
					     , b_station_id  => l_station_id
					     , b_locality    => nvl( l_locality, l_rdtlocality)
					     );
				fetch 	c_jsp
				into  	r_jsp;
				close 	c_jsp;
				--
				open	c_ssv( b_report_name => g_jr_urepssvpltcon 
					     , b_site_id     => l_site_id
					     , b_client_id   => l_client_id
					     , b_owner_id    => l_owner_id
					     , b_order_id    => l_order_id
					     , b_carrier_id  => l_carrier_id
					     , b_user_id     => l_user_id
					     , b_station_id  => l_station_id
					     , b_locality    => nvl( l_locality, l_rdtlocality)
					     );
				fetch 	c_ssv
				into  	r_ssv;
				close 	c_ssv;
				-- There are streamserve documents to be printed
				if	r_ssv > 0
				then
					cnl_streamserve_pck.create_packlist( p_site_id_i       	=> l_site_id
									   , p_client_id_i     	=> l_client_id
									   , p_owner_id_i      	=> l_owner_id
									   , p_order_id_i      	=> l_order_id
									   , p_carrier_id_i    	=> l_carrier_id
									   , p_pallet_id_i     	=> l_pallet_id
									   , p_container_id_i  	=> l_container_id
									   , p_reprint_yn_i    	=> g_no
									   , p_user_i          	=> l_user_id
									   , p_workstation_i   	=> l_station_id
									   , p_locality_i      	=> l_locality
									   , p_report_name_i   	=> g_jr_urepssvpltcon
									   , p_rtk_key         	=> l_rtk_key
									   , p_pdf_link_i      	=> l_pdf_link
									   , p_pdf_autostore_i 	=> l_pdf_autostore
									   , p_run_task_i	=> r_rtk
									   );  
					-- Add log record
					if	l_log = 'ON'
					then
						cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
											   , p_file_name_i		=> null
											   , p_source_package_i		=> l_pck
											   , p_source_routine_i		=> l_rtn
											   , p_routine_step_i		=> 'Call StreamServe create Packlist file procedure for Container Label only'
											   , p_code_parameters_i 	=> '"owner_id" "'||l_owner_id||'" '
															|| '"Carrier_id" "'||l_carrier_id||'" '
															|| '"reprint" "'||g_no||'" '
															|| '"user_id" "'||l_user_id||'" '
															|| '"station_id" "'||l_station_id||'" '
															|| '"report_name" "'||g_jr_urepssvpltcon||'" '
															|| '"pdf_link" "'||l_pdf_link||'" '
															|| '"pdf_autostore" "'||l_pdf_autostore||'" '
											   , p_order_id_i		=> l_order_id
											   , p_client_id_i		=> l_client_id
											   , p_pallet_id_i		=> l_pallet_id
											   , p_container_id_i		=> l_container_id
											   , p_site_id_i		=> l_site_id
											   );
					end if;
				end if;
				if 	r_jsp > 0
				then
					-- There are Jaspersoft documents to be printed				
					cnl_jaspersoft_pck.print_doc_p( p_site_id_i       	=> l_site_id
								      , p_client_id_i     	=> l_client_id
								     -- , p_owner_id_i    	  => l_owner_id
								      , p_order_id_i      	=> l_order_id
								     -- , p_carrier_id_i  	  => l_carrier_id
								      , p_pallet_id_i     	=> l_pallet_id
								      , p_container_id_i  	=> l_container_id
								      , p_reprint_yn_i    	=> g_no
								      , p_user_i          	=> l_user_id
								      , p_workstation_i   	=> l_station_id
								    --  , p_locality_i    	  => l_locality
								      , p_report_name_i   	=> g_jr_urepssvpltcon
								      , p_rtk_key         	=> l_rtk_key
								      , p_pdf_link_i      	=> l_pdf_link
								      , p_pdf_autostore_i 	=> l_pdf_autostore
								      , p_run_task_i		=> r_rtk
								      );
					-- Add log record
					if	l_log = 'ON'
					then
						cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
											   , p_file_name_i		=> null
											   , p_source_package_i		=> l_pck
											   , p_source_routine_i		=> l_rtn
											   , p_routine_step_i		=> 'Call cnl_jaspersoft_pck.print_doc_p for Container Label only'
											   , p_code_parameters_i 	=> '"reprint" "'||g_no||'" '
															|| '"user_id" "'||l_user_id||'" '
															|| '"station_id" "'||l_station_id||'" '
															|| '"report_name" "'||g_jr_urepssvpltcon||'" '
															|| '"pdf_link" "'||l_pdf_link||'" '
															|| '"pdf_autostore" "'||l_pdf_autostore||'" '
											   , p_order_id_i		=> l_order_id
											   , p_client_id_i		=> l_client_id
											   , p_pallet_id_i		=> l_pallet_id
											   , p_container_id_i		=> l_container_id
											   , p_site_id_i		=> l_site_id
											   );
					end if;
				end if;

			-- Call StreamServe Packlist file procedure for All documents
			when g_rtk_cmd_ssv_plt_all
			then
				-- No repack tasks anymore, process the Run_Task
				cnl_streamserve_pck.create_packlist( p_site_id_i       	=> l_site_id
								   , p_client_id_i     	=> l_client_id
								   , p_owner_id_i      	=> l_owner_id
								   , p_order_id_i      	=> l_order_id
								   , p_carrier_id_i    	=> l_carrier_id
								   , p_pallet_id_i     	=> l_pallet_id
								   , p_container_id_i  	=> l_container_id
								   , p_reprint_yn_i    	=> g_no
								   , p_user_i          	=> l_user_id
								   , p_workstation_i   	=> l_station_id
								   , p_locality_i      	=> l_locality
								   , p_report_name_i   	=> g_jr_urepssvplt
								   , p_rtk_key         	=> l_rtk_key
								   , p_pdf_link_i      	=> l_pdf_link
								   , p_pdf_autostore_i 	=> l_pdf_autostore
								   , p_run_task_i	=> r_rtk
								   );
				-- Add log record
				if	l_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
										   , p_file_name_i		=> null
										   , p_source_package_i		=> l_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Call StreamServe Packlist file procedure for All documents'
										   , p_code_parameters_i 	=> '"owner_id" "'||l_owner_id||'" '
														|| '"Carrier_id" "'||l_carrier_id||'" '
														|| '"reprint" "'||g_no||'" '
														|| '"user_id" "'||l_user_id||'" '
														|| '"station_id" "'||l_station_id||'" '
														|| '"report_name" "'||g_jr_urepssvplt||'" '
														|| '"pdf_link" "'||l_pdf_link||'" '
														|| '"pdf_autostore" "'||l_pdf_autostore||'" '
										   , p_order_id_i		=> l_order_id
										   , p_client_id_i		=> l_client_id
										   , p_pallet_id_i		=> l_pallet_id
										   , p_container_id_i		=> l_container_id
										   , p_site_id_i		=> l_site_id
										   );
				end if;

			-- Call StreamServe Packlist file procedure per pallet
			when g_rtk_cmd_ssv_plt_pal
			then
				cnl_streamserve_pck.create_packlist( p_site_id_i       	=> l_site_id
								   , p_client_id_i     	=> l_client_id
								   , p_owner_id_i      	=> l_owner_id
								   , p_order_id_i      	=> l_order_id
								   , p_carrier_id_i    	=> l_carrier_id
								   , p_pallet_id_i     	=> l_pallet_id
								   , p_container_id_i  	=> l_container_id
								   , p_reprint_yn_i    	=> g_no
								   , p_user_i          	=> l_user_id
								   , p_workstation_i   	=> l_station_id
								   , p_locality_i      	=> l_locality
								   , p_report_name_i   	=> g_jr_urepssvpltpal
								   , p_rtk_key         	=> l_rtk_key
								   , p_pdf_link_i      	=> l_pdf_link
								   , p_pdf_autostore_i 	=> l_pdf_autostore
								   , p_run_task_i	=> r_rtk
								   );
				-- Add log record
				if	l_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
										   , p_file_name_i		=> null
										   , p_source_package_i		=> l_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Call StreamServe Packlist file procedure per pallet'
										   , p_code_parameters_i 	=> '"owner_id" "'||l_owner_id||'" '
														|| '"Carrier_id" "'||l_carrier_id||'" '
														|| '"reprint" "'||g_no||'" '
														|| '"user_id" "'||l_user_id||'" '
														|| '"station_id" "'||l_station_id||'" '
														|| '"report_name" "'||g_jr_urepssvpltpal||'" '
														|| '"pdf_link" "'||l_pdf_link||'" '
														|| '"pdf_autostore" "'||l_pdf_autostore||'" '
										   , p_order_id_i		=> l_order_id
										   , p_client_id_i		=> l_client_id
										   , p_pallet_id_i		=> l_pallet_id
										   , p_container_id_i		=> l_container_id
										   , p_site_id_i		=> l_site_id
										   );
				end if;

			-- Call StreamServe Trolley list file procedure
			when g_rtk_cmd_ssv_trl_all
			then
				cnl_streamserve_pck.create_trolley_list( p_site_id_i       	=> l_site_id
								       , p_list_id_i       	=> l_list_id
								       , p_report_name_i   	=> g_jr_urepssvtrl
								       , p_user_i          	=> l_user_id
								       , p_workstation_i   	=> l_station_id
								       , p_rtk_key         	=> l_rtk_key
								       , p_pdf_link_i      	=> l_pdf_link
								       , p_pdf_autostore_i 	=> l_pdf_autostore
								       , p_run_task_i		=> r_rtk
								       );
				-- Add log record
				if	l_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
										   , p_file_name_i		=> null
										   , p_source_package_i		=> l_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Call StreamServe Trolley list file procedure'
										   , p_code_parameters_i 	=> '"list_id" "'||l_list_id||'" '
														|| '"user_id" "'||l_user_id||'" '
														|| '"station_id" "'||l_station_id||'" '
														|| '"report_name" "'||g_jr_urepssvtrl||'" '
														|| '"pdf_link" "'||l_pdf_link||'" '
														|| '"pdf_autostore" "'||l_pdf_autostore||'" '
										   , p_order_id_i		=> null
										   , p_client_id_i		=> null
										   , p_pallet_id_i		=> null
										   , p_container_id_i		=> null
										   , p_site_id_i		=> l_site_id
										   );
				end if;
			end case;
		end if;

		-- Check if run task already has been commited
		select	count(*) 
		into 	l_exists
		from 	dcsdba.run_task 
		where 	key = l_rtk_key;

		if	nvl(l_failed,'N') = 'N'
		then
			-- update the Run Task, set status to Complete
			if	l_exists = 0
			then
				update 	cnl_sys.cnl_tmp_run_task rtk
				set    	rtk.status	        = 'Complete'
				,	rtk.old_dstamp		= dstamp
				,	dstamp 			= sysdate
				where  	rtk.key    		= l_rtk_key
				;
			else
				update 	dcsdba.run_task rtk
				set    	rtk.status	        = 'Complete'
				,	rtk.old_dstamp		= dstamp
				,	dstamp 			= sysdate
				where  	rtk.key    		= l_rtk_key
				;
				delete	cnl_sys.cnl_tmp_run_task rtk
				where	rtk.key = l_rtk_key
				;
			end if;
			-- Add log record
			if	l_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
									   , p_file_name_i		=> null
									   , p_source_package_i		=> l_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Updating run task to status Complete'
									   , p_code_parameters_i 	=> null
									   , p_order_id_i		=> null
									   , p_client_id_i		=> null
									   , p_pallet_id_i		=> null
									   , p_container_id_i		=> null
									   , p_site_id_i		=> l_site_id
									   );
			end if;
		else
			if	l_exists = 0
			then
				update  cnl_sys.cnl_tmp_run_task rtk
				set  	rtk.status      = 'Failed'
				,	rtk.old_dstamp	= dstamp
				,	dstamp 		= sysdate
				where   rtk.key    	= l_rtk_key
				;
			else
				update  dcsdba.run_task rtk
				set  	rtk.status      = 'Failed'
				,	rtk.old_dstamp	= dstamp
				,	dstamp 		= sysdate
				where   rtk.key    	= l_rtk_key
				;
				insert	
				into 	dcsdba.run_task_err(key, line_id, text)
				values
				(	l_rtk_key
				,	1
				, 	l_fail_reason
				)
				;
				delete  cnl_sys.cnl_tmp_run_task rtk
				where	rtk.key = l_rtk_key
				;
			end if
			;
			-- Add log record
			if	l_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
									   , p_file_name_i		=> null
									   , p_source_package_i		=> l_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Updating run task to status Failed'
									   , p_code_parameters_i 	=> null
									   , p_order_id_i		=> null
									   , p_client_id_i		=> null
									   , p_pallet_id_i		=> null
									   , p_container_id_i		=> null
									   , p_site_id_i		=> l_site_id
									   );
			end if;
		end if;
	exception
		when others
		then
			case
			when 	c_ocr%isopen
			then
				close 	c_ocr;
			when 	c_ocr_unt%isopen
			then
				close 	c_ocr_unt;
			when 	c_jrp%isopen
			then
				close 	c_jrp;
			when 	c_jrt%isopen
			then
				close 	c_jrt;
			when 	c_jrl%isopen
			then
				close 	c_jrl;
			when 	c_ohr%isopen
			then
				close 	c_ohr;
			when 	c_wsn%isopen
			then
				close c_wsn;
			else
				null;
			end case;

			select	count(*) 
			into 	l_exists
			from 	dcsdba.run_task 
			where 	key = l_rtk_key;

			if	l_exists = 0
			then
				update  cnl_sys.cnl_tmp_run_task rtk
				set  	rtk.status      = 'Failed'
				where   rtk.key    	= l_rtk_key
				;
			else
				update  dcsdba.run_task rtk
				set  	rtk.status      = 'Failed'
				where   rtk.key    	= l_rtk_key
				;
				insert
				into 	dcsdba.run_task_err(key, line_id, text)
				values
				(	l_rtk_key
				,	1
				, 	'Exception in process run task procedure'
				)
				;
				delete	cnl_sys.cnl_tmp_run_task rtk
				where	rtk.key = l_rtk_key
				;
			end if;
			commit;
			-- Add log record
			if	l_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> p_key_i
									   , p_file_name_i		=> null
									   , p_source_package_i		=> l_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'An exception was raised. Run task set to status Failed'
									   , p_code_parameters_i 	=> null
									   , p_order_id_i		=> null
									   , p_client_id_i		=> null
									   , p_pallet_id_i		=> null
									   , p_container_id_i		=> null
									   , p_site_id_i		=> null
									   );
			end if;
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> l_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );

  end process_runtask;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 17-Jun-2016
-- Purpose : Process the Customs Streamliner ITN from WMS
------------------------------------------------------------------------------------------------
  procedure process_itn_csl ( p_key_i         in integer
                            , p_client_id_i   in varchar2
                            , p_from_status_i in varchar2 := null
                            , p_to_status_i   in varchar2 := null
                            )
  is
    cursor c_itn ( b_key in integer)
    is
      select key
      ,      code
      ,      notes
      ,      site_id
      ,      client_id
      ,      reference_id
      ,      update_qty
      from   dcsdba.inventory_transaction
      where  key = b_key
      ;
    cursor c_itn_rct ( b_client_id    in varchar2
                     , b_reference_id in varchar2
                     )
    is
      select count(*)
      from   dcsdba.inventory_transaction
      where  code         = 'Receipt'
      and    client_id    = b_client_id
      and    reference_id = b_reference_id
      ;    
    cursor c_ohr ( b_client_id in varchar2
                 , b_order_id   in varchar2
                 )
    is
      select nvl( export, g_no) export
      from   dcsdba.order_header
      where  client_id = b_client_id
      and    order_id  = b_order_id
      ;
  r_itn      c_itn%rowtype;

  l_csl_enabled integer := 0;
  l_exp_enabled integer := 0;
  l_export_yn   varchar2(1) := g_no;
  l_count       integer := 0;
  l_from_status varchar2(15);
  l_to_status   varchar2(15);

  begin
    l_from_status := p_from_status_i;
    l_to_status   := p_to_status_i;

    -- Check if CLIENT_ID is enabled as Bonded Warehouse Customer, configured in Client Visibility Group CSLENT
    l_csl_enabled := is_csl_enabled ( p_client_id_i => p_client_id_i); 

    if l_csl_enabled = 1
    then
      open  c_itn ( b_key => p_key_i);
      fetch c_itn
      into  r_itn;
      if c_itn%notfound
      then
        close c_itn;
        cnl_db_job_pck.submit_once( p_procedure_i => 'begin cnl_sys.cnl_wms_pck.process_itn_csl (' || p_key_i         || ','''
                                                                                                   || p_client_id_i   || ''','''
                                                                                                   || p_from_status_i || ''','''
                                                                                                   || p_to_status_i   || '''); end;'
                                  , p_code_i      => 'CSLR_' || p_key_i
                                  , p_delay_i     => 5
                                  );        
      else
        close c_itn;
        case r_itn.code
        when g_pae
        then
          -- check if any Receipts done for this PreAdvice to prevent Streamliner file is updated with empty lines
          open  c_itn_rct ( b_client_id    => r_itn.client_id
                          , b_reference_id => r_itn.reference_id
                          );
          fetch c_itn_rct
          into  l_count;
          close c_itn_rct;

          if l_to_status = g_complete
          and l_count > 0
          then
            cnl_streamsoft_pck.create_inbound_receipt ( p_site_id_i      => r_itn.site_id
                                                      , p_client_id_i    => r_itn.client_id
                                                      , p_reference_id_i => r_itn.reference_id
                                                      );
          end if;
        when g_ajt
        then
          if r_itn.update_qty < 0
          then
            cnl_streamsoft_pck.create_adjustment_minus ( p_site_id_i   => r_itn.site_id
                                                       , p_client_id_i => r_itn.client_id
                                                       , p_key_i       => r_itn.key
                                                       );
          end if;
        when g_odr
        then
        -- Generate Reconditioning file for Varian
         if l_to_status = g_ready_to_load
          then
            cnl_streamsoft_pck.create_outbound_entrepot ( p_site_id_i      => r_itn.site_id
                                                        , p_client_id_i    => r_itn.client_id
                                                        , p_reference_id_i => r_itn.reference_id
                                                        );
            if r_itn.client_id = 'VARIAN'
             then
	            cnl_streamsoft_pck.create_outbound_reconditioning ( p_site_id_i      => r_itn.site_id
	                                                        , p_client_id_i    => r_itn.client_id
	                                                        , p_reference_id_i => r_itn.reference_id
	                                                        );        
            end if;
          end if;
        end case;
      end if;
    else
      -- Check if CLIENT_ID is enabled as Export declarations client, configured in Client Visibility Group CSLEXP
      l_exp_enabled := is_exp_enabled ( p_client_id_i => p_client_id_i); 

      if l_exp_enabled = 1
      then
        open  c_itn ( b_key => p_key_i);
        fetch c_itn
        into  r_itn;
        if c_itn%notfound
        then
          close c_itn;
          cnl_db_job_pck.submit_once( p_procedure_i => 'begin cnl_sys.cnl_wms_pck.process_itn_csl (' || p_key_i         || ','''
                                                                                                     || p_client_id_i   || ''','''
                                                                                                     || p_from_status_i || ''','''
                                                                                                     || p_to_status_i   || '''); end;'
                                    , p_code_i      => 'CSLR_' || p_key_i
                                    , p_delay_i     => 5
                                    );        
        else
          close c_itn;
          if r_itn.code = g_odr
          then
            open  c_ohr ( b_client_id => r_itn.client_id
                        , b_order_id  => r_itn.reference_id
                        );
            fetch c_ohr
            into  l_export_yn;
            close c_ohr;
            --
            if l_to_status = g_ready_to_load
            and l_export_yn = g_yes
            then
              cnl_streamsoft_pck.create_outbound_export ( p_site_id_i      => r_itn.site_id
                                                        , p_client_id_i    => r_itn.client_id
                                                        , p_reference_id_i => r_itn.reference_id
                                                        );
            end if;
          end if;
        end if;
      end if;
    end if; 
  end process_itn_csl;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 10-Dec-2018
-- Purpose : Process WLGore VAT interface to Customs Streamliner from WMS
------------------------------------------------------------------------------------------------
  procedure process_wlgore_vat ( p_site_id_i      in varchar2
                               , p_client_id_i    in varchar2
                               , p_shipped_date_i in date
                               )
  is
    cursor c_smt ( b_site_id      in varchar2
                 , b_client_id    in varchar2
                 , b_shipped_date in date
                 )
    is
      select distinct
             site_id
      ,      client_id
      ,      order_id
      from   dcsdba.shipping_manifest
      where  site_id               = b_site_id
      and    client_id             = b_client_id
      and    trunc(shipped_dstamp) = trunc(b_shipped_date)
      order  by order_id
      ;

    r_smt     c_smt%rowtype;

    l_err_txt varchar2(500); 
  begin
    for r_smt in c_smt( b_site_id      => p_site_id_i
                      , b_client_id    => p_client_id_i
                      , b_shipped_date => p_shipped_date_i
                      )
    loop
      cnl_streamsoft_pck.create_inbound_wlgvat( p_site_id_i      => r_smt.site_id
                                              , p_client_id_i    => r_smt.client_id
                                              , p_reference_id_i => r_smt.order_id
                                              );  
    end loop;

  exception
    when others
    then
      case c_smt%isopen
      when true
      then
        close c_smt;
      else
        null;
      end case;

      l_err_txt     := substr( sqlerrm, 1, 500);

  end process_wlgore_vat;  
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 10-Dec-2018
-- Purpose : Process CBS interface to Customs Streamliner from WMS based on orders
------------------------------------------------------------------------------------------------
  procedure process_csl_cbs ( p_site_id_i      in varchar2
                            , p_client_id_i    in varchar2
                            , p_shipped_date_i in date
                            , p_csl_bu_i       in varchar2
                            , p_trans_type_i   in integer  := null
                            )
  is
    cursor c_smt ( b_site_id      in varchar2
                 , b_client_id    in varchar2
                 , b_shipped_date in date
                 )
    is
      select distinct
             site_id
      ,      client_id
      ,      order_id
      from   dcsdba.shipping_manifest
      where  site_id               = b_site_id
      and    client_id             = b_client_id
      and    trunc(shipped_dstamp) = trunc(b_shipped_date)
      order  by order_id
      ;

    r_smt     c_smt%rowtype;

    l_err_txt varchar2(500); 
  begin
    for r_smt in c_smt( b_site_id      => p_site_id_i
                      , b_client_id    => p_client_id_i
                      , b_shipped_date => p_shipped_date_i
                      )
    loop
      cnl_streamsoft_pck.create_outbound_cbs( p_site_id_i      => r_smt.site_id
                                            , p_client_id_i    => r_smt.client_id
                                            , p_reference_id_i => r_smt.order_id
                                            , p_csl_bu_i       => p_csl_bu_i
                                            , p_trans_type_i   => p_trans_type_i
                                            );    
    end loop;

  exception
    when others
    then
      case c_smt%isopen
      when true
      then
        close c_smt;
      else
        null;
      end case;

      l_err_txt     := substr( sqlerrm, 1, 500);

  end process_csl_cbs;  
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 09-Sep-2016
-- Purpose : Function to get the values from the parameters string
------------------------------------------------------------------------------------------------
  function get_parameter_value ( p_parameters_i       in varchar2
                               , p_report_parameter_i in varchar2
                               )
  return varchar2
  is
    l_index      number := 0;
    l_valuestart number := 0;
    l_valueend   number := 0;
    l_value      varchar(4000);
  begin
    l_index := instr(upper(p_parameters_i), '"' || upper(p_report_parameter_i) || '"' );
    if (l_index <= 0) 
    then
      -- parameter not found just return null
      return null;
    end if;

    l_valuestart := instr(p_parameters_i, '"', l_index+1, 2) + 1;
    l_valueend   := instr(p_parameters_i, '"', l_index+1, 3) -1;
    if (l_valuestart > 0 and
        l_valueend   > 0 and
        l_valueend   >= l_valuestart) 
    then
      l_value := substr(p_parameters_i, l_valuestart, (l_valueend - l_valuestart) + 1);
      return l_value;
    end if;

    return null;

  end get_parameter_value;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 09-Sep-2016
-- Purpose : Function to replace the parameters (e.g. <CLIENT_ID>) in select statement
------------------------------------------------------------------------------------------------
  function transform_select ( p_select_i     in varchar2
                            , p_parameters_i in varchar2
                            )
  return varchar2
  is
    l_valuestart  number := 0;
    l_valueend    number := 0;
    l_parameter   varchar2(100);
    l_value       varchar2(256);
    l_select varchar2(4000);
  begin
    l_select := p_select_i;

    loop
      l_valuestart := instr(l_select, '<', l_valuestart+1);
      if (l_valuestart = 0) 
      then
        -- no more parameters so exit
        exit;
      end if;

      l_valueend := instr(l_select, '>', l_valuestart);
      if (l_valueend = 0) 
      then
        -- no end value so exit loop
        exit;
      end if;

      l_parameter := substr(l_select, l_valuestart+1, l_valueend - l_valuestart - 1);
      l_value     := get_parameter_value ( p_parameters_i       => p_parameters_i
                                         , p_report_parameter_i => upper(l_parameter)
                                         );
      l_select    := replace(l_select, '<' || l_parameter || '>', '''' || l_value || '''');

    end loop;

    return l_select;

  end transform_select;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 09-Sep-2016
-- Purpose : Function to get the email addresses from the Advanced Print Mapping setup
------------------------------------------------------------------------------------------------
  function get_jr_email_recipients ( p_jrp_key_i    in number
                                   , p_parameters_i in varchar2
                                   )
  return varchar2
  is
    cursor c_jrl ( b_jrp_key number)
    is
      select email_address
      ,      email_select
      from   dcsdba.java_report_email
      where  key = b_jrp_key
      ;

    r_jrl              c_jrl%rowtype;

    l_email_recipients varchar2(4000);
    l_email_address    varchar2(256);
    l_email_select     varchar2(4000);
  begin
    for r_jrl in c_jrl (b_jrp_key => p_jrp_key_i) 
    loop
      if r_jrl.email_select is not null 
      then
        l_email_select := transform_select ( p_select_i     => r_jrl.email_select
                                           , p_parameters_i => p_parameters_i
                                           );
        begin
          execute immediate l_email_select
          into l_email_address;
        exception
          when no_data_found 
          then
            l_email_address := '';
          when others 
          then
            l_email_address := sqlerrm; --'';
        end;
      else
        l_email_address := r_jrl.email_address;
      end if;

      if l_email_address is not null
      then
        if l_email_recipients is not null
        then
          l_email_recipients := l_email_recipients 
                             || ';'
                             ;
        end if;

        l_email_recipients := l_email_recipients 
                           || l_email_address
                           ;
      end if;
    end loop;

    return l_email_recipients;

  end get_jr_email_recipients;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 24-Jan-2017
-- Purpose : Function to get the tracking URL from Centiro or Logitrack CMS
------------------------------------------------------------------------------------------------
  function get_tracking_url ( p_wms_carrier_id  in  varchar2
                            , p_wms_tracking_nr in  varchar2
                            )
    return varchar2
  is      
    cursor c_cda ( b_carrier_id  varchar2
                 , b_tracking_nr varchar2
                 )
    is
      select 	cda.cto_tracking_url
      from   cnl_container_data cda
      where  cda.carrier_id      = b_carrier_id
      and    cda.cto_tracking_nr = b_tracking_nr
      order  by cda.cto_ppr_dstamp desc	
      ;

	cursor c_saas ( b_carrier_id 	varchar2
		      , b_tracking_nr 	varchar2
		      )
	is
		select	saas.tracking_url
		from	cnl_cto_ship_labels saas
		where	saas.carrier_id = b_carrier_id
		and	saas.tracking_number = b_tracking_nr
		order by creation_dstamp desc
	;
	l_on_premise	varchar2(1000);
	l_saas		varchar2(1000);
    l_retval varchar2(1000);
  begin
    l_retval := null;

    open  c_cda ( b_carrier_id  => p_wms_carrier_id
                , b_tracking_nr => p_wms_tracking_nr
                );
    fetch c_cda
    into  l_on_premise;--l_retval;
    close c_cda;  

    open  c_saas ( b_carrier_id  => p_wms_carrier_id
                 , b_tracking_nr => p_wms_tracking_nr
                 );
    fetch c_saas
    into  l_saas;--l_retval;
    close c_saas;
    
	if	l_saas is not null
	then
		l_retval := l_saas;
	else
		l_retval := l_on_premise;
	end if;
    return l_retval;

  end get_tracking_url;
-----------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 28-Nov-2010
-- Purpose : Get Order Sequence
------------------------------------------------------------------------------------------------
procedure get_order_sequence ( p_client_id_i in  varchar2
                             , p_udp_1_i     in  varchar2 := null
                             , p_udp_2_i     in  varchar2 := null
                             , p_ose_type_i  in  varchar2
                             , p_date_i      in  date     := null
                             , p_sequence_o  out varchar2
                             )
is
  cursor c_ose ( b_client_id varchar2
               , b_udp_1     varchar2
               , b_udp_2     varchar2
               , b_ose_type  varchar2
               )
  is
    select ose.*
    from   cnl_order_sequences   ose
    where  ose.client_id         = b_client_id
    and    nvl(ose.udp_1,'|^|')  = nvl(b_udp_1,'|^|')
    and    nvl(ose.udp_2,'|^|')  = nvl(b_udp_2,'|^|')
    and    ose.ose_type          = b_ose_type
    and    nvl(trunc(p_date_i),trunc(sysdate)) >= trunc(ose.start_date)
    and    nvl(trunc(p_date_i),trunc(sysdate)) <= nvl(trunc(ose.end_date),trunc(sysdate+1))
    ;

  r_ose       c_ose%rowtype;
  l_client_id number;
  l_nbr       number;
  l_new_nbr   number;
  l_increment number;
  l_sequence  varchar2(20) := null;
  l_no_seq    varchar2(20) := 'NO SEQ. FOUND';
  e_error     exception;

  pragma autonomous_transaction;
begin

  if p_client_id_i is null
  then
    raise e_error;
  else
    open  c_ose ( b_client_id => p_client_id_i
                , b_udp_1     => p_udp_1_i
                , b_udp_2     => p_udp_2_i
                , b_ose_type  => p_ose_type_i
                );
    fetch c_ose
    into  r_ose;
    close c_ose;

    if r_ose.id is null
    then
      raise e_error;
    else
      l_nbr       := greatest(r_ose.last_used, nvl( r_ose.min_value, r_ose.last_used));
      l_increment := r_ose.increment_by;
      l_new_nbr   := l_nbr + l_increment;

      if l_new_nbr > nvl( r_ose.max_value, 9999999999)
      then
        raise e_error;
      else
        update cnl_order_sequences ose
        set    ose.last_used = l_new_nbr
        where  ose.id = r_ose.id
        ;
        commit
        ;

        l_sequence := r_ose.prefix
                   || substr( lpad( l_new_nbr
                                  , ( greatest(length( l_new_nbr), r_ose.zero_pad))
                                  , 0
                                  )
                            , greatest( -( greatest( length( l_new_nbr), r_ose.zero_pad))
                                      , -10
                                      )
                            )
                   || r_ose.suffix
                   ;
      end if;
    end if;
  end if;

  if l_sequence is null
  then
     p_sequence_o := l_no_seq;
  else
     p_sequence_o := l_sequence;
  end if;

exception
  when e_error
  then
    p_sequence_o := l_no_seq;
  when others
  then
    raise;

end get_order_sequence;
-----------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Dec-2018
-- Purpose : Insert Order Accessorial
------------------------------------------------------------------------------------------------
function ins_order_accessorial ( p_client_id_i    in  varchar2
                               , p_order_id_i     in  varchar2
                               , p_accessorial_i  in  varchar2
                               , p_timezonename_i in  varchar2 := 'Europe/Amsterdam'
                               , p_errortext_o    out varchar2
                               )
  return integer
is
  cursor c_ltt (b_mergeerror in varchar2)
  is
    select b_mergeerror
    ||     ' - '
    ||     text
    from   dcsdba.language_text
    where  language = 'EN_GB'
    and    label    = b_mergeerror
    ;

  r_ltt            c_ltt%rowtype;

  l_err            integer := 1; -- 1 = OK, 0 = Error
  l_err_code       varchar2(20);
  l_err_txt        varchar2(500);
begin

  l_err := dcsdba.libmergeorderaccessory.directorderaccessory ( p_mergeerror   => l_err_code
                                                              , p_toupdatecols => null
                                                              , p_mergeaction  => 'A'
                                                              , p_clientid     => p_client_id_i
                                                              , p_orderid      => p_order_id_i
                                                              , p_accessorial  => p_accessorial_i
                                                              , p_timezonename => p_timezonename_i
                                                              );

  if l_err = 0
  then
    if l_err_code is not null
    then
      open  c_ltt (b_mergeerror => l_err_code);
      fetch c_ltt
      into  l_err_txt;
      close c_ltt;
    end if;
  end if;

  p_errortext_o := l_err_txt;

  return l_err;

exception
  when others
  then
    raise;

end ins_order_accessorial;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 11-Feb-2018
-- Purpose : Synchronize Special Instruction Links for SKU's with specific HazmatID's
------------------------------------------------------------------------------------------------
  procedure sync_sku_special_links
  is
  begin
    --
    -- Create DG-LITHIUM
    --
    insert into dcsdba.special_ins_link (CODE, CLIENT_ID, SKU_ID, SCREEN_NAME)
    select 'DG-LITHIUM' 
    ,      sku.client_id
    ,      sku.sku_id
    ,      'vWZSRepackingRHE'
    from   dcsdba.sku
    ,      dcsdba.hazmat hmt
    where  sku.hazmat_id = hmt.hazmat_id 
    and    substr(hmt.hazmat_id,1,5) = 'RHSUN' 
    and    replace(hmt.user_def_type_4,',',null) in ('966II','967II','969II','970II')
    and    not exists (select 1
                       from   dcsdba.special_ins_link sik
                       where  sik.client_id = sku.client_id
                       and    sik.sku_id    = sku.sku_id
                       and    sik.code      = 'DG-LITHIUM'
                      )
    ;
    commit
    ;
    --
    -- Delete DG-LITHIUM
    --
    delete dcsdba.special_ins_link
    where  code = 'DG-LITHIUM'
    and    client_id || sku_id in
           (
           select sik.client_id
           ||     sik.sku_id
           from   dcsdba.special_ins_link sik
           where  sik.code = 'DG-LITHIUM'
           minus
           select sku.client_id
           ||     sku.sku_id
           from   dcsdba.sku
           ,      dcsdba.hazmat hmt
           where  sku.hazmat_id = hmt.hazmat_id 
           and    substr(hmt.hazmat_id,1,5) = 'RHSUN' 
           and    replace(hmt.user_def_type_4,',',null) in ('966II','967II','969II','970II')
           )
    ;
    commit
    ;
    --
    -- Create DG-ORANGELABEL
    --
    insert into dcsdba.special_ins_link (CODE, CLIENT_ID, SKU_ID, SCREEN_NAME) 
    select 'DG-ORANGELABEL'
    ,      sku.client_id
    ,      sku.sku_id
    ,      'vWZSRepackingRHE'
    from   dcsdba.sku
    ,      dcsdba.hazmat hmt
    where  sku.hazmat_id = hmt.hazmat_id 
    and    substr(hmt.hazmat_id,1,5) = 'RHSUN' 
    and    replace(hmt.user_def_type_4,',',null) in ('965II','968II')
    and    not exists (select 1
                       from   dcsdba.special_ins_link sik
                       where  sik.client_id = sku.client_id
                       and    sik.sku_id    = sku.sku_id
                       and    sik.code      = 'DG-ORANGELABEL'
                      )
    ;
    commit
    ;
    --
    -- Delete DG-ORANGELABEL
    --
    delete dcsdba.special_ins_link
    where  code = 'DG-ORANGELABEL'
    and    client_id || sku_id in
           (
           select sik.client_id
           ||     sik.sku_id
           from   dcsdba.special_ins_link sik
           where  sik.code = 'DG-ORANGELABEL'
           minus
           select sku.client_id
           ||     sku.sku_id
           from   dcsdba.sku
           ,      dcsdba.hazmat hmt
           where  sku.hazmat_id = hmt.hazmat_id 
           and    substr(hmt.hazmat_id,1,5) = 'RHSUN' 
           and    replace(hmt.user_def_type_4,',',null) in ('965II','968II')
           )
    ;
    commit
    ;
    --
    -- Create DG-DONOTSHIP
    --
    insert into dcsdba.special_ins_link (CODE, CLIENT_ID, SKU_ID, SCREEN_NAME)
    select 'DG-DONOTSHIP'
    ,      sku.client_id
    ,      sku.sku_id
    ,      'vWZSRepackingRHE'
    from   dcsdba.sku
    ,      dcsdba.hazmat hmt
    where  sku.hazmat_id = hmt.hazmat_id 
    and    substr(hmt.hazmat_id,1,5) = 'RHSUN' 
    and    replace(hmt.user_def_type_4,',',null) in ('9651A','9651B','966I','967I','9681A','9681B','969I','970I')
    and    not exists (select 1
                       from   dcsdba.special_ins_link sik
                       where  sik.client_id = sku.client_id
                       and    sik.sku_id    = sku.sku_id
                       and    sik.code      = 'DG-DONOTSHIP'
                      )
    ;
    commit
    ;
    --
    -- Delete DG-DONOTSHIP
    --
    delete dcsdba.special_ins_link
    where  code = 'DG-DONOTSHIP'
    and    client_id || sku_id in
           (
           select sik.client_id
           ||     sik.sku_id
           from   dcsdba.special_ins_link sik
           where  sik.code = 'DG-DONOTSHIP'
           minus
           select sku.client_id
           ||     sku.sku_id
           from   dcsdba.sku
           ,      dcsdba.hazmat hmt
           where  sku.hazmat_id = hmt.hazmat_id 
           and    substr(hmt.hazmat_id,1,5) = 'RHSUN' 
           and    replace(hmt.user_def_type_4,',',null) in ('9651A','9651B','966I','967I','9681A','9681B','969I','970I')
           )
    ;
    commit
    ;

  end sync_sku_special_links;
--
--
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 10-Okt-2018
-- Purpose : Adjust inventory
------------------------------------------------------------------------------------------------
	procedure cnl_inventory_adjustment( p_client_id_i       in varchar2
                                          , p_location_id_i     in varchar2
					  , p_owner_id_i        in varchar2 
					  , p_days_i            in number   
					  , p_user_id_i         in varchar2
					  , p_station_id_i      in varchar2
					  , p_reason_id_i       in varchar2
					  , p_site_id_i         in varchar2
					  )
	is
		-- Select all tags from location but not when same tag exists on another storage location
		cursor  c_tag( b_location_id    varchar2
			     , b_client_id      varchar2
                             , b_owner_id       varchar2 
                             , b_site_id        varchar2
                             , b_days           number
                             )
		is
			select	i.tag_id
			,	nvl(s.serial_at_receipt,'N') serial_at_receipt
			from    dcsdba.inventory i
			inner
			join	dcsdba.sku s
			on	s.client_id	= i.client_id
			and	s.sku_id	= i.sku_id
			where   i.location_id 	= b_location_id
			and     i.client_id   	= b_client_id
			and(	i.owner_id	= b_owner_id 
			 or	b_owner_id 	is null)
			and     i.site_id     	= b_site_id
			and     to_char(i.move_dstamp,'YYYYMMDD') <= to_char(sysdate-b_days,'YYYYMMDD')
			-- when SKU does not require serial at receipt no other constraints are involved.
			and(	nvl(s.serial_at_receipt,'N') = 'N' or
			-- When SKU does require serial at receipt we must check if QTY is matching with number of serials and need to ensure tag id is unique on the location
				(	nvl(s.serial_at_receipt,'N') = 'Y'
				and	(	select	count(*)
						from	dcsdba.serial_number sn
						where	sn.site_id 	= i.site_id
						and	sn.client_id 	= i.client_id
						and	sn.tag_id 	= i.tag_id
						and	sn.order_id 	is null
						and	sn.container_id	is null
						and	sn.pallet_id 	is null
					) = i.qty_on_hand
				 and	(	select	count(*)
						from	dcsdba.inventory i2
						inner
						join	dcsdba.location l
						on 	l.location_id 	= i2.location_id
						and	l.site_id 	= i2.site_id
						where	i2.tag_id 	= i.tag_id
						and	i2.client_id 	= i.client_id
						and	i2.site_id 	= i.site_id
						and	i2.owner_id 	= i.owner_id
						and	i2.location_id != b_location_id
						and	l.loc_type 	in ( 'Tag-FIFO'
									   , 'Tag-LIFO'
									   , 'Tag Operator'
									   , 'Bin','Bulk'
									   )
					) = 0
				)
			   )
		;

		-- select all serials linked to tag id
		cursor c_ser( b_site_id varchar2
			    , b_client_id varchar2
			    , b_tag_id varchar2
			    )
		is
			select	s.serial_number
			,	s.sku_id
			from 	dcsdba.serial_number s
			where	s.site_id 	= b_site_id
			and	s.client_id 	= b_client_id
			and	s.tag_id 	= b_tag_id
			and	s.order_id 	is null
			and	s.container_id 	is null
			and	s.pallet_id 	is null
		;
		--

		l_user_id       dcsdba.application_user.user_id%type;
		l_station_id    dcsdba.workstation.station_id%type;
		l_client_id     dcsdba.client.client_id%type;
		l_location_id   dcsdba.location.location_id%type;
		l_owner_id      dcsdba.owner.owner_id%type;
		l_days          number;
		l_reason_id     dcsdba.adjust_reason.reason_id%type;
		l_mergeerror    varchar2(10);
		l_site_id       dcsdba.site.site_id%type;
		l_ok            varchar2(1) := 'Y';
		l_update_col    varchar2(4000);
		l_sn_trans_key  integer;
	begin
		-- Set session user id
		if      p_user_id_i is null
		then
			l_user_id := 'SCHEDULER';
		else
			l_user_id := p_user_id_i;
		end if;

		-- set session station id
		if      p_station_id_i is null
		then
			l_station_id := 'SCHEDULER';
		else
			l_station_id := p_station_id_i;
		end if;

		-- Set client id
		if      p_client_id_i is null
		then
			l_ok := 'N';
		else
			l_client_id := p_client_id_i;
		end if;

		-- Set location id
		if      p_location_id_i is null
		then
			l_ok := 'N';
		else
			l_location_id := p_location_id_i;
		end if;

		-- Set owner id
		l_owner_id := p_owner_id_i;

		-- Set days old
		l_days := nvl(p_days_i,0);

		-- Set reason id
		if      p_reason_id_i is null
		then
			l_ok := 'N';
		else
			l_reason_id := p_reason_id_i;
		end if;

		-- Set site id
		if      p_site_id_i is null
		then
			l_ok := 'N';
		else
			l_site_id := p_site_id_i;
		end if;

		-- When all parameters are in place
		if      l_ok = 'Y'
		then
			-- set session settings
			dcsdba.libsession.InitialiseSession( UserID       => l_user_id
							   , GroupID      => null
							   , StationID    => l_station_id
							   , WksGroupID   => null
							   );
			-- Fetch tag id's and adjust
			<<tag_loop>>
			for     r_tag in c_tag( l_location_id
                                              , l_client_id
					      , l_owner_id
					      , l_site_id
					      , l_days
					      )
			loop
				l_mergeerror := dcsdba.libmergeinvadjust.directinventoryadjust( p_mergeerror    => l_mergeerror
                                                                                              , p_toupdatecols  => null
                                                                                              , p_mergeaction   => 'U'
                                                                                              , p_tagid         => r_tag.tag_id
                                                                                              , p_locationid    => l_location_id
                                                                                              , p_clientid      => l_client_id
                                                                                              , p_ownerid       => l_owner_id
                                                                                              , p_siteid        => l_site_id
                                                                                              , p_quantity      => 0
                                                                                              , p_reasonid      => l_reason_id
                                                                                              );
				-- serial deletion
				if	r_tag.serial_at_receipt = 'Y'
				then
					<<serial_loop>>
					for 	r_ser in c_ser( l_site_id
							      , l_client_id
							      , r_tag.tag_id
							      )
					loop
						l_sn_trans_key := dcsdba.sn_transaction_pk_seq.nextval;
						--
						insert into dcsdba.sn_transaction( key
										 , code
										 , dstamp
										 , user_id
										 , serial_number
										 , client_id
										 , sku_id
										 , tag_id
										 , status
										 , site_id
										 , uploaded
										 , repacked
										 , screen_mode
										 , station_id
										 )
						values( l_sn_trans_key 		--key
						      , 'Delete'		--code
						      , sysdate			--dstamp
						      , l_user_id		--user_id
						      , r_ser.serial_number	--serial_number
						      , l_client_id		--client_id
						      , r_ser.sku_id		--sku_id
						      , r_tag.tag_id		--tag_id
						      , 'I'			--status
						      , l_site_id		--site_id
						      , 'N'			--uploaded
						      , 'N'			--repacked
						      , 'P'			--screen_mode
						      , l_station_id		--station_id
						      )
						;
					end loop;--serial_loop

					-- Delete all serials
					delete 	
					from 	dcsdba.serial_number s
					where	s.site_id 	= l_site_id
					and	s.client_id 	= l_client_id
					and	s.tag_id 	= r_tag.tag_id
					and	s.order_id 	is null
					and	s.container_id 	is null
					and	s.pallet_id 	is null
					;
				end if;
			end loop; -- tag_loop
		else
			null;
		end if;
	exception
		when others
		then
			null;
	end cnl_inventory_adjustment;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 10-Okt-2018
-- Purpose : Procedure used by the trigger on shipping manifest table.
------------------------------------------------------------------------------------------------
 procedure upd_tracking_number( p_key_i			integer
			      , p_client_id_i		varchar2
			      , p_site_id_i		varchar2
			      , p_order_id_i		varchar2
			      , p_container_id_i	varchar2
			      , p_pallet_id_i		varchar2
			      , p_labelled_i		varchar2
			      , p_pallet_labelled_i	varchar2
			      ) 
 is
 begin
	update	dcsdba.shipping_manifest s
	set 	s.carrier_consignment_id = cnl_sys.cnl_edi_pck.get_tracking_nbr_f(p_client_id_i, p_site_id_i, p_order_id_i, p_container_id_i, p_pallet_id_i, p_labelled_i, p_pallet_labelled_i)
	where	s.key = p_key_i;
	commit;
 exception
	when others
	then
		null;
 end upd_tracking_number;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 03-Mar-2021
-- Purpose : Procedure to copy data from tmp run task to original run task
------------------------------------------------------------------------------------------------
procedure update_tmp_run_task_p
is
	cursor c_rtk
	is
		select 	t.*
		from	cnl_tmp_run_task t
		inner
		join	dcsdba.run_task r
		on	r.status	= 'In Progress'
		and	r.key 		= t.key
		where	t.status in ('Complete','Failed')
	;
	-- Select run tasks already 1 min on status in progress
	cursor c_chk
	is
		select	r.key
		from	dcsdba.run_task r
		where	r.status = 'In Progress'
		and	dstamp < sysdate -1/1440
	;
begin
	-- Update run tasks with tmp run task data
	for i in c_rtk
	loop
		update	dcsdba.run_task
		set 	status 		= i.status
		,	old_dstamp	= i.old_dstamp
		,	dstamp		= i.dstamp
		,	server_instance	= i.server_instance
		,	command		= i.command
		where	key 		= i.key
		;
		delete 	cnl_tmp_run_task
		where	key = i.key
		;
	end loop;

	-- Update run tasks where procedure failed
	for i in c_chk
	loop
		update	dcsdba.run_task
		set 	status 		= 'Failed'
		,	old_dstamp	= dstamp
		,	dstamp		= sysdate
		where	key 		= i.key
		;
		delete  cnl_tmp_run_task t
		where	t.key 		= i.key
		;
	end loop;

	-- Delete tmp run tasks witout original run tasks
	delete	cnl_tmp_run_task t
	where	t.key not in (	select	r.key
				from	dcsdba.run_task r
				where	r.key = t.key
			     )
	;
	commit;
exception
	when others
	then
		null;
end update_tmp_run_task_p;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 03-Mar-2021
-- Purpose : Procedure to save a copy of the run task record 
------------------------------------------------------------------------------------------------
procedure save_tmp_run_task_p( p_run_task_i dcsdba.run_task%rowtype)
is
	l_run_task dcsdba.run_task%rowtype := p_run_task_i;
	l_log		   varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_PRE-PRINT-LOG_ENABLE');
	pragma autonomous_transaction;
begin
	insert into cnl_tmp_run_task
	values
	(	l_run_task.key
	,	l_run_task.site_id
	,	l_run_task.station_id
	,	l_run_task.user_id
	,	l_run_task.status
	,	l_run_task.command
	,	l_run_task.pid
	,	l_run_task.old_dstamp
	,	l_run_task.dstamp
	,	l_run_task.language
	,	l_run_task.name
	,	l_run_task.time_zone_name
	,	l_run_task.nls_calendar
	,	l_run_task.print_label
	,	l_run_task.java_report
	,	l_run_task.run_light
	,	l_run_task.server_instance
	,	l_run_task.priority
	,	l_run_task.archive
	,	l_run_task.archive_ignore_screen
	,	l_run_task.archive_restrict_user
	,	l_run_task.client_id
	,	l_run_task.email_recipients
	,	l_run_task.email_attachment
	,	l_run_task.email_subject
	,	l_run_task.email_message
	,	l_run_task.master_key
	,	l_run_task.use_db_time_zone
	)
	;
	commit;
exception
	when others
	then
		if	l_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> null
								   , p_file_name_i		=> null
								   , p_source_package_i		=> 'cnl_wms_pck'
								   , p_source_routine_i		=> 'save_tmp_run_task_p'
								   , p_routine_step_i		=> 'Saving the temp run task'
								   , p_code_parameters_i 	=> null
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		null;
end save_tmp_run_task_p;
--

--
begin
  -- Initialization
  null;
end cnl_wms_pck;