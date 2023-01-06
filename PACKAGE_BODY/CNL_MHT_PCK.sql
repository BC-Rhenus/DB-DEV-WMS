CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_MHT_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Common functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--
  g_yes                      constant varchar2(1)              := 'Y';
  g_no                       constant varchar2(1)              := 'N';
  g_true                     constant varchar2(5)              := 'TRUE';
  g_false                    constant varchar2(5)              := 'FALSE';
  g_wms_db                   constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'DB_NAME');
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : Bernd Bitter, 24-06-2016
-- Purpose : API for Material Handling Equipment to create Parcel data
------------------------------------------------------------------------------------------------
  procedure create_parcel ( p_wms_unit_id_i     in  varchar2
                          , p_mht_unit_id_i     in  varchar2 := null
                          , p_mht_station_id_i  in  varchar2
                          , p_lft_status_i      in  varchar2
                          , p_lft_description_i in  varchar2 := null
                          , p_package_type_i    in  varchar2 := null
                          , p_weight_i          in  number
                          , p_height_i          in  number
                          , p_width_i           in  number
                          , p_depth_i           in  number
                          , p_ok_yn_o           out varchar2
                          , p_error_message_o   out varchar2
                          , p_print_label_yn_o  out varchar2
                          )
  is
    cursor c_ocr ( b_site_id   in varchar2
                 , b_parcel_id in varchar2
                 )
    is
      select ocr.container_id
      ,      ocr.container_type
      ,      ocr.pallet_id
      ,      ocr.config_id        pallet_type
      ,      ocr.container_n_of_n
      ,      ohr.from_site_id     site_id
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
      ,      'JDA2016' wms_database
      from   dcsdba.order_container ocr
      ,      dcsdba.order_header    ohr
      where  ohr.client_id    = ocr.client_id
      and    ohr.order_id     = ocr.order_id
      and    ohr.from_site_id = b_site_id
      and    ocr.container_id = b_parcel_id -- in WMS2016 ParcelID = Container_ID when from DWS
    ;
    cursor c_cda ( b_site_id      in varchar2
                 , b_client_id    in varchar2
                 , b_order_id     in varchar2
                 , b_parcel_id    in varchar2
                 , b_wms_database in varchar2
                 )
    is
      select cda.*
      from   cnl_container_data cda
      where  cda.site_id        = b_site_id
      and    cda.client_id      = b_client_id
      and    cda.order_id       = b_order_id
      and    cda.wms_database   = b_wms_database
      and    decode( b_wms_database, 'JDA2009', cda.pallet_id
                                              , cda.container_id
                   )            = b_parcel_id
      ;
    cursor c_crr ( b_site_id       in varchar2
                 , b_client_id     in varchar2
                 , b_carrier_id    in varchar2
                 , b_service_level in varchar2
                 )
    is
      select distinct
             user_def_type_2  cto_carrier
      ,      user_def_type_3  cto_service
      ,      g_yes            cto_enabled_yn
      ,      decode( nvl(user_def_type_7, g_no) , g_true, g_yes
                                                        , g_no
                   )          use_dws_yn
      from   dcsdba.carriers
      where  (
             site_id       = b_site_id
             or
             site_id       is null
             )
      and    client_id     = b_client_id
      and    carrier_id    = b_carrier_id
      and    service_level = b_service_level ;

    cursor c_wsn (b_station_id in varchar2)
    is
      select wsn.site_id
      ,      decode( nvl(wsn.disabled, g_no), g_no, g_yes  -- disabled N = enabled Y, disabled Y = enabled N
                                                  , g_no
                   ) dws_enabled_yn
      from   dcsdba.workstation wsn
      where  wsn.station_id = b_station_id
      ;

    r_ocr            c_ocr%rowtype;
    r_cda            c_cda%rowtype;
    r_crr            c_crr%rowtype;
    r_wsn            c_wsn%rowtype;

    l_err            varchar2(1) := g_no;
    l_err_txt        varchar2(400);
    l_wms_unit_id_i  varchar2(30);
    l_wms_database   varchar2(20);
    l_new_site_id    varchar2(20);
    l_new_client_id  varchar2(20);
    l_cto_enabled_yn varchar2(1) := g_no;
    l_dws_enabled_yn varchar2(1) := g_no;
    l_use_dws_yn     varchar2(1) := g_no;
    l_print_at_dws   varchar2(1) := g_no;
    l_result         integer;
    l_cto_carrier    varchar2(30);
    l_cto_service    varchar2(30);
    l_mht_site_id    varchar2(20);
  begin
    --
    l_wms_unit_id_i := replace( p_wms_unit_id_i, '#DWS#', null);
    --
    if nvl( p_weight_i, 0) = 0
    or nvl( p_height_i, 0) = 0
    or nvl( p_width_i, 0)  = 0
    or nvl( p_depth_i, 0)  = 0
    then
      l_err     := g_yes;
      l_err_txt := 'Weight and/or dimensions are zero: p_weight_i['
                || nvl( p_weight_i, 0) 
                || '], p_height_i['
                || nvl( p_height_i, 0) 
                || '], p_width_i['
                || nvl( p_width_i, 0) 
                || '], p_depth_i[' 
                || nvl( p_depth_i, 0)
                || '] can not be zero, check DWS.'
                ;
    end if;
    --
    if p_lft_status_i = g_no
    then
      l_err     := g_yes;
      l_err_txt := 'Parcel not Legal For Trade: p_lft_status_i [' 
                || nvl(p_lft_status_i,'NO VALUE')
                || '] , Non-LFT Error: ['
                || substr( p_lft_description_i, 1, 300)
                || ']'
                ;
    end if;
    --
    if p_mht_station_id_i is null
    then
      l_err     := g_yes;
      l_err_txt := 'MHT Station ID empty: p_mht_station_id_i [' 
                || nvl(p_mht_station_id_i,'NO VALUE')
                || '] can not be empty, check DWS.'
                ;
    end if;
    --
    if l_wms_unit_id_i is null
    then
      l_err     := g_yes;
      l_err_txt := 'WMS Unit ID empty: p_wms_unit_id_i [' 
                || nvl(l_wms_unit_id_i,'NO VALUE')
                || '] can not be empty, check DWS.'
                ;
    else      
      -- get the Site from Workstation for DWS system
      open  c_wsn ( b_station_id => p_mht_station_id_i);
      fetch c_wsn
      into  l_mht_site_id
      ,     l_dws_enabled_yn;
      close c_wsn;
      -- check if parcel exists in WMS
      open  c_ocr ( b_site_id   => l_mht_site_id
                  , b_parcel_id => l_wms_unit_id_i
                  );
      fetch c_ocr
      into  r_ocr;
      --
      if c_ocr%notfound
      then
        l_err     := g_yes;
        l_err_txt := 'WMS Unit ID unknown: l_wms_unit_id_i [' 
                  || l_wms_unit_id_i
                  || '] does not exist in WMS systems, check DWS.'
                  ;
      else
        l_wms_database := r_ocr.wms_database;
      end if;
      close c_ocr;
    end if;
    --
    if  l_err = g_no
    and l_wms_database is not null
    then
      -- Check if Centiro and or DWS is enabled for this Client/Carrier/Service
      open  c_crr ( b_site_id       => r_ocr.site_id
                  , b_client_id     => r_ocr.client_id
                  , b_carrier_id    => r_ocr.carrier_id
                  , b_service_level => r_ocr.service_level
                  );
      fetch c_crr
      into  r_crr;
      close c_crr;
      --
      l_cto_carrier    := r_crr.cto_carrier;
      l_cto_service    := r_crr.cto_service;
      l_cto_enabled_yn := r_crr.cto_enabled_yn;
      l_use_dws_yn     := r_crr.use_dws_yn;
      --
      -- get the correct site/client
      if upper(l_wms_database) != 'JDA2009'
      then
        l_new_site_id    := r_ocr.site_id;
        l_new_client_id  := r_ocr.client_id;
      end if;

      -- Update the Order Container record
      if upper(l_wms_database) != 'JDA2009'
      then
         -- update order_container in WMS2016
        update dcsdba.order_container ocr
        set    ocr.container_weight = p_weight_i
        ,      ocr.container_height = round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
        ,      ocr.container_width  = round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
        ,      ocr.container_depth  = round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
        ,      ocr.container_type   = nvl( p_package_type_i, r_ocr.container_type)
        where  ocr.client_id        = r_ocr.client_id
        and    ocr.order_id         = r_ocr.order_id
        and    ocr.container_id     = r_ocr.container_id
        ;
        commit
        ; 
      end if;              

      if upper(l_wms_database) = 'JDA2016'
      then
        -- Check if Container Data exists and create/update accordingly for WMS2016 parcels          
        open  c_cda ( b_site_id      => l_mht_site_id
                    , b_client_id    => l_new_client_id
                    , b_order_id     => r_ocr.order_id
                    , b_parcel_id    => l_wms_unit_id_i
                    , b_wms_database => l_wms_database
                    );
        fetch c_cda
        into  r_cda;
      end if;
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
                           , dws_unit_id
                           , dws_station_id
                           , dws_lft_status
                           , dws_lft_description
                           , dws_package_type
                           , dws_weight
                           , dws_height
                           , dws_width
                           , dws_depth
                           , dws_dstamp
                           , cto_enabled_yn
                           )
        values             ( r_ocr.container_id
                           , r_ocr.container_type
                           , r_ocr.pallet_id
                           , r_ocr.pallet_type
                           , r_ocr.container_n_of_n
                           , l_new_site_id
                           , l_new_client_id
                           , r_ocr.owner_id
                           , r_ocr.order_id
                           , r_ocr.customer_id
                           , l_cto_carrier
                           , l_cto_service
                           , r_ocr.weight
                           , r_ocr.height
                           , r_ocr.width
                           , r_ocr.depth
                           , l_wms_database
                           , p_mht_unit_id_i
                           , p_mht_station_id_i
                           , p_lft_status_i
                           , p_lft_description_i
                           , p_package_type_i
                           , p_weight_i
                           , decode( l_wms_database, 'JDA2016', round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                              , p_height_i
                                   )
                           , decode( l_wms_database, 'JDA2016', round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                              , p_width_i
                                   )
                           , decode( l_wms_database, 'JDA2016', round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                              , p_depth_i
                                   )
                           , current_timestamp
                           , l_cto_enabled_yn
                           );
      else
        -- update cda record with dws data
        update cnl_container_data      cda
        set    cda.dws_unit_id         = p_mht_unit_id_i
        ,      cda.dws_station_id      = p_mht_station_id_i
        ,      cda.dws_lft_status      = p_lft_status_i
        ,      cda.dws_lft_description = p_lft_description_i
        ,      cda.dws_package_type    = p_package_type_i
        ,      cda.dws_weight          = p_weight_i
        ,      cda.dws_height          = decode( l_wms_database, 'JDA2016', round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                                          , p_height_i
                                               )
        ,      cda.dws_width           = decode( l_wms_database, 'JDA2016', round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                                          , p_width_i
                                               )
        ,      cda.dws_depth           = decode( l_wms_database, 'JDA2016', round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                                          , p_depth_i
                                               )
        ,      cda.dws_dstamp          = current_timestamp
        ,      cda.cto_enabled_yn      = l_cto_enabled_yn
        where  decode( l_wms_database, 'JDA2009', cda.pallet_id
                                                , cda.container_id
                   )                   = l_wms_unit_id_i
        and    cda.site_id             = l_mht_site_id
        and    cda.client_id           = l_new_client_id
        and    cda.wms_database        = l_wms_database
        ;
      end if;

      -- Print at DWS ?
      if  l_dws_enabled_yn = g_yes
      and l_cto_enabled_yn = g_yes
      and l_use_dws_yn     = g_yes
      then
        l_print_at_dws := g_yes;
      else
        l_print_at_dws := g_no;
      end if;

      -- Create CTO_PACKPARCEL Run task to trigger the Centiro PackParcel interface
      --
      case  upper(l_wms_database)
      -- Create Run task in WMS2016
      when 'JDA2016'
      then
        if l_print_at_dws = g_yes
        then
          l_result := dcsdba.libruntask.createruntask ( stationid             => p_mht_station_id_i
                                                      , userid                => p_mht_station_id_i
                                                      , commandtext           => '"CTO_PACKPARCEL" "lp" "P" "1" '
                                                                              || '"from_site_id" "'   || r_ocr.site_id
                                                                              || '" "client_id" "'    || r_ocr.client_id
                                                                              || '" "owner_id" "'     || r_ocr.owner_id
                                                                              || '" "order_id" "'     || r_ocr.order_id
                                                                              || '" "container_id" "' || l_wms_unit_id_i
                                                                              || '"'
                                                      , nametext              => 'UREPCTOPACKPARCEL'
                                                      , siteid                => r_ocr.site_id
                                                      , tmplanguage           => 'EN_GB'
                                                      , p_javareport          => g_yes
                                                      , p_archive             => g_no
                                                      , p_runlight            => null
                                                      , p_serverinstance      => null
                                                      , p_priority            => null
                                                      , p_timezonename        => 'Europe/Amsterdam'
                                                      , p_archiveignorescreen => null
                                                      , p_archiverestrictuser => null
                                                      , p_clientid            => r_ocr.client_id
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
      else
        null;
      end case;
      -- close cursor c_cda
      close c_cda;
    end if;

    commit;
    if l_err = g_yes
    then
      p_ok_yn_o := g_no;
    else
      p_ok_yn_o := g_yes;
    end if;

    -- log DWS call into CNL_DWS_LOG table
    insert into cnl_dws_log ( wms_parcel_id
                            , dws_unit_id
                            , dws_station_id
                            , dws_lft_status
                            , dws_lft_description
                            , dws_package_type
                            , dws_weight
                            , dws_height
                            , dws_width
                            , dws_depth
                            , dws_dstamp
                            , wms_print_label_yn
                            , error_yn
                            , error_text
                            )
    values                  ( nvl( l_wms_unit_id_i, '0000000000')
                            , p_mht_unit_id_i
                            , p_mht_station_id_i
                            , p_lft_status_i
                            , p_lft_description_i
                            , p_package_type_i
                            , p_weight_i
                            , decode( l_wms_database, 'JDA2016', round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                               , p_height_i
                                    )
                            , decode( l_wms_database, 'JDA2016', round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                               , p_width_i
                                    )
                            , decode( l_wms_database, 'JDA2016', round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                                               , p_depth_i
                                    )
                            , current_timestamp
                            , l_print_at_dws
                            , l_err
                            , l_err_txt
                            );

    -- return out parameter values
    p_error_message_o  := l_err_txt;

    if  l_dws_enabled_yn = g_yes
    and l_cto_enabled_yn = g_yes
    and l_use_dws_yn     = g_yes
    then
      p_print_label_yn_o := g_yes;
    else
      p_print_label_yn_o := g_no;      
    end if;

  exception
    when others
    then
      l_err     := g_yes;
      l_err_txt := substr(sqlerrm, 1, 350);     

      p_ok_yn_o          := g_no;
      p_error_message_o  := l_err_txt;

      if  l_print_at_dws = g_yes
      then
        p_print_label_yn_o := g_yes;
      else
        p_print_label_yn_o := g_no;      
      end if;

      case
      when c_ocr%isopen
      then
        close c_ocr;
      when c_cda%isopen
      then
        close c_cda;
      when c_crr%isopen
      then
        close c_crr; 
      when c_wsn%isopen
      then
        close c_wsn;
      else
        null;
      end case;

  end create_parcel;
--
--
begin
  -- Initialization
  null;
end cnl_mht_pck;