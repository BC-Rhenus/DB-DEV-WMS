CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_API_PCK" is
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
  g_true                     constant varchar2(20)             := 'TRUE';
  g_false                    constant varchar2(20)             := 'FALSE';
  g_saveorderresponse        constant varchar2(20)             := 'SAVEORDERRESPONSE';
  g_packparcelresponse       constant varchar2(20)             := 'PACKPARCELRESPONSE';
  g_trackingeventupdate      constant varchar2(20)             := 'TRACKINGEVENTUPDATE';
  g_centiro_wms_source       constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'CENTIRO_WMS_SOURCE');  
  g_centiro_wms_source2      constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'CENTIRO_WMS_SOURCE2');    
  g_centiro_wms_dest         constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'CENTIRO_WMS_DEST');
  g_smtp_host                constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'SMTP_HOST');
  g_smtp_domain              constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'SMTP_DOMAIN');
  g_smtp_port                constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'SMTP_PORT');
  g_smtp_sender              constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'SMTP_SENDER');
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 09-Jun-2016
-- Purpose : API for Seeburger to process the TrackingEventUpdate
------------------------------------------------------------------------------------------------
  function cto_so_response ( p_messagename_i      in  varchar2
                           , p_sourcesystem_i     in  varchar2
                           , p_destsystem_i       in  varchar2
                           , p_clientid_i         in  varchar2
                           , p_orderno_i          in  varchar2
                           , p_carrier_i          in  varchar2 := null
                           , p_service_i          in  varchar2 := null
                           , p_deliverypoint_i    in  varchar2 := null
                           , p_fromsiteid_i       in  varchar2 := null
                           , p_shipbydate_i       in  varchar2 := null
                           , p_instructions_i     in  varchar2
                           , p_timezonename_i     in  varchar2 := 'Europe/Amsterdam'
                           , p_statusreasoncode_i in  varchar2
                           , p_errortext_o        out varchar2
                           )
    return integer
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
    cursor c_ltt (b_mergeerror in varchar2)
    is
      select b_mergeerror
      ||     ' - '
      ||     text
      from   dcsdba.language_text
      where  language = 'EN_GB'
      and    label    = b_mergeerror
      ;
    cursor c_ohr ( b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select to_char( ship_by_date, 'YYYYMMDDHH24MISS') ship_by_date
      from   dcsdba.order_header
      where  client_id = b_client_id
      and    order_id  = b_order_id
      ;  

    r_clt            c_clt%rowtype;
    r_ltt            c_ltt%rowtype;
    r_ohr            c_ohr%rowtype;

    l_err            integer := 1; -- 1 = OK, 0 = Error
    l_err_code       varchar2(20);
    l_err_txt        varchar2(500);
    l_client_id      varchar2(10);
    l_site_id        varchar2(10);
    l_ship_by_date   varchar2(14);
  begin
    --
    if p_statusreasoncode_i is null
    then
      l_err     := 0;
      l_err_txt := 'StatusReasonCode empty: p_statusreasoncode_i [' 
                || nvl(p_statusreasoncode_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_instructions_i is null
    then
      l_err     := 0;
      l_err_txt := 'Instructions empty: p_instructions_i [' 
                || nvl(p_instructions_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_fromsiteid_i is null
    and instr(p_clientid_i,'@') = 0 
    then
      l_err     := 0;
      l_err_txt := 'FromSiteID empty: p_fromsiteid_i [' 
                || nvl(p_fromsiteid_i,'NO VALUE')
                || '] can not be empty if SiteID is not in ClientID ['
                || p_clientid_i
                || '], check source file or mapping.'
                ;
    end if;
    --
    if p_orderno_i is null
    then
      l_err     := 0;
      l_err_txt := 'OrderID empty: p_orderno_i [' 
                || nvl(p_orderno_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_clientid_i is null
    then
      l_err     := 0;
      l_err_txt := 'ClientID empty: p_clientid_i [' 
                || nvl(p_clientid_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if 	nvl(p_destsystem_i,'NO VALUE') <> g_centiro_wms_source -- in Response Centiro system is source, WMS system is destination
    and	nvl(p_destsystem_i,'NO VALUE') <> g_centiro_wms_source2 -- in Response Centiro system is source, WMS system is destination
    then
      l_err     := 0;
      l_err_txt := 'Wrong destination: p_destsystem_i [' 
                || nvl(p_destsystem_i,'NO VALUE')
                || '] does not match the allowed destination ['
                || g_centiro_wms_source
                || '].'
                ;
    end if;
    --
    if nvl(p_sourcesystem_i,'NO VALUE') <> g_centiro_wms_dest -- in Response Centiro system is source, WMS system is destination
    then
      l_err     := 0;
      l_err_txt := 'Wrong source: p_sourcesystem_i [' 
                || nvl(p_sourcesystem_i,'NO VALUE')
                || '] does not match the allowed source ['
                || g_centiro_wms_dest
                || '].'
                ;
    end if;
    --
    if nvl(p_messagename_i,'NO VALUE') <> g_saveorderresponse
    then
      l_err     := 0;
      l_err_txt := 'Wrong message: p_messagename_i [' 
                || nvl(p_messagename_i,'NO VALUE')
                || '] does not match the cto_te_update API message ['
                || g_saveorderresponse
                || '].'
                ;
    end if;
    --
    open  c_clt ( b_cto_client => p_clientid_i);
    fetch c_clt
    into  r_clt;
    close c_clt;
    l_client_id := r_clt.client_id;
    l_site_id   := nvl(r_clt.site_id, p_fromsiteid_i);
    if 	l_site_id   = 'GBASF02'
    then
	l_site_id := 'GBMIK01';
    end if;-- Translate because Centiro has the old site id set
    --
    -- Check if ShipByDate already exists for order, in case no ShipByDate returned in response use original ShipByDate
    open  c_ohr ( b_client_id => l_client_id
                , b_order_id  => p_orderno_i
                );
    fetch c_ohr
    into  r_ohr;
    close c_ohr;
    l_ship_by_date := r_ohr.ship_by_date; -- in dateformat YYYYMMDDHH24MISS
    --
    if l_err = 1 -- no errors
    then            
      l_err := dcsdba.libmergeorder.directorderheader ( p_mergeerror       => l_err_code
                                                      , p_toupdatecols     => ':client_id::order_id::from_site_id::delivery_point::ship_by_date::instructions::carrier_id::service_level::status_reason_code::time_zone_name::nls_calendar:'
                                                      , p_mergeaction      => 'U'
                                                      , p_clientid         => l_client_id
                                                      , p_orderid          => p_orderno_i
                                                      , p_carrierid        => p_carrier_i
                                                      , p_servicelevel     => p_service_i
                                                      , p_deliverypoint    => substr(p_deliverypoint_i, 1, 15)
                                                      , p_fromsiteid       => l_site_id
                                                      , p_shipbydate       => to_date(nvl( p_shipbydate_i, l_ship_by_date) , 'YYYYMMDDHH24MISS')
                                                      , p_instructions     => substr(p_instructions_i, 1, 180)
                                                      , p_timezonename     => p_timezonename_i
                                                      , p_statusreasoncode => substr(p_statusreasoncode_i, 1, 10)
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
    end if;

    p_errortext_o := l_err_txt;

    return l_err;

  end cto_so_response;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 09-Jun-2016
-- Purpose : API for Seeburger to process the PackParcelResponse
------------------------------------------------------------------------------------------------
  function cto_pp_response ( p_messagename_i   in  varchar2
                           , p_sourcesystem_i  in  varchar2
                           , p_destsystem_i    in  varchar2
                           , p_processstatus_i in  varchar2
                           , p_errorcode_i     in  varchar2
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
                           , p_errortext_o     out varchar2
                           )
    return integer
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
    cursor c_ocr ( b_client_id    in varchar2
                 , b_parcel_id in varchar2
                 )
    is
      select 1
      from   dcsdba.order_container ocr
      where  ocr.client_id          = b_client_id
      and    (
             ocr.container_id       = b_parcel_id
             or
             ocr.pallet_id          = b_parcel_id
             )
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
    r_ocr              c_ocr%rowtype;

    l_err              integer := 1; -- 1 = OK, 0 = Error
    l_err_txt          varchar2(500);
    l_client_id        varchar2(50);
    l_integer          integer;
  begin
    --
    if p_trackingno_i is null
    and p_processstatus_i = 'OK'
    then
      l_err     := 0;
      l_err_txt := 'TrackingNo empty: p_trackingno_i [' 
                || nvl(p_trackingno_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_sequenceno_i is null
    and p_processstatus_i = 'OK'
    then
      l_err     := 0;
      l_err_txt := 'SequenceNo empty: p_sequenceno_i [' 
                || nvl(p_sequenceno_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_parcelid_i is null
    then
      l_err     := 0;
      l_err_txt := 'ParcelID empty: p_parcelid_i [' 
                || nvl(p_parcelid_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_orderno_i is null
    then
      l_err     := 0;
      l_err_txt := 'OrderID empty: p_orderno_i [' 
                || nvl(p_orderno_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_clientid_i is null
    then
      l_err     := 0;
      l_err_txt := 'ClientID empty: p_clientid_i [' 
                || nvl(p_clientid_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_errorcode_i is null
    and p_processstatus_i <> 'OK'
    then
      l_err     := 0;
      l_err_txt := 'ErrorCode empty: p_errorcode_i [' 
                || nvl(p_errorcode_i,'NO VALUE')
                || '] can not be empty when ProcessStatus = ''ERROR'', check source file or mapping.'
                ;
    end if;
    --
    if nvl(p_destsystem_i,'NO VALUE') not in ( g_centiro_wms_source -- in Response Centiro system is source, WMS system is destination
                                             , substr( g_centiro_wms_source, 1, 6) || '2009' -- Centiro response for WMS2009 via WMS2016
                                             ) 
    then
      l_err     := 0;
      l_err_txt := 'Wrong destination: p_destsystem_i [' 
                || nvl(p_destsystem_i,'NO VALUE')
                || '] does not match the allowed destination ['
                || g_centiro_wms_source
                || '].'
                ;
    end if;
    --
    if nvl(p_sourcesystem_i,'NO VALUE') <> g_centiro_wms_dest -- in Response Centiro system is source, WMS system is destination
    then
      l_err     := 0;
      l_err_txt := 'Wrong source: p_sourcesystem_i [' 
                || nvl(p_sourcesystem_i,'NO VALUE')
                || '] does not match the allowed source ['
                || g_centiro_wms_dest
                || '].'
                ;
    end if;
    --
    if nvl(p_messagename_i,'NO VALUE') <> g_packparcelresponse
    then
      l_err     := 0;
      l_err_txt := 'Wrong message: p_messagename_i [' 
                || nvl(p_messagename_i,'NO VALUE')
                || '] does not match the cto_te_update API message ['
                || g_packparcelresponse
                || '].'
                ;
    end if;
    --
    l_client_id := replace(p_clientid_i, 'GBASF02', 'GBMIK01');-- Translate because Centiro has the old site id set

    if l_err = 1 -- no errors
    then
      cnl_wms_pck.process_packparcelresponse ( p_errorcode_i    => p_errorcode_i
                                             , p_errormessage_i => p_errormessage_i
                                             , p_clientid_i     => l_client_id--p_clientid_i
                                             , p_carrier_i      => p_carrier_i
                                             , p_service_i      => p_service_i
                                             , p_orderno_i      => p_orderno_i
                                             , p_shipmentid_i   => p_shipmentid_i
                                             , p_sequenceno_i   => p_sequenceno_i
                                             , p_parcelid_i     => p_parcelid_i
                                             , p_trackingno_i   => p_trackingno_i
                                             , p_trackingurl_i  => p_trackingurl_i
                                             , p_error_o        => l_err
                                             , p_errortext_o    => l_err_txt
                                             );     
    end if;
    --

    p_errortext_o := l_err_txt;

    return l_err;

  end cto_pp_response;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 09-Jun-2016
-- Purpose : API for Seeburger to process the TrackingEventUpdate
------------------------------------------------------------------------------------------------
  function cto_te_update ( p_messagename_i    in  varchar2
                         , p_sourcesystem_i   in  varchar2
                         , p_destsystem_i     in  varchar2
                         , p_clientid_i       in  varchar2
                         , p_carrier_i        in  varchar2
                         , p_service_i        in  varchar2
                         , p_orderno_i        in  varchar2
                         , p_shipmentid_i     in  varchar2
                         , p_eventcode_i      in  varchar2
                         , p_eventtime_i      in  varchar2
                         , p_eventsignature_i in  varchar2 := null
                         , p_errortext_o      out varchar2
                         )
    return integer
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
    r_clt            c_clt%rowtype;

    l_err            integer := 1; -- 1 = OK, 0 = Error
    l_err_txt        varchar2(500);
    l_client_id      varchar2(10);
    l_site_id        varchar2(10);
  begin
    --
    if p_eventtime_i is null
    then
      l_err     := 0;
      l_err_txt := 'EventTime empty: p_eventtime_i [' 
                || nvl(p_eventtime_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_orderno_i is null
    then
      l_err     := 0;
      l_err_txt := 'OrderID empty: p_orderno_i [' 
                || nvl(p_orderno_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_clientid_i is null
    then
      l_err     := 0;
      l_err_txt := 'ClientID empty: p_clientid_i [' 
                || nvl(p_clientid_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if 	nvl(p_destsystem_i,'NO VALUE') <> g_centiro_wms_source -- in Response Centiro system is source, WMS system is destination
    and	nvl(p_destsystem_i,'NO VALUE') <> g_centiro_wms_source2 -- in Response Centiro system is source, WMS system is destination
    then
      l_err     := 0;
      l_err_txt := 'Wrong destination: p_destsystem_i [' 
                || nvl(p_destsystem_i,'NO VALUE')
                || '] does not match the allowed destination ['
                || g_centiro_wms_source
                || '].'
                ;
    end if;
    --
    if nvl(p_sourcesystem_i,'NO VALUE') <> g_centiro_wms_dest -- in Response Centiro system is source, WMS system is destination
    then
      l_err     := 0;
      l_err_txt := 'Wrong source: p_sourcesystem_i [' 
                || nvl(p_sourcesystem_i,'NO VALUE')
                || '] does not match the allowed source ['
                || g_centiro_wms_dest
                || '].'
                ;
    end if;
    --
    if nvl(p_messagename_i,'NO VALUE') <> g_trackingeventupdate
    then
      l_err     := 0;
      l_err_txt := 'Wrong message: p_messagename_i [' 
                || nvl(p_messagename_i,'NO VALUE')
                || '] does not match the cto_te_update API message ['
                || g_trackingeventupdate
                || '].'
                ;
    end if;
    --
    open  c_clt ( b_cto_client => p_clientid_i);
    fetch c_clt
    into  r_clt;
    close c_clt;
    l_client_id := r_clt.client_id;
    l_site_id   := r_clt.site_id;
    if 	l_site_id   = 'GBASF02'
    then
	l_site_id := 'GBMIK01';
    end if;-- Translate because Centiro has the old site id set
    --
    if l_err = 1 -- no errors
    then            
      cnl_wms_pck.process_trackingeventupdate ( p_clientid_i       => l_client_id
                                              , p_orderno_i        => p_orderno_i
                                              , p_eventtime_i      => p_eventtime_i
                                              , p_eventsignature_i => substr( p_eventsignature_i, 1, 25)
                                              , p_error_o          => l_err
                                              , p_errortext_o      => l_err_txt
                                              );
    end if;

    p_errortext_o := l_err_txt;

    return l_err;

  end cto_te_update;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 19-Aug-2016
-- Purpose : Refresh daily materialized views
------------------------------------------------------------------------------------------------
  procedure refresh_daily_mviews
  is
  begin
     -- Refresh daily mviews (non fast refresh) incl. a compile before and after to make sure MV is refreshed
     execute immediate 'alter materialized view CNL_ACTIVE_CLIENTS compile';
     --
     dbms_mview.refresh ('cnl_active_clients'
                        ,'C'
                        );
     --
     execute immediate 'alter materialized view CNL_ACTIVE_CLIENTS compile';

  end refresh_daily_mviews;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Sep-2016
-- Purpose : Set session settings for Seeburger
------------------------------------------------------------------------------------------------
  function set_wms_session_settings ( p_session_id_i in  number := null)
    return number
  is
    l_sessionid number;
  begin
    dcsdba.libsession.InitialiseSession ( UserID       => 'SEEBURGER'
                                        , GroupID      => null
                                        , StationID    => 'SEEBURGER'
                                        , WksGroupID   => null
                                        , p_SessionID  => p_session_id_i
                                        , p_Locality   => null
                                        , p_Language   => null
                                        , p_IntExpSecs => null
                                        , p_IntTimSecs => null
                                        );
    l_sessionid := dcsdba.libsession.sessionsessionid;

    return l_sessionid;

  end set_wms_session_settings;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Sep-2016
-- Purpose : Set session settings for Seeburger
------------------------------------------------------------------------------------------------
  function get_wms_order_sequence ( p_client_id_i in  varchar2
                                  , p_ose_type_i  in  varchar2
                                  , p_udp_1_i     in  varchar2 := null
                                  , p_udp_2_i     in  varchar2 := null
                                  , p_date_i      in  varchar2 := null -- format YYYYMMDD
                                  ) 
    return varchar2
  is
    l_sequence varchar2(20);
    l_date     date;
    l_err      integer := 1; -- 1 = OK, 0 = Error
  begin
    if  p_date_i is not null
    then
      if  length( p_date_i) = 8
      then
        l_date := to_date( p_date_i, 'YYYYMMDD');
      else
        l_err      := 0;
        l_sequence := 'Date not in YYYYMMDD';
      end if;
    end if;
    --
    if l_err = 1
    then
      cnl_wms_pck.get_order_sequence ( p_client_id_i => p_client_id_i
                                     , p_udp_1_i     => p_udp_1_i
                                     , p_udp_2_i     => p_udp_2_i
                                     , p_ose_type_i  => p_ose_type_i
                                     , p_date_i      => l_date
                                     , p_sequence_o  => l_sequence
                                     );
    end if;

    return l_sequence;
  exception
    when others
    then
      if substr( sqlerrm, 1, 9) = 'ORA-01839'
      then
        l_sequence := 'Date not in YYYYMMDD';
        return l_sequence;
      else
        raise;
      end if;

  end get_wms_order_sequence;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 24-Jan-2017
-- Purpose : Function to get the tracking URL from Centiro
------------------------------------------------------------------------------------------------
  function get_tracking_url ( p_carrier_id_i  in  varchar2
                            , p_tracking_nr_i in  varchar2
                            )
    return varchar2
  is      

    l_retval varchar2(1000);
  begin
    l_retval := null;

    l_retval := cnl_wms_pck.get_tracking_url ( p_wms_carrier_id  => p_carrier_id_i
                                             , p_wms_tracking_nr => p_tracking_nr_i
                                             );      
    if l_retval is null
    then
      l_retval := 'No tracking URL available.'; 
    end if;

    return l_retval;

  end get_tracking_url;
-----------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Dec-2018
-- Purpose : Insert Order Accessorial function for BIS
------------------------------------------------------------------------------------------------
  function ins_order_accessorial ( p_client_id_i    in  varchar2
                                 , p_order_id_i     in  varchar2
                                 , p_accessorial_i  in  varchar2
                                 , p_timezonename_i in  varchar2 := 'Europe/Amsterdam'
                                 , p_errortext_o    out varchar2
                                 )
    return integer
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

    r_clt            c_clt%rowtype;

    l_err            integer := 1; -- 1 = OK, 0 = Error
    l_err_txt        varchar2(500);
    l_client_id      varchar2(10);
  begin
    if p_accessorial_i is null
    then
      l_err     := 0;
      l_err_txt := 'Accessorial empty: p_accessorial_i [' 
                || nvl(p_accessorial_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_order_id_i is null
    then
      l_err     := 0;
      l_err_txt := 'OrderID empty: p_order_id_i [' 
                || nvl(p_order_id_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_client_id_i is null
    then
      l_err     := 0;
      l_err_txt := 'ClientID empty: p_client_id_i [' 
                || nvl(p_client_id_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    open  c_clt ( b_cto_client => p_client_id_i);
    fetch c_clt
    into  r_clt;
    close c_clt;
    l_client_id := r_clt.client_id;
    --
    if l_err = 1 -- no errors
    then            
      l_err := cnl_wms_pck.ins_order_accessorial ( p_client_id_i    => l_client_id
                                                 , p_order_id_i     => p_order_id_i
                                                 , p_accessorial_i  => p_accessorial_i
                                                 , p_timezonename_i => p_timezonename_i
                                                 , p_errortext_o    => l_err_txt
                                                 );
    end if;

    p_errortext_o := l_err_txt;

    return l_err;

  end ins_order_accessorial;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 11-Feb-2018
-- Purpose : Synchronize Special Instruction Links for SKU's with specific HazmatID's
------------------------------------------------------------------------------------------------
  procedure sync_sku_special_links
  is
  begin
    cnl_wms_pck.sync_sku_special_links;
  end sync_sku_special_links;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 17-09-2018
-- Purpose : Send e-mail via WMS procedure
------------------------------------------------------------------------------------------------
  procedure send_email ( p_email_from_i      in varchar2 := null
                       , p_email_to_i        in varchar2
                       , p_subject_i         in varchar2
                       , p_message_body_i    in varchar2
                       , p_attachment_name_i in varchar2 := null
                       , p_attachment_file_i in blob     := null
                       )
  is
    l_sender varchar2(256);
  begin
    l_sender := nvl(p_email_from_i, g_smtp_sender);
    -- set the smtp defaults
    dcsdba.libemailtask.setmailsender (inmailsender => l_sender);
    dcsdba.libemailtask.setmailhost   (inmailhost   => g_smtp_host);
    dcsdba.libemailtask.setmaildomain (indomain     => g_smtp_domain);
    -- send the e-mail    
    dcsdba.libemailtask.sendmail ( sendto   => p_email_to_i
                                 , subject  => p_subject_i 
                                 , message  => p_message_body_i
                                 , filename => p_attachment_name_i
                                 , blobfile => p_attachment_file_i
                                 );
  end send_email;                       
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 24-Nov-2016
-- Purpose : API for Seeburger to process the Customs Streamliner Release
------------------------------------------------------------------------------------------------
  function process_csl_release ( p_message_type_i   in  varchar2
                               , p_date_i           in  varchar2
                               , p_time_i           in  varchar2
                               , p_company_i        in  varchar2
                               , p_businessunit_i   in  varchar2
                               , p_warehouse_i      in  varchar2
                               , p_dossier_type_i   in  varchar2
                               , p_dossier_id_i     in  varchar2
                               , p_dossier_status_i in  varchar2
                               , p_linkid_i         in  varchar2
                               , p_errortext_o      out varchar2
                               )
    return integer
  is
    cursor c_clt ( b_client_id in varchar2)
    is
      select 1
      from   dcsdba.client_group_clients
      where  client_id    = b_client_id
      and    client_group = 'CSLENT'    -- Client Visibility Group with Bonded Warehouse Customers
      ;
    cursor c_spe ( b_businessunit_i in varchar2)
    is
      select replace(profile_id, '-ROOT-_USER_STREAMLINER_BUSINESSUNIT_', null) client_id 
      from   dcsdba.system_profile
      where  text_data = b_businessunit_i
      ;                                     

    l_clt_int   integer;
    l_client_id varchar2(20);
    l_err       integer := 1; -- 1 = OK, 0 = Error
    l_err_txt   varchar2(500);

  begin
    if p_linkid_i is null
    then
      l_err     := 0;
      l_err_txt := 'Warehouse empty: p_linkid_i [' 
                || nvl(p_linkid_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_warehouse_i is null
    then
      l_err     := 0;
      l_err_txt := 'Warehouse empty: p_warehouse_i [' 
                || nvl(p_warehouse_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_businessunit_i is null
    then
      l_err     := 0;
      l_err_txt := 'BusinessUnit empty: p_businessunit_i [' 
                || nvl(p_businessunit_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if p_company_i is null
    then
      l_err     := 0;
      l_err_txt := 'Company empty: p_company_i [' 
                || nvl(p_company_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if nvl(p_dossier_status_i,'NO VALUE') != 'OK'
    then
      l_err     := 0;
      l_err_txt := 'Wrong status: p_dossier_status_i [' 
                || nvl(p_dossier_status_i,'NO VALUE')
                || '] can only be [OK].'
                ;
    end if;
    --
    if p_dossier_id_i is null
    then
      l_err     := 0;
      l_err_txt := 'Warehouse empty: p_dossier_id_i [' 
                || nvl(p_dossier_id_i,'NO VALUE')
                || '] can not be empty, check source file or mapping.'
                ;
    end if;
    --
    if nvl(p_dossier_type_i,'NO VALUE') not in ('I','O')
    then
      l_err     := 0;
      l_err_txt := 'Wrong type: p_dossier_type_i [' 
                || nvl(p_dossier_type_i,'NO VALUE')
                || '] can only be [I] (Inbound) or [O] (Outbound).'
                ;
    end if;
    --
    if nvl(p_message_type_i,'NO VALUE') <> 'RELEASE'
    then
      l_err     := 0;
      l_err_txt := 'Wrong message type: p_message_type_i [' 
                || nvl(p_message_type_i,'NO VALUE')
                || '] is not [RELEASE].'
                ;
    end if;
    --
    if l_err = 1 -- no errors
    then
      -- Get ClientID from BusinessUnit (Setup in WMS2016 in SystemProfile
      open  c_spe ( b_businessunit_i => p_businessunit_i);
      fetch c_spe
      into  l_client_id;
      close c_spe;

      -- Check if Client is enabled for Streamliner in WMS2016
      -- If enabled in WMS2016 call Process_CSL_Release function in WMS2016
      --
      open  c_clt (b_client_id => l_client_id);
      fetch c_clt
      into  l_clt_int;

      if c_clt%found
      then
        -- call process_csl_release process locally
        cnl_streamsoft_pck.process_csl_release ( p_message_type_i   => p_message_type_i
                                               , p_date_i           => p_date_i
                                               , p_time_i           => p_time_i
                                               , p_company_i        => p_company_i
                                               , p_businessunit_i   => p_businessunit_i
                                               , p_warehouse_i      => p_warehouse_i
                                               , p_dossier_type_i   => p_dossier_type_i
                                               , p_dossier_id_i     => p_dossier_id_i
                                               , p_dossier_status_i => p_dossier_status_i
                                               , p_linkid_i         => p_linkid_i
                                               , p_error_o          => l_err
                                               , p_errortext_o      => l_err_txt
                                               );
      end if;
      close c_clt;
    end if;

    p_errortext_o := l_err_txt;

    return l_err;

  exception
    when others
    then
      case
      when c_clt%isopen
      then
        close c_clt;
      else
        null;
      end case;

      l_err         := 0;
      p_errortext_o := substr( sqlerrm, 1, 500);

      return l_err;

    raise;

  end process_csl_release;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 02-Dec-2016
-- Purpose : Process for creating a StockList from WMS for Streamliner (Balance List)
------------------------------------------------------------------------------------------------
  procedure process_csl_balllist ( p_site_id_i   in  varchar2
                                 , p_client_id_i in  varchar2
                                 )
  is
  begin
    cnl_streamsoft_pck.create_stock_list ( p_site_id_i   => p_site_id_i
                                         , p_client_id_i => p_client_id_i
                                         );
  end process_csl_balllist;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 10-Dec-2018
-- Purpose : Process WLGore VAT interface to Customs Streamliner from WMS
------------------------------------------------------------------------------------------------
  procedure process_wlgore_vat ( p_site_id_i      in varchar2
                               , p_client_id_i    in varchar2
                               , p_shipped_date_i in date
                               )
  is
  begin
    cnl_wms_pck.process_wlgore_vat( p_site_id_i      => p_site_id_i
                                  , p_client_id_i    => p_client_id_i
                                  , p_shipped_date_i => p_shipped_date_i
                                  ); 
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
  begin
    cnl_wms_pck.process_csl_cbs( p_site_id_i      => p_site_id_i
                               , p_client_id_i    => p_client_id_i
                               , p_shipped_date_i => p_shipped_date_i
                               , p_csl_bu_i       => p_csl_bu_i
                               , p_trans_type_i   => p_trans_type_i
                               ); 
  end process_csl_cbs;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 28-Dec-2018
-- Purpose : Process CSL Product interface to WMS (Wrapper for libmergesku.directsku function)
------------------------------------------------------------------------------------------------
  function process_csl_product( p_mergeerror           in out varchar2
                              , p_toupdatecols         in     varchar2 := null
                              , p_mergeaction          in     varchar2
                              , p_clientid             in     varchar2
                              , p_skuid                in     varchar2
                              , p_ean                  in     varchar2 := null
                              , p_upc                  in     varchar2 := null
                              , p_description          in     varchar2
                              , p_productgroup         in     varchar2 := null
                              , p_eachheight           in     number   := null
                              , p_eachweight           in     number   := null
                              , p_eachvolume           in     number   := null
                              , p_eachvalue            in     number   := null
                              , p_qcstatus             in     varchar2 := null
                              , p_shelflife            in     integer  := null
                              , p_qcfrequency          in     integer  := null
                              , p_splitlowest          in     varchar2 := null
                              , p_conditionreqd        in     varchar2 := null
                              , p_expiryreqd           in     varchar2 := null
                              , p_originreqd           in     varchar2 := null
                              , p_serialatpack         in     varchar2 := null
                              , p_serialatpick         in     varchar2 := null
                              , p_serialatreceipt      in     varchar2 := null
                              , p_serialrange          in     varchar2 := null
                              , p_serialformat         in     varchar2 := null
                              , p_serialvalidmerge     in     varchar2 := null
                              , p_serialnoreuse        in     varchar2 := null
                              , p_picksequence         in     integer  := null
                              , p_pickcountqty         in     number   := null
                              , p_countfrequency       in     integer  := null
                              , p_oapwipenabled        in     varchar2 := null
                              , p_kitsku               in     varchar2 := null
                              , p_kitsplit             in     varchar2 := null
                              , p_kittriggerqty        in     number   := null
                              , p_kitqtydue            in     number   := null
                              , p_kittinglocid         in     varchar2 := null
                              , p_allocationgroup      in     varchar2 := null
                              , p_putawaygroup         in     varchar2 := null
                              , p_abcdisable           in     varchar2 := null
                              , p_handlingclass        in     varchar2 := null
                              , p_obsoleteproduct      in     varchar2 := null
                              , p_newproduct           in     varchar2 := null
                              , p_disallowupload       in     varchar2 := null
                              , p_disallowcrossdock    in     varchar2 := null
                              , p_manufdstampreqd      in     varchar2 := null
                              , p_manufdstampdflt      in     varchar2 := null
                              , p_minshelflife         in     integer  := null
                              , p_colour               in     varchar2 := null
                              , p_skusize              in     varchar2 := null
                              , p_hazmat               in     varchar2 := null
                              , p_hazmatid             in     varchar2 := null
                              , p_shipshelflife        in     integer  := null
                              , p_nmfcnumber           in     integer  := null
                              , p_incubrule            in     varchar2 := null
                              , p_incubhours           in     integer  := null
                              , p_eachwidth            in     number   := null
                              , p_eachdepth            in     number   := null
                              , p_reordertriggerqty    in     number   := null
                              , p_lowtriggerqty        in     number   := null
                              , p_disallowmergerules   in     varchar2 := null
                              , p_packdespatchrepack   in     varchar2 := null
                              , p_specid               in     varchar2 := null
                              , p_userdeftype1         in     varchar2 := null
                              , p_userdeftype2         in     varchar2 := null
                              , p_userdeftype3         in     varchar2 := null
                              , p_userdeftype4         in     varchar2 := null
                              , p_userdeftype5         in     varchar2 := null
                              , p_userdeftype6         in     varchar2 := null
                              , p_userdeftype7         in     varchar2 := null
                              , p_userdeftype8         in     varchar2 := null
                              , p_userdefchk1          in     varchar2 := null
                              , p_userdefchk2          in     varchar2 := null
                              , p_userdefchk3          in     varchar2 := null
                              , p_userdefchk4          in     varchar2 := null
                              , p_userdefdate1         in     date     := null
                              , p_userdefdate2         in     date     := null
                              , p_userdefdate3         in     date     := null
                              , p_userdefdate4         in     date     := null
                              , p_userdefnum1          in     number   := null
                              , p_userdefnum2          in     number   := null
                              , p_userdefnum3          in     number   := null
                              , p_userdefnum4          in     number   := null
                              , p_userdefnote1         in     varchar2 := null
                              , p_userdefnote2         in     varchar2 := null
                              , p_timezonename         in     varchar2 := null
                              , p_beamunits            in     integer  := null
                              , p_cewarehousetype      in     varchar2 := null
                              , p_cecustomsexcise      in     varchar2 := null
                              , p_cestandardcost       in     number   := null
                              , p_cestandardcurrency   in     varchar2 := null
                              , p_disallowclustering   in     varchar2 := null
                              , p_clientgroup          in     varchar2 := null
                              , p_maxstack             in     integer  := null
                              , p_stackdescription     in     varchar2 := null
                              , p_stacklimitation      in     integer  := null
                              , p_cedutystamp          in     varchar2 := null
                              , p_captureweight        in     varchar2 := null
                              , p_weighatreceipt       in     varchar2 := null
                              , p_upperweighttolerance in     number   := null
                              , p_lowerweighttolerance in     number   := null
                              , p_serialatloading      in     varchar2 := null
                              , p_serialatkitting      in     varchar2 := null
                              , p_serialatunkitting    in     varchar2 := null
                              , p_cecommoditycode      in     varchar2 := null
                              , p_cecoo                in     varchar2 := null
                              , p_cecwc                in     varchar2 := null
                              , p_cevatcode            in     varchar2 := null
                              , p_ceproducttype        in     varchar2 := null
                              , p_commoditycode        in     varchar2 := null
                              , p_commoditydesc        in     varchar2 := null
                              , p_familygroup          in     varchar2 := null
                              , p_breakpack            in     varchar2 := null
                              , p_clearable            in     varchar2 := null
                              , p_stagerouteid         in     varchar2 := null
                              , p_serialmaxrange       in     integer  := null
                              , p_serialdynamicrange   in     varchar2 := null
                              , p_expiryatrepack       in     varchar2 := null
                              , p_udfatrepack          in     varchar2 := null
                              , p_manufactureatrepack  in     varchar2 := null
                              , p_repackbypiece        in     varchar2 := null
                              , p_eachquantity         in     number   := null
                              , p_collectivemode       in     varchar2 := null
                              , p_packedheight         in     number   := null
                              , p_packedwidth          in     number   := null
                              , p_packeddepth          in     number   := null
                              , p_packedvolume         in     number   := null
                              , p_packedweight         in     number   := null
                              , p_awkward              in     varchar2 := null
                              , p_twomanlift           in     varchar2 := null
                              , p_decatalogued         in     varchar2 := null
                              )
  return integer
  is
    cursor c_clt ( b_client_id in varchar2)
    is
      select 1
      from   dcsdba.client_group_clients
      where  client_id    = b_client_id
      and    client_group = 'CSLENT'    -- Client Visibility Group with Bonded Warehouse Customers
      ;

    l_clt_int    integer;
    l_session_id number;
    l_integer    integer;
    l_client_id  varchar2(20);
  begin
    -- Convert ClientID
    select decode( p_clientid, 'CEU',        'CRLBB'
                             , 'FLIRSTB',    'FLIRS'
                             , 'FORWARD',    'FRWRD'
                             , 'RAYMARINE',  'FLIRB'
                             , 'WELCHALLYN', 'WELCH'
                                           , p_clientid
                 )
    into l_client_id
    from dual;

    -- Check if Client is enabled for Streamliner in WMS2016
    -- If enabled in WMS2016 call DirectSKU function in WMS2016
    --
    open  c_clt (b_client_id => l_client_id);
    fetch c_clt
    into  l_clt_int;

    if c_clt%found
    then
      -- call DirectSKU function locally
      l_session_id := set_wms_session_settings(p_session_id_i => null);
      l_integer    := dcsdba.libmergesku.directsku( p_mergeerror             => p_mergeerror            
                                                  , p_toupdatecols           => p_toupdatecols          
                                                  , p_mergeaction            => p_mergeaction           
                                                  , p_clientid               => l_client_id              
                                                  , p_skuid                  => p_skuid                 
                                                  , p_ean                    => p_ean                   
                                                  , p_upc                    => p_upc                   
                                                  , p_description            => p_description           
                                                  , p_productgroup           => p_productgroup          
                                                  , p_eachheight             => p_eachheight            
                                                  , p_eachweight             => p_eachweight            
                                                  , p_eachvolume             => p_eachvolume            
                                                  , p_eachvalue              => p_eachvalue             
                                                  , p_qcstatus               => p_qcstatus              
                                                  , p_shelflife              => p_shelflife             
                                                  , p_qcfrequency            => p_qcfrequency           
                                                  , p_splitlowest            => p_splitlowest           
                                                  , p_conditionreqd          => p_conditionreqd         
                                                  , p_expiryreqd             => p_expiryreqd            
                                                  , p_originreqd             => p_originreqd            
                                                  , p_serialatpack           => p_serialatpack          
                                                  , p_serialatpick           => p_serialatpick          
                                                  , p_serialatreceipt        => p_serialatreceipt       
                                                  , p_serialrange            => p_serialrange           
                                                  , p_serialformat           => p_serialformat          
                                                  , p_serialvalidmerge       => p_serialvalidmerge      
                                                  , p_serialnoreuse          => p_serialnoreuse         
                                                  , p_picksequence           => p_picksequence          
                                                  , p_pickcountqty           => p_pickcountqty          
                                                  , p_countfrequency         => p_countfrequency        
                                                  , p_oapwipenabled          => p_oapwipenabled         
                                                  , p_kitsku                 => p_kitsku                
                                                  , p_kitsplit               => p_kitsplit              
                                                  , p_kittriggerqty          => p_kittriggerqty         
                                                  , p_kitqtydue              => p_kitqtydue             
                                                  , p_kittinglocid           => p_kittinglocid          
                                                  , p_allocationgroup        => p_allocationgroup       
                                                  , p_putawaygroup           => p_putawaygroup          
                                                  , p_abcdisable             => p_abcdisable            
                                                  , p_handlingclass          => p_handlingclass         
                                                  , p_obsoleteproduct        => p_obsoleteproduct       
                                                  , p_newproduct             => p_newproduct            
                                                  , p_disallowupload         => p_disallowupload        
                                                  , p_disallowcrossdock      => p_disallowcrossdock     
                                                  , p_manufdstampreqd        => p_manufdstampreqd       
                                                  , p_manufdstampdflt        => p_manufdstampdflt       
                                                  , p_minshelflife           => p_minshelflife          
                                                  , p_colour                 => p_colour                
                                                  , p_skusize                => p_skusize               
                                                  , p_hazmat                 => p_hazmat                
                                                  , p_hazmatid               => p_hazmatid              
                                                  , p_shipshelflife          => p_shipshelflife         
                                                  , p_nmfcnumber             => p_nmfcnumber            
                                                  , p_incubrule              => p_incubrule             
                                                  , p_incubhours             => p_incubhours            
                                                  , p_eachwidth              => p_eachwidth             
                                                  , p_eachdepth              => p_eachdepth             
                                                  , p_reordertriggerqty      => p_reordertriggerqty     
                                                  , p_lowtriggerqty          => p_lowtriggerqty         
                                                  , p_disallowmergerules     => p_disallowmergerules    
                                                  , p_packdespatchrepack     => p_packdespatchrepack    
                                                  , p_specid                 => p_specid                
                                                  , p_userdeftype1           => p_userdeftype1          
                                                  , p_userdeftype2           => p_userdeftype2          
                                                  , p_userdeftype3           => p_userdeftype3          
                                                  , p_userdeftype4           => p_userdeftype4          
                                                  , p_userdeftype5           => p_userdeftype5          
                                                  , p_userdeftype6           => p_userdeftype6          
                                                  , p_userdeftype7           => p_userdeftype7          
                                                  , p_userdeftype8           => p_userdeftype8          
                                                  , p_userdefchk1            => p_userdefchk1           
                                                  , p_userdefchk2            => p_userdefchk2           
                                                  , p_userdefchk3            => p_userdefchk3           
                                                  , p_userdefchk4            => p_userdefchk4           
                                                  , p_userdefdate1           => p_userdefdate1          
                                                  , p_userdefdate2           => p_userdefdate2          
                                                  , p_userdefdate3           => p_userdefdate3          
                                                  , p_userdefdate4           => p_userdefdate4          
                                                  , p_userdefnum1            => p_userdefnum1           
                                                  , p_userdefnum2            => p_userdefnum2           
                                                  , p_userdefnum3            => p_userdefnum3           
                                                  , p_userdefnum4            => p_userdefnum4           
                                                  , p_userdefnote1           => p_userdefnote1          
                                                  , p_userdefnote2           => p_userdefnote2          
                                                  , p_timezonename           => p_timezonename          
                                                  , p_beamunits              => p_beamunits             
                                                  , p_cewarehousetype        => p_cewarehousetype       
                                                  , p_cecustomsexcise        => p_cecustomsexcise       
                                                  , p_cestandardcost         => p_cestandardcost        
                                                  , p_cestandardcurrency     => p_cestandardcurrency    
                                                  , p_disallowclustering     => p_disallowclustering    
                                                  , p_clientgroup            => p_clientgroup           
                                                  , p_maxstack               => p_maxstack              
                                                  , p_stackdescription       => p_stackdescription      
                                                  , p_stacklimitation        => p_stacklimitation       
                                                  , p_cedutystamp            => p_cedutystamp           
                                                  , p_captureweight          => p_captureweight         
                                                  , p_weighatreceipt         => p_weighatreceipt        
                                                  , p_upperweighttolerance   => p_upperweighttolerance  
                                                  , p_lowerweighttolerance   => p_lowerweighttolerance  
                                                  , p_serialatloading        => p_serialatloading       
                                                  , p_serialatkitting        => p_serialatkitting       
                                                  , p_serialatunkitting      => p_serialatunkitting     
                                                  , p_cecommoditycode        => p_cecommoditycode       
                                                  , p_cecoo                  => p_cecoo                 
                                                  , p_cecwc                  => p_cecwc                 
                                                  , p_cevatcode              => p_cevatcode             
                                                  , p_ceproducttype          => p_ceproducttype         
                                                  , p_commoditycode          => p_commoditycode         
                                                  , p_commoditydesc          => p_commoditydesc         
                                                  , p_familygroup            => p_familygroup           
                                                  , p_breakpack              => p_breakpack             
                                                  , p_clearable              => p_clearable             
                                                  , p_stagerouteid           => p_stagerouteid          
                                                  , p_serialmaxrange         => p_serialmaxrange        
                                                  , p_serialdynamicrange     => p_serialdynamicrange    
                                                  , p_expiryatrepack         => p_expiryatrepack        
                                                  , p_udfatrepack            => p_udfatrepack           
                                                  , p_manufactureatrepack    => p_manufactureatrepack   
                                                  , p_repackbypiece          => p_repackbypiece         
                                                  , p_eachquantity           => p_eachquantity          
                                                  , p_packedheight           => p_packedheight          
                                                  , p_packedwidth            => p_packedwidth           
                                                  , p_packeddepth            => p_packeddepth           
                                                  , p_packedvolume           => p_packedvolume          
                                                  , p_packedweight           => p_packedweight          
                                                  , p_awkward                => p_awkward               
                                                  , p_twomanlift             => p_twomanlift            
                                                  , p_decatalogued           => p_decatalogued          
                                                  , p_stockcheckruleid       => null
                                                  , p_unkittinginherit       => null
                                                  , p_serialatstockcheck     => null
                                                  , p_serialatstockadjust    => null
                                                  , p_kitshipcomponents      => null
                                                  , p_unallocatable          => null
                                                  , p_batchatkitting         => null
                                                  , p_serialpereach          => null
                                                  , p_vmiallowallocation     => null
                                                  , p_vmiallowinterfaced     => null
                                                  , p_vmiallowmanual         => null
                                                  , p_vmiallowreplenish      => null
                                                  , p_vmiagingdays           => null
                                                  , p_vmioverstockqty        => null
                                                  , p_scraponreturn          => null
                                                  , p_harmonisedproductcode  => null
                                                  , p_hanginggarment         => null
                                                  , p_conveyable             => null
                                                  , p_fragile                => null
                                                  , p_gender                 => null
                                                  , p_highsecurity           => null
                                                  , p_ugly                   => null
                                                  , p_collatable             => null
                                                  , p_ecommerce              => null
                                                  , p_promotion              => null
                                                  , p_foldable               => null
                                                  , p_style                  => null
                                                  , p_businessunitcode       => null
                                                  , p_tagmerge               => null
                                                  , p_carrierpalletmixing    => null
                                                  , p_specialcontainertype   => null
                                                  , p_disallowrdtoverpicking => null
                                                  , p_noallocbackorder       => null
                                                  , p_returnminshelflife     => null
                                                  , p_weighatgridpick        => null
                                                  , p_ceexciseproductcode    => null
                                                  , p_cedegreeplato          => null
                                                  , p_cedesignationorigin    => null
                                                  , p_cedensity              => null
                                                  , p_cebrandname            => null
                                                  , p_cealcoholicstrength    => null
                                                  , p_cefiscalmark           => null
                                                  , p_cesizeofproducer       => null
                                                  , p_cecommercialdesc       => null
                                                  , p_serialnooutbound       => null
                                                  , p_fullpalletatreceipt    => null
                                                  , p_alwaysfullpallet       => null
                                                  , p_subwithinproductgrp    => null
                                                  , p_serialcheckstring      => null
                                                  , p_carrierproducttype     => null
                                                  , p_maxpackconfigs         => null
                                                  , p_parcelpackingbypiece   => null
                                                  ); 

    end if;
    close c_clt;

    p_mergeerror := p_mergeerror;

    return l_integer;

  exception
    when others
    then
      case
      when c_clt%isopen
      then
        close c_clt;
      else
        null;
      end case;

      l_integer    := 0;
      p_mergeerror := substr( sqlerrm, 1, 10);

      return l_integer;

    raise;

  end process_csl_product;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 28-Dec-2018
-- Purpose : Process CSL Inbound Header interface to WMS (Wrapper for libmergepreadvice.directpreadviceheader function)
------------------------------------------------------------------------------------------------
  function process_csl_inb_header( p_mergeerror         in out varchar2
                                 , p_toupdatecols       in     varchar2 := null
                                 , p_mergeaction        in     varchar2
                                 , p_clientid           in     varchar2
                                 , p_preadviceid        in     varchar2
                                 , p_preadvicetype      in     varchar2 := null
                                 , p_siteid             in     varchar2 := null
                                 , p_ownerid            in     varchar2
                                 , p_supplierid         in     varchar2 := null
                                 , p_status             in     varchar2 := null
                                 , p_bookrefid          in     varchar2 := null
                                 , p_duedstamp          in     date     := null
                                 , p_contact            in     varchar2 := null
                                 , p_contactphone       in     varchar2 := null
                                 , p_contactfax         in     varchar2 := null
                                 , p_contactemail       in     varchar2 := null
                                 , p_name               in     varchar2 := null
                                 , p_address1           in     varchar2 := null
                                 , p_address2           in     varchar2 := null
                                 , p_town               in     varchar2 := null
                                 , p_county             in     varchar2 := null
                                 , p_postcode           in     varchar2 := null
                                 , p_country            in     varchar2 := null
                                 , p_vatnumber          in     varchar2 := null
                                 , p_returnflag         in     varchar2 := null
                                 , p_samplingtype       in     varchar2 := null
                                 , p_returnedorderid    in     varchar2 := null
                                 , p_emailconfirm       in     varchar2 := null
                                 , p_collectionreqd     in     varchar2 := null
                                 , p_consignment        in     varchar2 := null
                                 , p_loadsequence       in     integer  := null
                                 , p_notes              in     varchar2 := null
                                 , p_disallowmergerules in     varchar2 := null
                                 , p_oaprma             in     number   := null
                                 , p_userdeftype1       in     varchar2 := null
                                 , p_userdeftype2       in     varchar2 := null
                                 , p_userdeftype3       in     varchar2 := null
                                 , p_userdeftype4       in     varchar2 := null
                                 , p_userdeftype5       in     varchar2 := null
                                 , p_userdeftype6       in     varchar2 := null
                                 , p_userdeftype7       in     varchar2 := null
                                 , p_userdeftype8       in     varchar2 := null
                                 , p_userdefchk1        in     varchar2 := null
                                 , p_userdefchk2        in     varchar2 := null
                                 , p_userdefchk3        in     varchar2 := null
                                 , p_userdefchk4        in     varchar2 := null
                                 , p_userdefdate1       in     date     := null
                                 , p_userdefdate2       in     date     := null
                                 , p_userdefdate3       in     date     := null
                                 , p_userdefdate4       in     date     := null
                                 , p_userdefnum1        in     number   := null
                                 , p_userdefnum2        in     number   := null
                                 , p_userdefnum3        in     number   := null
                                 , p_userdefnum4        in     number   := null
                                 , p_userdefnote1       in     varchar2 := null
                                 , p_userdefnote2       in     varchar2 := null
                                 , p_timezonename       in     varchar2 := null
                                 , p_disallowreplens    in     varchar2 := null
                                 , p_clientgroup        in     varchar2 := null
                                 , p_supplierreference  in     varchar2 := null
                                 , p_carriername        in     varchar2 := null
                                 , p_carrierreference   in     varchar2 := null
                                 , p_tod                in     varchar2 := null
                                 , p_todplace           in     varchar2 := null
                                 , p_modeoftransport    in     varchar2 := null
                                 , p_yardcontainertype  in     varchar2 := null
                                 , p_yardcontainerid    in     varchar2 := null
                                 , p_ceconsignmentid    in     varchar2 := null
                                 , p_collectivemode     in     varchar2 := null
                                 , p_contactmobile      in     varchar2 := null
                                 , p_masterpreadvice    in     varchar2 := null
                                 )
  return integer
  is
    cursor c_clt ( b_client_id in varchar2)
    is
      select 1
      from   dcsdba.client_group_clients
      where  client_id    = b_client_id
      and    client_group = 'CSLENT'    -- Client Visibility Group with Bonded Warehouse Customers
      ;
    cursor c_onr ( b_client_id in varchar2)
    is
      select owner_id
      from   dcsdba.owner
      where  client_id = b_client_id
      ;

    l_clt_int    integer;
    l_session_id number;
    l_integer    integer;
    l_site_id    varchar2(20);
    l_client_id  varchar2(20);
    l_owner_id   varchar2(20);
  begin
    -- Convert SiteID
    select decode( p_siteid, 'RCLEHV', 'NLSBR01'
                           , 'RCLTLB', 'NLTLG01'
                                     , p_siteid
                 )
    into l_site_id
    from dual;
    -- Convert ClientID
    select decode( p_clientid, 'CEU',        'CRLBB'
                             , 'FLIRSTB',    'FLIRS'
                             , 'FORWARD',    'FRWRD'
                             , 'RAYMARINE',  'FLIRB'
                             , 'WELCHALLYN', 'WELCH'
                                           , p_clientid
                 )
    into l_client_id
    from dual;
    -- Get OwnerID by ClientID
    open  c_onr( b_client_id => l_client_id);
    fetch c_onr
    into  l_owner_id;
    close c_onr;
    if l_owner_id != l_client_id
    then
      l_owner_id := null;
    end if;

    -- Check if Client is enabled for Streamliner in WMS2016
    -- If enabled in WMS2016 call DirectPreAdviceHeader function in WMS2016 
    --
    open  c_clt (b_client_id => l_client_id);
    fetch c_clt
    into  l_clt_int;

    if c_clt%found
    then
      -- call DirectSKU function locally
      l_session_id := set_wms_session_settings(p_session_id_i => null);
      l_integer    := dcsdba.libmergepreadvice.directpreadviceheader( p_mergeerror         => p_mergeerror        
                                                                    , p_toupdatecols       => p_toupdatecols      
                                                                    , p_mergeaction        => p_mergeaction       
                                                                    , p_clientid           => l_client_id          
                                                                    , p_preadviceid        => p_preadviceid       
                                                                    , p_preadvicetype      => p_preadvicetype     
                                                                    , p_siteid             => l_site_id            
                                                                    , p_ownerid            => l_owner_id           
                                                                    , p_supplierid         => p_supplierid        
                                                                    , p_status             => p_status            
                                                                    , p_bookrefid          => p_bookrefid         
                                                                    , p_duedstamp          => p_duedstamp         
                                                                    , p_contact            => p_contact           
                                                                    , p_contactphone       => p_contactphone      
                                                                    , p_contactfax         => p_contactfax        
                                                                    , p_contactemail       => p_contactemail      
                                                                    , p_name               => p_name              
                                                                    , p_address1           => p_address1          
                                                                    , p_address2           => p_address2          
                                                                    , p_town               => p_town              
                                                                    , p_county             => p_county            
                                                                    , p_postcode           => p_postcode          
                                                                    , p_country            => p_country           
                                                                    , p_vatnumber          => p_vatnumber         
                                                                    , p_returnflag         => p_returnflag        
                                                                    , p_samplingtype       => p_samplingtype      
                                                                    , p_returnedorderid    => p_returnedorderid   
                                                                    , p_emailconfirm       => p_emailconfirm      
                                                                    , p_collectionreqd     => p_collectionreqd    
                                                                    , p_consignment        => p_consignment       
                                                                    , p_loadsequence       => p_loadsequence      
                                                                    , p_notes              => p_notes             
                                                                    , p_disallowmergerules => p_disallowmergerules
                                                                    , p_oaprma             => p_oaprma            
                                                                    , p_userdeftype1       => p_userdeftype1      
                                                                    , p_userdeftype2       => p_userdeftype2      
                                                                    , p_userdeftype3       => p_userdeftype3      
                                                                    , p_userdeftype4       => p_userdeftype4      
                                                                    , p_userdeftype5       => p_userdeftype5      
                                                                    , p_userdeftype6       => p_userdeftype6      
                                                                    , p_userdeftype7       => p_userdeftype7      
                                                                    , p_userdeftype8       => p_userdeftype8      
                                                                    , p_userdefchk1        => p_userdefchk1       
                                                                    , p_userdefchk2        => p_userdefchk2       
                                                                    , p_userdefchk3        => p_userdefchk3       
                                                                    , p_userdefchk4        => p_userdefchk4       
                                                                    , p_userdefdate1       => p_userdefdate1      
                                                                    , p_userdefdate2       => p_userdefdate2      
                                                                    , p_userdefdate3       => p_userdefdate3      
                                                                    , p_userdefdate4       => p_userdefdate4      
                                                                    , p_userdefnum1        => p_userdefnum1       
                                                                    , p_userdefnum2        => p_userdefnum2       
                                                                    , p_userdefnum3        => p_userdefnum3       
                                                                    , p_userdefnum4        => p_userdefnum4       
                                                                    , p_userdefnote1       => p_userdefnote1      
                                                                    , p_userdefnote2       => p_userdefnote2      
                                                                    , p_timezonename       => p_timezonename      
                                                                    , p_disallowreplens    => p_disallowreplens   
                                                                    , p_clientgroup        => p_clientgroup       
                                                                    , p_statusreasoncode   => null                
                                                                    , p_priority           => null                
                                                                    , p_supplierreference  => p_supplierreference 
                                                                    , p_carriername        => p_carriername       
                                                                    , p_carrierreference   => p_carrierreference  
                                                                    , p_tod                => p_tod               
                                                                    , p_todplace           => p_todplace          
                                                                    , p_modeoftransport    => p_modeoftransport   
                                                                    , p_yardcontainertype  => p_yardcontainertype 
                                                                    , p_yardcontainerid    => p_yardcontainerid   
                                                                    , p_ceconsignmentid    => p_ceconsignmentid   
                                                                    , p_contactmobile      => p_contactmobile     
                                                                    , p_masterpreadvice    => p_masterpreadvice   
                                                                    , p_ceinvoicenumber    => null                
                                                                    );
    else
      -- Convert SiteID
      select decode( p_siteid, 'NLSBR01', 'RCLEHV'
                             , 'NLTLG01', 'RCLTLB'           
                                        , p_siteid  
                   )
      into l_site_id
      from dual;
      -- Convert ClientID
      select decode( p_clientid, 'CRLBB', 'CEU'
                               , 'FLIRS', 'FLIRSTB'
                               , 'FRWRD', 'FORWARD'
                               , 'FLIRB', 'RAYMARINE'
                               , 'WELCH', 'WELCHALLYN'
                                        , p_clientid
                   )
      into l_client_id
      from dual;
      -- Get OwnerID by ClientID
      open  c_onr( b_client_id => l_client_id);
      fetch c_onr
      into  l_owner_id;
      close c_onr;
      if l_owner_id != l_client_id
      then
        l_owner_id := null;
      end if;

    end if;
    close c_clt;

    p_mergeerror := p_mergeerror;

    return l_integer;

  exception
    when others
    then
      case
      when c_clt%isopen
      then
        close c_clt;
      else
        null;
      end case;

      l_integer    := 0;
      p_mergeerror := substr( sqlerrm, 1, 10);

      return l_integer;

    raise;

  end process_csl_inb_header;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 28-Dec-2018
-- Purpose : Process CSL Product interface to WMS (Wrapper for libmergepreadvice.directpreadviceline function)
------------------------------------------------------------------------------------------------
  function process_csl_inb_line( p_mergeerror         in out varchar2
                               , p_toupdatecols       in     varchar2 := null
                               , p_mergeaction        in     varchar2
                               , p_clientid           in     varchar2
                               , p_preadviceid        in     varchar2
                               , p_lineid             in     integer  := null
                               , p_hostpreadviceid    in     varchar2 := null
                               , p_hostlineid         in     varchar2 := null
                               , p_skuid              in     varchar2 := null
                               , p_configid           in     varchar2 := null
                               , p_batchid            in     varchar2 := null
                               , p_expirydstamp       in     date     := null
                               , p_manufdstamp        in     date     := null
                               , p_palletconfig       in     varchar2 := null
                               , p_originid           in     varchar2 := null
                               , p_conditionid        in     varchar2 := null
                               , p_tagid              in     varchar2 := null
                               , p_lockcode           in     varchar2 := null
                               , p_speccode           in     varchar2 := null
                               , p_qtydue             in     number
                               , p_notes              in     varchar2 := null
                               , p_sapplant           in     varchar2 := null
                               , p_sapstoreloc        in     varchar2 := null
                               , p_disallowmergerules in     varchar2 := null
                               , p_userdeftype1       in     varchar2 := null
                               , p_userdeftype2       in     varchar2 := null
                               , p_userdeftype3       in     varchar2 := null
                               , p_userdeftype4       in     varchar2 := null
                               , p_userdeftype5       in     varchar2 := null
                               , p_userdeftype6       in     varchar2 := null
                               , p_userdeftype7       in     varchar2 := null
                               , p_userdeftype8       in     varchar2 := null
                               , p_userdefchk1        in     varchar2 := null
                               , p_userdefchk2        in     varchar2 := null
                               , p_userdefchk3        in     varchar2 := null
                               , p_userdefchk4        in     varchar2 := null
                               , p_userdefdate1       in     date     := null
                               , p_userdefdate2       in     date     := null
                               , p_userdefdate3       in     date     := null
                               , p_userdefdate4       in     date     := null
                               , p_userdefnum1        in     number   := null
                               , p_userdefnum2        in     number   := null
                               , p_userdefnum3        in     number   := null
                               , p_userdefnum4        in     number   := null
                               , p_userdefnote1       in     varchar2 := null
                               , p_userdefnote2       in     varchar2 := null
                               , p_timezonename       in     varchar2 := null
                               , p_clientgroup        in     varchar2 := null
                               , p_trackinglevel      in     varchar2 := null
                               , p_qtyduetolerance    in     number   := null
                               , p_cecoo              in     varchar2 := null
                               , p_ownerid            in     varchar2 := null
                               , p_ceconsignmentid    in     varchar2 := null
                               , p_collectivemode     in     varchar2 := null
                               )
  return integer
  is
    cursor c_clt ( b_client_id in varchar2)
    is
      select 1
      from   dcsdba.client_group_clients
      where  client_id    = b_client_id
      and    client_group = 'CSLENT'    -- Client Visibility Group with Bonded Warehouse Customers
      ;

    l_clt_int    integer;
    l_session_id number;
    l_integer    integer;
    l_client_id  varchar2(20);
  begin
    -- Convert ClientID
    select decode( p_clientid, 'CEU',        'CRLBB'
                             , 'FLIRSTB',    'FLIRS'
                             , 'FORWARD',    'FRWRD'
                             , 'RAYMARINE',  'FLIRB'
                             , 'WELCHALLYN', 'WELCH'
                                           , p_clientid
                 )
    into l_client_id
    from dual;

    -- Check if Client is enabled for Streamliner in WMS2016
    -- If enabled in WMS2016 call DirectPreAdviceLine function in WMS2016 
    --
    open  c_clt (b_client_id => l_client_id);
    fetch c_clt
    into  l_clt_int;

    if c_clt%found
    then
      -- call DirectSKU function locally
      l_session_id := set_wms_session_settings(p_session_id_i => null);
      l_integer    := dcsdba.libmergepreadvice.directpreadviceline( p_mergeerror          => p_mergeerror        
                                                                  , p_toupdatecols        => p_toupdatecols      
                                                                  , p_mergeaction         => p_mergeaction       
                                                                  , p_clientid            => l_client_id          
                                                                  , p_preadviceid         => p_preadviceid       
                                                                  , p_lineid              => p_lineid            
                                                                  , p_hostpreadviceid     => p_hostpreadviceid   
                                                                  , p_hostlineid          => p_hostlineid        
                                                                  , p_skuid               => p_skuid             
                                                                  , p_configid            => p_configid          
                                                                  , p_batchid             => p_batchid           
                                                                  , p_expirydstamp        => p_expirydstamp      
                                                                  , p_manufdstamp         => p_manufdstamp       
                                                                  , p_palletconfig        => p_palletconfig      
                                                                  , p_originid            => p_originid          
                                                                  , p_conditionid         => p_conditionid       
                                                                  , p_tagid               => p_tagid             
                                                                  , p_lockcode            => p_lockcode          
                                                                  , p_speccode            => p_speccode          
                                                                  , p_qtydue              => p_qtydue            
                                                                  , p_notes               => p_notes             
                                                                  , p_sapplant            => p_sapplant          
                                                                  , p_sapstoreloc         => p_sapstoreloc       
                                                                  , p_disallowmergerules  => p_disallowmergerules
                                                                  , p_userdeftype1        => p_userdeftype1      
                                                                  , p_userdeftype2        => p_userdeftype2      
                                                                  , p_userdeftype3        => p_userdeftype3      
                                                                  , p_userdeftype4        => p_userdeftype4      
                                                                  , p_userdeftype5        => p_userdeftype5      
                                                                  , p_userdeftype6        => p_userdeftype6      
                                                                  , p_userdeftype7        => p_userdeftype7      
                                                                  , p_userdeftype8        => p_userdeftype8      
                                                                  , p_userdefchk1         => p_userdefchk1       
                                                                  , p_userdefchk2         => p_userdefchk2       
                                                                  , p_userdefchk3         => p_userdefchk3       
                                                                  , p_userdefchk4         => p_userdefchk4       
                                                                  , p_userdefdate1        => p_userdefdate1      
                                                                  , p_userdefdate2        => p_userdefdate2      
                                                                  , p_userdefdate3        => p_userdefdate3      
                                                                  , p_userdefdate4        => p_userdefdate4      
                                                                  , p_userdefnum1         => p_userdefnum1       
                                                                  , p_userdefnum2         => p_userdefnum2       
                                                                  , p_userdefnum3         => p_userdefnum3       
                                                                  , p_userdefnum4         => p_userdefnum4       
                                                                  , p_userdefnote1        => p_userdefnote1      
                                                                  , p_userdefnote2        => p_userdefnote2      
                                                                  , p_timezonename        => p_timezonename      
                                                                  , p_clientgroup         => p_clientgroup       
                                                                  , p_trackinglevel       => p_trackinglevel     
                                                                  , p_qtyduetolerance     => p_qtyduetolerance   
                                                                  , p_cecoo               => p_cecoo             
                                                                  , p_ownerid             => p_ownerid           
                                                                  , p_ceconsignmentid     => p_ceconsignmentid   
                                                                  , p_ceunderbond         => null
                                                                  , p_celink              => null
                                                                  , p_productprice        => null
                                                                  , p_productcurrency     => null
                                                                  , p_ceinvoicenumber     => null
                                                                  , p_serialvalidmerge    => null
                                                                  , p_samplingtype        => null
                                                                  , p_expectednetweight   => null
                                                                  , p_expectedgrossweight => null
                                                                  ); 

    end if;
    close c_clt;

    p_mergeerror := p_mergeerror;

    return l_integer;

  exception
    when others
    then
      case
      when c_clt%isopen
      then
        close c_clt;
      else
        null;
      end case;

      l_integer    := 0;
      p_mergeerror := substr( sqlerrm, 1, 10);

      return l_integer;

    raise;

  end process_csl_inb_line;
--
--
begin
  -- Initialization
  execute immediate 'alter session set nls_territory=''AMERICA''';
end cnl_api_pck;