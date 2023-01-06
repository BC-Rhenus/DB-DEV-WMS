CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_API_PCK" is
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
    return integer;
  --
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
    return integer;
  --                   
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
    return integer;
  --
  procedure refresh_daily_mviews;
  --
  function set_wms_session_settings ( p_session_id_i in  number := null)
    return number;
  --
  function get_wms_order_sequence ( p_client_id_i in  varchar2
                                  , p_ose_type_i  in  varchar2
                                  , p_udp_1_i     in  varchar2 := null
                                  , p_udp_2_i     in  varchar2 := null
                                  , p_date_i      in  varchar2 := null -- format YYYYMMDD
                                  ) 
    return varchar2;
  --
  function get_tracking_url ( p_carrier_id_i  in  varchar2
                            , p_tracking_nr_i in  varchar2
                            )
    return varchar2;
  --
  function ins_order_accessorial ( p_client_id_i    in  varchar2
                                 , p_order_id_i     in  varchar2
                                 , p_accessorial_i  in  varchar2
                                 , p_timezonename_i in  varchar2 := 'Europe/Amsterdam'
                                 , p_errortext_o    out varchar2
                                 )
    return integer;
  --
  procedure sync_sku_special_links;
  --
  procedure send_email ( p_email_from_i      in varchar2 := null
                       , p_email_to_i        in varchar2
                       , p_subject_i         in varchar2
                       , p_message_body_i    in varchar2
                       , p_attachment_name_i in varchar2 := null
                       , p_attachment_file_i in blob     := null
                       );
  --
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
    return integer;
  --
  procedure process_csl_balllist ( p_site_id_i   in  varchar2
                                 , p_client_id_i in  varchar2
                                 );
  --
  procedure process_wlgore_vat ( p_site_id_i      in varchar2
                               , p_client_id_i    in varchar2
                               , p_shipped_date_i in date
                               );
  --
  procedure process_csl_cbs ( p_site_id_i      in varchar2
                            , p_client_id_i    in varchar2
                            , p_shipped_date_i in date
                            , p_csl_bu_i       in varchar2 
                            , p_trans_type_i   in integer  := null 
                            );
  --
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
    return integer;
  --
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
  return integer;
  --
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
  return integer;
  --
end cnl_api_pck;