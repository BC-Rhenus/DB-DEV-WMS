CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_STREAMSOFT_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with Stream Software's Customs Streamliner (Customs Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/

  procedure create_ssw_file ( p_ssw_file_type_i in  varchar2
                            , p_msg_id_i        in  varchar2
                            , p_company_i       in  varchar2
                            , p_bu_ppl_i        in  varchar2
                            , p_reference_id_i  in  varchar2
                            , p_content_i       in  varchar2
                            , p_filename_o      out varchar2
                            );
  --
  function get_additionalcode ( p_commodity_code_i in varchar2
                              , p_tax_high_low_i   in varchar2
                              )
  return varchar2;
  --
  procedure create_inbound_receipt ( p_site_id_i      in  varchar2       
                                   , p_client_id_i    in  varchar2
                                   , p_reference_id_i in  varchar2
                                   );
  --
  procedure create_inbound_wlgvat( p_site_id_i      in  varchar2       
                                 , p_client_id_i    in  varchar2
                                 , p_reference_id_i in  varchar2
                                 );
  --
  procedure create_adjustment_minus ( p_site_id_i   in  varchar2       
                                    , p_client_id_i in  varchar2
                                    , p_key_i       in  varchar2
                                    );
  --
  procedure create_outbound_entrepot ( p_site_id_i      in  varchar2       
                                     , p_client_id_i    in  varchar2
                                     , p_reference_id_i in  varchar2
                                     );
  --
  procedure create_outbound_reconditioning ( p_site_id_i      in  varchar2       
                                           , p_client_id_i    in  varchar2
                                           , p_reference_id_i in  varchar2
                                           );
  --
  procedure create_outbound_export ( p_site_id_i      in  varchar2       
                                   , p_client_id_i    in  varchar2
                                   , p_reference_id_i in  varchar2
                                   );
  --
  procedure create_outbound_cbs( p_site_id_i      in  varchar2       
                               , p_client_id_i    in  varchar2
                               , p_reference_id_i in  varchar2
                               , p_csl_bu_i       in  varchar2
                               , p_trans_type_i   in  integer  := null
                               );
  --
  procedure create_stock_list ( p_site_id_i      in  varchar2       
                              , p_client_id_i    in  varchar2
                              );
  --
  procedure process_csl_release ( p_message_type_i       in  varchar2
                                , p_date_i               in  varchar2
                                , p_time_i               in  varchar2
                                , p_company_i            in  varchar2
                                , p_businessunit_i       in  varchar2
                                , p_warehouse_i          in  varchar2
                                , p_dossier_type_i       in  varchar2
                                , p_dossier_id_i         in  varchar2
                                , p_dossier_status_i     in  varchar2
                                , p_linkid_i             in  varchar2
                                , p_error_o              out integer
                                , p_errortext_o          out varchar2
                                );
  --
end cnl_streamsoft_pck;