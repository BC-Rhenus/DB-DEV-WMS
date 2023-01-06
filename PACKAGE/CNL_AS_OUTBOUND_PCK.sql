CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_OUTBOUND_PCK" is
/**********************************************************************************
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
    function last_box( p_site_id_i      varchar2
                     , p_client_id_i    varchar2
                     , p_order_id_i     varchar2
                     , p_container_id_i varchar2
                     )
        return number;
    --
    -- 
    procedure wms_get_orders_started(p_order_i varchar2,
                                     p_list_i varchar2,
                                     p_client_i varchar2,
                                     p_site_i varchar2);
    --
    -- 
    procedure manual_order_start;
    --
    --
    function get_carton_weight( p_container_id_i  in  varchar2
                              , p_client_id_i     in  varchar2
                              , p_site_id_i       in  varchar2
                              )
        return number;
    --
    --
    procedure manual_pick_finished( p_itl_key_i      in number,
                                    p_pallet_i       in varchar2,
                                    p_container_i    in varchar2,
                                    p_station_i      in varchar2,
                                    p_site_i         in varchar2,
                                    p_client_i       in varchar2,
                                    p_to_location_i  in varchar2,
                                    p_consol_link_i  in number
                                  );
    --
    --
    procedure pick_confirmation( p_hme_tbl_key_i    in number
                               , p_hme_key_i        in number
                               , p_error_yn_o       in out varchar2
                               , p_error_code_o     in out varchar2
                               , p_error_text_o     in out varchar2
                               , p_container_id_o   in out varchar2
                               );    
    --
    --
    procedure complete_pick_tasks( p_container_id_i in varchar2);
    --
    --
    procedure order_status_change( p_order_id_i     in varchar2
                                 , p_client_id_i    in varchar2
                                 , p_status_i       in varchar2
                                 );
    --
    --
     procedure run_cluster_config( p_cluster_group_id_i varchar2
                                 , p_site_id_i          varchar2
                                 , p_client_id_i        varchar2 default null
                                 );
    --
    --
    procedure marshal_envelope( p_from_loc_id_i     in varchar2
                              , p_to_loc_id_i       in varchar2
                              , p_container_type_i  in varchar2
                              , p_site_id_i         in varchar2
                              );
    --
    --
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
                               );
end cnl_as_outbound_pck;