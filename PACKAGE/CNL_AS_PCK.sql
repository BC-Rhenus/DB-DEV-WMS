CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 07-05-2018
**********************************************************************************
*
* Description: 
* several utilities for AS packages.
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
    function get_system_profile ( p_profile_id_i in varchar2)
        return varchar2;
    --
    --
    function check_user_id( p_user_i varchar2)
        return varchar2;
    --
    --
    function check_station_id( p_station_i varchar2)
        return varchar2;
    --
    --
    function get_consol_link
        return number;
    --
    --
    function get_move_task_key
        return number;
    --
    --
    function get_pick_label_id
        return number;
    --
    --
    function wht_chk_req(p_weight_i number,
                         p_site_i varchar2)
      return number;
    --
    --
    function picksequence_value(p_weight_i varchar2,
                                p_site_i varchar2)
        return varchar2;
    --
    --
    function wht_tolerance(p_site_i in varchar2)
        return number;
    --
    --
    function get_clientgroups( p_site_id_i     varchar2)
        return varchar;
    --
    --
    function chk_client( p_site_id_i     varchar2
                       , p_client_id_i   varchar2
                       )
        return number;
    --
    --
    function separate_containers(p_station_i in varchar2)
        return varchar2;
    --
    --
    function get_drop_location(p_site_i varchar2)
        return varchar;
    --
    --
    procedure insert_itl( p_mt_key_i        in  number
                        , p_to_status_i     in  varchar2
                        , p_ok_yn_o         out varchar2
                        );
    --
    --
    procedure create_message_exchange( p_message_id_i               in  varchar2
                                     , p_message_status_i           in  varchar2
                                     , p_message_type_i             in  varchar2
                                     , p_trans_code_i               in  varchar2
                                     , p_host_message_table_key_i   in  varchar2
                                     , P_key_o                      out number
                                     );
    --
    --
    procedure process_message_response;
    procedure process_as_pick_confirm;
    procedure process_as_cubing_results;
    procedure process_as_other_messages;
    --
    --
    procedure log_container_suspect( p_container_id_i  in  varchar2
                                   , p_client_id_i     in  varchar2
                                   , p_order_id_i      in  varchar2
                                   , p_description_i   in  varchar2
                                   );
    --
    --
    procedure create_log_record( p_source_i         in varchar2
                               , p_description_i    in varchar2
                               );
    --
    --
    procedure serial_validation( p_tu_id_i       in  varchar2
                               , p_owner_id_i    in  varchar2
                               , p_order_id_i    in  varchar2
                               , p_product_id_i  in  varchar2
                               , p_serial_id_i   in  varchar2
                               , p_continue_o    out varchar2
                               , p_suspect_o     out varchar2
                               , p_message_o     out varchar2
                               ); 
    --
    --
    procedure as_housekeeping;
    --
    --
    procedure add_vas_activity( p_container_id_i           in varchar2 default null
                              , p_client_id_i              in varchar2 default null
                              , p_order_id_i               in varchar2 default null
                              , p_sku_id_i                 in varchar2 default null
                              , p_activity_name_i          in varchar2 
                              , p_activity_sequence_i      in varchar2 default null
                              , p_activity_instruction_i   in varchar2 default null
                              );
    --
    --
    procedure fetch_client_vas_activity( p_client_id_i      in varchar2
                                       , p_container_i      in varchar2 
                                       , p_order_id_i       in varchar2
                                       );
end cnl_as_pck;