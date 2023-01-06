CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_MHT_PCK" is
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
                          );
  --
end cnl_mht_pck;