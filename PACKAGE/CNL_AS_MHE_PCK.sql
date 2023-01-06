CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_MHE_PCK" is
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
    -- Triggers documents and tells if box must be closed Y/N 
    procedure print_doc ( p_wms_unit_id_i     in  varchar2
                        , p_mhe_position_i    in  varchar2 default null
                        , p_mht_unit_id_i     in  varchar2 default null
                        , p_mht_station_id_i  in  varchar2
                        , p_print_doc_o       out varchar2 -- If documents will be printed for box.
                        , p_close_box_o       out varchar2 -- If box must be closed Y/N can be shown on display.
                        , p_pass_trough_o     out varchar2 -- skip packing and printing
                        , p_instruction_o     out varchar2 -- Instructions for operator.
                        , p_ok_yn_o           out varchar2 -- When no an error occured. 
                        , p_error_message_o   out varchar2 -- Shown on display at packing area when an error occured
                        );
    -- Processes DWS data and Generates shipping label
    procedure create_parcel ( p_wms_unit_id_i     in  varchar2
                            , p_mhe_position_i    in  varchar2 default null
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
                            , p_sort_pos_o        out varchar2
			    , p_ctosaas_yn_o	  out varchar2
                            );
    -- Returns final sortation location.
    procedure get_sort_pos( p_wms_unit_id_i    in  varchar2
                          , p_mhe_position_i   in  varchar2 default null
                          , p_mht_station_id_i in  varchar2
                          , p_sort_pos_o       out varchar2
                          );
    -- Complete sortation
    procedure comp_sort( p_wms_unit_id_i    in  varchar2
                       , p_mhe_position_i   in  varchar2 default null
                       , p_mht_pal_id_i     in  varchar2
                       , p_mht_pal_type_i   in  varchar2 default null
                       , p_mht_station_id_i in  varchar2
                       , p_ok_yn_o          out varchar2
                       , p_err_message_o    out varchar2  
                       );
    -- Validate Parcel
    procedure validate_parcel( p_wms_unit_id_i      in  varchar2
                             , p_mhe_position_i     in  varchar2 default null
                             , p_mht_station_id_i   in  varchar2
                             , p_tracking_nr_o      out varchar2
                             , p_operator_o         out integer
                             , p_skip_val_o         out integer
                             );
    --
	procedure tu_pushed_in_vas_p( p_tu_id_i		varchar2
				    , p_site_id_i	varchar2
				    );
	--Fetch Centiro Saas shipping label from database
	function get_ship_label_f( p_box_id_i	in varchar2)
		return clob;


end cnl_as_mhe_pck;