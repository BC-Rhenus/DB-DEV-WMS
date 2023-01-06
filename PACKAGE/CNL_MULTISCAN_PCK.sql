CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_MULTISCAN_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Package to process data captured with the KHT Multiscan device
*
* Sku dimensions and pack configuration details are captured.
* Some values are optional because the operator does not have the actual build in front of him.
* Client + SKU, EAN, UPC or Supplier SKU are required and are validated
* The rest must be captured and depending on the situation are required or are optional.
**********************************************************************************
* $Log: $
**********************************************************************************/
  
  function  chk_client_f  ( p_client_id in varchar2)                    -- WMS client id
    return integer;
--
  Function chk_sku_f  ( p_id          in varchar2
                      , p_client_id   in varchar2
                      )
    return integer;
--
  Function get_clnt_trck_lvl_f (p_client_id varchar2)
    return varchar2;
--  
  procedure proc_data_p   ( p_user          varchar2                            -- Multiscan user                           
                          , p_client_id     in varchar2                         -- WMS client id
                          , p_id            in varchar2                         -- SKU, EAN, UPC, TUC or Supplier SKU
                          , p_num_trck_lvl  in number                           -- Number of tracking levels
                          , p_pallet_type   in varchar2                         -- Pallet type used (EURO, BLOK, NOPALLET, OTHER)
                          , p_trck_lvl_1    in varchar2                         -- Lowest tracking level
                          , p_depth_1       in number                           -- Each depth
                          , p_width_1       in number                           -- Each width
                          , p_height_1      in number                           -- Each height
                          , p_weight_1      in number                           -- Each weight
                          , p_trck_lvl_2    in varchar2   default null          -- Second tracking level
                          , p_ratio_1_to_2  in number     default null          -- Ratio num lowest tracking level in second tracking level
                          , p_depth_2       in number     default null          -- Second tracking level depth
                          , p_width_2       in number     default null          -- Second tracking level width
                          , p_height_2      in number     default null          -- Second tracking level height
                          , p_weight_2      in number     default null          -- Second tracking level weight
                          , p_trck_lvl_3    in varchar2   default null          -- Third tracking level
                          , p_ratio_2_to_3  in number     default null          -- Ratio num second tracking level in third tracking level
                          , p_depth_3       in number     default null          -- Third tracking level depth
                          , p_width_3       in number     default null          -- Third tracking level width
                          , p_height_3      in number     default null          -- Third tracking level height
                          , p_weight_3      in number     default null          -- Third tracking level weight                          
                          , p_trck_lvl_4    in varchar2   default null          -- Fourth tracking level
                          , p_ratio_3_to_4  in number     default null          -- Ratio num third tracking level in fourth tracking level
                          , p_depth_4       in number     default null          -- Fourth tracking level depth
                          , p_width_4       in number     default null          -- Fourth tracking level width
                          , p_height_4      in number     default null          -- Fourth tracking level height
                          , p_weight_4      in number     default null          -- Fourth tracking level weight                          
                          , p_trck_lvl_5    in varchar2   default null          -- Fifth tracking level
                          , p_ratio_4_to_5  in number     default null          -- Ratio num fourth tracking level in fifth tracking level
                          , p_depth_5       in number     default null          -- Fifth tracking level depth
                          , p_width_5       in number     default null          -- Fifth tracking level width
                          , p_height_5      in number     default null          -- Fifth tracking level height
                          , p_weight_5      in number     default null          -- Fifth tracking level weight                          
                          , p_trck_lvl_6    in varchar2   default null          -- Sixth tracking level
                          , p_ratio_5_to_6  in number     default null          -- Ratio num fifth tracking level in sixth tracking level
                          , p_depth_6       in number     default null          -- Sixth tracking level depth
                          , p_width_6       in number     default null          -- Sixth tracking level width
                          , p_height_6      in number     default null          -- Sixth tracking level height
                          , p_weight_6      in number     default null          -- Sixth tracking level weight                          
                          , p_trck_lvl_7    in varchar2   default null          -- Seventh tracking level
                          , p_ratio_6_to_7  in number     default null          -- Ratio num sixth tracking level in seventh tracking level
                          , p_depth_7       in number     default null          -- Seventh tracking level depth
                          , p_width_7       in number     default null          -- Seventh tracking level width
                          , p_height_7      in number     default null          -- Seventh tracking level height
                          , p_weight_7      in number     default null          -- Seventh tracking level weight                          
                          , p_trck_lvl_8    in varchar2   default null          -- Eighth tracking level
                          , p_ratio_7_to_8  in number     default null          -- Ratio num seventh tracking level in eighth tracking level
                          , p_depth_8       in number     default null          -- Eighth tracking level depth
                          , p_width_8       in number     default null          -- Eighth tracking level width
                          , p_height_8      in number     default null          -- Eighth tracking level height
                          , p_weight_8      in number     default null          -- Eighth tracking level weight                          
                          , p_layer_height  in number     default null          -- Hieght of a single layer on a pallet
                          , p_each_per_layer in number    default null          -- number of lowest tracking level in a single layer
                          , p_num_layers    in number     default null          -- Number of layers on a pallet
                          , p_ok            out integer    
                          , p_message       out varchar2
                          );
--    
end cnl_multiscan_pck;