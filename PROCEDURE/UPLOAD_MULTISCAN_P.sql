CREATE OR REPLACE PROCEDURE "CNL_SYS"."UPLOAD_MULTISCAN_P" as
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Paul Jaegers
* $Date: 05-05-2021
**********************************************************************************
*
* Description: 
* procedure to upload file to CNL_MULTISCAN_DATA
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
  cursor c_data is
    select * from CNL_SYS.multiscan_data_load where nvl(status,'NULL') != 'OK';

    p_ok number;
    p_message varchar2(1000);
begin
  for r_data in c_data loop
    cnl_multiscan_pck.proc_data_p
			  ( r_data.USERS 		-- Multiscan user              
                          , r_data.client_id 		-- WMS client id
                          , r_data.SKU_ID		-- SKU, EAN, UPC, TUC or Supplier SKU
                          , r_data.NBR_TRCK_LVLS	-- Number of tracking levels
                          , r_data.pallet_type		-- Pallet type used (EURO, BLOK, NOPALLET, OTHER)
                          , r_data.track_level_1	-- Lowest tracking level
                          , r_data.each_depth		-- Each depth
                          , r_data.each_width		-- Each width
                          , r_data.each_height		-- Each height
                          , r_data.each_weight		-- Each weight
                          , r_data.track_level_2	-- Second tracking level
                          , r_data.ratio_1_to_2		-- Ratio num lowest tracking level in second tracking level
                          , r_data.depth_2		-- Second tracking level depth
                          , r_data.width_2		-- Second tracking level width
                          , r_data.height_2		-- Second tracking level height
                          , r_data.weight_2		-- Second tracking level weight
                          , r_data.track_level_3	-- Third tracking level
                          , r_data.ratio_2_to_3		-- Ratio num second tracking level in third tracking level
                          , r_data.depth_3		-- Third tracking level depth
                          , r_data.width_3		-- Third tracking level width
                          , r_data.height_3		-- Third tracking level height
                          , r_data.weight_3		-- Third tracking level weight                          
                          , r_data.track_level_4	-- Fourth tracking level
                          , r_data.ratio_3_to_4		-- Ratio num third tracking level in fourth tracking level
                          , r_data.depth_4		-- Fourth tracking level depth
                          , r_data.width_4		-- Fourth tracking level width
                          , r_data.height_4		-- Fourth tracking level height
                          , r_data.weight_4		-- Fourth tracking level weight                          
                          , r_data.track_level_5	-- Fifth tracking level
                          , r_data.ratio_4_to_5		-- Ratio num fourth tracking level in fifth tracking level
                          , r_data.depth_5		-- Fifth tracking level depth
                          , r_data.width_5		-- Fifth tracking level width
                          , r_data.height_5		-- Fifth tracking level height
                          , r_data.weight_5		-- Fifth tracking level weight                          
                          , r_data.track_level_6	-- Sixth tracking level
                          , r_data.ratio_5_to_6		-- Ratio num fifth tracking level in sixth tracking level
                          , r_data.depth_6 		-- Sixth tracking level depth
                          , r_data.width_6		-- Sixth tracking level width
                          , r_data.height_6		-- Sixth tracking level height
                          , r_data.weight_6		-- Sixth tracking level weight                          
                          , r_data.track_level_7	-- Seventh tracking level
                          , r_data.ratio_6_to_7		-- Ratio num sixth tracking level in seventh tracking level
                          , r_data.depth_7		-- Seventh tracking level depth
                          , r_data.width_7		-- Seventh tracking level width
                          , r_data.height_7		-- Seventh tracking level height
                          , r_data.weight_7		-- Seventh tracking level weight                          
                          , r_data.track_level_8	-- Eighth tracking level
                          , r_data.ratio_7_to_8		-- Ratio num seventh tracking level in eighth tracking level
                          , r_data.depth_8		-- Eighth tracking level depth
                          , r_data.width_8		-- Eighth tracking level width
                          , r_data.height_8		-- Eighth tracking level height
                          , r_data.weight_8		-- Eighth tracking level weight                          
                          , r_data.layer_height		-- Hieght of a single layer on a pallet
                          , r_data.each_per_layer	-- number of lowest tracking level in a single layer
                          , r_data.num_layers		-- Number of layers on a pallet
			  , p_ok			-- 1 OK , 0 not OK
			  , p_message			-- Message that describes the result.
               );

	 update multiscan_data_load
	 set    status = case when p_ok = 1 then 'OK' when p_ok = 0 then 'Not OK' end
	 ,      response_message = p_message
	 where  sku_id = r_data.SKU_ID;


  end loop;
  --commit;
  exception when others then
    null;
end;