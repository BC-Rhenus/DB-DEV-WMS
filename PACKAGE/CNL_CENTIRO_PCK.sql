CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_CENTIRO_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with Centiro (Delivery Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/

	procedure create_saveorder( p_site_id_i		in  varchar2
                                  , p_client_id_i 	in  varchar2
				  , p_order_id    	in  varchar2
				  );
	--
	procedure create_packparcel( p_site_id_i      	in  varchar2
				   , p_client_id_i    	in  varchar2
				   , p_order_id_i     	in  varchar2
				   , p_pallet_id_i    	in  varchar2 := null
				   , p_container_id_i 	in  varchar2 := null
				   , p_printer_i      	in  varchar2
				   , p_copies_i       	in  number
				   , p_print2file_i   	in  varchar2
				   , p_rtk_key_i	in  integer 
				   , p_run_task_i	in  dcsdba.run_task%rowtype
				   );
	--
	procedure create_cancelparcel( p_site_id_i	in  varchar2
				     , p_client_id_i  	in  varchar2
				     , p_order_id_i   	in  varchar2
				     , p_parcel_id    	in  varchar2
				     );
	--
	procedure update_container_data( p_container_id_i	in  varchar2
				       , p_container_type_i   	in  varchar2
				       , p_pallet_id_i        	in  varchar2
				       , p_pallet_type_i      	in  varchar2
				       , p_container_n_of_n_i 	in  number
				       , p_site_id_i          	in  varchar2
				       , p_client_id_i        	in  varchar2
				       , p_owner_id_i         	in  varchar2
				       , p_order_id_i         	in  varchar2
				       , p_customer_id_i      	in  varchar2
				       , p_carrier_id_i       	in  varchar2
				       , p_service_level_i    	in  varchar2
				       , p_wms_weight_i       	in  number
				       , p_wms_height_i       	in  number
				       , p_wms_width_i        	in  number
				       , p_wms_depth_i        	in  number
				       , p_wms_database_i     	in  varchar2
				       , p_cto_enabled_yn     	in  varchar2
				       , p_cto_pp_filename_i  	in  varchar2
				       , p_cto_pp_dstamp_i    	in  cnl_container_data.cto_pp_dstamp%type
				       , p_cto_carrier_i      	in  varchar2
				       , p_cto_service_i      	in  varchar2
				       );
	--  
end cnl_centiro_pck;