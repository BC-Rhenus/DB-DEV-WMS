CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_CTO_ADDSHIPMENT_PCK" 
is 

-- Add shipment 
procedure fetch_orders_addship_p( p_site_id_i 	dcsdba.site.site_id%type
				, p_cs_force_i	varchar2 default 'N'
				);

-- Force carrier update
procedure force_carrier_update_p( p_site_id_i 		dcsdba.site.site_id%type
			        , p_client_id_i		dcsdba.client.client_id%type
			        , p_printer_i		varchar2
			        , p_order_id_i		dcsdba.order_header.order_id%type		default null
			        , p_shipment_id_i	dcsdba.order_header.uploaded_ws2pc_id%type	default null
			        , p_rtk_key_i		dcsdba.run_task.key%type
			        );
function parcels_exist_f( p_shipment_id_i	dcsdba.order_header.uploaded_ws2pc_id%type
			, p_site_id_i		dcsdba.site.site_id%type
			, p_client_id_i		dcsdba.client.client_id%type
			)
	return integer;
procedure Shipment_closed_p( p_shipment_id_i 	in dcsdba.order_header.uploaded_ws2pc_id%type
			   , p_order_id_i	in dcsdba.order_header.order_id%type
			   , p_client_id_i	in dcsdba.order_header.client_id%type
			   , p_dif_carrier_o	out varchar2
			   , p_carrier_id_o	out varchar2
			   , p_service_level_o	out varchar2
			   , p_new_shp_id_o	out number
			   , p_ok_o		out varchar2
			   );
end cnl_cto_addshipment_pck;