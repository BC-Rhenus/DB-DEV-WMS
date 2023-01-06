CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_CTO_PARCEL_PCK" 
is
-- fetch parcels
procedure add_print_parcels_p( p_site_id_i	in dcsdba.site.site_id%type
			     , p_client_id_i    in dcsdba.client.client_id%type
			     , p_order_id_i	in dcsdba.order_header.order_id%type 		default null
			     , p_pallet_id_i	in dcsdba.order_container.pallet_id%type	default null
			     , p_container_id_i	in dcsdba.order_container.container_id%type	default null
			     , p_shipment_id_i	in dcsdba.order_header.uploaded_ws2pc_id%type	default null
			     , p_rtk_key_i	in dcsdba.run_task.key%type	
			     , p_printer_i      in varchar2
			     , p_copies_i       in varchar2 
			     , p_dws_i		in varchar2
			     , p_station_id_i	in dcsdba.workstation.station_id%type 		default null
			     , p_shp_closed_o	out varchar2
			     );
-- Fetch label file to print			     
function get_label_file_f( p_shipment_id_i	in varchar2
			 , p_parcel_id_i	in varchar2
			 , p_run_task_key_i	in integer
			 )
	return clob;
-- Decode label file
function base64decode( p_clob 	clob
     		     , p_encode	varchar2 default 'N'
		     )
	return clob;

end cnl_cto_parcel_pck;