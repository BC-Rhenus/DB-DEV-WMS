CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_CTO_CANCEL_SHP_OR_PAR_PCK" 
is
-- Cancel shipment
procedure cancel_shipment_p( p_shipment_id_i	in  varchar2
			   , p_result_o		out varchar2
			   );

-- Cancel parcel
procedure cancel_parcel_p( p_client_id_i	dcsdba.client.client_id%type
			 , p_site_id_i		dcsdba.site.site_id%type
			 , p_shipment_id_i	varchar2
			 , p_parcel_id_i	dcsdba.order_container.container_id%type
			 , p_canc_shp_if_last_i	in varchar2
			 );

end cnl_cto_cancel_shp_or_par_pck;