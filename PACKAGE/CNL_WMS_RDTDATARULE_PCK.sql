CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_RDTDATARULE_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: WMS functionality used by RDT data rule
**********************************************************************************
* $Log: $
**********************************************************************************/
	function box_count( p_pallet_id_i in varchar2
			  , p_box_count_i in varchar2
			  )
	return varchar2;
	--
  	function f_pal_or_con( p_id 		in varchar2
			     , p_site_id 	in varchar2
			     )
	return varchar2;
	--
	function f_disallow_short_ship( p_pallet_id in varchar2)
	return integer;
	--
	function f_all_on_shipdock_instage( p_pallet_id in varchar2)
	return integer;
	--
	function f_parcel_pallet_yn( p_pallet_id in varchar2)
	return integer;
	--
	function f_check_serial_on_pal( p_pallet_id 	in varchar2
				      , p_site_id 	in varchar2
				      )
	return integer;
	--
	function f_chk_container_location( p_pallet_id 	in varchar2
					 , p_site_id 	in varchar2
					 )
	return integer;
	--
	function add_restrict_pick_f( p_station_id_i	varchar2
				    , p_site_id_i 	varchar2
				    , p_list_id_i 	varchar2
				    )
	return integer;
	--
	function over_pick_f( p_task_id_i	in dcsdba.order_header.order_id%type
			    , p_sku_id_i	in dcsdba.sku.sku_id%type
			    , p_site_id_i	in dcsdba.site.site_id%type
			    , p_station_id_i	in dcsdba.workstation.station_id%type
			    , p_user_id_i	in dcsdba.application_user.user_id%type
			    )
	return integer;
	--
end cnl_wms_rdtdatarule_pck;