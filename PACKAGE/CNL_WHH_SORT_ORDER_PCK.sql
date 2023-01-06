CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WHH_SORT_ORDER_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functions and procedures related to warehouse handling order sorting
**********************************************************************************
* $Log: $
**********************************************************************************/
	-- Fetch a location to sort the container to.
	function whh_get_location_f( p_container_id_i  	in  dcsdba.move_task.container_id%type 
				   , p_workstation_i	in  dcsdba.workstation.station_id%type
				   , p_message_o       	out varchar2
				   , p_last_cont_o     	out varchar2
				   , p_order_id_o	out dcsdba.order_header.order_id%type
				   , p_client_id_o	out dcsdba.order_header.client_id%type
				   )
		return        dcsdba.location.location_id%type;

	-- Validate and Confirm sortation is finished
	procedure confirm_sortation_p( p_location_id_i  in  dcsdba.location.location_id%type
				     , p_order_id_i     in  dcsdba.order_header.order_id%type
				     , p_container_id_i in  dcsdba.move_task.container_id%type default null
				     , p_last_cont_i    in  varchar2
				     , p_client_id_i    in  dcsdba.order_header.client_id%type default null
				     , p_loc_chkstrng_i in  dcsdba.location.check_string%type
				     , p_site_id_i      in  dcsdba.order_header.from_site_id%type
				     , p_ok_yn_o        out varchar2
				     , p_message_o      out varchar2
				     );

	-- Sortation location is cleared up and order has been removed.
	procedure delete_sort_order_p( p_location_id_i	in dcsdba.location.location_id%type
				     , p_order_id_i     in  dcsdba.order_header.order_id%type
				     );
	--	
end cnl_whh_sort_order_pck;