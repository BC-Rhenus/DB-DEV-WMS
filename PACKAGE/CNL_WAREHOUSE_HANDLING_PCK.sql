CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WAREHOUSE_HANDLING_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functions and procedures related to warehouse handlilng application
**********************************************************************************
* $Log: $
**********************************************************************************/

	procedure collect_whh_pallets_p;
	--
	procedure set_qc_parameters_p( p_order_id_i		in dcsdba.order_header.order_id%type
				     , p_client_id_i		in dcsdba.order_header.client_id%type
				     , p_site_id_i		in dcsdba.order_header.from_site_id%type
				     , p_qc_req_yn_i		in cnl_sys.cnl_wms_qc_order.qc_req_yn%type -- QC is required 
				     , p_qc_batch_yn_i		in cnl_sys.cnl_wms_qc_order.qc_batch_yn%type -- QC batch id is required
				     , p_qc_qty_def_yn_i 	in cnl_sys.cnl_wms_qc_order.qc_qty_def_yn%type -- QTY is default 1 during QC
				     , p_qc_sku_select_yn_i	in cnl_sys.cnl_wms_qc_order.qc_sku_select_yn%type -- SKU can be selected from overview during QC
				     , p_qc_qty_upd_yn_i	in cnl_sys.cnl_wms_qc_order.qc_qty_upd_yn%type -- Default QTY can be changed
				     , p_qc_serial_yn_i		in cnl_sys.cnl_wms_qc_order.qc_serial_yn%type -- Serial check is required
				     );
	--
	procedure process_container_check_p( p_id_i		in number 
					   , p_container_id_i	in dcsdba.order_container.container_id%type
					   , p_pallet_id_i	in dcsdba.order_container.pallet_id%type default null
					   , p_client_id_i	in dcsdba.order_container.client_id%type
					   , p_site_id_i	in dcsdba.inventory.site_id%type
					   , p_order_id_i	in dcsdba.order_container.order_id%type
					   , p_location_id_i	in dcsdba.inventory.location_id%type default null
					   , p_station_id_i	in dcsdba.workstation.station_id%type
					   , p_user_id_i	in dcsdba.inventory_transaction.user_id%type
					   , p_notes_i		in dcsdba.inventory_transaction.user_def_note_1%type -- Used for reason description secondary QC
					   , p_extra_notes_i	in dcsdba.inventory_transaction.extra_notes%type -- Does not work with standard JDA API
					   , p_elapsed_time_i	in dcsdba.inventory_transaction.elapsed_time%type default null
					   , p_check_type_i	in varchar2 -- 'CHKI','CHKS'
					   , p_check_ok_yn_i	in varchar2 -- 'Y','N'
					   , p_release_cont_i	in varchar2 -- Y or N to release the container in the warehouse for further processing.
					   , p_reset_times_i	in integer
					   , p_qc_serial_yn_i	in varchar2 -- serial check must be processed differently.
					   , p_success_yn_o	out varchar2
					   );
	--	
	procedure release_marshal_task_p( p_container_id_i	in	dcsdba.move_task.container_id%type default null
					, p_pallet_id_i		in 	dcsdba.move_task.pallet_id%type default null
					, p_site_id_i		in 	dcsdba.move_task.site_id%type
					, p_station_id_i	in 	dcsdba.move_task.station_id%type
					, p_user_id_i		in 	dcsdba.move_task.user_id%type
					, p_force_release	in 	integer -- 0 = Not enforce a release, 1 = enforce release
					, P_ok_yn_o		out	integer -- 0 = not ok, 1 = ok
					, p_comment_o		out	varchar2 --varchar2(200)
					);
	--
	function get_sku_id_f( p_client_id_i		in dcsdba.sku.client_id%type
			     , p_id_i			in varchar2
			     , p_qty_o			out number
			     )
	return	varchar2;
	--
	procedure fetch_client_vas_activity( p_pallet_i		in varchar2
					   , p_site_id_i	in varchar2
					   );
	--
end cnl_warehouse_handling_pck;