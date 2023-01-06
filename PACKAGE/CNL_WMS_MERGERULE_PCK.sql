CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_MERGERULE_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functions and procedures called using merge rules in WMS
**********************************************************************************
* $Log: $
**********************************************************************************/
  function new_cons_id ( p_string in varchar2)
    return varchar2;
  --
  function est_nbr_pal_f ( p_order_id  in varchar2
                         , p_client_id in varchar2
                         ) 
    return number; 
  --
  function est_nbr_box_f ( p_order_id  in varchar2
                         , p_client_id in varchar2
                         ) 
    return number;
  --
  function call_shipbydate ( p_client_id       in varchar2
                           , p_creation_date   in timestamp with local Time zone
                           , p_code            in varchar2
                           , p_order_id        in varchar2
                           , p_ship_by_date    in timestamp with local Time zone default null
                           , p_carrier_id      in varchar2 default null
                           , p_service_level   in varchar2 default null
                           , p_calendar_id     in varchar2 default null
                           )
    return timestamp with local Time zone;
  --
    function add_vas_activity ( p_container_id_i          in varchar2 default null
                              , p_client_id_i             in varchar2 
                              , p_order_id_i              in varchar2 
                              , p_sku_id_i                in varchar2 default null
                              , p_activity_name_i         in varchar2
                              , p_activity_sequence_i     in number   default null
                              , p_activity_instruction_i  in varchar2 default null
                              )
        return integer;
  --
	function call_cust_ship_by_date_f(	p_creation_date_i 	timestamp with local time zone
					 ,	p_client_id_i 		varchar2
				         , 	p_customer_id_i 	varchar2
				         )
		return timestamp with local time zone ;
  --
	function get_work_day_f( p_date_i 	in timestamp with local Time zone
			       , p_days_i	in number
			       , p_plusminus_i	in varchar2 -- - or +
			       , p_calendar_id  in varchar2 default null
			       )
		return timestamp with local Time zone;
  --
	function set_qc_parameters_f ( p_order_id_i		in dcsdba.order_header.order_id%type
				     , p_client_id_i		in dcsdba.order_header.client_id%type
				     , p_site_id_i		in dcsdba.order_header.from_site_id%type
				     , p_qc_req_yn_i		in cnl_sys.cnl_wms_qc_order.qc_req_yn%type -- QC is required 
				     , p_qc_batch_yn_i		in cnl_sys.cnl_wms_qc_order.qc_batch_yn%type -- QC batch id is required
				     , p_qc_qty_def_yn_i 	in cnl_sys.cnl_wms_qc_order.qc_qty_def_yn%type -- QTY is default 1 during QC
				     , p_qc_sku_select_yn_i	in cnl_sys.cnl_wms_qc_order.qc_sku_select_yn%type -- SKU can be selected from overview during QC
				     , p_qc_qty_upd_yn_i	in cnl_sys.cnl_wms_qc_order.qc_qty_upd_yn%type -- Default QTY can be changed
				     , p_qc_serial_yn_i		in cnl_sys.cnl_wms_qc_order.qc_serial_yn%type default null -- If serial checking is required yn
				     )
		return integer;
  --
	function sku_default_pallet_type_f( p_client_id_i	in dcsdba.client.client_id%type
					  , p_sku_id_i		in dcsdba.sku.sku_id%type
					  )
	return dcsdba.pallet_config.config_id%type;
  --
	function update_order_header_extend_f( p_order_id_i	in cnl_sys.cnl_wms_order_header_extend.order_id%type
					     , p_client_id_i	in cnl_sys.cnl_wms_order_header_extend.client_id%type
					     , p_string_i	in varchar2
					     )
	return integer;
  --
	function update_order_line_extend_f( p_order_id_i	in cnl_sys.cnl_wms_order_line_extend.order_id%type
					   , p_client_id_i	in cnl_sys.cnl_wms_order_line_extend.client_id%type
					   , p_line_id_i	in cnl_sys.cnl_wms_order_line_extend.line_id%type
					   , p_string_i		in varchar2
					   )
	return integer;
 --
	function is_number (p_string in varchar2)
		return int;
 -- CR 	   : BDS-5460
	function insert_special_ins_f( p_code_i		in  dcsdba.special_ins.code%type
				     , p_client_id_i	in  dcsdba.special_ins.client_id%type
				     , p_reference_id_i	in  dcsdba.special_ins.reference_id%type
				     , p_line_id_i	in  dcsdba.special_ins.line_id%type default null
				     , p_type_i		in  dcsdba.special_ins.type%type
				     , p_text_i		in  dcsdba.special_ins.text%type default null
				     )
		return integer;
 -- CR 	:BDS-5469
	function update_serial_receipt_id_f( p_pre_advice_id_i	dcsdba.pre_advice_header.pre_advice_id%type
					   , p_client_id_i	dcsdba.client.client_id%type
					   )
		return integer;

end cnl_wms_mergerule_pck;