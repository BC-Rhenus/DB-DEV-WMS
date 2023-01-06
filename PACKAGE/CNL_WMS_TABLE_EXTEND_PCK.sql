CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_TABLE_EXTEND_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $ Martijn Swinkels
* $Date: $ 15-Feb-2021
**********************************************************************************
* Description: package used to operate on table set as an extention on existing WMS tables
**********************************************************************************
* $Log: $
**********************************************************************************
* 
**********************************************************************************/
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
procedure update_extend_table_job_f( p_table_name_i	in varchar2 
			           , p_table_pk_i	in varchar2	
			           , p_string_i		in varchar2
			           );
procedure reset_header_p( p_order_id_i	in cnl_sys.cnl_wms_order_header_extend.order_id%type
			, p_client_id_i in cnl_sys.cnl_wms_order_header_extend.client_id%type
			);
--
end cnl_wms_table_extend_pck;