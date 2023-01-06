CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CTO_CLOSE_SHIPMENT_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $ 1  
* $Author: $ Martijn Swinkels
* $Date: $ 14-Feb-2022
**********************************************************************************
* Description: Base Centiro integration code
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--
--
-- Private variable declarations
--
	-- Name of package used for logging and tracing
	g_pck			varchar2(30)		:= 'cnl_cto_close_shipment_pck';
	-- Database environment
	g_database		varchar2(10)		:= cnl_cto_pck.fetch_database_f;
	-- http reponse code
	g_http_response_code	varchar(30);
	-- http response reason	
	g_http_response_reason	varchar2(2000);

-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 14-Feb-2022
-- Purpose : Close shipments
------------------------------------------------------------------------------------------------
procedure close_shipments( p_site_id_i		varchar2
			 , p_client_id_i	varchar2 default null
			 , p_carrier_id_i	varchar2
			 , p_service_level_i	varchar2 default null
			 )
is	
	cursor	c_shipments( b_run_id 		integer
			   )
	is
		select 	shipment_id
		from	cnl_cto_shipments_closed
		where	run_id = b_run_id
	;
	l_url			varchar2(1000)	:= cnl_util_pck.get_constant('CTO_CLOSE_SHIPMENT');
	l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_wallet		varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw		varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');

	l_run_id		integer	:= cnl_cto_close_shp_seq1.nextval;
	l_body_request		pljson		:= pljson();
	l_response_code		varchar2(30);
	l_response_reason	varchar2(4000);
	l_body_response		pljson		:= pljson();
	l_shipment_id		dcsdba.order_header.uploaded_ws2pc_id%type;
	l_shipment_ids		pljson_list	:= pljson_list();
	l_trace_key		integer;
	l_rtn			varchar2(30) 	:= 'close_shipments';
	l_cnt			integer	:= 0;
begin
	-- Flag all shipments to close
	update	cnl_cto_shipments_closed
	set 	run_id = l_run_id
	where	nvl(processed,'N') = 'N'
	and	run_id is null
	and	site_id = p_site_id_i
	and	(client_id = p_client_id_i or p_client_id_i is null)
	and	carrier_id = p_carrier_id_i
	and	(service_level = p_service_level_i or p_service_level_i is null)
	;
	commit;

	-- Create list with shipment id's
	for i in c_shipments(l_run_id)
	loop
		l_shipment_ids.append(to_char(l_shipment_id));
		l_cnt	:= l_cnt +1;
	end loop;

	if	l_cnt > 0
	then
		l_body_request.put('shipmentIdentifiers',	l_shipment_ids);

		-- add web service tace
		cnl_cto_pck.create_cto_trace_record( l_body_request, null, null, l_rtn, null, l_trace_key);

		-- Call web service
		l_response_code := cnl_cto_pck.call_webservice_f( p_url_i		=> l_url
								, p_proxy_i		=> l_proxy
								, p_user_name_i		=> null
								, p_password_i		=> null
								, p_wallet_i		=> l_wallet
								, p_wallet_password_i	=> l_wall_passw
								, p_post_get_del_p	=> 'POST'
								, p_json_body_i		=> l_body_request
								, p_json_body_o		=> l_body_response
								, p_response_reason_o	=> l_response_reason
								);

		g_http_response_code 		:= l_response_code;
		g_http_response_reason		:= l_response_reason;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Http response :' || g_http_response_code ||', '||g_http_response_reason
							 );
		-- add web service tace
		cnl_cto_pck.create_cto_trace_record( null, l_body_response, l_response_code, l_rtn, l_trace_key, l_trace_key);

		if l_response_code = '200'
		then
			update	cnl_cto_shipments_closed
			set 	processed = 'Y'
			where	run_id = l_run_id
			;
			commit;
		else
			update	cnl_cto_shipments_closed
			set 	run_id = null
			where	run_id = l_run_id
			;
			commit;
		end if;

	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No Shipments to close found.'
							 );
	end if;


exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR'
							 );

end close_shipments;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : initialization
------------------------------------------------------------------------------------------------
begin
	null;
end cnl_cto_close_shipment_pck;