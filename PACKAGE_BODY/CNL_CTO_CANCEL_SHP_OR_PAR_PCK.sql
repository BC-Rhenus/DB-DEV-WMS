CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CTO_CANCEL_SHP_OR_PAR_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $ 1  
* $Author: $ Martijn Swinkels
* $Date: $ 18/05/2021
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
	g_pck			varchar2(30)		:= 'cnl_cto_cancel_shp_or_par_pck';
	-- Database environment
	g_database		varchar2(10)		:= cnl_cto_pck.fetch_database_f;

-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 18/05/2021
-- Purpose : Cancel the shipment in Centiro
------------------------------------------------------------------------------------------------
procedure cancel_shipment_p( p_shipment_id_i	in  varchar2
			   , p_result_o		out varchar2
			   )
is
	l_rtn			varchar2(30) 	:= 'cancel_shipment_p';
	l_url			varchar2(1000)	:= cnl_util_pck.get_constant('CTO_CANCEL_SHIPMENT_WEBSERVICE_URL');
	l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_wallet		varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw		varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');
	l_body_request		pljson		:= pljson();
	l_response_code		varchar2(30);
	l_response_reason	varchar2(4000);
	l_body_response		pljson		:= pljson();
	l_trace_key		integer;
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start cancelling shipment '
						 || p_shipment_id_i
						 ||'.'
						 );
	-- Build json root
	l_body_request.put('shipmentIdentifier', 	p_shipment_id_i);
	l_body_request.put('shipmentType', 		'outbound');

	-- add web service trace
	cnl_cto_pck.create_cto_trace_record( l_body_request, null, null, l_rtn, null, l_trace_key);

	-- Call web service
	l_response_code := cnl_cto_pck.call_webservice_f( p_url_i		=> l_url
							, p_proxy_i		=> l_proxy
							, p_user_name_i		=> null
							, p_password_i		=> null
							, p_wallet_i		=> l_wallet
							, p_wallet_password_i	=> l_wall_passw
							, p_post_get_del_p	=> 'DELETE'
							, p_json_body_i		=> l_body_request
							, p_json_body_o		=> l_body_response
							, p_response_reason_o	=> l_response_reason
							);

	-- add web service tace
	cnl_cto_pck.create_cto_trace_record( null, l_body_response, l_response_code, l_rtn, l_trace_key, l_trace_key);

	if	l_response_code = '200'
	then	
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Shipment '
							 || p_shipment_id_i
							 || ' successfully Cancelled.'
							 );
		update	cnl_cto_ship_labels
		set	status		= 'Cancelled'
		,	update_dstamp 	= sysdate
		where	shipment_id 	= p_shipment_id_i
		;
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Shipment '
							 || p_shipment_id_i
							 || ' not cancelled.'
							 );		
	end if;	

	p_result_o := l_response_code;

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

end cancel_shipment_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 18/05/2021
-- Purpose : Cancel parcel in Centiro
------------------------------------------------------------------------------------------------
procedure cancel_parcel_p( p_client_id_i	in dcsdba.client.client_id%type
			 , p_site_id_i		in dcsdba.site.site_id%type
			 , p_shipment_id_i	in varchar2
			 , p_parcel_id_i	in dcsdba.order_container.container_id%type
			 , p_canc_shp_if_last_i	in varchar2
			 )
is
	l_rtn	varchar2(30) := 'cancel_parcel_p';
	l_url			varchar2(1000)	:= cnl_util_pck.get_constant('CTO_CANCEL_PARCEL_WEBSERVICE_URL');
	l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_wallet		varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw		varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');
	l_body_request		pljson		:= pljson();
	l_response_code		varchar2(30);
	l_response_reason	varchar2(4000);
	l_body_response		pljson		:= pljson();
	l_trace_key		integer;
	l_shipment		pljson		:= pljson();
	l_parcel		pljson		:= pljson();
	l_parcels		pljson_list	:= pljson_list();
	l_database		varchar2(30);
	l_site			dcsdba.site.site_id%type;
begin
	select 	name
	into	l_database
	from	v$database
	;

	if	p_canc_shp_if_last_i = 'Y'
	then
		l_body_request.put('cancelShipmentIfLastParcel',	true);
	else
		l_body_request.put('cancelShipmentIfLastParcel',	false);
	end if;

	-- Sender is client.site@rhenus.com
	-- MASIM IS USED AS TEST SENDER in TEST and DEV is all test user
	if	l_database	= 'DEVCNLJW'
	then
		l_shipment.put('senderCode',			'testsender@rhenus.com');
	else
		-- Translate site id GBMIK01 to GBASF02 as legacy issue
		if 	p_site_id_i = 'GBMIK01'
		then
			l_site := 'GBASF02';
		else
			l_site := p_site_id_i;
		end if;
		l_shipment.put('senderCode',			p_client_id_i||'.'||l_site||'@rhenus.com');
	end if;

	l_shipment.put('shipmentType', 				'outbound');
	l_shipment.put('externalShipmentIdentifier',		p_shipment_id_i);

	l_parcel.put('externalParcelIdentifier',		p_parcel_id_i);
	l_parcels.append(l_parcel);

	l_shipment.put('parcels',				l_parcels);
	l_body_request.put('shipment',				l_shipment);

	-- add web service trace
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

	-- add web service tace
	cnl_cto_pck.create_cto_trace_record( null, l_body_response, l_response_code, l_rtn, l_trace_key, l_trace_key);

	if	l_response_code = '200'
	then	
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Parcel '
							 || p_parcel_id_i
							 || ' from shipment '
							 || p_shipment_id_i
							 || ' successfully Deleted.'
							 );
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Parcel '
							 || p_parcel_id_i
							 || ' from shipment '
							 || p_shipment_id_i
							 || ' not deleted.'
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

end cancel_parcel_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : initialization
------------------------------------------------------------------------------------------------
begin
	null;
end cnl_cto_cancel_shp_or_par_pck;