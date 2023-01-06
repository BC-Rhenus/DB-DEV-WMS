CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_SPR_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with Sapper (Output Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations

	type g_ref_cur is ref cursor;
--	
--
-- Private constant declarations
--
	g_pck           varchar2(30) := 'cnl_spr_pck';
	g_logging       boolean := true;
	g_tracing       boolean := true;
	g_authenticate  cnl_sys.cnl_spr_authenticate_key.authenticate_key%type;
	g_database      varchar2(10) := fetch_database_f;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 10-Okt-2019
-- Purpose : Insert inventory transcation.
------------------------------------------------------------------------------------------------
procedure create_itl_p( p_status		in out 	integer
		      , p_code 			in 	dcsdba.inventory_transaction.code%type
		      , p_updateqty		in 	dcsdba.inventory_transaction.update_qty%type
		      , p_originalqty 		in 	dcsdba.inventory_transaction.original_qty%type default null
		      , p_clientid 		in 	dcsdba.inventory_transaction.client_id%type
		      , p_skuid 		in 	dcsdba.inventory_transaction.sku_id%type default null
		      , p_tagid 		in 	dcsdba.inventory_transaction.tag_id%type
		      , p_batchid 		in 	dcsdba.inventory_transaction.batch_id%type default null
		      , p_conditionid 		in 	dcsdba.inventory_transaction.condition_id%type default null
		      , p_tolocation 		in 	dcsdba.inventory_transaction.to_loc_id%type
		      , p_fromlocation 		in 	dcsdba.inventory_transaction.from_loc_id%type default null
		      , p_finallocation 	in 	dcsdba.inventory_transaction.final_loc_id%type default null
		      , p_referenceid 		in 	dcsdba.inventory_transaction.reference_id%type default null
		      , p_lineid 		in 	dcsdba.inventory_transaction.line_id%type default null
		      , p_reasonid 		in 	dcsdba.inventory_transaction.reason_id%type default null
		      , p_stationid 		in 	dcsdba.inventory_transaction.station_id%type default null
		      , p_userid 		in 	dcsdba.inventory_transaction.user_id%type default null
		      , p_tmpnotes 		in 	dcsdba.inventory_transaction.notes%type default null
		      , p_elapsedtime 		in 	dcsdba.inventory_transaction.elapsed_time%type default null
		      , p_sessiontype 		in 	dcsdba.inventory_transaction.session_type%type default 'm'
		      , p_summaryrecord 	in 	dcsdba.inventory_transaction.summary_record%type default 'y'
		      , p_siteid 		in 	dcsdba.inventory_transaction.site_id%type default null
		      , p_fromsiteid 		in 	dcsdba.inventory_transaction.from_site_id%type default null
		      , p_tositeid 		in 	dcsdba.inventory_transaction.to_site_id%type default null
		      , p_containerid 		in 	dcsdba.inventory_transaction.container_id%type default null
		      , p_palletid 		in 	dcsdba.inventory_transaction.pallet_id%type default null
		      , p_listid 		in 	dcsdba.inventory_transaction.list_id%type default null
		      , p_expirydstamp 		in 	dcsdba.inventory_transaction.expiry_dstamp%type default null
		      , p_ownerid 		in 	dcsdba.inventory_transaction.owner_id%type default null
		      , p_originid 		in 	dcsdba.inventory_transaction.origin_id%type default null
		      , p_workgroup 		in 	dcsdba.inventory_transaction.work_group%type default null
		      , p_consignment 		in 	dcsdba.inventory_transaction.consignment%type default null
		      , p_manufdstamp 		in 	dcsdba.inventory_transaction.manuf_dstamp%type default null
		      , p_taskcategory 		in 	dcsdba.inventory_transaction.task_category%type default null
		      , p_lockstatus 		in 	dcsdba.inventory_transaction.lock_status%type default null
		      , p_qcstatus 		in 	dcsdba.inventory_transaction.qc_status%type default null
		      , p_supplierid 		in 	dcsdba.inventory_transaction.supplier_id%type default null
		      , p_samplingtype 		in 	dcsdba.inventory_transaction.sampling_type%type default null
		      , p_userdeftype1 		in 	dcsdba.inventory_transaction.user_def_type_1%type default null
		      , p_userdeftype2 		in 	dcsdba.inventory_transaction.user_def_type_2%type default null
		      , p_userdeftype3 		in 	dcsdba.inventory_transaction.user_def_type_3%type default null
		      , p_userdeftype4 		in 	dcsdba.inventory_transaction.user_def_type_4%type default null
		      , p_userdeftype5 		in 	dcsdba.inventory_transaction.user_def_type_5%type default null
		      , p_userdeftype6 		in 	dcsdba.inventory_transaction.user_def_type_6%type default null
		      , p_userdeftype7 		in 	dcsdba.inventory_transaction.user_def_type_7%type default null
		      , p_userdeftype8 		in 	dcsdba.inventory_transaction.user_def_type_8%type default null
		      , p_userdefchk1 		in 	dcsdba.inventory_transaction.user_def_chk_1%type default null
		      , p_userdefchk2 		in 	dcsdba.inventory_transaction.user_def_chk_2%type default null
		      , p_userdefchk3 		in 	dcsdba.inventory_transaction.user_def_chk_3%type default null
		      , p_userdefchk4 		in 	dcsdba.inventory_transaction.user_def_chk_4%type default null
		      , p_userdefdate1 		in 	dcsdba.inventory_transaction.user_def_date_1%type default null
		      , p_userdefdate2 		in 	dcsdba.inventory_transaction.user_def_date_2%type default null
		      , p_userdefdate3 		in 	dcsdba.inventory_transaction.user_def_date_3%type default null
		      , p_userdefdate4 		in 	dcsdba.inventory_transaction.user_def_date_4%type default null
		      , p_userdefnum1 		in 	dcsdba.inventory_transaction.user_def_num_1%type default null
		      , p_userdefnum2		in 	dcsdba.inventory_transaction.user_def_num_2%type default null
		      , p_userdefnum3 		in 	dcsdba.inventory_transaction.user_def_num_3%type default null
		      , p_userdefnum4 		in 	dcsdba.inventory_transaction.user_def_num_4%type default null
		      , p_userdefnote1 		in 	dcsdba.inventory_transaction.user_def_note_1%type default null
		      , p_userdefnote2 		in 	dcsdba.inventory_transaction.user_def_note_2%type default null
		      , p_jobid 		in 	dcsdba.inventory_transaction.job_id%type default null
		      , p_jobunit 		in 	dcsdba.inventory_transaction.job_unit%type default null
		      , p_tmpmanning 		in 	dcsdba.inventory_transaction.manning%type default null
		      , p_speccode 		in 	dcsdba.inventory_transaction.spec_code%type default null
		      , p_estimatedtime 	in 	dcsdba.inventory_transaction.estimated_time%type default null
		      , p_completedstamp 	in 	dcsdba.inventory_transaction.complete_dstamp%type default null
		      , p_configid 		in 	dcsdba.inventory_transaction.config_id%type default null
		      , p_ceorigrotationid 	in 	dcsdba.inventory_transaction.ce_orig_rotation_id%type default null
		      , p_cerotationid 		in 	dcsdba.inventory_transaction.ce_rotation_id%type default null
		      , p_ceconsignid 		in 	dcsdba.inventory_transaction.ce_consignment_id%type default null
		      , p_cereceipttype 	in 	dcsdba.inventory_transaction.ce_receipt_type%type default null
		      , p_ceorigin 		in 	dcsdba.inventory_transaction.ce_originator%type default null
		      , p_ceoriginref 		in 	dcsdba.inventory_transaction.ce_originator_reference%type default null
		      , p_cecoo 		in 	dcsdba.inventory_transaction.ce_coo%type default null
		      , p_cecwc 		in 	dcsdba.inventory_transaction.ce_cwc%type default null
		      , p_ceucr 		in 	dcsdba.inventory_transaction.ce_ucr%type default null
		      , p_ceunderbond 		in 	dcsdba.inventory_transaction.ce_under_bond%type default null
		      , p_cedocdstamp 		in 	dcsdba.inventory_transaction.ce_document_dstamp%type default null
		      , p_uploadedcustoms 	in 	dcsdba.inventory_transaction.uploaded_customs%type default 'y'
		      , p_lockcode 		in 	dcsdba.inventory_transaction.lock_code%type default null
		      , p_printlabel 		in 	dcsdba.inventory_transaction.print_label_id%type default null
		      , p_asnid 		in 	dcsdba.inventory_transaction.asn_id%type default null
		      , p_customerid 		in 	dcsdba.inventory_transaction.customer_id%type default null
		      , p_cedutystamp 		in 	dcsdba.inventory_transaction.ce_duty_stamp%type default null
		      , p_palletgrouped 	in 	dcsdba.inventory_transaction.pallet_grouped%type default null
		      , p_consollink 		in 	dcsdba.inventory_transaction.consol_link%type default null
		      , p_jobsiteid 		in 	dcsdba.inventory_transaction.job_site_id%type default null
		      , p_jobclientid 		in 	dcsdba.inventory_transaction.job_client_id%type default null
		      , p_extranotes 		in 	dcsdba.inventory_transaction.extra_notes%type default null
		      , p_stagerouteid 		in 	dcsdba.inventory_transaction.stage_route_id%type default null
		      , p_stagerouteseq 	in 	dcsdba.inventory_transaction.stage_route_sequence%type default null
		      , p_pfconsollink 		in 	dcsdba.inventory_transaction.pf_consol_link%type default null
		      , p_ceavailstatus 	in 	dcsdba.inventory_transaction.ce_avail_status%type default null
		      , p_masterpahid 		in 	dcsdba.inventory_transaction.master_pah_id%type default null
		      , p_masterpalid 		in 	dcsdba.inventory_transaction.master_pal_id%type default null
		      , p_uploaded 		in 	dcsdba.inventory_transaction.uploaded%type default 'n'
		      , p_custshpmntno 		in 	dcsdba.inventory_transaction.customer_shipment_number%type default null
		      , p_shipmentno 		in 	dcsdba.inventory_transaction.shipment_number%type default null
		      , p_fromstatus 		in 	dcsdba.inventory_transaction.from_status%type default null
		      , p_tostatus 		in 	dcsdba.inventory_transaction.to_status%type default null
		      , p_palletconfig 		in 	dcsdba.inventory_transaction.pallet_config%type default null
		      , p_masterorderid 	in 	dcsdba.inventory_transaction.master_order_id%type default null
		      , p_masterorderlineid 	in 	dcsdba.inventory_transaction.master_order_line_id%type default null
		      , p_kitplanid 		in 	dcsdba.inventory_transaction.kit_plan_id%type default null
		      , p_plansequence 		in 	dcsdba.inventory_transaction.plan_sequence%type default null
		      , p_cecollicountexpected 	in 	dcsdba.inventory_transaction.ce_colli_count_expected%type default null
		      , p_cecollicount 		in 	dcsdba.inventory_transaction.ce_colli_count%type default null
		      , p_cesealsok 		in 	dcsdba.inventory_transaction.ce_seals_ok%type default null
		      , p_ceinvoicenumber 	in 	dcsdba.inventory_transaction.ce_invoice_number%type default null
		      , p_olduserdeftype1 	in 	dcsdba.inventory_transaction.old_user_def_type_1%type default null
		      , p_olduserdeftype2 	in 	dcsdba.inventory_transaction.old_user_def_type_2%type default null
		      , p_olduserdeftype3 	in 	dcsdba.inventory_transaction.old_user_def_type_3%type default null
		      , p_olduserdeftype4 	in 	dcsdba.inventory_transaction.old_user_def_type_4%type default null
		      , p_olduserdeftype5 	in 	dcsdba.inventory_transaction.old_user_def_type_5%type default null
		      , p_olduserdeftype6 	in 	dcsdba.inventory_transaction.old_user_def_type_6%type default null
		      , p_olduserdeftype7 	in 	dcsdba.inventory_transaction.old_user_def_type_7%type default null
		      , p_olduserdeftype8 	in 	dcsdba.inventory_transaction.old_user_def_type_8%type default null
		      , p_olduserdefchk1 	in 	dcsdba.inventory_transaction.old_user_def_chk_1%type default null
		      , p_olduserdefchk2 	in 	dcsdba.inventory_transaction.old_user_def_chk_2%type default null
		      , p_olduserdefchk3 	in 	dcsdba.inventory_transaction.old_user_def_chk_3%type default null
		      , p_olduserdefchk4 	in 	dcsdba.inventory_transaction.old_user_def_chk_4%type default null
		      , p_olduserdefdate1 	in 	dcsdba.inventory_transaction.old_user_def_date_1%type default null
		      , p_olduserdefdate2 	in 	dcsdba.inventory_transaction.old_user_def_date_2%type default null
		      , p_olduserdefdate3 	in 	dcsdba.inventory_transaction.old_user_def_date_3%type default null
		      , p_olduserdefdate4 	in 	dcsdba.inventory_transaction.old_user_def_date_4%type default null
		      , p_olduserdefnum1 	in 	dcsdba.inventory_transaction.old_user_def_num_1%type default null
		      , p_olduserdefnum2 	in 	dcsdba.inventory_transaction.old_user_def_num_2%type default null
		      , p_olduserdefnum3 	in 	dcsdba.inventory_transaction.old_user_def_num_3%type default null
		      , p_olduserdefnum4 	in 	dcsdba.inventory_transaction.old_user_def_num_4%type default null
		      , p_olduserdefnote1 	in 	dcsdba.inventory_transaction.old_user_def_note_1%type default null
		      , p_olduserdefnote2 	in 	dcsdba.inventory_transaction.old_user_def_note_2%type default null
		      , p_laborassignment 	in 	dcsdba.inventory_transaction.labor_assignment%type default null
		      , p_laborgridsequence 	in 	dcsdba.inventory_transaction.labor_grid_sequence%type default null
		      , p_kitceconsignid 	in 	dcsdba.inventory_transaction.kit_ce_consignment_id%type default null
		      )
is
	l_rtn		varchar2(30) := 'create_itl_p';
	l_key 		integer;
	l_status	integer;
	pragma 	autonomous_transaction;
begin
		
	dcsdba.libinvtrans.createinvtransproc( status			=> l_status
					     , transcode        	=> p_code
					     , updateqty        	=> p_updateqty
					     , originalqty      	=> p_originalqty
					     , clientid         	=> p_clientid
					     , skuid            	=> p_skuid
					     , tagid            	=> p_tagid
					     , batchid          	=> p_batchid
					     , conditionid      	=> p_conditionid
					     , tolocation       	=> p_tolocation
					     , fromlocation     	=> p_fromlocation
					     , finallocation    	=> p_finallocation
					     , referenceid      	=> p_referenceid
					     , lineid           	=> p_lineid
					     , reasonid         	=> p_reasonid
					     , stationid        	=> p_stationid
					     , userid           	=> p_userid
					     , tmpnotes         	=> p_tmpnotes
					     , elapsedtime      	=> p_elapsedtime
					     , sessiontype      	=> p_sessiontype
					     , summaryrecord    	=> p_summaryrecord
					     , siteid           	=> p_siteid
					     , fromsiteid       	=> p_fromsiteid
					     , tositeid         	=> p_tositeid
					     , containerid      	=> p_containerid
					     , palletid         	=> p_palletid
					     , listid           	=> p_listid
					     , expirydstamp     	=> p_expirydstamp
					     , ownerid          	=> p_ownerid
					     , originid         	=> p_originid
					     , workgroup        	=> p_workgroup
					     , consignment      	=> p_consignment
					     , manufdstamp      	=> p_manufdstamp
					     , taskcategory     	=> p_taskcategory
					     , lockstatus       	=> p_lockstatus
					     , qcstatus         	=> p_qcstatus
					     , supplierid       	=> p_supplierid
					     , samplingtype     	=> p_samplingtype
					     , userdeftype1     	=> p_userdeftype1
					     , userdeftype2     	=> p_userdeftype2
					     , userdeftype3     	=> p_userdeftype3
					     , userdeftype4     	=> p_userdeftype4
					     , userdeftype5     	=> p_userdeftype5
					     , userdeftype6     	=> p_userdeftype6
					     , userdeftype7     	=> p_userdeftype7
					     , userdeftype8     	=> p_userdeftype8
					     , userdefchk1     		=> p_userdefchk1
					     , userdefchk2     		=> p_userdefchk2
					     , userdefchk3     		=> p_userdefchk3
					     , userdefchk4     		=> p_userdefchk4
					     , userdefdate1     	=> p_userdefdate1
					     , userdefdate2     	=> p_userdefdate2
					     , userdefdate3     	=> p_userdefdate3
					     , userdefdate4     	=> p_userdefdate4
					     , userdefnum1     		=> p_userdefnum1
					     , userdefnum2     		=> p_userdefnum2
					     , userdefnum3     		=> p_userdefnum3
					     , userdefnum4     		=> p_userdefnum4
					     , userdefnote1     	=> p_userdefnote1
					     , userdefnote2     	=> p_userdefnote2
					     , jobid    		=> p_jobid
					     , jobunit     		=> p_jobunit
					     , tmpmanning     		=> p_tmpmanning
					     , speccode     		=> p_speccode
					     , estimatedtime    	=> p_estimatedtime
					     , completedstamp   	=> p_completedstamp
					     , configid     		=> p_configid
					     , ceorigrotationid 	=> p_ceorigrotationid
					     , cerotationid     	=> p_cerotationid
					     , ceconsignid     		=> p_ceconsignid
					     , cereceipttype    	=> p_cereceipttype
					     , ceorigin     		=> p_ceorigin
					     , ceoriginref     		=> p_ceoriginref
					     , cecoo     		=> p_cecoo
					     , cecwc     		=> p_cecwc
					     , ceucr     		=> p_ceucr
					     , ceunderbond     		=> p_ceunderbond
					     , cedocdstamp     		=> p_cedocdstamp
					     , uploadedcustoms  	=> p_uploadedcustoms
					     , lockcode     		=> p_lockcode
					     , printlabel     		=> p_printlabel
					     , asnid     		=> p_asnid
					     , customerid     		=> p_customerid
					     , cedutystamp     		=> p_cedutystamp
					     , palletgrouped    	=> p_palletgrouped
					     , consollink     		=> p_consollink
					     , jobsiteid     		=> p_jobsiteid
					     , jobclientid     		=> p_jobclientid
					     , extranotes     		=> p_extranotes
					     , stagerouteid     	=> p_stagerouteid
					     , stagerouteseq    	=> p_stagerouteseq
					     , pfconsollink     	=> p_pfconsollink
					     , ceavailstatus    	=> p_ceavailstatus
					     , masterpahid     		=> p_masterpahid
					     , masterpalid     		=> p_masterpalid
					     , uploaded     		=> p_uploaded
					     , custshpmntno     	=> p_custshpmntno
					     , fromstatus     		=> p_tostatus
					     , tostatus     		=> p_fromstatus
					     , shipmentno     		=> p_shipmentno
					     , palletconfig     	=> p_palletconfig
					     , masterorderid    	=> p_masterorderid
					     , masterorderlineid	=> p_masterorderlineid
					     , kitplanid     		=> p_kitplanid
					     , plansequence     	=> p_plansequence
					     , cecollicountexpected	=> p_cecollicountexpected
					     , cecollicount     	=> p_cecollicount
					     , cesealsok     		=> p_cesealsok
					     , ceinvoicenumber  	=> p_ceinvoicenumber
					     , olduserdeftype1  	=> p_olduserdeftype1
					     , olduserdeftype2  	=> p_olduserdeftype2
					     , olduserdeftype3  	=> p_olduserdeftype3
					     , olduserdeftype4  	=> p_olduserdeftype4
					     , olduserdeftype5  	=> p_olduserdeftype5
					     , olduserdeftype6  	=> p_olduserdeftype6
					     , olduserdeftype7  	=> p_olduserdeftype7
					     , olduserdeftype8  	=> p_olduserdeftype8
					     , olduserdefchk1   	=> p_olduserdefchk1
					     , olduserdefchk2   	=> p_olduserdefchk2
					     , olduserdefchk3   	=> p_olduserdefchk3
					     , olduserdefchk4   	=> p_olduserdefchk4
					     , olduserdefdate1  	=> p_olduserdefdate1
					     , olduserdefdate2  	=> p_olduserdefdate2
					     , olduserdefdate3  	=> p_olduserdefdate3
					     , olduserdefdate4  	=> p_olduserdefdate4
					     , olduserdefnum1   	=> p_olduserdefnum1
					     , olduserdefnum2   	=> p_olduserdefnum2
					     , olduserdefnum3   	=> p_olduserdefnum3
					     , olduserdefnum4   	=> p_olduserdefnum4
					     , olduserdefnote1  	=> p_olduserdefnote1
					     , olduserdefnote2  	=> p_olduserdefnote2
					     , laborassignment  	=> p_laborassignment
					     , laborgridsequence	=> p_laborgridsequence
					     , kitceconsignid   	=> p_kitceconsignid
					     );
	p_status := l_status;
	commit;
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
end create_itl_p;
------------------------------------------------------------------------------------------------
-- Author  : Jayalakshmi G 29-Mar-2022
-- Purpose : Function to fetch database
------------------------------------------------------------------------------------------------

	function fetch_database_f return varchar2 is
		l_rtn       varchar2(30) := 'fetch_database_f';
		l_database  varchar2(10);
	begin
		select
			name
		into l_database
		from
			v$database;

		return l_database;
	exception
		when no_data_found then
			cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
			, p_sql_error_message_i => sqlerrm				-- SQL error message
			, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
			, p_package_name_i => g_pck				-- Package name the error occured
			, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
			,
			                     p_routine_parameters_i => null					-- list of all parameters involved
			                     , p_comments_i => null					-- Additional comments describing the issue
			                     );

			return null;
	end fetch_database_f;

------------------------------------------------------------------------------------------------
-- Author  :  M. Swinkels 22/04/2021, Jayalakshmi G 30-Mar-2022
-- Purpose : Check JSON format is valid
------------------------------------------------------------------------------------------------

	function check_json_format_f (
		p_string_i in clob
	) return boolean is
		l_rtn     varchar2(30) := 'check_json_format_f';
		l_result  pljson := pljson();
	begin
		l_result := pljson(p_string_i);

		return true;
	exception
		when others then
		-- add log record
			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Response has an invalid JSON format. Reconstruct reponse message.');
			return false;
	end check_json_format_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021, Jayalakshmi G 30-Mar-2022
-- Purpose : Fetch authenticate key from Sapper web service
------------------------------------------------------------------------------------------------

	procedure fetch_authenticate_key_p  (
	p_logging_i  in  varchar2
)
is

		l_url            varchar2(1000) := cnl_util_pck.get_constant('SAPPER_AUTHENTICATE_WEBSERVICE_URL');
		l_proxy          varchar2(50) := cnl_util_pck.get_constant('PROXY_SERVER');
		l_user_name      varchar2(50) := cnl_util_pck.get_constant('SAPPER_AUTHENTICATE_USERNAME');
		l_password       varchar2(30) := cnl_util_pck.get_constant('SAPPER_AUTHENTICATE_PASSWORD');
		l_wallet         varchar2(400) := cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
		l_wall_passw     varchar2(50) := cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');
		l_response_code  varchar2(30);
		l_request        pljson := pljson();
		l_response_body  pljson;
		l_err            varchar2(4000);
		l_key            varchar2(2000);
		l_rtn            varchar2(30) := 'fetch_authenticate_key_p';
		l_trace_key      integer;
		l_attempt        integer := 0;
	begin
	if (p_logging_i ='Y')
	then
	-- add log record
		cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
		                                          || '.'
		                                          || l_rtn, 'Start fetching authentification key.');
	end if;
	-- build json request
		l_request.put('userName', l_user_name);
		l_request.put('password', l_password);
		l_request.put('tokenDuration', 5);

	-- add web servce tace
		cnl_spr_pck.create_spr_trace_record(l_request, null, null, l_rtn, null,
		                                    l_trace_key);
		<< call_webservice >> 
		while l_attempt < 5 loop
			
			l_attempt := l_attempt + 1;

		-- Call web service
			l_response_code := call_webservice_f(p_url_i => l_url,
								p_proxy_i => l_proxy,
								p_user_name_i => l_user_name,
								p_password_i => l_password,
								p_wallet_i => l_wallet,
								p_wallet_password_i => l_wall_passw,
								p_post_get_del_p => 'POST',
								p_logging_i => p_logging_i,
								p_json_body_i => l_request,
								p_json_body_o => l_response_body,
								p_response_reason_o => l_err);
						    
		-- add web servce trace

			cnl_spr_pck.create_spr_trace_record(null, l_response_body, l_response_code, l_rtn, l_trace_key,
			                                    l_trace_key);

		-- Add logging
		if (p_logging_i ='Y')
		then
			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Attempt '
			                                                    || to_char(l_attempt)
			                                                    || ': http response = '
			                                                    || l_response_code
			                                                    || ', '
			                                                    || l_err);
		end if;
			if l_response_code = '200' then
			-- Extract key from string
				l_key := pljson_ext.get_string(l_response_body, 'token');
			

			-- Insert new key
				update cnl_sys.cnl_spr_authenticate_key
				set
					authenticate_key = l_key,
					attempt = 1,
					dstamp = sysdate;

				commit;
		if (p_logging_i ='Y')
		then
			-- add log record
				cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
				                                          || '.'
				                                          || l_rtn, 'New authenticate key succesfully saved. key: ' || l_key);
		end if;

				exit call_webservice;
			else
				update cnl_sys.cnl_spr_authenticate_key
				set
					attempt = l_attempt;
					--dstamp = sysdate;

				commit;
			if (p_logging_i ='Y')
			then
			-- add log record
				cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
				                                          || '.'
				                                          || l_rtn, 'Failed to fetch new authenticate key during attempt '
				                                                    || to_char(l_attempt)
				                                                    || '.');
			end if;
			-- Wait 10 seconds then try again.
				dbms_lock.sleep(10);
			end if;

		end loop;

	exception
		when others then
			cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
			, p_sql_error_message_i => sqlerrm				-- SQL error message
			, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
			, p_package_name_i => g_pck				-- Package name the error occured
			, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
			,
			                     p_routine_parameters_i => null					-- list of all parameters involved
			                     , p_comments_i => null					-- Additional comments describing the issue
			                     );
		-- add log record

			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Exception check CNL_ERROR.');
	end fetch_authenticate_key_p;


  
------------------------------------------------------------------------------------------------
-- Author  : Jayalakshmi G 30-03-2022
-- Purpose : Validate address using order_id 
------------------------------------------------------------------------------------------------

procedure validate_address_p(
	p_site_id_i  in  varchar2,
	p_logging_i  in varchar2
)
  is
	l_source_id		 varchar2(50);
	l_check_type		 integer:=3;
	l_match_type		 integer:=3;
	l_match_level		 number;
	l_status_reason_code	 varchar2(50);
	l_client_id		 varchar2(100);
	l_order_id_resp		 varchar2(100);
	l_or_cl_id_resp		 varchar2(100);
	l_client_id_resp         varchar2(50);
	l_customer_id		 varchar2(50);
	l_tmp_notes		 varchar2(50);
	l_string_check		 varchar2(50);
	l_attempt		 integer:= 0;
	l_itl_status		 integer;
	l_site_id		 varchar2(50):=p_site_id_i;
	l_instruction	cnl_sys.cnl_ohr_instructions.instructions%type;
	l_body_request pljson ;
	l_sub_request pljson 		;
	l_body_response	pljson		:= pljson();
	l_tmp_json pljson		:= pljson();
	l_parent_json	pljson_list	:= pljson_list();
	l_json_list pljson_list 	;


	l_trace_key		        integer;
	l_rtn				varchar2(30) := 'validate_address';
	l_response_code			varchar2(30);
	l_url                       	varchar2(1000)	:= cnl_util_pck.get_constant('SAPPER_ADD_VERIFY_WEBSERVICE_URL');
	l_proxy				varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_wallet		        varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw	        	varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');
	l_response_reason	        varchar2(4000);
   
   --cursor to fetch order details from order header table
   type g_ord_rec 	is record( client_id		dcsdba.order_header.client_id%type
				 , name			dcsdba.order_header.name%type
				 , address		dcsdba.order_header.address1%type
				 , town			dcsdba.order_header.town%type
				 , postcode		dcsdba.order_header.postcode%type
				 , tandata_id		dcsdba.country.tandata_id%type
				 , order_id		dcsdba.order_header.order_id%type);
	-- Table with order record types
	type g_ord_tab 	is table of g_ord_rec;
	-- Table 
	g_ord	g_ord_tab	:= g_ord_tab();
   
   --cursor to fetch 16 or less records per request
   cursor add_o(site_id_key in varchar2)
	is
	select
	o.client_id,
	o.name  ,
	o.address1, 
	o.town 	,
	o.postcode,
	c.tandata_id, 
	o.order_id
	from
	dcsdba.order_header  o,
	dcsdba.country       c,
	dcsdba.client        cl
	where
	o.From_site_id= l_site_id
	and o.country = c.iso3_id
	and o.client_id = cl.client_id
	and o.status = 'Hold'
	and o.status_reason_code = 'AVPENDING'
	and rownum<17
	order by order_id;
	
	-- cursor to check if saved instructions exists
	cursor	c_instructions(p_order_id_i dcsdba.order_header.order_id%type,
			       p_client_id_i dcsdba.order_header.client_id%type)
	is
		select 	i.instructions
		from	cnl_sys.cnl_ohr_instructions i
		where	i.order_id 	= p_order_id_i
		and	i.client_id	= p_client_id_i
		and	i.site_id	= p_site_id_i
	;
	
	--cursor to fetch instructions from order header 
	cursor	c_ord_instructions(p_order_id_i dcsdba.order_header.order_id%type,
				   p_client_id_i dcsdba.order_header.client_id%type)
	is
		select 	o.instructions
		from	dcsdba.order_header o
		where	o.order_id 	= p_order_id_i
		and	o.client_id	= p_client_id_i
		and	o.from_site_id	= p_site_id_i
	;
	
  begin 
    case
    when	g_database 	= 'DEVCNLJW'
    then	l_source_id	:= 'NLTLG03DEV';
    when	g_database 	= 'TSTCNLJW'
    then	l_source_id	:= 'NLTLG03TST';
    when	g_database 	= 'ACCCNLJW'
    then	l_source_id	:= 'NLTLG03ACC';
    when	g_database 	= 'PRDCNLJW'
    then	l_source_id	:= 'NLTLG03PRD';
    end case;	  
	
    -- Initialise Session so auditing and users in transactions are shown as below
    dcsdba.libsession.InitialiseSession( userid			=> 'SAPPER'
					   , groupid		=> null
					   , stationid		=> 'Automatic'
					   , wksgroupid		=> null
					   );
	 -- Add logging
	if(p_logging_i ='Y')
	then
		cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
			 , 'Start address check procedure.Update Status_reason_code to AVPENDING ' );
	end if;
	
		l_status_reason_code := 'AVPENDING';
		-- update all orders that will be processed during this run by setting the status reason code to AVPENDING
		if l_site_id is not null then
			update_orderheader(l_status_reason_code,null,null,p_site_id_i);
		end if;
	
	<<fetch_orderdetails>>
	loop
	open add_o(l_site_id);
	fetch add_o
	bulk	collect
	into g_ord;
	exit 
		when	g_ord.count = 0;
		
l_body_request:=  pljson();
l_sub_request := pljson();	
l_json_list := pljson_list();	
dbms_output.put_line('enter loop'||g_ord.count);

	--create json parent segment
	l_body_request.put('sourceid', l_source_id);
	l_body_request.put('checkType', l_check_type);
	l_body_request.put('matchType', l_match_type);
	
	
for i in 1..g_ord.count
loop
	dbms_output.put_line('i val in loop: '||i);
	--append order_id to client_id
	l_client_id:=g_ord(i).client_id||':'||g_ord(i).order_id;
	dbms_output.put_line('l_client_id: '||l_client_id);
	-- adding a sub segment
	l_sub_request.put('clid',l_client_id);
	l_sub_request.put('name',  g_ord(i).name);
	l_sub_request.put('street',  g_ord(i).address);
	l_sub_request.put('city',  g_ord(i).town);
	l_sub_request.put('zip',  g_ord(i).postcode);
	l_sub_request.put('country',  g_ord(i).tandata_id);

	--appending  all sub segments to make a list
	l_json_list.append(l_sub_request);
	
	--appending sub segment to parent segment
	l_body_request.put('entities', l_json_list);
end loop;
	-- add web service trace
	create_spr_trace_record( l_body_request, null, null, l_rtn, null, l_trace_key);

	-- Call web service
	--<<call_webservice>>
	--while 	l_attempt < 6
	--loop
		
		dbms_output.put_line('l_attempt: '|| l_attempt);
		l_response_code := call_webservice_f( p_url_i		  => l_url
							, p_proxy_i		  => l_proxy
							, p_user_name_i	  => null
							, p_password_i	  => null
							, p_wallet_i	  => l_wallet
							, p_wallet_password_i => l_wall_passw
							, p_post_get_del_p	  => 'POST'
							, p_logging_i => p_logging_i
							, p_json_body_i	  => l_body_request
							, p_json_body_o	  => l_body_response
							, p_response_reason_o => l_response_reason);
	
		dbms_output.put_line('l_response_code: '||l_response_code);
		dbms_output.put_line('l_response_reason: '||l_response_reason);
	
		-- Add logging
		if(p_logging_i ='Y')
		then
		cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
					, 'Finished validating the address by web service.  Now continue processing response.'
					); 
		end if;
	
		-- add web service trace
		create_spr_trace_record( null, l_body_response, l_response_code, l_rtn , l_trace_key, l_trace_key);
	
				
		--update order header based on the web service response
		if( l_response_code = '1010')
		then 
		
		exit fetch_orderdetails;
		
		elsif	l_response_code = '200'
		then
		
			-- Extract subsegment for from response
			l_parent_json :=pljson_ext.get_json_list(l_body_response,'result');
			
			-- Extract matchlevel and client from subsegment
			for i in 1..l_parent_json.count
			loop 
				l_tmp_json := pljson(l_parent_json.get(i));
				
				--extract match_level order_id and client_id from response
				l_match_level := pljson_ext.get_number(l_tmp_json, 'matchlevel');
				l_or_cl_id_resp := pljson_ext.get_string(l_tmp_json, 'clid');
				l_order_id_resp:= SUBSTR(l_or_cl_id_resp,INSTR(l_or_cl_id_resp, ':')+1,length(l_or_cl_id_resp));
				l_client_id_resp := substr(l_or_cl_id_resp, 1,instr(l_or_cl_id_resp, ':') - 1);
				select o.customer_id into l_customer_id from dcsdba.order_header o where client_id = l_client_id_resp and order_id = l_order_id_resp;	
				
				if(p_logging_i ='Y')
				then
				cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
											,'match_level value: '||l_match_level|| ' clid value: '||l_or_cl_id_resp||' l_order_id_resp: '||l_order_id_resp
											||' l_client_id_resp : '||l_client_id_resp
											);
				end if;
				
				--save inctruction before processing the order details in order_header table						
				if ( l_order_id_resp is not null)
				then
						--save instruction before updating
					open 	c_instructions(l_order_id_resp,l_client_id_resp); 
					fetch	c_instructions
					into 	l_instruction;
					if	c_instructions%found
							then
								close	c_instructions;
							-- add log record
							if(p_logging_i ='Y')
							then
							cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
										 , 'Order '
										 || l_order_id_resp
										 || ' contains an instruction in CNL_OHR_INSTRUCTIONS table. Updating status_reason_code to CSREQUIRED.'
										 );	
							end if;
					else
							close	c_instructions;
		
							-- Check if order contains an instruction
							open	c_ord_instructions(l_order_id_resp,l_client_id_resp);
							fetch	c_ord_instructions
							into	l_instruction;
							close	c_ord_instructions;
							
							-- add log record
							if(p_logging_i ='Y')
							then
							cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
										 , 'Order '
										 || l_order_id_resp
										 || ' contains an instruction in order_header table. 
										 Saving instruction so and updating status_reason_code to CSREQUIRED.'
										 );
							end if;
							
							if	l_instruction is not null
							and	l_instruction not like 'SPRMSG#%'
							then
								-- Save original instructions so it can be placed back when shipment is created succesfully
								insert
								into	cnl_sys.cnl_ohr_instructions
								(	site_id	
								,	client_id
								,	order_id
								,	instructions
								)
								values
								(	l_site_id
								,	l_client_id_resp
								,	l_order_id_resp
								,	l_instruction
								)
								;
								commit;
							end if;
								-- add log record
								if(p_logging_i ='Y')
								then
								cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
										 , 'Instruction fetched from order_header table and stored in cnl_ohr_instructions.' );
								end if;
								
					end if;	
				end if;
					
				if (l_match_level = 0  and  l_order_id_resp is not null)
				then
					-- update status reason code only
					update_orderheader('CSREQUIRED',l_order_id_resp,l_client_id_resp);
					l_tmp_notes := 'AVPENDING --> CSREQUIRED';
					
					--add logging
					if(p_logging_i ='Y')
					then	
					cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck || '.'|| l_rtn,
						'updated status_reason_code in order_header table to CSREQUIRED ');
					end if;
					
				else
						
					-- update status reason code only
					update_orderheader('AVERROR',l_order_id_resp,l_client_id_resp);
					l_tmp_notes := 'AVPENDING --> AVERROR';
					
					-- add logging
					if(p_logging_i ='Y')
					then
					cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
						, 'updated status_reason_code in order_header table to AVERROR for the attempt: '||l_attempt ||'.'
						);
					end if;
				end if;
			-- Add ITL transaction for status reason code update
			cnl_spr_pck.create_itl_p( p_status		=> l_itl_status
						, p_code 		=> 'Order Status'
						, p_updateqty		=> 0	
						, p_clientid 		=> l_client_id_resp
						, p_referenceid 	=> l_order_id_resp
						, p_stationid 		=> 'Automatic'
						, p_userid 		=> 'Sapper'
						, p_tmpnotes 		=> l_tmp_notes
						, p_siteid		=> l_site_id
						, p_ownerid 		=> l_client_id_resp
						, p_customerid 		=> l_customer_id
						, p_fromstatus 		=> 'Hold'
						, p_tostatus 		=> 'Hold'
						, p_tagid		=> null
						, p_tolocation		=> null
						, p_extranotes		=> 'Sapper validation done'
						);
				
			end loop;
			--exit call_webservice;
					else
					-- add log record
					while(l_attempt <5)
					loop
					l_attempt	:= l_attempt + 1;
					if(p_logging_i ='Y')
					then
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										, 'Failed to validate address during attempt: '||to_char(l_attempt) ||'.'
										);
					end if;
					-- Wait 10 seconds then try again.
					
					dbms_lock.sleep(10);
					end loop;
				
		end if;
	--end loop;
	close add_o;
	end loop;
			     
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
		-- add log record
		cnl_sys.cnl_spr_pck.create_spr_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR.'
							 );
  end validate_address_p;
------------------------------------------------------------------------------------------------
-- Author  : Jayalakshmi G 28/03/2022
-- Purpose : Create log record
------------------------------------------------------------------------------------------------

	procedure create_spr_log_record (
		p_source_i       in  varchar2,
		p_description_i  in  varchar2
	) is
		l_rtn varchar2(30) := 'create_spr_log_record';
		pragma autonomous_transaction;
	begin
	--set_spr_logging_p;
		if g_logging then
			insert into cnl_spr_log (
				dstamp,
				source,
				description
			) values (
				sysdate,
				substr(p_source_i, 1, 100),
				substr(p_description_i, 1, 4000)
			);

			commit;
		end if;
	exception
		when others then
			cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
			, p_sql_error_message_i => sqlerrm				-- SQL error message
			, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
			, p_package_name_i => g_pck				-- Package name the error occured
			, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
			,
			                     p_routine_parameters_i => null					-- list of all parameters involved
			                     , p_comments_i => null					-- Additional comments describing the issue
			                     );

			commit;
	end create_spr_log_record;

------------------------------------------------------------------------------------------------
-- Author  : Jaya 28/03/2022
-- Purpose : Create trace record
------------------------------------------------------------------------------------------------

	procedure create_spr_trace_record (
		p_request_i           in   pljson default null,
		p_response_i          in   pljson default null,
		p_status_code_i       in   varchar2 default null,
		p_web_service_name_i  in   varchar2,
		p_key_i               in   integer default null,
		p_key_o               out  integer
	) is
		l_body  clob;
		l_rtn   varchar2(30) := 'create_spr_trace_record';
		l_key   integer;
		pragma autonomous_transaction;
	begin
		dbms_lob.createtemporary(l_body, false);
		if g_tracing then
			if p_response_i is null then
				p_request_i.to_clob(l_body, false);
				l_key := cnl_spr_webservice_body_seq1.nextval;
				p_key_o := l_key;
				insert into cnl_spr_webservice_body (
					key,
					dstamp,
					request,
					response,
					status_code,
					web_service_name
				) values (
					l_key,
					sysdate,
					l_body,
					null,
					null,
					p_web_service_name_i
				);

			else
				p_response_i.to_clob(l_body, false);
				update cnl_spr_webservice_body
				set
					response = l_body,
					status_code = p_status_code_i,
					web_service_name = p_web_service_name_i
				where
					key = p_key_i;

			end if;

			commit;
		end if;

		dbms_lob.freetemporary(l_body);
	exception
		when others then
			dbms_lob.freetemporary(l_body);
			cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
			, p_sql_error_message_i => sqlerrm				-- SQL error message
			, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
			, p_package_name_i => g_pck				-- Package name the error occured
			, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
			,
			                     p_routine_parameters_i => null					-- list of all parameters involved
			                     , p_comments_i => null					-- Additional comments describing the issue
			                     );

			commit;
	end create_spr_trace_record;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : set authentcate key
------------------------------------------------------------------------------------------------

	function set_authenticate_key 
		return cnl_sys.cnl_spr_authenticate_key.authenticate_key%type is
		l_retval  cnl_sys.cnl_spr_authenticate_key.authenticate_key%type;
		l_rtn     varchar2(30) := 'set_authenticate_key';
	begin
		select
			authenticate_key
		into l_retval
		from
			cnl_sys.cnl_spr_authenticate_key
		where
			rownum = 1;

		return l_retval;
	exception
		when no_data_found then
			cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
			, p_sql_error_message_i => sqlerrm				-- SQL error message
			, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
			, p_package_name_i => g_pck				-- Package name the error occured
			, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
			,
			                     p_routine_parameters_i => null					-- list of all parameters involved
			                     , p_comments_i => null					-- Additional comments describing the issue
			                     );
	end set_authenticate_key;

------------------------------------------------------------------------------------------------
-- Author  :  M. Swinkels - Jayalakshmi G  28/03/2022
-- Purpose : Call webervoce
------------------------------------------------------------------------------------------------

	function call_webservice_f (
		p_url_i              in   varchar2,
		p_proxy_i            in   varchar2,
		p_user_name_i        in   varchar2, -- Used for authentification if needed
		p_password_i         in   varchar2,
		p_wallet_i           in   varchar2, -- path of wallet 
		p_wallet_password_i  in   varchar2,
		p_post_get_del_p     in   varchar2,
		p_logging_i	     in   varchar2,
		p_json_body_i        in   pljson,
		p_json_body_o        out  pljson,
		p_response_reason_o  out  varchar2
	) return varchar2 is

		l_body_req          varchar2(32767) := p_json_body_i.to_char(false);
		l_body_res          clob;
		l_body_text         varchar2(32767);
		l_request           utl_http.req;
		l_response          utl_http.resp;
		l_response_code     varchar2(30);
		l_authenticate_url  varchar2(1000) := cnl_util_pck.get_constant('SAPPER_AUTHENTICATE_WEBSERVICE_URL');
		l_rtn               varchar2(30) := 'call_webservice_f';
		l_exception_body    pljson := pljson();
		l_loop_counter      integer := 1;
		l_trace_key         integer;
	--
	begin
	if(p_logging_i ='Y')
	then
	-- add log record
		cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
		                                          || '.'
		                                          || l_rtn, 'Start preparing HTTP header. Timeout is set to 5 seconds.');
	end if;
		utl_http.set_transfer_timeout(5);
		utl_http.set_proxy(p_proxy_i);
		utl_http.set_wallet(p_wallet_i, p_wallet_password_i);
		utl_http.set_response_error_check(enable => false);
		utl_http.set_detailed_excp_support(enable => true);
		dbms_lob.createtemporary(l_body_res, true);
		begin
		-- add log record
		if(p_logging_i ='Y')
		then
			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Begin http request (content-type/json).');
		
		end if;
		
		-- Iniitiate request	
			l_request := utl_http.begin_request(p_url_i, p_post_get_del_p, utl_http.http_version_1_1);
		

		-- add log record
		if(p_logging_i ='Y')
		then
			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Content length = ' || to_char(length(l_body_req)));
		end if;
		
		-- Set request headers

			utl_http.set_header(l_request, 'Content-Type', 'application/json; charset=utf-8');
			utl_http.set_header(l_request, 'Content-Length', length(l_body_req));

		-- Only use authenticate key when not fetching authentication token
			if p_url_i != l_authenticate_url then
				g_authenticate := set_authenticate_key;
				utl_http.set_header(l_request, 'Authorization', 'Bearer ' || g_authenticate);
			end if;

		-- add log record
		if(p_logging_i ='Y')
		then
			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Write request text');
		end if;
			
		-- Write request
			utl_http.write_text(l_request, l_body_req);

		-- Send request and get response
			l_response := utl_http.get_response(r => l_request);
			l_response_code := l_response.status_code;
			p_response_reason_o := substr(l_response.reason_phrase, 1, 4000);

		-- add log record
		if(p_logging_i ='Y')
		then
			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'response code = '
			                                                    || l_response_code
			                                                    || ', response phrase = '
			                                                    || p_response_reason_o);
		

		-- add log record

			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'Start Reading response');
		end if;

		-- Read response
			begin
				loop
				
				-- add log record
				if(p_logging_i ='Y')
				then
					cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
					                                          || '.'
					                                          || l_rtn, 'Read line ' || to_char(l_loop_counter));
				end if;

					l_loop_counter := l_loop_counter + 1;

				--clean temp text 
					l_body_text := null;

				-- Read next line
					utl_http.read_text(l_response, l_body_text, 30000);

				--add line to clob
					dbms_lob.writeappend(l_body_res, length(l_body_text), l_body_text);
				end loop;
			exception
				when utl_http.end_of_body then
				-- add log record
				if( p_logging_i = 'Y')
				then
					cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
					                                          || '.'
					                                          || l_rtn, 'Finished reading response.');
				end if;
			end;

			p_json_body_o := pljson(l_body_res);

		-- Check if response body is formatted as json
			if check_json_format_f(l_body_res) then
			if(p_logging_i ='Y')
			then
				cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
				                                          || '.'
				                                          || l_rtn, 'valid Json format');
									  end if;
			else
			if(p_logging_i ='Y')
			then
				cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
				                                          || '.'
				                                          || l_rtn, 'Not in JSON format');
									  end if;
			end if;

		-- add log record

			if(p_logging_i ='Y')
			then

			cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
			                                          || '.'
			                                          || l_rtn, 'return reponse code: '
			                                                    || l_response_code ||' Web service call ended successfuly.');
			end if;
		-- End htto resposne
			utl_http.end_response(l_response);
		-- Clean up memeory from temporarye clob
			dbms_lob.freetemporary(l_body_res);
		-- Return reponse code
			return l_response_code;
		exception
			when utl_http.transfer_timeout then
				dbms_lob.freetemporary(l_body_res);
				if l_request.http_version != null then
					utl_http.end_request(l_request);
				end if;
				if l_response.http_version != null then
					utl_http.end_response(l_response);
				end if;
				l_exception_body.put('message', 'timeout occured');
				p_json_body_o := l_exception_body;
				p_response_reason_o := 'Timeout occured';
				return '1009';
			when others then
				dbms_lob.freetemporary(l_body_res);
				if l_request.http_version != null then
					utl_http.end_request(l_request);
				end if;
				if l_response.http_version != null then
					utl_http.end_response(l_response);
				end if;
				cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
				, p_sql_error_message_i => sqlerrm				-- SQL error message
				, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
				, p_package_name_i => g_pck				-- Package name the error occured
				, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
				,
				                     p_routine_parameters_i => null					-- list of all parameters involved
				                     , p_comments_i => 'Unhandled exception occured'	-- Additional comments describing the issue
				                     );

				l_exception_body.put('message', 'unhandled exception occured');
				p_json_body_o := l_exception_body;
				p_response_reason_o := '-12535';
				return '1010';
		end;

	end call_webservice_f;
------------------------------------------------------------------------------------------------
-- Author  : Jayalakshmi G 28/03/2022
-- Purpose : update order_header
------------------------------------------------------------------------------------------------

	procedure update_orderheader (
		p_status_i		in  varchar2,
		p_order_id_i		in  varchar2 default null,
		p_client_id_i		in  varchar2 default null,
		p_site_id_i		in varchar2 default null
	) is
		l_rtn varchar2(30) := 'update_orderheader';
		pragma autonomous_transaction;
	begin
	/*cnl_sys.cnl_spr_pck.create_spr_log_record(g_pck
				 || '.'
				 || l_rtn, ' updating table order_header');*/
	if (p_status_i='CSREQUIRED')
	then
	update dcsdba.order_header
		set
		status_reason_code = 'CSREQUIRED'
		where
		order_id = p_order_id_i
		and client_id =p_client_id_i;
	elsif(p_status_i='AVERROR')
	then
	update dcsdba.order_header o
		set
		o.status_reason_code = 'AVERROR',
		o.instructions = 'SPRMSG# Address not deliverable'
		where
		o.order_id = p_order_id_i
		and client_id =p_client_id_i;
	elsif(p_status_i = 'AVPENDING')
	then
	update dcsdba.order_header o
			set
				o.status_reason_code = 'AVPENDING'
			where
				    o.status = 'Hold'
				and o.status_reason_code = 'AVREQUIRED'
				and o.from_site_id = p_site_id_i;			
	end if;
			commit;
		
	exception
		when others then
			cnl_sys.cnl_util_pck.add_cnl_error(p_sql_code_i => sqlcode				-- Oracle SQL code or user defined error code
			, p_sql_error_message_i => sqlerrm				-- SQL error message
			, p_line_number_i => dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
			, p_package_name_i => g_pck				-- Package name the error occured
			, p_routine_name_i => l_rtn				-- Procedure or function generarting the error
			,
			                     p_routine_parameters_i => null					-- list of all parameters involved
			                     , p_comments_i => null					-- Additional comments describing the issue
			                     );

			commit;
	end update_orderheader;



end cnl_spr_pck;