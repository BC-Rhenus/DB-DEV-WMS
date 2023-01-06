CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CTO_PCK" 
is
/**********************************************************************************
* $Archive: $
* $Revision: $ 1  
* $Author: $ Martijn Swinkels
* $Date: $ 22/04/2021
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
	g_pck		varchar2(30) := 'cnl_cto_pck';
	g_logging	boolean;
	g_tracing	boolean;
	g_authenticate	cnl_sys.cnl_cto_authenticate_key.authenticate_key%type;
	g_database	varchar2(10);
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Jun-2022
-- Purpose : Function to check if the restriction (extra_parameters) in the Java_Report_map is valid
------------------------------------------------------------------------------------------------
function is_ohr_restriction_valid( p_client_id_i   in varchar2
                                 , p_order_id_i    in varchar2
                                 , p_where_i       in varchar2
                                 )
    return integer
  is
  type ref_cur is ref cursor;
    c_print  ref_cur;
    l_retval integer := 0;  -- 0 = not valid, 1 = valid
    l_query  varchar2(4000);
  begin
    l_query := 'select 1 from dcsdba.order_header where'
            || ' client_id = :client_id'
            || ' and order_id  = :order_id'
            || ' and ('
            || p_where_i
            || ')'
            ;
    begin
      open  c_print 
      for   l_query using p_client_id_i, p_order_id_i;
      fetch c_print 
      into  l_retval;
      close c_print;
    end;

    return nvl(l_retval, 0);

end is_ohr_restriction_valid;

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
-- Author  : M. Swinkels 22/04/2021
-- Purpose : check if valid json
------------------------------------------------------------------------------------------------
function check_json_format_f( p_string_i	in clob)
	return boolean
is
	l_rtn		varchar2(30) := 'check_json_format_f';
	l_result 	pljson := pljson();
begin
	l_result	:= pljson(p_string_i);
	-- add log record
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Response is correctly formatted JSON.');
	return true;
exception
	when others 
	then
		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Response has an invalid JSON format. Reconstruct reponse message.');
		return false;
end check_json_format_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Create label with random text max 264 characters
------------------------------------------------------------------------------------------------
function create_zpl_text_label_f( p_text_i	in varchar2) 
	return varchar2
is
	l_text		varchar2(4000);
	l_temp_text	varchar2(100);
	l_row		integer := 150;
	l_rtn		varchar2(30) 	:= 'fetch_database_f';
	l_label		varchar2(500);
	l_escape_text	varchar2(264)	:= '^XA^LH,20^CFV^FO30,50^FD '
					|| 'Unexpected error'
					|| '^FS^FO30,150^FD '
					|| 'while creating an'
					|| '^FS^FO30,250^FD '
					|| 'error label'
					|| '^FS^FO30,350^FD'
					|| 'Contact administrator'
					|| '^FS^XZ';
begin
	if	p_text_i is null
	then
		l_label	:= l_escape_text;
	else
		l_label	:= '^XA^LH,10^CFV^FO30,50^FD ';
		l_text		:= substr(p_text_i,1,3500);

		-- Max 11 lines of text
		for	i in 1..11
		loop
			if	l_text	is null
			then
				exit;
			end if;
			-- Clear temp text and then add first 24 characters and remove from main tring
			l_temp_text	:= null;
			l_temp_text 	:= substr(l_text,1,24);
			l_text 		:= substr(l_text,25,500);

			-- if main string does not start with a space add characters to so words do not break up
			while	substr(l_text,1,1) != ' '
			and	l_text is not null
			loop
				l_temp_text	:= l_temp_text ||substr(l_text,1,1);
				l_text 		:= substr(l_text,2,500);
			end loop;

			-- remove spaces at the beginning of temo text
			while 	substr(l_temp_text,1,1) = ' '
			and	l_temp_text is not null
			loop
				l_temp_text	:= substr(l_temp_text,2,500);
			end loop;

			if	length(l_label || '^FS^FO30,'||to_char(l_row)||'^FD '||l_temp_text||'^FS^XZ') <= 500
			then
				l_label	:= l_label || '^FS^FO30,'||to_char(l_row)||'^FD '||l_temp_text;
				l_row 	:= l_row + 100;
			end if;
		end loop;
		l_label	:= l_label || '^FS^XZ';
	end if;
	return l_label;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> 'Iss creating ZPL text label'	-- Additional comments describing the issue
						  );
		return l_escape_text;
end create_zpl_text_label_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : fetch database
------------------------------------------------------------------------------------------------
function fetch_database_f
	return varchar2
is
	l_rtn		varchar2(30) := 'fetch_database_f';
	l_database	varchar2(10);
begin
	select 	name 
	into 	l_database
	from 	v$database
	;
	return l_database;
exception
	when NO_DATA_FOUND
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		return null;
end fetch_database_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : set database
------------------------------------------------------------------------------------------------
procedure set_database_f
is
	l_rtn		varchar2(30) := 'set_database_f';
begin
	g_database	:= fetch_database_f;
exception
	when NO_DATA_FOUND
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
end set_database_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : set authentcate key
------------------------------------------------------------------------------------------------
function set_authenticate_key
	return cnl_sys.cnl_cto_authenticate_key.authenticate_key%type
is
	l_retval	cnl_sys.cnl_cto_authenticate_key.authenticate_key%type;
	l_rtn		varchar2(30) := 'set_authenticate_key';
begin
	select	authenticate_key
	into 	l_retval
	from	cnl_sys.cnl_cto_authenticate_key
	where 	rownum = 1
	;
	return l_retval;
exception
	when NO_DATA_FOUND
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
end set_authenticate_key;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Call webervoce
------------------------------------------------------------------------------------------------
function call_webservice_f( p_url_i		in varchar2
			  , p_proxy_i		in varchar2
			  , p_user_name_i	in varchar2 -- Used for authentification if needed
			  , p_password_i	in varchar2 
			  , p_wallet_i		in varchar2 -- path of wallet 
			  , p_wallet_password_i	in varchar2 
			  , p_post_get_del_p	in varchar2
			  , p_json_body_i	in pljson
			  , p_json_body_o	out pljson
			  , p_response_reason_o	out varchar2
			)
	return varchar2
is
--	l_body_req		varchar2(32767) := p_json_body_i.to_char( false);
	l_body_reqq		clob;
	l_body_req		clob;-- := p_json_body_i.get_clob();
	l_chunk			varchar2(32767);
	l_body_res		clob;
	l_body_text		varchar2(32767);
	l_request		utl_http.req;
	l_response		utl_http.resp;
	l_response_code		varchar2(30);
	l_authenticate_url	varchar2(1000) := cnl_util_pck.get_constant('CTO_AUTHENTICATE_WEBSERVICE_URL');
	l_rtn			varchar2(30) := 'call_webservice_f';
	l_exception_body	pljson	:= pljson();
	l_loop_counter		integer :=1;
	--
begin
	-- add log record
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Start preparing HTTP header. Timeout is set to 10 seconds.');
	utl_http.set_transfer_timeout( 5 );
	utl_http.set_proxy(p_proxy_i);
	utl_http.set_wallet(p_wallet_i, p_wallet_password_i);
	utl_http.set_response_error_check( enable => false );
	utl_http.set_detailed_excp_support( enable => true );

	begin --procedure receive_json(in_json in json) is
		dbms_lob.createtemporary(l_body_reqq, true);
		p_json_body_i.to_clob(l_body_reqq, false, dbms_lob.lobmaxsize);
		l_body_req := l_body_reqq;
		dbms_lob.freetemporary(l_body_reqq);
	end;

	dbms_lob.createtemporary(l_body_res, true);

	begin
		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Begin http request (content-type/json).');

		-- Iniitiate request	
		l_request := utl_http.begin_request( p_url_i, p_post_get_del_p, utl_http.HTTP_VERSION_1_1 );

		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Content length = '||to_char(length( l_body_req )));

		-- Set request headers
		utl_http.set_header( l_request, 'Content-Type', 'application/json; charset=utf-8' );
		utl_http.set_header( l_request, 'Content-Length', length( l_body_req ) );

		-- Only use authenticate key when not fetching authentication token
		if	p_url_i != l_authenticate_url
		then
			g_authenticate 	:= set_authenticate_key;
			utl_http.set_header( l_request, 'Authorization', 'Bearer ' || g_authenticate );
		end if;

		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Write request text');

		-- Write request
		<<write_request>>
		loop
			if 	length(l_body_req) = 0
			then
				exit write_request;
			end if;
			l_chunk := substr(l_body_req,1,30000);
			l_body_req := substr(l_body_req,30001);
			utl_http.write_text( l_request, l_chunk );
		end loop;
		-- Send request and get response
		l_response 		:= utl_http.get_response( r	=> l_request );		
		l_response_code 	:= l_response.status_code;
		p_response_reason_o	:= substr(l_response.reason_phrase,1,4000);

		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, ', response code = '|| l_response_code||', response phrase = '|| p_response_reason_o);

		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Start Reading response');

		-- Read response
		begin
			loop
				-- add log record
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Read line '|| to_char(l_loop_counter));

				l_loop_counter	:= l_loop_counter + 1;

				--clean temp text 
				l_body_text	:= null;

				-- Read next line
				utl_http.read_text(l_response ,l_body_text, 30000);

				--add line to clob
				dbms_lob.writeappend( l_body_res,length(l_body_text), l_body_text);
			end loop;
		exception
			when utl_http.END_OF_BODY
			then
				-- add log record
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Finished reading response.');
		end;			

		-- Check if response body is formatted as json
		if	check_json_format_f(l_body_res)
		then
			p_json_body_o 	:= pljson( l_body_res );
		else
			-- add log record
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Response not in Json format construct json');

			if	l_response_code = '200'
			and	p_url_i = cnl_util_pck.get_constant('CTO_CANCEL_SHIPMENT_WEBSERVICE_URL')
			then
				l_exception_body.put('message','Shipment successfully deleted');
				p_json_body_o 	:= l_exception_body;
			else			
				l_exception_body.put('reconstructedmessage',replace(replace(replace(l_body_res, chr(10),''),chr(11),''),chr(13),''));			
				p_json_body_o 	:= l_exception_body;
			end if;
		end if;

		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'return reponse code '|| l_response_code|| '.');
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Web service call ended successfuly.');

		-- End htto resposne
		utl_http.end_response( l_response );
		-- Clean up memeory from temporarye clob
		dbms_lob.freetemporary(l_body_res);
		-- Return reponse code
		return l_response_code;
	exception
		when utl_http.transfer_timeout 
		then
			dbms_lob.freetemporary(l_body_res);
			if	l_request.http_version 	!= null
			then
				utl_http.end_request( l_request );
			end if;
			if	l_response.http_version != null
			then
				utl_http.end_response( l_response );
			end if;	
			l_exception_body.put('message', 'timeout occured');
			p_json_body_o		:= l_exception_body;
			p_response_reason_o	:= 'Timeout occured';
			return '1009';
		when others 
		then
			dbms_lob.freetemporary(l_body_res);
			if	l_request.http_version != null
			then
				utl_http.end_request( l_request );
			end if;
			if	l_response.http_version != null
			then
				utl_http.end_response( l_response );
			end if;	

			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null					-- list of all parameters involved
							  , p_comments_i		=> 'Unhandled exception occured'	-- Additional comments describing the issue
							  );
			l_exception_body.put('message', 'unhandled exception occured');
			p_json_body_o		:= l_exception_body;
			p_response_reason_o	:= '-12535';
			return '1010';
	end;
end call_webservice_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Tracing enabled yes or no
------------------------------------------------------------------------------------------------
function set_cto_tracing_f
	return boolean
is
	l_text_data	dcsdba.system_profile.text_data%type;
	l_retval	boolean;
begin
	select	upper(text_data)
	into	l_text_data
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_CENTIRO_TRACING_CTOTRACING'
	;
	if	l_text_data = 'TRUE'
	then
		l_retval	:= true;
	else
		l_retval	:= false;
	end if;
	return l_retval;
exception
	when NO_DATA_FOUND
	then
		return false;
end set_cto_tracing_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Tracing enabled yes or no
------------------------------------------------------------------------------------------------
procedure set_cto_tracing_p
is
begin
	g_tracing	:= set_cto_tracing_f;
end set_cto_tracing_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Logging enabled yes or no
------------------------------------------------------------------------------------------------
function set_cto_logging_f
	return boolean
is
	l_text_data	dcsdba.system_profile.text_data%type;
	l_retval	boolean;
begin
	select	upper(text_data)
	into	l_text_data
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_CENTIRO_LOGGING_CTOLOGGING'
	;
	if	l_text_data = 'TRUE'
	then
		l_retval	:= true;
	else
		l_retval	:= false;
	end if;
	return l_retval;
exception
	when NO_DATA_FOUND
	then
		return false;
end set_cto_logging_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Logging enabled yes or no
------------------------------------------------------------------------------------------------
procedure set_cto_logging_p
is
begin
	g_logging	:= set_cto_logging_f;
end set_cto_logging_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Fetch authenticate key from Centiro web service
------------------------------------------------------------------------------------------------
procedure fetch_authenticate_key_p
is
	l_url			varchar2(1000)	:= cnl_util_pck.get_constant('CTO_AUTHENTICATE_WEBSERVICE_URL');
	l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_user_name		varchar2(30)	:= cnl_util_pck.get_constant('CTO_AUTHENTICATE_USER_NAME');
	l_password		varchar2(30)	:= cnl_util_pck.get_constant('CTO_AUTHENTICATE_PASSW');
	l_wallet		varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw		varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');
	l_response_code		varchar2(30);	
	l_request		pljson := pljson();
	l_response_body		pljson;
	l_err			varchar2(4000);	
	l_key			varchar2(200);
	l_rtn			varchar2(30) := 'fetch_authenticate_key_p';
	l_trace_key 		integer;
	l_attempt		integer	:= 0;
begin
	-- add log record
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start fetching authentification key.'
						 );

	-- build json request
	l_request.put( 'userName'	, l_user_name);
	l_request.put( 'password'	, l_password);

	-- add web servce tace
	cnl_cto_pck.create_cto_trace_record( l_request, null, null, l_rtn, null, l_trace_key);

	<<call_webservice>>
	while 	l_attempt < 5
	loop
		l_attempt	:= l_attempt + 1;

		-- Replace with UTIL package later!!
		l_response_code := call_webservice_f( p_url_i			=> l_url
						    , p_proxy_i			=> l_proxy
						    , p_user_name_i		=> l_user_name
						    , p_password_i		=> l_password
						    , p_wallet_i		=> l_wallet
						    , p_wallet_password_i	=> l_wall_passw
						    , p_post_get_del_p		=> 'POST'
						    , p_json_body_i		=> l_request
						    , p_json_body_o		=> l_response_body
						    , p_response_reason_o	=> l_err
						    );

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Attempt '|| to_char(l_attempt) ||': http response = '|| l_response_code || ', '|| l_err
							 );

		-- add web servce tace
		cnl_cto_pck.create_cto_trace_record( null, l_response_body, l_response_code, l_rtn, l_trace_key, l_trace_key);

		if	l_response_code = '200'
		then
			-- Extract key from string
			l_key	:= pljson_ext.get_string(l_response_body, 'authenticationTicket');

			-- Insert new key
			update cnl_sys.cnl_cto_authenticate_key
			set 	authenticate_key	= l_key
			,	attempt			= 1
			;
			commit;

			-- add log record
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'New authenticate key succesfully saved.'
								 );
			exit call_webservice;
		else
			update cnl_sys.cnl_cto_authenticate_key
			set 	attempt			= l_attempt
			;
			commit;
			-- add log record
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Failed to fetch new authenticate key during attempt '||to_char(l_attempt) ||'.'
								 );
			-- Wait 10 seconds then try again.
			dbms_lock.sleep(10);
		end if;
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
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR.'
							 );
end fetch_authenticate_key_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Create trace record
------------------------------------------------------------------------------------------------
procedure create_cto_trace_record( p_request_i		in pljson 	default null
				 , p_response_i		in pljson 	default null
				 , p_status_code_i	in varchar2 	default null
				 , p_web_service_name_i	in varchar2
				 , p_key_i		in integer 	default null
				 , p_key_o		out integer
				 )
is
	l_body		clob;
	l_rtn		varchar2(30) := 'create_cto_trace_record';
	l_key		integer;
	pragma		autonomous_transaction;
begin
	set_cto_tracing_p;
	dbms_lob.createtemporary(l_body, false);
	if	g_tracing
	then
		if	p_response_i is null
		then
			p_request_i.to_clob(l_body,  false );
			l_key 	:= cnl_cto_webservice_body_seq1.nextval;
			p_key_o	:= l_key;

			insert
			into	cnl_cto_webservice_body
			(	key
			,	dstamp
			,	request
			,	response
			,	status_code
			,	web_service_name
			)
			values
			(	l_key
			,	sysdate
			,	l_body
			,	null
			,	null
			,	p_web_service_name_i
			)
			;
		else
			p_response_i.to_clob(l_body,  false );

			update	cnl_cto_webservice_body
			set	response 	= l_body
			,	status_code	= p_status_code_i
			where	key 		= p_key_i
			;
		end if;
		commit;
	end if;
	dbms_lob.freetemporary(l_body);	
exception
	when others
        then
		dbms_lob.freetemporary(l_body);	

		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		commit;
end create_cto_trace_record;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Create or update monitoring log record
------------------------------------------------------------------------------------------------
procedure print_monitoring_log_record( p_run_task_key_i			in integer
				     , p_add_or_update_i		in varchar2 -- A or U
				     , p_parcel_id_i			in varchar2 default null
				     , p_shipment_id_i			in varchar2 default null
				     , p_order_id_i			in varchar2 default null
				     , p_client_id_i			in varchar2 default null
				     , p_run_task_creation_i		in timestamp with local time zone default null
				     , p_procedure_start_i		in timestamp with local time zone default null
				     , p_parcel_details_fetched_i	in timestamp with local time zone default null
				     , p_call_webservice_i		in timestamp with local time zone default null
				     , p_webservice_response_i		in timestamp with local time zone default null
				     , p_update_wms_i			in timestamp with local time zone default null
				     , p_send_to_printer_i		in varchar2 default null-- Y or N
				     , p_finished_i			in varchar2 default null-- Y or N
				     )
is
	l_rtn		varchar2(30) := 'print_monitoring_log_record';
	pragma		autonomous_transaction;
begin
	if 	p_add_or_update_i = 'A'
	then
		insert 
		into 	cto_print_performance_log
		(run_task_key, parcel_id, shipment_id, order_id, run_task_creation, procedure_start, parcel_details_fetched, call_webservice, webservice_response, update_wms, send_to_printer, finished)
		values
		(	p_run_task_key_i, p_parcel_id_i, p_shipment_id_i, p_order_id_i, p_run_task_creation_i, p_procedure_start_i, p_parcel_details_fetched_i, p_call_webservice_i, p_webservice_response_i, p_update_wms_i, decode(p_send_to_printer_i,'Y',sysdate,null), decode(p_finished_i,'Y',sysdate,null))
		; 
	else
		update	cto_print_performance_log
		set 	run_task_creation 	= nvl(p_run_task_creation_i, run_task_creation)
		,	procedure_start		= nvl(p_procedure_start_i, procedure_start)
		, 	parcel_details_fetched	= nvl(p_parcel_details_fetched_i, parcel_details_fetched)
		, 	call_webservice		= nvl(p_call_webservice_i, call_webservice)
		, 	webservice_response	= nvl(p_webservice_response_i, webservice_response)
		, 	update_wms		= nvl(p_update_wms_i, update_wms)
		, 	send_to_printer		= nvl(decode(p_send_to_printer_i,'Y',sysdate,null), send_to_printer)
		,	finished		= nvl(decode(p_finished_i,'Y',sysdate,null), finished)
		,	order_id		= nvl(p_order_id_i, order_id)
		, 	shipment_id		= nvl(p_shipment_id_i, shipment_id)
		,	parcel_id 		= nvl(p_parcel_id_i, parcel_id)
		where	run_task_key 		= p_run_task_key_i
		;
	end if;
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
		commit;
end print_monitoring_log_record;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Create log record
------------------------------------------------------------------------------------------------
procedure create_cto_log_record( p_source_i         in varchar2
			       , p_description_i    in varchar2 
			       )
is
	l_rtn		varchar2(30) := 'create_cto_log_record';
	pragma		autonomous_transaction;
begin
	set_cto_logging_p;

	if 	g_logging
	then
		insert 
		into 	cnl_cto_log
		( 	dstamp
		, 	source
		, 	description
		)
		values
		(	sysdate
		, 	substr(p_source_i,1,100)
		, 	substr(p_description_i,1,4000)
		);
		commit;
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
		commit;
end create_cto_log_record;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Centiro ACSS / Add shipment
------------------------------------------------------------------------------------------------
procedure centiro_acss_addshp_p( p_site_id_i	dcsdba.site.site_id%type
			       , p_cs_force_i	varchar2
			       )
is
	l_rtn		varchar2(30) 	:= 'centiro_acss_addshp_p';
	l_acss		varchar2(10)	:= cnl_sys.cnl_util_pck.get_system_profile_f( '-ROOT-_USER_CENTIRO_ACSSENABLED_ACSSENABLED');
begin
	if	l_acss 		= 'TRUE'
	or 	p_cs_force_i 	= 'Y'
	then
		execute immediate 'alter session set time_zone = ''Europe/Amsterdam''';

		cnl_sys.cnl_cto_addshipment_pck.fetch_orders_addship_p(p_site_id_i, p_cs_force_i);
		commit;
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
		commit;
end centiro_acss_addshp_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 03/05/2021
-- Purpose : Add / print parcel
------------------------------------------------------------------------------------------------
procedure centiro_add_print_parcels_p( p_site_id_i	in dcsdba.site.site_id%type
				     , p_client_id_i    in dcsdba.client.client_id%type
				     , p_order_id_i	in dcsdba.order_header.order_id%type 		default null
				     , p_pallet_id_i	in dcsdba.order_container.pallet_id%type	default null
				     , p_container_id_i	in dcsdba.order_container.container_id%type	default null
				     , p_rtk_key_i	in dcsdba.run_task.key%type
				     , p_workstation_i	in dcsdba.workstation.station_id%type
				     )
is
	-- Fetch shipent id when no order id in place
	cursor	c_shp_id
	is
		select	distinct 
			o.uploaded_ws2pc_id
		from	dcsdba.order_header o
		inner
		join	dcsdba.order_container c
		on	o.order_id	= c.order_id
		and	o.client_id	= c.client_id
		and	(	(	c.pallet_id	= p_pallet_id_i
				or 	p_pallet_id_i 	is null
				)
		or	(	c.container_id		= p_container_id_i
			or 	p_container_id_i 	is null
				)
			)
		and	rownum = 1
	;

	cursor c_ord(b_shipment_id number)
	is
		select	o.order_id
		from	dcsdba.order_header o
		where	o.uploaded_ws2pc_id = b_shipment_id
		and	(
			select	count(order_id)
			from	dcsdba.order_header o
			where	o.uploaded_ws2pc_id = b_shipment_id
			) = 1
	;

	-- fetch printers
	cursor c_prt(b_order_id varchar2, b_client_id varchar2)
	is
		select 	e.export_target
		,	m.client_id
		,	m.copies	map_copies
		,	e.copies	export_copies
		from 	dcsdba.java_report_export e
		inner
		join	dcsdba.java_report_map m
		on 	m.report_name	= 'UREPCTOPACKPARCEL' 
		and 	m.station_id 	= p_workstation_i
		and	(nvl(m.client_id,'X') 	= nvl(b_client_id,'X') or m.client_id is null)
		and	(m.site_id 	= p_site_id_i 	or p_site_id_i is null)
		and	m.key 		= e.key
		and	(	(	b_order_id	is not null
				and	b_client_id 	is not null
				and	1 		= cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i   => b_client_id
											      , p_order_id_i    => b_order_id
											      , p_where_i       => nvl(m.extra_parameters, '1=1')
											      )
				)
			or	(	(	b_client_id 	is null
					or	b_order_id 	is null
					)
				and	m.extra_parameters is null
				)
			)
		order 
		by	e.export_target
		,	m.client_id nulls last
	;

	cursor 	c_rtk
	is
		select 	old_dstamp
		from	dcsdba.run_task
		where	key = p_rtk_key_i
	;

	l_rtn		varchar2(30) := 'centiro_add_print_parcels_p';
	l_shipment_id	dcsdba.order_header.uploaded_ws2pc_id%type;
	l_printers	varchar2(200);
	l_copies 	varchar2(50);
	l_dws		varchar2(1);
	l_order_id	dcsdba.order_header.order_id%type;
	l_pallet_id	dcsdba.order_container.pallet_id%type;
	l_container_id	dcsdba.order_container.container_id%type;
	l_client_id	dcsdba.client.client_id%type;
	l_station	dcsdba.workstation.station_id%type;
	l_error		varchar2(1) := 'N';
	l_rtk_time	timestamp with local time zone;
	l_shp_closed	varchar2(1);
	l_ok		varchar2(1)	:= 'Y';
	l_dif_carrier	varchar2(1)	:= 'N';
	l_new_rt_key	integer;
	l_to_mail	varchar2(250);
	l_from_mail	varchar2(250);
	l_carrier_id	dcsdba.order_header.carrier_id%type;
	l_service_lvl	dcsdba.order_header.service_level%type;
	l_new_shp_id	number;
	l_printer	varchar2(50);
begin
	execute immediate 'alter session set time_zone = ''Europe/Amsterdam''';

	-- Set DB environment
	set_database_f;

	open	c_rtk;
	fetch	c_rtk into l_rtk_time;
	close	c_rtk;

	-- add monitoring log record
	print_monitoring_log_record( p_run_task_key_i			=> p_rtk_key_i
				   , p_add_or_update_i			=> 'A'
				   , p_parcel_id_i			=> null
				   , p_shipment_id_i			=> null
				   , p_order_id_i			=> p_order_id_i
				   , p_client_id_i			=> p_client_id_i
				   , p_run_task_creation_i		=> l_rtk_time
				   , p_procedure_start_i		=> sysdate
				   , p_parcel_details_fetched_i		=> null
				   , p_call_webservice_i		=> null
				   , p_webservice_response_i		=> null
				   , p_update_wms_i			=> null
				   , p_send_to_printer_i		=> null
				   , p_finished_i			=> null
				   );

	if	p_workstation_i 	is null
	then
		l_dws	:= 'N';
	else
		begin
			-- is linked to DWS Y/N
			select 	nvl(user_def_chk_1,'N') linked_to_dws
			into	l_dws
			from   	dcsdba.workstation
			where  	station_id = p_workstation_i
			;
		exception
			when NO_DATA_FOUND
			then
				-- add log record
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Could not find the workstation '
									 || p_workstation_i
									 || ' in WMS while processing order '
									 || p_order_id_i
									 || ' from client id '
									 || p_client_id_i
									 || ' can''t continue processing.'
									 );	
				l_error	:= 'Y';
		end;
	end if;

	if	l_error = 'N'
	then
		-- Fetch shipment id
		if	p_order_id_i is null
		or	p_order_id_i = 'XXXXX'
		then
			open	c_shp_id;
			fetch	c_shp_id
			into	l_shipment_id;
			close	c_shp_id;
		else
			select	o.uploaded_ws2pc_id
			into	l_shipment_id
			from	dcsdba.order_header o
			where	o.order_id 	= p_order_id_i
			and	o.client_id	= p_client_id_i
			and	o.from_site_id	= p_site_id_i
			;			
		end if;		

		-- A shipment can be multi order. So when more than one order exists for a shipment id ordre number is left empty.
		if	p_order_id_i is null
		or 	p_order_id_i = 'XXXXX'
		then	
			open	c_ord(l_shipment_id);
			fetch	c_ord
			into	l_order_id;
			if	c_ord%notfound
			then
				l_order_id	:= null;
			end if;
			close	c_ord; 
		else
			l_order_id	:= p_order_id_i;
		end if;

		if	p_client_id_i = 'XXXXX'
		then	
			l_client_id	:= null;
		else
			l_client_id	:= p_client_id_i;
		end if;

		-- fetch printer and nbr of copies
		for	i in c_prt(l_order_id, l_client_id)
		loop
			if 	l_printer is null
			then
				l_printer := i.export_target;
			elsif	l_printer = i.export_target
			then
				continue;
			end if;

			if 	l_printers is null
			then
				l_printers := i.export_target;
			else
				l_printers := l_printers||','||i.export_target;
			end if;
			if	( i.export_copies is null or i.export_copies = 0)
			and	( i.map_copies 	is null or i.map_copies = 0)
			then
				if	l_copies is null
				then
					l_copies 	:= '1';
				else
					l_copies	:= l_copies||',1';
				end if;
			elsif	i.export_copies is not null 
			and 	i.export_copies > 0
			then
				if	l_copies is null
				then			
					l_copies 	:= to_char(i.export_copies);
				else
					l_copies 	:= l_copies||','||to_char(i.export_copies);
				end if;
			else
				if 	l_copies is null
				then
					l_copies	:= to_char(nvl(i.map_copies,1));
				else
					l_copies 	:= l_copies||','||to_char(nvl(i.map_copies,1));
				end if;
			end if;
		end loop;

		l_printer := null;

		if	p_pallet_id_i = 'XXXXX'
		then	
			l_pallet_id	:= null;
		else
			l_pallet_id	:= p_pallet_id_i;
		end if;
		if	p_container_id_i = 'XXXXX'
		then	
			l_container_id	:= null;
		else
			l_container_id	:= p_container_id_i;
		end if;
		if	p_workstation_i = 'XXXXX'
		then	
			l_station	:= null;
		else
			l_station	:= p_workstation_i;
		end if;

		-- Continue	
		cnl_sys.cnl_cto_parcel_pck.add_print_parcels_p( p_site_id_i		=> p_site_id_i
							      , p_client_id_i    	=> p_client_id_i
							      , p_order_id_i		=> l_order_id
							      , p_pallet_id_i		=> l_pallet_id
							      , p_container_id_i	=> l_container_id
							      , p_shipment_id_i		=> l_shipment_id
							      , p_rtk_key_i		=> p_rtk_key_i
							      , p_printer_i      	=> l_printers
							      , p_copies_i       	=> l_copies
							      , p_dws_i			=> l_dws
							      ,	p_station_id_i		=> l_station
							      , p_shp_closed_o		=> l_shp_closed
							      );
	end if;

	if	l_shp_closed = 'Y'
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Shipment '||to_char(l_shipment_id)||' is closed. Start fetching original carrier information.'
							 );

		begin
			select	o.carrier_id
			,	o.service_level
			into	l_carrier_id
			,	l_service_lvl
			from	dcsdba.order_header o
			where	(o.order_id = l_order_id or l_order_id is null)
			and	o.uploaded_ws2pc_id = l_shipment_id
			and	rownum = 1
			;
		exception
			when others 
			then 
				null;
		end;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Fetched carrier '||l_carrier_id||' and service level '||l_service_lvl||' from original shipment '||to_char(l_shipment_id)||'.'
							 );

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Inserting record in cnl_cto_shp_closed_early_log. parameters (Site: '||p_site_id_i||',client: '||p_client_id_i||',order: '||l_order_id||',old shipment id: '||l_shipment_id||',old carriert id: '||l_carrier_id||',old service: '||l_service_lvl||').'
							 );

		insert into cnl_cto_shp_closed_early_log
		values 
		(	p_site_id_i
		,	p_client_id_i
		,	l_order_id
		,	l_shipment_id
		,	null
		,	l_carrier_id
		,	null
		,	l_service_lvl
		,	null
		,	sysdate
		,	'X'
		,	null
		);

		if 	p_site_id_i = 'NLSBR01'
		then
			begin
				select 	value 
				into 	l_to_mail
				from 	cnl_constants 
				where 	name = 'CTO_SHP_CLOSED_EMAIL_SON'
				;
				select 	value 
				into 	l_from_mail
				from 	cnl_constants 
				where 	name = 'CTO_SHIPMENT_CLOSED_FROM_EMAIL'
				;
			exception 
				when others then null;
			end;
		else
			begin
				select 	value 
				into 	l_to_mail
				from 	cnl_constants 
				where 	name = 'CTO_SHP_CLOSED_EMAIL_TLB'
				;
				select 	value 
				into 	l_from_mail
				from 	cnl_constants 
				where 	name = 'CTO_SHIPMENT_CLOSED_FROM_EMAIL'
				;
			exception 
				when others then null;
			end;
		end if;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Shipment '||to_char(l_shipment_id)||' is closed. Sending email to notify shipment is closed.'
							 );
		declare
			v_from 	varchar2(50)	:= l_from_mail;
			v_to	varchar2(250) 	:= l_to_mail; 
			v_env	varchar2(400);
		begin
			if	g_database = 'DEVCNLJW'
			then	
				v_env	:= 'MAIL GENERATED FROM DEVELOPMENT. ';
			elsif 	g_database = 'TSTCNLJW'
			then
				v_env	:= 'MAIL GENERATED FROM TEST. ';
			elsif 	g_database = 'ACCCNLJW'
			then
				v_env	:= 'MAIL GENERATED FROM ACCEPTANCE. ';
			else
				v_env := null;
			end if;


			cnl_api_pck.send_email( p_email_from_i      => v_from
					      , p_email_to_i        => v_to
					      , p_subject_i         => 'Shipment '||to_char(l_shipment_id)||' for order '||l_order_id||' already closed'
					      , p_message_body_i    => v_env||'This mail is send because an attempts was made to add a parcel to shipment '||to_char(l_shipment_id)||' for order '||l_order_id||'. This shipment is already closed.' 
					      , p_attachment_name_i => null
					      , p_attachment_file_i => null
					      );
		exception
			when others 
			then 
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Sending mail to notify about closed shipment '||to_char(l_shipment_id)||' failed.'
									 );

		end;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Start add shipment to create additional shipment for old shipment '||to_char(l_shipment_id)||'.'
							 );

		cnl_sys.cnl_cto_addshipment_pck.Shipment_closed_p( p_shipment_id_i 	=> l_shipment_id
								 , p_order_id_i		=> l_order_id
								 , p_client_id_i	=> p_client_id_i
								 , p_dif_carrier_o	=> l_dif_carrier
								 , p_carrier_id_o	=> l_carrier_id
								 , p_service_level_o	=> l_service_lvl
								 , p_new_shp_id_o	=> l_new_shp_id
								 , p_ok_o		=> l_ok
								 );
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Additional shipment returned parameters (new carrier id: '||l_carrier_id||',new service: '||l_service_lvl||',old shipment id: '||to_char(l_shipment_id)||',new shipment id: '||l_new_shp_id||',OK yes or no: '||l_ok||'.'
							 );

		if	l_ok = 'N'
		or	l_dif_carrier = 'Y'
		then
			if 	l_ok = 'N'
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'update cnl_cto_shp_closed_log with parameters (Created: N,reason: Centiro error, new carrier id: '||l_carrier_id||',new service: '||l_service_lvl||',old shipment id: '||to_char(l_shipment_id)||',new shipment id: '||l_new_shp_id||',OK yes or no: '||l_ok||'.'
									 );

				update 	cnl_cto_shp_closed_early_log
				set	new_shipment_id = l_new_shp_id
				,	new_carrier_id = l_carrier_id
				,	new_service_level = l_service_lvl
				,	created = 'N'
				,	reason = 'Centiro error'
				where	site_id = p_site_id_i
				and	client_id = p_client_id_i
				and	order_id = l_order_id
				and	old_shipment_id = l_shipment_id
				;
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'update cnl_cto_shp_closed_log with parameters (Created: N,reason: Add shipment returned different carrier or service for order, new carrier id: '||l_carrier_id||',new service: '||l_service_lvl||',old shipment id: '||to_char(l_shipment_id)||',new shipment id: '||l_new_shp_id||',OK yes or no: '||l_ok||'.'
									 );

				update 	cnl_cto_shp_closed_early_log
				set	new_shipment_id = l_new_shp_id
				,	new_carrier_id = l_carrier_id
				,	new_service_level = l_service_lvl
				,	created = 'N'
				,	reason = 'Add shipment returned different carrier or service for order'
				where	site_id = p_site_id_i
				and	client_id = p_client_id_i
				and	order_id = l_order_id
				and	old_shipment_id = l_shipment_id
				;

			end if;


			declare
				cursor c_get_address
				is
					select	user_def_note_2
					from	dcsdba.client
					where	client_id = p_client_id_i
				;

				v_from 		varchar2(50)	:= l_from_mail;
				v_to		varchar2(250) 	:= l_to_mail; 
				v_address	varchar2(250);		
				v_env		varchar2(400);
			begin
				if	g_database = 'DEVCNLJW'
				then	
					v_env	:= 'MAIL GENERATED FROM DEVELOPMENT. ';
				elsif 	g_database = 'TSTCNLJW'
				then
					v_env	:= 'MAIL GENERATED FROM TEST. ';
				elsif 	g_database = 'ACCCNLJW'
				then
					v_env	:= 'MAIL GENERATED FROM ACCEPTANCE. ';
				else
					v_env := null;
				end if;

				open	c_get_address;
				fetch 	c_get_address
				into	v_address;
				close	c_get_address;

				if	v_to is not null 
				and 	v_address is not null
				then
					v_to := v_to||','||v_address;
				elsif	v_to is null
				and 	v_address is not null
				then
					v_to := v_address;
				end if;

				if	l_ok = 'N'
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Creating new shipment for old closed Shipment '||to_char(l_shipment_id)||' failed. Sending email to notify that creation of new shipment failed.'
										 );

					cnl_api_pck.send_email( p_email_from_i      => v_from
							      , p_email_to_i        => v_to
							      , p_subject_i         => 'Shipment '||to_char(l_shipment_id)||' for order '||l_order_id||' already closed'
							      , p_message_body_i    => v_env||'This mail is send because an attempts was made to add a parcel to shipment '||to_char(l_shipment_id)||' for order '||l_order_id||'. This shipment is already closed and the creation of a new shipment failed.' 
							      , p_attachment_name_i => null
							      , p_attachment_file_i => null
							      );
				else
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Creating new shipment returned a different carrier for Shipment '||to_char(l_shipment_id)||' trouble shooting needed. Sending email to notify.'
										 );

					cnl_api_pck.send_email( p_email_from_i      => v_from
							      , p_email_to_i        => v_to
							      , p_subject_i         => 'Shipment '||to_char(l_shipment_id)||' for order '||l_order_id||' already closed'
							      , p_message_body_i    => v_env||'This mail is send because an attempts was made to add a parcel to shipment '||to_char(l_shipment_id)||' for order '||l_order_id||'. This shipment is already closed and the creation of a new shipment failed because the new shipment received a different carrier and service combination.' 
							      , p_attachment_name_i => null
							      , p_attachment_file_i => null
							      );
				end if;					
			exception
				when others 
				then 
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Sending mail to notify about failed new shipment '||to_char(l_shipment_id)||' failed.'
										 );

			end;

			-- adding error label
			insert
			into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
						   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
						   , update_dstamp, printer_name, dws, copies, shp_label_base64
						   )
			values
			(	nvl(p_client_id_i,'NOCLIENT')
			,	nvl(p_site_id_i,'NOSITE')
			,	p_order_id_i
			,	l_shipment_id
			,	nvl(p_container_id_i,nvl(p_pallet_id_i,'NOPARCELID'))
			,	p_pallet_id_i
			,	p_container_id_i
			,	null
			,	null
			,	null
			,	null
			,	null
			,	p_rtk_key_i
			,	null
			,	null
			,	cnl_cto_pck.create_zpl_text_label_f( 'Shipment  '
								  || to_char(l_shipment_id)
								  || ' is already closed and failed to create a new shipment.'
								  || ' Contact your support for further steps.'
								  )
			,	null
			,	null
			,	null
			,	null
			,	null
			,	sysdate
			,	'Error'
			,	null
			,	substr(l_printers,1,instr(l_printers,',')-1)
			,	l_dws
			,	1
			,	cnl_cto_parcel_pck.base64decode( cnl_cto_pck.create_zpl_text_label_f( 'Shipment  '
													|| to_char(l_shipment_id)
													|| ' is already closed and failed to create a new shipment.'
													|| ' Contact your support for further steps.'
													)

								    , 'Y'
								    )
			);
		else
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'update cnl_cto_shp_closed_log with parameters (Created: Y, new carrier id: '||l_carrier_id||',new service: '||l_service_lvl||',old shipment id: '||to_char(l_shipment_id)||',new shipment id: '||l_new_shp_id||',OK yes or no: '||l_ok||'.'
								 );
			update 	cnl_cto_shp_closed_early_log
			set	new_shipment_id = l_new_shp_id
			,	new_carrier_id = l_carrier_id
			,	new_service_level = l_service_lvl
			,	created = 'Y'
			where	site_id = p_site_id_i
			and	client_id = p_client_id_i
			and	order_id = l_order_id
			and	old_shipment_id = l_shipment_id
			;

			l_new_rt_key	:= dcsdba.run_task_pk_seq.nextval;

			insert
			into 	dcsdba.run_task
			(	key, site_id, station_id, user_id, status, command, old_dstamp, dstamp, language, name, time_zone_name, nls_calendar, client_id, master_key, use_db_time_zone)
			select	l_new_rt_key
			,	site_id
			,	station_id
			,	user_id
			,	'Pending'
			,	'$HOME/reports/CentiroSaas/cto_add_print_parcel.sh -S "'||site_id||'" -C "'||p_client_id_i||'" -P "'||p_pallet_id_i||'" -c "'||p_container_id_i||'" -O "'||p_order_id_i||'" -R "'||to_char(l_new_rt_key)||'" -W "'||p_workstation_i||'" -T "addandprint"'
			,	sysdate
			, 	sysdate
			,	language
			,	name
			,	time_zone_name
			,	nls_calendar
			,	client_id
			,	l_new_rt_key
			,	use_db_time_zone
			from	dcsdba.run_task
			where	key	= p_rtk_key_i
			;
		end if;			
	end if;
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
		commit;
end centiro_add_print_parcels_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Get zpl label Convert clob to varchar
------------------------------------------------------------------------------------------------
function get_label_file_f( p_shipment_id_i	in varchar2
			 , p_parcel_id_i	in varchar2
			 , p_run_task_key_i	in integer
			 )
	return clob
is
	l_retval	varchar2(32767);
begin
	l_retval	:= cnl_cto_parcel_pck.get_label_file_f( p_shipment_id_i	=> p_shipment_id_i
					 		      , p_parcel_id_i	=> p_parcel_id_i
							      , p_run_task_key_i=> p_run_task_key_i
							      );
	return l_retval;
end get_label_file_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 29/07/2021
-- Purpose : force carrier update while order is already processed
------------------------------------------------------------------------------------------------
procedure force_carrier_update_p( p_site_id_i 		dcsdba.site.site_id%type
			        , p_client_id_i		dcsdba.client.client_id%type
			        , p_printer_i		varchar2
			        , p_order_id_i		dcsdba.order_header.order_id%type		default null
			        , p_shipment_id_i	varchar2					default null
			        , p_rtk_key_i		dcsdba.run_task.key%type
			        )
is
	-- Check if there are saved instructions
	cursor	c_instructions
	is
		select 	i.instructions
		from	cnl_sys.cnl_ohr_instructions i
		where	i.order_id 	= p_order_id_i
		and	i.client_id	= p_client_id_i
		and	i.site_id	= p_site_id_i
	;
	-- Fetch instructions from order header to define if they need to be saved.
	cursor	c_ord_instructions
	is
		select 	o.instructions
		from	dcsdba.order_header o
		where	o.order_id 	= p_order_id_i
		and	o.client_id	= p_client_id_i
		and	o.from_site_id	= p_site_id_i
	;
	-- Check if label printer is in use on application\
	cursor c_printer_exists
	is
		select	count(*)
		from 	dcsdba.java_report_export e
		where	export_target = p_printer_i
	;

	l_shipment_id	dcsdba.order_header.uploaded_ws2pc_id%type;
	l_order_id	dcsdba.order_header.order_id%type;
	l_instruction	cnl_sys.cnl_ohr_instructions.instructions%type;
	l_exists	integer;
	l_rtn		varchar2(30) 	:= 'forced_carrier_swap_p';
begin
	-- Add log record
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start forced carrier update with parameters: '
						 || 'Site id '		|| p_site_id_i
						 || ', client id '	|| p_client_id_i
						 || ', printer '	|| p_printer_i
						 || ', order id '	|| p_order_id_i
						 || ', shipment id '	|| p_shipment_id_i
						 || ', run task key '	|| p_rtk_key_i
						 );

	-- When no printer is defined we can't print labels and so forced carrier update can not be executed.
	if	p_printer_i is null
	or	p_printer_i = 'XXXXX'
	then
		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No printer defined for forced carrier update on order '
							 || p_order_id_i
							 || ' from client id '
							 || p_client_id_i
							 || ' can''t continue processing.'
							 );
		-- Save original instructions so we can make space for the error message
		if	p_order_id_i is not null
		and	p_order_id_i != 'XXXXX'
		then
			open 	c_instructions;
			fetch	c_instructions
			into 	l_instruction;
			if	c_instructions%found
			then		
				close	c_instructions;

				-- add log record
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Original instructions already saved. Now update order '
									 || p_order_id_i
									 || ' from client id '
									 || p_client_id_i
									 || ' with error message.'
									 );
				-- Update order with error message 
				update	dcsdba.order_header o
				set	o.instructions 		= 'CTOMSG# printer is required when running carrier force update using WMS report'
				,	o.status_reason_code 	= 'CSERROR'
				,	o.status 		= 'Hold'
				where	o.order_id 		= p_order_id_i 
				and	o.client_id 		= p_client_id_i
				and	o.from_site_id 		= p_site_id_i
				;
				commit;
			else				
				close	c_instructions;

				-- Check if order contains an instruction
				open	c_ord_instructions;
				fetch	c_ord_instructions
				into	l_instruction;
				close	c_ord_instructions;

				if	l_instruction is not null
				and	l_instruction not like 'CTOMSG#%'
				then
					-- add log record
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Order '
										 || p_order_id_i
										 || ' from client '
										 || p_client_id_i
										 || ' contains an instruction. Saving instruction so it can be restored when shipment is created succesfully.'
										 );

					-- Save original instructions so it can be placed back when shipment is created succesfully
					insert
					into	cnl_sys.cnl_ohr_instructions
					(	site_id	
					,	client_id
					,	order_id
					,	instructions
					)
					values
					(	p_site_id_i
					,	p_client_id_i
					,	p_order_id_i
					,	l_instruction
					)
					;
				end if;	

				-- add log record
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Update order '
									 || p_order_id_i
									 || ' from client '
									 || p_client_id_i 
									 || ' with error message.'
									 );

				-- Update order with error message
				update	dcsdba.order_header o
				set	o.instructions 		= 'CTOMSG# printer is required when running carrier force update using WMS report'
				,	o.status_reason_code 	= 'CSERROR'
				,	o.status 		= 'Hold'
				where	o.order_id 		= p_order_id_i 
				and	o.client_id 		= p_client_id_i
				and	o.from_site_id 		= p_site_id_i
				;
				commit;
			end if;
		elsif	p_shipment_id_i is not null
		and 	p_shipment_id_i != 'XXXXX'
		then
			null;
			-- Can't update all orders linked to a single shipment id
		end if;			
	else
		-- Check if provided printer exists. When it does not exist we can't print labels
		open 	c_printer_exists;
		fetch	c_printer_exists
		into	l_exists;
		close	c_printer_exists;

		-- When printer does not exist in advanced print mapping we can't print.
		if	l_exists = 0
		then
			-- add log record
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'The selected printer '
								 || p_printer_i
								 || ' for forced carrier update on order '
								 || p_order_id_i
								 || ' from client '
								 || p_client_id_i
								 || ' does not exist in advanced print mapping so process must be stopped.'
								 );

			-- Save original instructions so it can be placed back when shipment is created succesfully.
			if	p_order_id_i is not null
			and	p_order_id_i != 'XXXXX'
			then
				open 	c_instructions;
				fetch	c_instructions
				into 	l_instruction;
				if	c_instructions%found
				then
					close	c_instructions;

					-- add log record
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Original instructions already saved. Now update order '
										 || p_order_id_i
										 || ' from client id '
										 || p_client_id_i
										 || ' with error message.'
										 );

					-- Update order with error message
					update	dcsdba.order_header o
					set	o.instructions 		= 'CTOMSG# provided printer does not exist in advanced print mapping.'
					,	o.status_reason_code 	= 'CSERROR'
					,	o.status 		= 'Hold'
					where	o.order_id 		= p_order_id_i 
					and	o.client_id 		= p_client_id_i
					and	o.from_site_id 		= p_site_id_i
					;
					commit;
				else
					close	c_instructions;

					-- Check if order contains an instruction
					open	c_ord_instructions;
					fetch	c_ord_instructions
					into	l_instruction;
					close	c_ord_instructions;

					if	l_instruction is not null
					and	l_instruction not like 'CTOMSG#%'
					then
						-- add log record
						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
											 , 'Order '
											 || p_order_id_i
											 || ' from client '
											 || p_client_id_i
											 || ' contains an instruction. Saving instruction so it can be restored when shipment is created succesfully.'
											 );

						-- Save original instructions so it can be placed back when shipment is created succesfully
						insert
						into	cnl_sys.cnl_ohr_instructions
						(	site_id	
						,	client_id
						,	order_id
						,	instructions
						)
						values
						(	p_site_id_i
						,	p_client_id_i
						,	p_order_id_i
						,	l_instruction
						)
						;
					end if;	

					-- add log record
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Update order '
										 || p_order_id_i
										 || ' from client '
										 || p_client_id_i 
										 || ' with error message.'
										 );

					-- Update order with error message
					update	dcsdba.order_header o
					set	o.instructions 		= 'CTOMSG# provided printer does not exist in advanced print mapping.'
					,	o.status_reason_code 	= 'CSERROR'
					,	o.status 		= 'Hold'
					where	o.order_id 		= p_order_id_i 
					and	o.client_id 		= p_client_id_i
					and	o.from_site_id 		= p_site_id_i
					;
					commit;
				end if;
			elsif	p_shipment_id_i is not null
			and 	p_shipment_id_i != 'XXXXX'
			then
				null;
				-- Can't update all orders linked to a single shipment id
			end if;			

		else
			-- Replace XXXXXX with null. Originally it was null repleced with XXXXX for shel script
			if	p_shipment_id_i	= 'XXXXX'
			then
				l_shipment_id := null;
			else
				l_shipment_id := to_number(p_shipment_id_i);
			end if;

			-- Replace XXXXXX with null. Originally it was null repleced with XXXXX for shel script
			if	p_order_id_i	= 'XXXXX'
			then
				l_order_id := null;
			else
				l_order_id := p_order_id_i;
			end if;

			-- Start 2nd step 
			cnl_sys.cnl_cto_addshipment_pck.force_carrier_update_p( p_site_id_i	=> p_site_id_i
									      , p_client_id_i	=> p_client_id_i
									      , p_printer_i	=> p_printer_i
									      , p_order_id_i	=> l_order_id
									      , p_shipment_id_i	=> l_shipment_id
									      , p_rtk_key_i	=> p_rtk_key_i
									      );
		end if;						
	end if;

exception
	when others
	then
		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception error occured while procesing forced carrier update on '
							 || p_order_id_i
							 || ' from client '
							 || p_client_id_i 
							 || ' check CNL_ERROR.'
							 );

		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		commit;
end force_carrier_update_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 29/07/2021
-- Purpose : Update shipment with some new values
------------------------------------------------------------------------------------------------
procedure update_shipment_p
is
	l_rtn		varchar2(30) 	:= 'update_shipment_p';
begin
	-- add log record
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start shipments update.'
						 );

	cnl_cto_update_shipment_pck.fetch_orders_updship_p;

	-- add log record
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Shipments updates finished.'
						 );

exception
	when others
	then
		-- add log record
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception error occured while procesing shipment update'
							 );

		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		commit;
end update_shipment_p;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Innitiate
------------------------------------------------------------------------------------------------
begin
	null;
end cnl_cto_pck;