CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_SPR_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with Sapper
**********************************************************************************
* $Log: $
**********************************************************************************/

-- Add inventory transaction record
procedure create_itl_p( p_status		in out 	integer
		      , p_code 			in 	dcsdba.inventory_transaction.code%type
		      , p_updateqty		in 	dcsdba.inventory_transaction.update_qty%type
		      , p_originalqty 		in 	dcsdba.inventory_transaction.original_qty%type default nuLL
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
		      );
		      
-- Fetch the database environment 
function fetch_database_f return varchar2;

	
-- Procedure to fetch the authenticate key from sapper web service
procedure fetch_authenticate_key_p (
	p_logging_i  in  varchar2
);

-- Procedure to verify if address is valid
procedure validate_address_p (
	p_site_id_i  in  varchar2,
	p_logging_i  in varchar2
);

--Purpose : Create log record
procedure create_spr_log_record (
		p_source_i       in  varchar2,
		p_description_i  in  varchar2
	);
	
--Add webservice trace record
procedure create_spr_trace_record (
		p_request_i           in   pljson default null,
		p_response_i          in   pljson default null,
		p_status_code_i       in   varchar2 default null,
		p_web_service_name_i  in   varchar2,
		p_key_i               in   integer default null,
		p_key_o               out  integer
	);
--Call webservice
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
	) return varchar2;
--update order header
procedure update_orderheader (
		p_status_i		in  varchar2,
		p_order_id_i		in  varchar2 default null,
		p_client_id_i 		in varchar2 default null,
		p_site_id_i		in varchar2 default null
	) ;
end cnl_spr_pck;