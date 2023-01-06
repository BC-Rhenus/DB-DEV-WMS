CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_STREAMSERVE_PLJSON_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with StreamServe (Output Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--

------------------------------------------------------------------------------------------------
-- Author  :
-- Purpose : Create StreamServe Header block
------------------------------------------------------------------------------------------------
	g_add	                pljson		:= pljson();
        g_add_hdr		pljson		:= pljson();
	g_add_ptr	        pljson		:= pljson();
	g_add_sim_lot           pljson		:= pljson();
	g_add_sim_lot_loop      pljson		:= pljson();
	g_add_sim_lot_list      pljson_list     := pljson_list();
	g_add_smt               pljson		:= pljson();
	g_add_ads               pljson		:= pljson();
	g_ole                   pljson		:= pljson();
	g_ole_list              pljson_list     := pljson_list();
	g_ole_sku               pljson		:= pljson();
	g_ole_haz               pljson		:= pljson();
	g_ole_sha               pljson		:= pljson();
	g_ole_sha_list          pljson_list     := pljson_list();
	g_add_lot               pljson		:= pljson();
	g_add_lot_loop          pljson		:= pljson();
	g_add_lot_list          pljson_list	:= pljson_list();
	g_add_snr               pljson		:= pljson();	
	g_add_snr_loop          pljson		:= pljson();
	g_add_snr_list          pljson_list     := pljson_list();
	g_sim                   pljson		:= pljson();
	g_sim_loop              pljson		:= pljson();
	g_sim_list              pljson_list     := pljson_list();
	g_mtk                   pljson		:= pljson();
	g_mtk_pll               pljson		:= pljson();
	l_report   		pljson		:= pljson();
        l_body_response		pljson		:= pljson();

	l_body_request		pljson		:= pljson();

	g_database		varchar2(10)	:= fetch_database_f;

	g_logging	        boolean;
	g_yes			constant varchar2(1)              := 'Y';
	g_no                    constant varchar2(1)              := 'N';
	g_log			varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_SSV-LOG_ENABLE');
	g_print_id		integer;
	g_file_name		varchar2(100);
	g_pck			varchar2(30) := 'cnl_streamserve_pljson_pck';
	g_wms_hazardous_ads_id  constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'WMS_HAZARDOUS_ADS_ID');
	g_streamserve_wms_db    constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'DB_NAME');
	g_plt                   constant varchar2(3)              := 'PLT';
	g_trl                   constant varchar2(3)              := 'TRL';
	g_wms                   constant varchar2(3)              := 'WMS';
	g_adl                   constant varchar2(3)              := 'ADL';
	g_bto                   constant varchar2(3)              := 'BTO';
	g_cid                   constant varchar2(3)              := 'CID';
	g_clt                   constant varchar2(3)              := 'CLT';
	g_crr                   constant varchar2(3)              := 'CRR';
	g_hub                   constant varchar2(3)              := 'HUB';
	g_sfm                   constant varchar2(3)              := 'SFM';
	g_shp                   constant varchar2(3)              := 'SHP';
	g_sid                   constant varchar2(3)              := 'SID';
	g_sto                   constant varchar2(3)              := 'STO';
	g_haz                   constant varchar2(3)              := 'HAZ';

        g_tracing	        boolean;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Logging enabled yes or no
------------------------------------------------------------------------------------------------
function set_bis_logging_f
	return boolean
is
	l_text_data	dcsdba.system_profile.text_data%type;
	l_retval	boolean;
begin
	select	upper(text_data)
	into	l_text_data
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_CENTIRO_LOGGING_BISLOGGING'
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
end set_bis_logging_f;

procedure set_bis_logging_p
is
begin
	g_logging	:= set_bis_logging_f;
end set_bis_logging_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 22/04/2021
-- Purpose : Create log record
------------------------------------------------------------------------------------------------
procedure create_bis_log_record( p_source_i         in varchar2
			       , p_description_i    in varchar2 
			       )
is
	l_rtn		varchar2(30) := 'create_bis_log_record';
	pragma		autonomous_transaction;
begin
	set_bis_logging_p;

    --    dbms_output.put_line('source: '||substr(p_source_i,1,100));
--	dbms_output.put_line('Description: '||substr(p_description_i,1,4000));
	--if 	g_logging
	--then
		insert 
		into 	cnl_bis_log
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
	--end if;
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
end create_bis_log_record;


------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Nov-2016
-- Purpose : Create StreamServe Address block
------------------------------------------------------------------------------------------------
	function add_ads(  p_field_prefix_i     in  varchar2
			 , p_ads_type_i         in  varchar2
			 , p_name_1_i           in  varchar2  := null
			 , p_address_1_i        in  varchar2  := null
			 , p_city_i             in  varchar2  := null
			 , p_zip_code_i         in  varchar2  := null
			 , p_state_code_i       in  varchar2  := null
			 , p_cty_iso_i          in  varchar2  := null
			 , p_address_2_i        in  varchar2  := null
			 , p_address_3_i        in  varchar2  := null
			 , p_address_4_i        in  varchar2  := null
			 , p_phone_i            in  varchar2  := null
			 , p_mobile_i           in  varchar2  := null
			 , p_fax_i              in  varchar2  := null
			 , p_email_i            in  varchar2  := null
			 , p_contact_name_i     in  varchar2  := null
			 , p_web_i              in  varchar2  := null
			 , p_ads_udf_type_1_i   in  varchar2  := null
			 , p_ads_udf_type_2_i   in  varchar2  := null
			 , p_ads_udf_type_3_i   in  varchar2  := null
			 , p_ads_udf_type_4_i   in  varchar2  := null
			 , p_ads_udf_type_5_i   in  varchar2  := null
			 , p_ads_udf_type_6_i   in  varchar2  := null
			 , p_ads_udf_type_7_i   in  varchar2  := null
			 , p_ads_udf_type_8_i   in  varchar2  := null
			 , p_ads_udf_num_1_i    in  number    := null
			 , p_ads_udf_num_2_i    in  number    := null
			 , p_ads_udf_num_3_i    in  number    := null
			 , p_ads_udf_num_4_i    in  number    := null
			 , p_ads_udf_chk_1_i    in  varchar2  := null
			 , p_ads_udf_chk_2_i    in  varchar2  := null
			 , p_ads_udf_chk_3_i    in  varchar2  := null
			 , p_ads_udf_chk_4_i    in  varchar2  := null
			 , p_ads_udf_dstamp_1_i in  timestamp := null
			 , p_ads_udf_dstamp_2_i in  timestamp := null
			 , p_ads_udf_dstamp_3_i in  timestamp := null
			 , p_ads_udf_dstamp_4_i in  timestamp := null
			 , p_ads_udf_note_1_i   in  varchar2  := null
			 , p_ads_udf_note_2_i   in  varchar2  := null
			 , p_directions_i       in  varchar2  := null
			 , p_vat_number_i       in  varchar2  := null
			 , p_address_type_1     in  varchar2  := null
			 , p_address_id_i       in  varchar2  := null
			 )
			 return boolean
	is
		-- Get ISO country code
		cursor	c_cty ( b_cty_iso in varchar2)
		is
			select	cty.iso2_id
			,      	cty.iso3_id
			,      	decode( cty.ce_eu_type, 'EU', g_yes, g_no)                 eu_type
			,      	upper(ltt.text)         cty_desc
			from   	dcsdba.country          cty
			,      	dcsdba.language_text    ltt
			where  	cty.iso3_id             = substr(label, 4) 
			and    	substr(ltt.label, 1, 3) = 'WLK' 
			and    	ltt.language            = 'EN_GB'
			and    	(cty.iso3_id = b_cty_iso or cty.iso2_id = b_cty_iso)
		;

		r_cty             	c_cty%rowtype;

		l_field_prefix_i  	varchar2(35); 
		l_rtn			varchar2(30) := 'add_ads';
		l_retval		boolean 	:= true;
	begin
		-- add log record
		g_add_ads := pljson();
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_ads_type_i
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"address_id" "'||p_address_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		l_field_prefix_i := p_field_prefix_i || '_' || p_ads_type_i;
		-- get country details
		open	c_cty ( b_cty_iso => p_cty_iso_i);
		fetch 	c_cty
		into  	r_cty;
		close 	c_cty;

		g_add_ads.put('TYPE',p_ads_type_i);
		g_add_ads.put('NAME_1',p_name_1_i);
		g_add_ads.put('ADDRESS_1',p_address_1_i);
		g_add_ads.put('CITY',p_city_i);
		g_add_ads.put('ZIP_CODE',p_zip_code_i);
		g_add_ads.put('STATE_CODE',p_state_code_i);
		g_add_ads.put('CTY_ISO2',r_cty.iso2_id);
		g_add_ads.put('CTY_ISO3',r_cty.iso3_id);
		g_add_ads.put('CTY_DESC',r_cty.cty_desc);
		g_add_ads.put('EU_IND',r_cty.eu_type);
		g_add_ads.put('ADDRESS_2',p_address_2_i);
		g_add_ads.put('ADDRESS_3',p_address_3_i);
		g_add_ads.put('ADDRESS_4',p_address_4_i);
		g_add_ads.put('PHONE',p_phone_i);
		g_add_ads.put('MOBILE',p_mobile_i);
		g_add_ads.put('FAX',p_fax_i);
		g_add_ads.put('EMAIL',p_email_i);
		g_add_ads.put('CONTACT_NAME',p_contact_name_i);
		g_add_ads.put('WEB',p_web_i);
		g_add_ads.put('USER_DEF_TYPE_1',p_ads_udf_type_1_i);
		g_add_ads.put('USER_DEF_TYPE_2',p_ads_udf_type_2_i);
		g_add_ads.put('USER_DEF_TYPE_3',p_ads_udf_type_3_i);
		g_add_ads.put('USER_DEF_TYPE_4',p_ads_udf_type_4_i);
		g_add_ads.put('USER_DEF_TYPE_5',p_ads_udf_type_5_i);
		g_add_ads.put('USER_DEF_TYPE_6',p_ads_udf_type_6_i);
		g_add_ads.put('USER_DEF_TYPE_7',p_ads_udf_type_7_i);
		g_add_ads.put('USER_DEF_TYPE_8',p_ads_udf_type_8_i);
		g_add_ads.put('USER_DEF_NUM_1',to_char( p_ads_udf_num_1_i, 'fm999999990.999990'));
		g_add_ads.put('USER_DEF_NUM_2',to_char( p_ads_udf_num_2_i, 'fm999999990.999990'));
		g_add_ads.put('USER_DEF_NUM_3',to_char( p_ads_udf_num_3_i, 'fm999999990.999990'));
		g_add_ads.put('USER_DEF_NUM_4',to_char( p_ads_udf_num_4_i, 'fm999999990.999990'));
		g_add_ads.put('USER_DEF_CHK_1',p_ads_udf_chk_1_i);
		g_add_ads.put('USER_DEF_CHK_2',p_ads_udf_chk_2_i);
		g_add_ads.put('USER_DEF_CHK_3',p_ads_udf_chk_3_i);
		g_add_ads.put('USER_DEF_CHK_4',p_ads_udf_chk_4_i);
		g_add_ads.put('USER_DEF_DATE_1',to_char( p_ads_udf_dstamp_1_i, 'DD-MM-YYYY'));
		g_add_ads.put('USER_DEF_TIME_1',to_char( p_ads_udf_dstamp_1_i, 'HH24:MI:SS'));
		g_add_ads.put('USER_DEF_DATE_2',to_char( p_ads_udf_dstamp_2_i, 'DD-MM-YYYY'));
		g_add_ads.put('USER_DEF_TIME_2',to_char( p_ads_udf_dstamp_2_i, 'HH24:MI:SS'));
		g_add_ads.put('USER_DEF_DATE_3',to_char( p_ads_udf_dstamp_3_i, 'DD-MM-YYYY'));
		g_add_ads.put('USER_DEF_TIME_3',to_char( p_ads_udf_dstamp_3_i, 'HH24:MI:SS'));
		g_add_ads.put('USER_DEF_DATE_4',to_char( p_ads_udf_dstamp_4_i, 'DD-MM-YYYY'));
		g_add_ads.put('USER_DEF_TIME_4',to_char( p_ads_udf_dstamp_4_i, 'HH24:MI:SS'));
		g_add_ads.put('USER_DEF_NOTE_1',p_ads_udf_note_1_i);
		g_add_ads.put('USER_DEF_NOTE_2',p_ads_udf_note_2_i);
		g_add_ads.put('DIRECTIONS',p_directions_i);
		g_add_ads.put('VAT_NUMBER',p_vat_number_i);
		g_add_ads.put('ADDRESS_TYPE',p_address_type_1);
		g_add_ads.put('ADDRESS_ID',p_address_id_i);
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_ads_type_i
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"address_id" "'||p_address_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		return	l_retval; 
	exception 
		when	others
		then
			case 
			when	c_cty%isopen
			then
				close	c_cty;
			else
				null;
			end case;
		return	l_retval;
	end add_ads;

------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 27-Sep-2016
-- Purpose : Create StreamServe Shipment and Order Header block
------------------------------------------------------------------------------------------------
	function add_smt(  p_field_prefix_i in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 )
		return boolean
	is
		-- Fetch order header details
		cursor c_ohr( b_client_id  varchar2
			    , b_order_id   varchar2
			    )
		is
			select	ohr.*
			from   	dcsdba.order_header ohr
			where  	ohr.client_id = b_client_id
			and    	ohr.order_id  = b_order_id 
		;

		cursor c_fields
		is
		select * from bis_layout_fields;

		-- fetch unit details
		cursor c_ocr( b_client_id  varchar2
			    , b_order_id   varchar2
			    )
		is	
			select	smt.client_id
			,      	smt.order_id
			,      	nvl( smt.labelled, g_no) is_cont_yn
			,      	decode( smt.labelled, g_yes, smt.container_id, smt.pallet_id) pallet_id
			,      	decode( smt.labelled, g_yes, round( ( smt.container_depth * smt.container_width * smt.container_height) / 1000000, 6), round( ( smt.pallet_depth    * smt.pallet_width    * smt.pallet_height)    / 1000000, 6)) volume
			,      	decode( smt.labelled, g_yes, smt.container_weight, smt.pallet_weight ) weight
			,      	1 cnt
			,      	decode( smt.labelled, g_yes, 1, nvl( smt.transport_boxes, 1)) no_of_boxes
			from   	dcsdba.shipping_manifest smt
			where  	smt.client_id            = b_client_id
			and    	smt.order_id             = b_order_id
			union  	-- for pallets which are not 'marshalled' yet
			select 	ocr.client_id
			,      	ocr.order_id
			,      	nvl( ocr.labelled, g_no)                            is_cont_yn
			,      	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id) pallet_id
			,      	decode( ocr.labelled, g_yes, round( ( ocr.container_depth * ocr.container_width * ocr.container_height) / 1000000, 6), round( ( ocr.pallet_depth    * ocr.pallet_width    * ocr.pallet_height)    / 1000000, 6)) volume
			,      	decode( ocr.labelled, g_yes, ocr.container_weight, ocr.pallet_weight ) weight
			,      	1 cnt
			,      	decode( ocr.labelled, g_yes, 1, nvl( ocr.transport_boxes, 1)) no_of_boxes
			from   	dcsdba.order_container   ocr
			where  	ocr.client_id            = b_client_id
			and    	ocr.order_id             = b_order_id
			and    	not exists( 	select 1
						from   dcsdba.shipping_manifest smt
						where  smt.client_id            = ocr.client_id
						and    smt.order_id             = ocr.order_id
						and    smt.pallet_id            = ocr.pallet_id
					  )
			order  
			by 	1,2,4
		;
		-- Fetch client details
		cursor c_clt( b_client_id in varchar2)
		is
			select clt.name
			,      clt.address1
			,      clt.address2
			,      clt.postcode
			,      clt.town
			,      clt.county         
			,      clt.country
			,      clt.contact_phone  
			,      clt.contact_mobile 
			,      clt.contact_fax    
			,      clt.contact_email  
			,      clt.contact        contact_name
			,      clt.notes
			,      clt.url
			,      clt.user_def_type_1
			,      clt.user_def_type_2
			,      clt.user_def_type_3
			,      clt.user_def_type_4
			,      clt.user_def_type_5
			,      clt.user_def_type_6
			,      clt.user_def_type_7
			,      clt.user_def_type_8
			,      clt.user_def_num_1
			,      clt.user_def_num_2
			,      clt.user_def_num_3
			,      clt.user_def_num_4
			,      clt.user_def_chk_1
			,      clt.user_def_chk_2
			,      clt.user_def_chk_3
			,      clt.user_def_chk_4
			,      clt.user_def_date_1
			,      clt.user_def_date_2
			,      clt.user_def_date_3
			,      clt.user_def_date_4
			,      clt.user_def_note_1
			,      clt.user_def_note_2
			,      clt.vat_number
			from   dcsdba.client clt
			where  clt.client_id = b_client_id
		;
		-- Fetch carrier details
		cursor c_crr( b_client_id     in varchar2
			    , b_carrier_id    in varchar2
			    , b_service_level in varchar2
			    )
		is
			select crr.name
			,      crr.address1
			,      crr.address2
			,      crr.postcode
			,      crr.town
			,      crr.county         
			,      crr.country
			,      crr.contact_phone  
			,      crr.contact_mobile 
			,      crr.contact_fax    
			,      crr.contact_email  
			,      crr.contact        contact_name
			,      crr.notes
			,      crr.url
			,      crr.user_def_type_1
			,      crr.user_def_type_2
			,      crr.user_def_type_3
			,      crr.user_def_type_4
			,      crr.user_def_type_5
			,      crr.user_def_type_6
			,      crr.user_def_type_7
			,      crr.user_def_type_8
			,      crr.user_def_num_1
			,      crr.user_def_num_2
			,      crr.user_def_num_3
			,      crr.user_def_num_4
			,      crr.user_def_chk_1
			,      crr.user_def_chk_2
			,      crr.user_def_chk_3
			,      crr.user_def_chk_4
			,      crr.user_def_date_1
			,      crr.user_def_date_2
			,      crr.user_def_date_3
			,      crr.user_def_date_4
			,      crr.user_def_note_1
			,      crr.user_def_note_2
			from   dcsdba.carriers crr
			where  crr.client_id     = b_client_id
			and    crr.carrier_id    = b_carrier_id
			and    crr.service_level = b_service_level
		;
		-- Fetch address details
		cursor c_ads( b_client_id    in varchar2
			    , b_address_id   in varchar2
			    )
		is
			select ads.address_id
			,      ads.address_type
			,      ads.name
			,      ads.address1
			,      ads.town
			,      ads.postcode
			,      ads.county
			,      ads.country
			,      ads.address2
			,      ads.contact_phone
			,      ads.contact_mobile
			,      ads.contact_fax
			,      ads.contact_email
			,      ads.contact        contact_name
			,      ads.url
			,      ads.user_def_type_1
			,      ads.user_def_type_2
			,      ads.user_def_type_3
			,      ads.user_def_type_4
			,      ads.user_def_type_5
			,      ads.user_def_type_6
			,      ads.user_def_type_7
			,      ads.user_def_type_8
			,      ads.user_def_num_1
			,      ads.user_def_num_2
			,      ads.user_def_num_3
			,      ads.user_def_num_4
			,      ads.user_def_chk_1
			,      ads.user_def_chk_2
			,      ads.user_def_chk_3
			,      ads.user_def_chk_4
			,      ads.user_def_date_1
			,      ads.user_def_date_2
			,      ads.user_def_date_3
			,      ads.user_def_date_4
			,      ads.user_def_note_1
			,      ads.user_def_note_2
			,      ads.directions
			,      ads.vat_number
			from   dcsdba.address ads
			where  ads.client_id  = b_client_id
			and    ads.address_id = b_address_id
		;                    
		-- Fetch track and trace URL
		cursor c_url( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select	ccd.cto_tracking_url url
			from	cnl_sys.cnl_container_data ccd
			where	ccd.cto_tracking_url is not null
			and	ccd.client_id = b_client_id
			and	ccd.order_id = b_order_id
			and 	rownum = 1
		;
		--
		r_url	        c_url%rowtype;
		r_ohr           c_ohr%rowtype;
		r_ocr           c_ocr%rowtype;
		r_clt           c_clt%rowtype;
		r_crr           c_crr%rowtype;
		r_ads           c_ads%rowtype;

		l_volume        number(15,6);
		l_weight        number(15,6);
		l_pieces        number(10);
		l_no_of_boxes   number(10);
		l_rtn		varchar(30) := 'add_smt';
		l_retval	boolean 	:= true;
	begin
	       g_add_smt := pljson();
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding SMT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		-- Fetch order header details
		open	c_ohr( b_client_id => p_client_id_i
			     , b_order_id  => p_order_nr_i
			     );
		fetch 	c_ohr
		into  	r_ohr;
		-- Fetch track and trace URL
		open 	c_url( b_client_id => p_client_id_i
			     , b_order_id => p_order_nr_i
			     );
		fetch  	c_url 
		into   	r_url;
		close  	c_url;
		-- Add order header details

		g_add_smt.put('SMT_ID',	                '');	
		g_add_smt.put('SMT_ORDER_NR',	        p_order_nr_i);
		g_add_smt.put('SMT_WAYBILL_NR',	        r_ohr.trax_id);
		g_add_smt.put('SMT_TRACKING_URL',	r_url.url);
		g_add_smt.put('SMT_SHIP_DATE',	        to_char( nvl( r_ohr.shipped_date, sysdate), 'DD-MM-YYYY'));
		g_add_smt.put('SMT_ORDER_DATE',	        to_char( r_ohr.order_date, 'DD-MM-YYYY'));
		g_add_smt.put('SMT_REQ_DEL_DATE',	to_char( r_ohr.deliver_by_date, 'DD-MM-YYYY'));
		g_add_smt.put('SMT_INV_AMOUNT',	        ltrim( to_char( r_ohr.inv_total_1, 'fm99999990.00')));
		g_add_smt.put('SMT_CURRENCY_CODE',	r_ohr.inv_currency);
		g_add_smt.put('SMT_COD_YN',	        nvl( r_ohr.cod, g_no));
		g_add_smt.put('SMT_COD_AMOUNT',	        ltrim( to_char( r_ohr.cod_value, 'fm9999990.00')));
		g_add_smt.put('SMT_COD_CURRENCY_CODE',	r_ohr.cod_currency);
		g_add_smt.put('SMT_GOOD_DESC',	        '');
		g_add_smt.put('SMT_DAN_GOOD_YN',	'');
		g_add_smt.put('SMT_ORIGIN_PORT',	'');
		g_add_smt.put('SMT_DESTINATION_PORT',	r_ohr.delivery_point);
		g_add_smt.put('SMT_DELIVERY_COND_CODE',	r_ohr.tod);
		g_add_smt.put('SMT_CARRIER_ID',	            r_ohr.carrier_id);
		g_add_smt.put('SMT_SERVICE_LEVEL',	    r_ohr.service_level);
		g_add_smt.put('SMT_COUNTRY_DEPARTURE_ISO2', '');
		g_add_smt.put('SMT_COUNTRY_DEPARTURE_ISO3', '');
		g_add_smt.put('SMT_COUNTRY_DEPARTURE_NUM3', '');
		g_add_smt.put('SMT_COUNTRY_DEPARTURE_DESC', '');
		g_add_smt.put('SMT_COUNTRY_ORIGIN_ISO2',    '');
		g_add_smt.put('SMT_COUNTRY_ORIGIN_ISO3',    '');
		g_add_smt.put('SMT_COUNTRY_ORIGIN_NUM3',    '');
		g_add_smt.put('SMT_COUNTRY_ORIGIN_DESC',    '');


		--dbms_output.put_line(g_add_smt.to_char( true ));

		-- summarize unit details
		for 	r_ocr in c_ocr( b_client_id => p_client_id_i
				      , b_order_id  => p_order_nr_i
				      )
		loop
			l_weight      := nvl( l_weight, 0) + round( r_ocr.weight, 2);
			l_volume      := nvl( l_volume, 0) + round( r_ocr.volume, 6);
			l_pieces      := nvl( l_pieces, 0) + r_ocr.cnt;
			l_no_of_boxes := nvl( l_no_of_boxes, 0) + r_ocr.no_of_boxes;
		end loop;
		-- add unit totals

		g_add_smt.put('SMT_TOTAL_ITEMS',	        to_char( l_no_of_boxes));
		g_add_smt.put('SMT_TOTAL_BOXES_IN_UNITS',	to_char( l_no_of_boxes));
		g_add_smt.put('SMT_TOTAL_WEIGHT',	        ltrim( to_char(l_weight, 'fm999990.90')));
		g_add_smt.put('SMT_TOTAL_PIECES',	        to_char(l_pieces));
		g_add_smt.put('SMT_TOTAL_SHIP_UNITS',	to_char(l_pieces));
		g_add_smt.put('SMT_TOTAL_VOLUME',	        ltrim( to_char( l_volume, 'fm999990.90')));
		g_add_smt.put('SMT_TOTAL_DRY_ICE_WEIGHT',	'');
		g_add_smt.put('SMT_FREIGHT_CHARGES',	r_ohr.freight_charges);
		g_add_smt.put('SMT_COLLECT_ACCOUNT_NR',	'');
		g_add_smt.put('SMT_THIRD_ACCOUNT_NR',	'');
		g_add_smt.put('SMT_REFERENCE_NR',	        r_ohr.purchase_order);		

                --dbms_output.put_line(g_add_smt.to_char( true ));

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OHR segment'
								   , p_code_parameters_i 	=> null
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		-- switch to OHR field prefix
		g_add_smt.put('OHR_WORK_ORDER_TYPE',	r_ohr.work_order_type);
		g_add_smt.put('OHR_ORDER_TYPE',	        r_ohr.order_type);
		g_add_smt.put('OHR_STATUS',          	r_ohr.status);
		g_add_smt.put('OHR_MOVE_TASK_STATUS',	r_ohr.move_task_status);
		g_add_smt.put('OHR_PRIORITY',	        to_char( r_ohr.priority));
		g_add_smt.put('OHR_REPACK',	        r_ohr.repack_loc_id);
		g_add_smt.put('OHR_REPACK_LOC_ID',      r_ohr.repack_loc_id);
		g_add_smt.put('OHR_SHIP_DOCK',	        r_ohr.ship_dock);
		g_add_smt.put('OHR_WORK_GROUP',	        r_ohr.work_group);
		g_add_smt.put('OHR_CONSIGNMENT',	r_ohr.consignment);
		g_add_smt.put('OHR_DELIVERY_POINT',	r_ohr.delivery_point);
		g_add_smt.put('OHR_LOAD_SEQUENCE',	to_char( r_ohr.load_sequence));
		g_add_smt.put('OHR_TO_SITE_ID',   	r_ohr.to_site_id);
		g_add_smt.put('OHR_OWNER_ID',	        r_ohr.owner_id);
		g_add_smt.put('OHR_CUSTOMER_ID',	r_ohr.customer_id);
		g_add_smt.put('OHR_SHIP_BY_DATE',	to_char( r_ohr.ship_by_date, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_SHIP_BY_TIME',	to_char( r_ohr.ship_by_date, 'HH24:MI:SS'));
		g_add_smt.put('OHR_DELIVER_BY_DATE',	to_char( r_ohr.deliver_by_date, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_DELIVER_BY_TIME',	to_char( r_ohr.deliver_by_date, 'HH24:MI:SS'));
		g_add_smt.put('OHR_DELIVERED_DSTAMP',	to_char( r_ohr.delivered_dstamp, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_DELIVERED_TIME',	to_char( r_ohr.delivered_dstamp, 'HH24:MI:SS'));
		g_add_smt.put('OHR_SIGNATORY',	r_ohr.signatory);
		g_add_smt.put('OHR_PURCHASE_ORDER',	r_ohr.purchase_order);
		g_add_smt.put('OHR_DISPATCH_METHOD',	r_ohr.dispatch_method);
		g_add_smt.put('OHR_SERVICE_LEVEL',	r_ohr.service_level);
		g_add_smt.put('OHR_FASTEST_CARRIER',	r_ohr.fastest_carrier);
		g_add_smt.put('OHR_CHEAPEST_CARRIER',	r_ohr.cheapest_carrier);
		g_add_smt.put('OHR_INV_ADDRESS_ID',	r_ohr.inv_address_id);
		g_add_smt.put('OHR_INSTRUCTIONS',	r_ohr.instructions);
		g_add_smt.put('OHR_ORDER_VOLUME',	ltrim( to_char( r_ohr.order_volume, 'fm999990.90')));
		g_add_smt.put('OHR_ORDER_WEIGHT',	 ltrim( to_char( r_ohr.order_weight, 'fm999990.90')));
	        g_add_smt.put('OHR_ROUTE_PLANNED',	r_ohr.route_planned);
		g_add_smt.put('OHR_UPLOADED' , r_ohr.uploaded );
		g_add_smt.put('OHR_UPLOADED_WS2PC_ID' , to_char( r_ohr.uploaded_ws2pc_id) );
		g_add_smt.put('OHR_UPLOADED_DSTAMP' , to_char( r_ohr.uploaded_dstamp, 'DD-MM-YYYY') );
		g_add_smt.put('OHR_UPLOADED_FILENAME' , r_ohr.uploaded_filename );
		g_add_smt.put('OHR_UPLOADED_VVIEW' , r_ohr.uploaded_vview );
		g_add_smt.put('OHR_UPLOADED_HEADER_KEY' , to_char( r_ohr.uploaded_header_key) );
		g_add_smt.put('OHR_PSFT_DMND_SRCE' , r_ohr.psft_dmnd_srce );
		g_add_smt.put('OHR_PSFT_ORDER_ID' , r_ohr.psft_order_id );
		g_add_smt.put('OHR_SITE_REPLEN' , r_ohr.site_replen );
		g_add_smt.put('OHR_ORDER_ID_LINK' , r_ohr.order_id_link );
		g_add_smt.put('OHR_ALLOCATION_RUN' , to_char( r_ohr.allocation_run) );
		g_add_smt.put('OHR_NO_SHIPMENT_EMAIL' , r_ohr.no_shipment_email );
		g_add_smt.put('OHR_CID_NUMBER' , r_ohr.cid_number );
		g_add_smt.put('OHR_SID_NUMBER' , r_ohr.sid_number );
		g_add_smt.put('OHR_LOCATION_NUMBER' , r_ohr.location_number);
		g_add_smt.put('OHR_FREIGHT_CHARGES' , r_ohr.freight_charges);
		g_add_smt.put('OHR_DISALLOW_MERGE_RULES' , r_ohr.disallow_merge_rules);
		g_add_smt.put('OHR_ORDER_SOURCE' , r_ohr.order_source);
		g_add_smt.put('OHR_EXPORT' , r_ohr.export);
		g_add_smt.put('OHR_NUM_LINES' , to_char( r_ohr.num_lines));
		g_add_smt.put('OHR_HIGHEST_LABEL' , to_char( r_ohr.highest_label));
		g_add_smt.put('OHR_USER_DEF_TYPE_1' , r_ohr.user_def_type_1);
		g_add_smt.put('OHR_USER_DEF_TYPE_2' , r_ohr.user_def_type_2);
		g_add_smt.put('OHR_USER_DEF_TYPE_3' , r_ohr.user_def_type_3);
		g_add_smt.put('OHR_USER_DEF_TYPE_4', r_ohr.user_def_type_4);
		g_add_smt.put('OHR_USER_DEF_TYPE_5', r_ohr.user_def_type_5);
		g_add_smt.put('OHR_USER_DEF_TYPE_6', r_ohr.user_def_type_6);		
                g_add_smt.put('OHR_USER_DEF_TYPE_7', r_ohr.user_def_type_7);
		g_add_smt.put('OHR_USER_DEF_TYPE_8', r_ohr.user_def_type_8);
		g_add_smt.put('OHR_USER_DEF_CHK_1', r_ohr.user_def_chk_1);
		g_add_smt.put('OHR_USER_DEF_CHK_2', r_ohr.user_def_chk_2);
		g_add_smt.put('OHR_USER_DEF_CHK_3', r_ohr.user_def_chk_3);
		g_add_smt.put('OHR_USER_DEF_CHK_4', r_ohr.user_def_chk_4);
		g_add_smt.put('OHR_USER_DEF_DATE_1', to_char( r_ohr.user_def_date_1, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_USER_DEF_TIME_1', to_char( r_ohr.user_def_date_1, 'HH24:MI:SS'));
		g_add_smt.put('OHR_USER_DEF_DATE_2', to_char( r_ohr.user_def_date_2, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_USER_DEF_TIME_2', to_char( r_ohr.user_def_date_2, 'HH24:MI:SS'));
		g_add_smt.put('OHR_USER_DEF_DATE_3', to_char( r_ohr.user_def_date_3, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_USER_DEF_TIME_3', to_char( r_ohr.user_def_date_3, 'HH24:MI:SS'));
		g_add_smt.put('OHR_USER_DEF_DATE_4', to_char( r_ohr.user_def_date_4, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_USER_DEF_TIME_4', to_char( r_ohr.user_def_date_4, 'HH24:MI:SS'));
		g_add_smt.put('OHR_USER_DEF_NUM_1', to_char( r_ohr.user_def_num_1, 'fm999999990.999990'));
		g_add_smt.put('OHR_USER_DEF_NUM_2', to_char( r_ohr.user_def_num_2, 'fm999999990.999990'));
		g_add_smt.put('OHR_USER_DEF_NUM_3', to_char( r_ohr.user_def_num_3, 'fm999999990.999990'));
		g_add_smt.put('OHR_USER_DEF_NUM_4', to_char( r_ohr.user_def_num_4, 'fm999999990.999990'));
		g_add_smt.put('OHR_USER_DEF_NOTE_1', r_ohr.user_def_note_1);
		g_add_smt.put('OHR_USER_DEF_NOTE_2', r_ohr.user_def_note_2);
		g_add_smt.put('OHR_ROUTE_ID', r_ohr.route_id);
		g_add_smt.put('OHR_CROSS_DOCK_TO_SITE', r_ohr.cross_dock_to_site);
		g_add_smt.put('OHR_WEB_SERVICE_ALLOC_IMMED', r_ohr.web_service_alloc_immed);
		g_add_smt.put('OHR_WEB_SERVICE_ALLOC_CLEAN', r_ohr.web_service_alloc_clean);
		g_add_smt.put('OHR_DISALLOW_SHORT_SHIP', r_ohr.disallow_short_ship);
		g_add_smt.put('OHR_UPLOADED_CUSTOMS', r_ohr.uploaded_customs);
		g_add_smt.put('OHR_UPLOADED_LABOR', r_ohr.uploaded_labor);
		g_add_smt.put('OHR_CANCEL_REASON_CODE', '');
		g_add_smt.put('OHR_STATUS_REASON_CODE', r_ohr.status_reason_code);
		g_add_smt.put('OHR_STAGE_ROUTE_ID', r_ohr.stage_route_id);
		g_add_smt.put('OHR_SINGLE_ORDER_SORTATION', r_ohr.single_order_sortation);
		g_add_smt.put('OHR_ARCHIVED', r_ohr.archived);
		g_add_smt.put('OHR_CLOSURE_DATE', to_char( r_ohr.closure_date, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_CLOSURE_TIME', to_char( r_ohr.closure_date, 'HH24:MI:SS'));
		g_add_smt.put('OHR_ORDER_CLOSED', r_ohr.order_closed);
		g_add_smt.put('OHR_TOTAL_REPACK_CONTAINERS', to_char( r_ohr.total_repack_containers));
		g_add_smt.put('OHR_FORCE_SINGLE_CARRIER', r_ohr.force_single_carrier);
		g_add_smt.put('OHR_HUB_CARRIER_ID', r_ohr.hub_carrier_id);
		g_add_smt.put('OHR_HUB_SERVICE_LEVEL', r_ohr.hub_service_level);
		g_add_smt.put('OHR_ORDER_GROUPING_ID', r_ohr.order_grouping_id);
		g_add_smt.put('OHR_SHIP_BY_DATE_ERR', r_ohr.ship_by_date_err);
		g_add_smt.put('OHR_DEL_BY_DATE_ERR',r_ohr.del_by_date_err);
		g_add_smt.put('OHR_SHIP_BY_DATE_ERR_MSG', r_ohr.ship_by_date_err_msg);
		g_add_smt.put('OHR_DEL_BY_DATE_ERR_MSG', r_ohr.del_by_date_err_msg);
		g_add_smt.put('OHR_ORDER_VALUE', ltrim( to_char( r_ohr.order_value, 'fm99999990.90')));
		g_add_smt.put('OHR_EXPECTED_VOLUME', ltrim(to_char( r_ohr.expected_volume, 'fm999990.90')));
		g_add_smt.put('OHR_EXPECTED_WEIGHT', ltrim( to_char( r_ohr.expected_weight, 'fm9999990.90')));
		g_add_smt.put('OHR_EXPECTED_VALUE', ltrim( to_char( r_ohr.expected_value, 'fm99999990.90')));
		g_add_smt.put('OHR_TOD', r_ohr.tod);
		g_add_smt.put('OHR_TOD_PLACE', r_ohr.tod_place);
		g_add_smt.put('OHR_LANGUAGE', r_ohr.language);
		g_add_smt.put('OHR_SELLER_NAME', r_ohr.seller_name);
		g_add_smt.put('OHR_SELLER_PHONE', r_ohr.seller_phone);
		g_add_smt.put('OHR_DOCUMENTATION_TEXT_1', r_ohr.documentation_text_1);
		g_add_smt.put('OHR_DOCUMENTATION_TEXT_2', r_ohr.documentation_text_2);
		g_add_smt.put('OHR_DOCUMENTATION_TEXT_3', r_ohr.documentation_text_3);
		g_add_smt.put('OHR_COD', r_ohr.cod);
		g_add_smt.put('OHR_COD_VALUE', ltrim( to_char( r_ohr.cod_value, 'fm99999990.90')));
		g_add_smt.put('OHR_COD_CURRENCY', r_ohr.cod_currency);
		g_add_smt.put('OHR_COD_TYPE', r_ohr.cod_type);
		g_add_smt.put('OHR_VAT_NUMBER', r_ohr.vat_number);
		g_add_smt.put('OHR_INV_VAT_NUMBER', r_ohr.inv_vat_number);
		g_add_smt.put('OHR_HUB_VAT_NUMBER', r_ohr.hub_vat_number);
		g_add_smt.put('OHR_PRINT_INVOICE', r_ohr.print_invoice);
		g_add_smt.put('OHR_INV_REFERENCE', r_ohr.inv_reference);
		g_add_smt.put('OHR_INV_DSTAMP' , to_char( r_ohr.inv_dstamp, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_INV_CURRENCY', r_ohr.inv_currency);
		g_add_smt.put('OHR_LETTER_OF_CREDIT', r_ohr.letter_of_credit);
		g_add_smt.put('OHR_PAYMENT_TERMS', r_ohr.payment_terms);
		g_add_smt.put('OHR_SUBTOTAL_1', ltrim( to_char( r_ohr.subtotal_1, 'fm99999990.90')));
		g_add_smt.put('OHR_SUBTOTAL_2', ltrim( to_char( r_ohr.subtotal_2, 'fm99999990.90')));
		g_add_smt.put('OHR_SUBTOTAL_3', ltrim( to_char( r_ohr.subtotal_3, 'fm99999990.90')));
		g_add_smt.put('OHR_SUBTOTAL_4', ltrim( to_char( r_ohr.subtotal_4, 'fm99999990.90')));
		g_add_smt.put('OHR_FREIGHT_COST', ltrim( to_char( r_ohr.freight_cost, 'fm99999990.90')));
		g_add_smt.put('OHR_FREIGHT_TERMS', r_ohr.freight_terms);
		g_add_smt.put('OHR_INSURANCE_COST', ltrim( to_char( r_ohr.insurance_cost, 'fm99999990.90')));
		g_add_smt.put('OHR_MISC_CHARGES', ltrim( to_char( r_ohr.misc_charges, 'fm99999990.90')));
		g_add_smt.put('OHR_DISCOUNT', ltrim( to_char( r_ohr.discount, 'fm99999990.90')));
		g_add_smt.put('OHR_OTHER_FEE', ltrim( to_char( r_ohr.other_fee, 'fm99999990.90')));
		g_add_smt.put('OHR_INV_TOTAL_1', ltrim( to_char( r_ohr.inv_total_1, 'fm99999990.90')));
		g_add_smt.put('OHR_INV_TOTAL_2', ltrim( to_char( r_ohr.inv_total_2, 'fm99999990.90')));
		g_add_smt.put('OHR_INV_TOTAL_3', ltrim( to_char( r_ohr.inv_total_3, 'fm99999990.90')));
		g_add_smt.put('OHR_INV_TOTAL_4', ltrim( to_char( r_ohr.inv_total_4, 'fm99999990.90')));
		g_add_smt.put('OHR_TAX_RATE_1', ltrim( to_char( r_ohr.tax_rate_1, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_BASIS_1', ltrim( to_char( r_ohr.tax_basis_1, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_AMOUNT_1', ltrim( to_char( r_ohr.tax_amount_1, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_RATE_2', ltrim( to_char( r_ohr.tax_rate_2, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_BASIS_2', ltrim( to_char( r_ohr.tax_basis_2, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_AMOUNT_2', ltrim( to_char( r_ohr.tax_amount_2, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_RATE_3', ltrim( to_char( r_ohr.tax_rate_3, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_BASIS_3', ltrim( to_char( r_ohr.tax_basis_3, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_AMOUNT_3', ltrim( to_char( r_ohr.tax_amount_3, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_RATE_4', ltrim( to_char( r_ohr.tax_rate_4, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_BASIS_4', ltrim( to_char( r_ohr.tax_basis_4, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_AMOUNT_4', ltrim( to_char( r_ohr.tax_amount_4, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_RATE_5', ltrim( to_char( r_ohr.tax_rate_5, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_BASIS_5', ltrim( to_char( r_ohr.tax_basis_5, 'fm999999990.990')));
		g_add_smt.put('OHR_TAX_AMOUNT_5', ltrim( to_char( r_ohr.tax_amount_5, 'fm999999990.990')));
		g_add_smt.put('OHR_ORDER_REFERENCE', r_ohr.order_reference);
		g_add_smt.put('OHR_PROFORMA_INVOICE_NUM', r_ohr.proforma_invoice_num);
		g_add_smt.put('OHR_TRAX_ID', r_ohr.trax_id);
		g_add_smt.put('OHR_START_BY_DATE', to_char( r_ohr.start_by_date, 'DD-MM-YYYY'));
		g_add_smt.put('OHR_START_BY_TIME', to_char( r_ohr.start_by_date, 'HH24:MI:SS'));
		g_add_smt.put('OHR_EXCLUDE_POSTCODE', r_ohr.exclude_postcode);
		g_add_smt.put('OHR_METAPACK_CARRIER_PRE', r_ohr.metapack_carrier_pre);
		g_add_smt.put('OHR_GROSS_WEIGHT', to_char( r_ohr.gross_weight, 'fm99999990.90'));
		-- new fields after 2009
		g_add_smt.put('OHR_MASTER_ORDER', r_ohr.master_order);
		g_add_smt.put('OHR_MASTER_ORDER_ID', r_ohr.master_order_id);
		g_add_smt.put('OHR_TM_STOP_SEQ', r_ohr.tm_stop_seq);
		g_add_smt.put('OHR_TM_STOP_NAM', r_ohr.tm_stop_nam);
		g_add_smt.put('OHR_FREIGHT_CURRENCY', r_ohr.freight_currency);
		g_add_smt.put('OHR_SOFT_ALLOCATED', r_ohr.soft_allocated);
		g_add_smt.put('OHR_MRN', r_ohr.mrn);
		g_add_smt.put('OHR_NCTS', r_ohr.ncts);
		g_add_smt.put('OHR_MPACK_CONSIGNMENT', r_ohr.mpack_consignment);
		g_add_smt.put('OHR_MPACK_PRE_CAR_ERR', r_ohr.mpack_pre_car_err);
		g_add_smt.put('OHR_MPACK_PACK_ERR', r_ohr.mpack_pack_err);
		g_add_smt.put('OHR_MPACK_NOMINATED_DSTAMP', r_ohr.mpack_nominated_dstamp);
		g_add_smt.put('OHR_MPACK_PRE_CAR_DSTAMP', r_ohr.mpack_pre_car_dstamp);
		g_add_smt.put('OHR_MPACK_PACK_DSTAMP', r_ohr.mpack_pack_dstamp);
		g_add_smt.put('OHR_GLN', r_ohr.gln);
		g_add_smt.put('OHR_HUB_GLN', r_ohr.hub_gln);
		g_add_smt.put('OHR_INV_GLN', r_ohr.inv_gln);
		g_add_smt.put('OHR_ALLOW_PALLET_PICK', r_ohr.allow_pallet_pick);
		g_add_smt.put('OHR_SPLIT_SHIPPING_UNITS', r_ohr.split_shipping_units);
		g_add_smt.put('OHR_VOL_PCK_SSCC_LABEL', r_ohr.vol_pck_sscc_label);
		g_add_smt.put('OHR_ALLOCATION_PRIORITY', r_ohr.allocation_priority);
		g_add_smt.put('OHR_TRAX_USE_HUB_ADDR', r_ohr.trax_use_hub_addr);
		g_add_smt.put('OHR_CONSIGNMENT_GROUPING_ID', r_ohr.consignment_grouping_id);
		g_add_smt.put('OHR_SHIPMENT_GROUPING_ID', r_ohr.shipment_grouping_id);
		g_add_smt.put('OHR_WORK_GROUPING_ID', r_ohr.work_grouping_id);
		g_add_smt.put('OHR_DIRECT_TO_STORE', r_ohr.direct_to_store);
		g_add_smt.put('OHR_VOL_CTR_LABEL_FORMAT', r_ohr.vol_ctr_label_format);
		g_add_smt.put('OHR_CE_ORDER_ID', r_ohr.ce_order_id);
		g_add_smt.put('OHR_RETAILER_ID', r_ohr.retailer_id);
		g_add_smt.put('OHR_FOREIGN_DOCUMENTATION', r_ohr.foreign_documentation);
		g_add_smt.put('OHR_CARRIER_BAGS', r_ohr.carrier_bags);

                --dbms_output.put_line(g_add_smt.to_char( true ));
                g_add.put('Order Header block',	g_add_smt);
		g_add_smt := pljson();
		-- Add Addresses
		-- SFM (If HUB Address is OLL or OLH (Carrier_ID), else SFM Address from ADDRESS table)
		if	r_ohr.hub_carrier_id in ('OLL','OLH')
		and 	r_ohr.hub_name       is not null
		then
		      if add_ads ( p_field_prefix_i     => p_field_prefix_i
				, p_ads_type_i         => g_sfm
				, p_name_1_i           => r_ohr.hub_name
			        , p_address_1_i        => r_ohr.hub_address1
			        , p_city_i             => r_ohr.hub_town
			        , p_zip_code_i         => r_ohr.hub_postcode
			        , p_state_code_i       => r_ohr.hub_county
			        , p_cty_iso_i          => r_ohr.hub_country
			        , p_address_2_i        => r_ohr.hub_address2
			        , p_address_3_i        => null
			        , p_address_4_i        => null
			        , p_phone_i            => r_ohr.hub_contact_phone
			        , p_mobile_i           => r_ohr.hub_contact_mobile
			        , p_fax_i              => r_ohr.hub_contact_fax
			        , p_email_i            => r_ohr.hub_contact_email
			        , p_contact_name_i     => r_ohr.hub_contact
			        , p_web_i              => null
			        , p_ads_udf_type_1_i   => null
			        , p_ads_udf_type_2_i   => null
			        , p_ads_udf_type_3_i   => null
			        , p_ads_udf_type_4_i   => null
			        , p_ads_udf_type_5_i   => null
			        , p_ads_udf_type_6_i   => null
			        , p_ads_udf_type_7_i   => null
			        , p_ads_udf_type_8_i   => null
			        , p_ads_udf_num_1_i    => null
			        , p_ads_udf_num_2_i    => null
			        , p_ads_udf_num_3_i    => null
			        , p_ads_udf_num_4_i    => null
			        , p_ads_udf_chk_1_i    => null
			        , p_ads_udf_chk_2_i    => null
			        , p_ads_udf_chk_3_i    => null
			        , p_ads_udf_chk_4_i    => null
			        , p_ads_udf_dstamp_1_i => null
			        , p_ads_udf_dstamp_2_i => null
			        , p_ads_udf_dstamp_3_i => null
			        , p_ads_udf_dstamp_4_i => null
			        , p_ads_udf_note_1_i   => null
			        , p_ads_udf_note_2_i   => null
			        , p_directions_i       => null
			        , p_vat_number_i       => r_ohr.hub_vat_number
			        , p_address_type_1     => null
			        , p_address_id_i       => null
			        )
		then
	                 -- Compound address
	                g_add.put('compound address',	g_add_ads);
	                g_add_ads			:= pljson();
                end if;
		  --dbms_output.put_line(g_add_ads.to_char( true ));
		else
			open	c_ads( b_client_id  => p_client_id_i
				     , b_address_id => g_sfm
				     );
			fetch 	c_ads
			into  	r_ads;
			if 	c_ads%found
			then
			     if add_ads ( p_field_prefix_i     => p_field_prefix_i
					, p_ads_type_i         => g_sfm
					, p_name_1_i           => r_ads.name
					, p_address_1_i        => r_ads.address1
					, p_city_i             => r_ads.town
					, p_zip_code_i         => r_ads.postcode
					, p_state_code_i       => r_ads.county
					, p_cty_iso_i          => r_ads.country
					, p_address_2_i        => r_ads.address2
					, p_address_3_i        => null
					, p_address_4_i        => null
					, p_phone_i            => r_ads.contact_phone
					, p_mobile_i           => r_ads.contact_mobile
					, p_fax_i              => r_ads.contact_fax
					, p_email_i            => r_ads.contact_email
					, p_contact_name_i     => r_ads.contact_name
					, p_web_i              => r_ads.url
					, p_ads_udf_type_1_i   => r_ads.user_def_type_1
					, p_ads_udf_type_2_i   => r_ads.user_def_type_2
					, p_ads_udf_type_3_i   => r_ads.user_def_type_3
					, p_ads_udf_type_4_i   => r_ads.user_def_type_4
					, p_ads_udf_type_5_i   => r_ads.user_def_type_5
					, p_ads_udf_type_6_i   => r_ads.user_def_type_6
					, p_ads_udf_type_7_i   => r_ads.user_def_type_7
					, p_ads_udf_type_8_i   => r_ads.user_def_type_8
					, p_ads_udf_num_1_i    => r_ads.user_def_num_1
					, p_ads_udf_num_2_i    => r_ads.user_def_num_2
					, p_ads_udf_num_3_i    => r_ads.user_def_num_3
					, p_ads_udf_num_4_i    => r_ads.user_def_num_4
					, p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
					, p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
					, p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
					, p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
					, p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
					, p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
					, p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
					, p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
					, p_ads_udf_note_1_i   => r_ads.user_def_note_1
					, p_ads_udf_note_2_i   => r_ads.user_def_note_2
					, p_directions_i       => r_ads.directions
					, p_vat_number_i       => r_ads.vat_number
					, p_address_type_1     => r_ads.address_type
					, p_address_id_i       => r_ads.address_id
					)
				then
				-- Compound address
	                          g_add.put('adress1',		g_add_ads);
	                          g_add_ads			:= pljson();
                              end if;
			      --dbms_output.put_line(g_add_ads.to_char( true ));
			end if;
			close	c_ads;        
		end if;
		-- HUB - Hub Address
  	        if add_ads(  p_field_prefix_i     => p_field_prefix_i
		       , p_ads_type_i         => g_hub
		       , p_name_1_i           => r_ohr.hub_name
		       , p_address_1_i        => r_ohr.hub_address1
		       , p_city_i             => r_ohr.hub_town
		       , p_zip_code_i         => r_ohr.hub_postcode
		       , p_state_code_i       => r_ohr.hub_county
		       , p_cty_iso_i          => r_ohr.hub_country
		       , p_address_2_i        => r_ohr.hub_address2
		       , p_address_3_i        => null
		       , p_address_4_i        => null
		       , p_phone_i            => r_ohr.hub_contact_phone
		       , p_mobile_i           => r_ohr.hub_contact_mobile
		       , p_fax_i              => r_ohr.hub_contact_fax
		       , p_email_i            => r_ohr.hub_contact_email
		       , p_contact_name_i     => r_ohr.hub_contact
		       , p_web_i              => null
		       , p_ads_udf_type_1_i   => null
		       , p_ads_udf_type_2_i   => null
		       , p_ads_udf_type_3_i   => null
		       , p_ads_udf_type_4_i   => null
		       , p_ads_udf_type_5_i   => null
		       , p_ads_udf_type_6_i   => null
		       , p_ads_udf_type_7_i   => null
		       , p_ads_udf_type_8_i   => null
		       , p_ads_udf_num_1_i    => null
		       , p_ads_udf_num_2_i    => null
		       , p_ads_udf_num_3_i    => null
		       , p_ads_udf_num_4_i    => null
		       , p_ads_udf_chk_1_i    => null
		       , p_ads_udf_chk_2_i    => null
		       , p_ads_udf_chk_3_i    => null
		       , p_ads_udf_chk_4_i    => null
		       , p_ads_udf_dstamp_1_i => null
		       , p_ads_udf_dstamp_2_i => null
		       , p_ads_udf_dstamp_3_i => null
		       , p_ads_udf_dstamp_4_i => null
		       , p_ads_udf_note_1_i   => null
		       , p_ads_udf_note_2_i   => null
		       , p_directions_i       => null
		       , p_vat_number_i       => r_ohr.hub_vat_number
		       , p_address_type_1     => null
		       , p_address_id_i       => null
		       )
		then
		        -- Compound address
	                g_add.put('hub adress',		g_add_ads);
	                g_add_ads			:= pljson();
                end if;
		--dbms_output.put_line(g_add_ads.to_char( true ));

		-- STO - Delivery Address
		if add_ads(p_field_prefix_i     => p_field_prefix_i
	               , p_ads_type_i         => g_sto
		       , p_name_1_i           => r_ohr.name
		       , p_address_1_i        => r_ohr.address1
		       , p_city_i             => r_ohr.town
		       , p_zip_code_i         => r_ohr.postcode
		       , p_state_code_i       => r_ohr.county
		       , p_cty_iso_i          => r_ohr.country
		       , p_address_2_i        => r_ohr.address2
		       , p_address_3_i        => null
		       , p_address_4_i        => null
		       , p_phone_i            => r_ohr.contact_phone
		       , p_mobile_i           => r_ohr.contact_mobile
		       , p_fax_i              => r_ohr.contact_fax
		       , p_email_i            => r_ohr.contact_email
		       , p_contact_name_i     => r_ohr.contact
		       , p_web_i              => null
		       , p_ads_udf_type_1_i   => null
		       , p_ads_udf_type_2_i   => null
		       , p_ads_udf_type_3_i   => null
		       , p_ads_udf_type_4_i   => null
		       , p_ads_udf_type_5_i   => null
		       , p_ads_udf_type_6_i   => null
		       , p_ads_udf_type_7_i   => null
		       , p_ads_udf_type_8_i   => null
		       , p_ads_udf_num_1_i    => null
		       , p_ads_udf_num_2_i    => null
		       , p_ads_udf_num_3_i    => null
		       , p_ads_udf_num_4_i    => null
		       , p_ads_udf_chk_1_i    => null
		       , p_ads_udf_chk_2_i    => null
		       , p_ads_udf_chk_3_i    => null
		       , p_ads_udf_chk_4_i    => null
		       , p_ads_udf_dstamp_1_i => null
		       , p_ads_udf_dstamp_2_i => null
		       , p_ads_udf_dstamp_3_i => null
		       , p_ads_udf_dstamp_4_i => null
		       , p_ads_udf_note_1_i   => null
		       , p_ads_udf_note_2_i   => null
		       , p_directions_i       => null
		       , p_vat_number_i       => r_ohr.vat_number
		       , p_address_type_1     => null
		       , p_address_id_i       => r_ohr.customer_id
		       )
		then
		        -- STO - Delivery Address
	                g_add.put('Delivery Address',		g_add_ads);
	                g_add_ads				:= pljson();
		end if;
		--dbms_output.put_line(g_add_ads.to_char( true ));
		-- SHP - Shipper Address
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => g_shp
			     );
		fetch 	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
		      if  add_ads(p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_shp
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       )
		    then
			        -- STO - Delivery Address
	                g_add.put('STO - Delivery Address',	g_add_ads);
	                g_add_ads				:= pljson();
                    end if;
		end if;
		--dbms_output.put_line(g_add_ads.to_char( true ));
		close	c_ads;        

		-- BTO - Invoice Address
		if add_ads( p_field_prefix_i     => p_field_prefix_i
		       , p_ads_type_i         => g_bto
		       , p_name_1_i           => r_ohr.inv_name
		       , p_address_1_i        => r_ohr.inv_address1
		       , p_city_i             => r_ohr.inv_town
		       , p_zip_code_i         => r_ohr.inv_postcode
   		       , p_state_code_i       => r_ohr.inv_county
		       , p_cty_iso_i          => r_ohr.inv_country
		       , p_address_2_i        => r_ohr.inv_address2
		       , p_address_3_i        => null
		       , p_address_4_i        => null
		       , p_phone_i            => r_ohr.inv_contact_phone
		       , p_mobile_i           => r_ohr.inv_contact_mobile
		       , p_fax_i              => r_ohr.inv_contact_fax
		       , p_email_i            => r_ohr.inv_contact_email
		       , p_contact_name_i     => r_ohr.inv_contact
		       , p_web_i              => null
		       , p_ads_udf_type_1_i   => null
		       , p_ads_udf_type_2_i   => null
		       , p_ads_udf_type_3_i   => null
		       , p_ads_udf_type_4_i   => null
		       , p_ads_udf_type_5_i   => null
		       , p_ads_udf_type_6_i   => null
		       , p_ads_udf_type_7_i   => null
		       , p_ads_udf_type_8_i   => null
		       , p_ads_udf_num_1_i    => null
		       , p_ads_udf_num_2_i    => null
		       , p_ads_udf_num_3_i    => null
		       , p_ads_udf_num_4_i    => null
		       , p_ads_udf_chk_1_i    => null
		       , p_ads_udf_chk_2_i    => null
		       , p_ads_udf_chk_3_i    => null
		       , p_ads_udf_chk_4_i    => null
		       , p_ads_udf_dstamp_1_i => null
		       , p_ads_udf_dstamp_2_i => null
		       , p_ads_udf_dstamp_3_i => null
		       , p_ads_udf_dstamp_4_i => null
		       , p_ads_udf_note_1_i   => null
		       , p_ads_udf_note_2_i   => null
		       , p_directions_i       => null
		       , p_vat_number_i       => r_ohr.inv_vat_number
		       , p_address_type_1     => null
		       , p_address_id_i       => r_ohr.inv_address_id
		       )
		then
		        --  BTO - Invoice Address
	                g_add.put('Invoice Address',	g_add_ads);
	                g_add_ads			:= pljson();
		end if;
                --dbms_output.put_line(g_add_ads.to_char( true ));
		-- CRR - Carrier Address
		open  	c_crr( b_client_id     => p_client_id_i
			     , b_carrier_id    => r_ohr.carrier_id
			     , b_service_level => r_ohr.service_level
			     );
		fetch	c_crr
		into  	r_crr;
		if 	c_crr%found
		then
			if add_ads( p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_crr
			       , p_name_1_i           => r_crr.name
			       , p_address_1_i        => r_crr.address1
			       , p_city_i             => r_crr.town
			       , p_zip_code_i         => r_crr.postcode
			       , p_state_code_i       => r_crr.county
			       , p_cty_iso_i          => r_crr.country
			       , p_address_2_i        => r_crr.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_crr.contact_phone
			       , p_mobile_i           => r_crr.contact_mobile
			       , p_fax_i              => r_crr.contact_fax
			       , p_email_i            => r_crr.contact_email
			       , p_contact_name_i     => r_crr.contact_name
			       , p_web_i              => r_crr.url
			       , p_ads_udf_type_1_i   => r_crr.user_def_type_1
			       , p_ads_udf_type_2_i   => r_crr.user_def_type_2
			       , p_ads_udf_type_3_i   => r_crr.user_def_type_3
			       , p_ads_udf_type_4_i   => r_crr.user_def_type_4
			       , p_ads_udf_type_5_i   => r_crr.user_def_type_5
			       , p_ads_udf_type_6_i   => r_crr.user_def_type_6
			       , p_ads_udf_type_7_i   => r_crr.user_def_type_7
			       , p_ads_udf_type_8_i   => r_crr.user_def_type_8
			       , p_ads_udf_num_1_i    => r_crr.user_def_num_1
			       , p_ads_udf_num_2_i    => r_crr.user_def_num_2
			       , p_ads_udf_num_3_i    => r_crr.user_def_num_3
			       , p_ads_udf_num_4_i    => r_crr.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_crr.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_crr.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_crr.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_crr.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_crr.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_crr.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_crr.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_crr.user_def_date_4
			       , p_ads_udf_note_1_i   => r_crr.user_def_note_1
			       , p_ads_udf_note_2_i   => r_crr.user_def_note_2
			       , p_directions_i       => r_crr.notes
			       , p_vat_number_i       => null
			       , p_address_type_1     => null
			       , p_address_id_i       => null
			       )
			then
		        --  CRR - Carrier Address
	                   g_add.put('Carrier Address',		g_add_ads);
	                   g_add_ads				:= pljson();
			end if;
	        --dbms_output.put_line(g_add_ads.to_char( true ));
		end if;
		close 	c_crr;        

		-- ADL - Additional Address (CID in Order)
		open  	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => r_ohr.cid_number
			     );
		fetch	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			if add_ads( p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_adl
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       )
			  then
		        --  ADL - Additional Address (CID in Order)
	                    g_add.put('Additional Address (CID in Order)',	g_add_ads);
	                    g_add_ads						:= pljson();
		        end if;
		--dbms_output.put_line(g_add_ads.to_char( true )); 
		end if;
		close 	c_ads;        

		-- CID - Consignee ID Address (CID in Order)
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => r_ohr.cid_number
			     );
		fetch	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			if add_ads(p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_cid
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       )
			then
		        --  CID - Consignee ID Address (CID in Order)
	                   g_add.put('Consignee ID Address (CID in Order)',	g_add_ads);
	                   g_add_ads						:= pljson();
			end if;
			--dbms_output.put_line(g_add_ads.to_char( true )); 
		end if;
		close	c_ads;        

		-- SID - Shipment ID Address (SID in Order)
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => r_ohr.sid_number
			     );
		fetch 	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			if add_ads(  p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_sid
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       )
			then
		        -- SID - Shipment ID Address (SID in Order)
	                   g_add.put('Shipment ID Address (SID in Order)',	g_add_ads);
	                   g_add_ads							:= pljson();
			end if;
		        --dbms_output.put_line(g_add_ads.to_char( true )); 
		end if;
		close 	c_ads;        

		-- CLT - Client Address
		open	c_clt( b_client_id => p_client_id_i);
		fetch 	c_clt
		into  	r_clt;
		if 	c_clt%found
		then
			if add_ads( p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_clt
			       , p_name_1_i           => r_clt.name
			       , p_address_1_i        => r_clt.address1
			       , p_city_i             => r_clt.town
			       , p_zip_code_i         => r_clt.postcode
			       , p_state_code_i       => r_clt.county
			       , p_cty_iso_i          => r_clt.country
			       , p_address_2_i        => r_clt.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_clt.contact_phone
			       , p_mobile_i           => r_clt.contact_mobile
			       , p_fax_i              => r_clt.contact_fax
			       , p_email_i            => r_clt.contact_email
			       , p_contact_name_i     => r_clt.contact_name
			       , p_web_i              => r_clt.url
			       , p_ads_udf_type_1_i   => r_clt.user_def_type_1
			       , p_ads_udf_type_2_i   => r_clt.user_def_type_2
			       , p_ads_udf_type_3_i   => r_clt.user_def_type_3
			       , p_ads_udf_type_4_i   => r_clt.user_def_type_4
			       , p_ads_udf_type_5_i   => r_clt.user_def_type_5
			       , p_ads_udf_type_6_i   => r_clt.user_def_type_6
			       , p_ads_udf_type_7_i   => r_clt.user_def_type_7
			       , p_ads_udf_type_8_i   => r_clt.user_def_type_8
			       , p_ads_udf_num_1_i    => r_clt.user_def_num_1
			       , p_ads_udf_num_2_i    => r_clt.user_def_num_2
			       , p_ads_udf_num_3_i    => r_clt.user_def_num_3
			       , p_ads_udf_num_4_i    => r_clt.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_clt.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_clt.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_clt.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_clt.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_clt.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_clt.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_clt.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_clt.user_def_date_4
			       , p_ads_udf_note_1_i   => r_clt.user_def_note_1
			       , p_ads_udf_note_2_i   => r_clt.user_def_note_2
			       , p_directions_i       => r_clt.notes
			       , p_vat_number_i       => r_clt.vat_number
			       , p_address_type_1     => null
			       , p_address_id_i       => null
			       )
			then
		        -- CLT - Client Address
	                  g_add.put('Client Address',		g_add_ads);
	                  g_add_ads				:= pljson();
                        end if;
			--dbms_output.put_line(g_add_ads.to_char( true )); 
		end if;	
		close c_clt;        

		-- HAZ - Hazardous Goods Shipper Address (AddressID defined in Constants table e.g. 'HAZARDOUS' )
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => g_wms_hazardous_ads_id
			     );
		fetch	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			if add_ads(p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_haz
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       )
			then
		        -- HAZ - Hazardous Goods Shipper Address (AddressID defined in Constants table e.g. 'HAZARDOUS' )
	                  g_add.put('HAZ Address',	g_add_ads);
	                  g_add_ads			:= pljson();
                        end if;

		end if;
		close	c_ads;        
		close	c_ohr;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finihed adding SMT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
           return	true;
	exception
		when	others
		then
			case 
			when	c_ohr%isopen
			then
				close	c_ohr;
			when 	c_ocr%isopen
			then
				close 	c_ocr;
			when 	c_clt%isopen
			then
				close 	c_clt;
			when 	c_crr%isopen
			then
				close 	c_crr;
			when 	c_ads%isopen
			then
				close 	c_ads;
			else
				null;
			end case;
	    return	l_retval;
	end add_smt;

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Create StreamServe Shipment Items Lot block
------------------------------------------------------------------------------------------------
	function add_sim_lot(  p_field_prefix_i in  varchar2
			     , p_segment_nr_i   in  number
			     , p_client_id_i    in  varchar2
			     , p_order_nr_i     in  varchar2
			     , p_pallet_id_i    in  varchar2 := null
			     , p_container_id_i in  varchar2 := null
			     , p_is_cont_yn_i   in  varchar2
			     )
	return boolean
	is
		-- Fetch total QTY from inventory, shipping manifest, move_task
		cursor c_qty_chk( b_client_id    in varchar2
				, b_order_id     in varchar2
				, b_pallet_id    in varchar2
				, b_container_id in varchar2
				)
		is
			select	(	select	sum(smt.qty_shipped)         	qty
					from   	dcsdba.shipping_manifest     	smt
					where  	smt.client_id                	= b_client_id
					and    	smt.order_id                 	= b_order_id
					and    	(	nvl( smt.pallet_id, '@#')    	= nvl( b_pallet_id,'@#') or
							b_pallet_id is null)
					and    	(	nvl( smt.container_id, '@#') 	= nvl( b_container_id, '@#') or
							b_container_id is null)
				) 	total_qty_smt
		      ,		( 	select 	sum(mtk.qty_to_move)		qty
					from   	dcsdba.move_task              	mtk
					where   mtk.client_id 			= b_client_id
					and    	mtk.task_id                   	= b_order_id
					and    	(	nvl( mtk.pallet_id, '@#')    	= nvl( b_pallet_id,'@#') or
							b_pallet_id is null)
					and    	(	nvl( mtk.container_id, '@#') 	= nvl( b_container_id, '@#') or 
							b_container_id is null)
					and	mtk.status 			= 'Consol'
					and    	not exists 	(	select 	1
									from   dcsdba.shipping_manifest smt
									where  smt.client_id            = b_client_id
									and    smt.order_id             = b_order_id
									and    smt.pallet_id            = mtk.pallet_id
									and    smt.container_id         = mtk.container_id
								)
				) 	total_qty_mvt
		      ,		(	select	sum(qty_on_hand)		qty
					from	dcsdba.inventory		ivy
					where	ivy.client_id 			= b_client_id
					and    	(	nvl( ivy.pallet_id, '@#')    	= nvl( b_pallet_id,'@#') or
							b_pallet_id is null)
					and    	(	nvl( ivy.container_id, '@#') 	= nvl( b_container_id, '@#') or
							b_container_id is null)
					and    	not exists 	(	select 	1
									from   dcsdba.shipping_manifest smt
									where  smt.client_id            = b_client_id
									and    smt.order_id             = b_order_id
									and    smt.pallet_id            = ivy.pallet_id
									and    smt.container_id         = ivy.container_id
								)
				)	total_qty_ivy
		from dual
		;	

		-- Fetch details from shipping_manifest or inventory and move task combo
		cursor c_ocr_dtl( b_client_id    in varchar2
				, b_order_id     in varchar2
				, b_pallet_id    in varchar2
				, b_container_id in varchar2
				)
		is
			select	smt.tag_id                   
			,      	smt.sku_id
			,      	smt.batch_id                 
			,      	smt.expiry_dstamp            
			,      	smt.origin_id                
			,      	sum(smt.qty_shipped)		qty
			,      	smt.container_id
			,      	smt.condition_id
			,      	smt.receipt_dstamp
			,      	smt.manuf_dstamp
			,      	smt.receipt_id
			from   	dcsdba.shipping_manifest     	smt
			where  	smt.client_id                	= b_client_id
			and    	smt.order_id                 	= b_order_id
			and    	nvl( smt.pallet_id, '@#')    	= nvl( b_pallet_id, nvl( smt.pallet_id, '@#'))
			and    	nvl( smt.container_id, '@#') 	= nvl( b_container_id, nvl( smt.container_id, '@#'))
			group  
			by 	smt.tag_id
			,      	smt.sku_id
			,      	smt.batch_id
			,      	smt.expiry_dstamp
			,      	smt.origin_id
			,      	smt.container_id
			,      	smt.condition_id
			,      	smt.receipt_dstamp
			,      	smt.manuf_dstamp
			,      	smt.receipt_id
			union  	-- For pallets which are not 'Marshalled'
			select 	tag_id
			,	sku_id
			,	batch_id
			,	expiry_dstamp
			,	origin_id
			,	sum(qty_to_move)          	qty
			,	decode(to_container_id,null,container_id,to_container_id)
			,	condition_id
			,	receipt_dstamp
			,	manuf_dstamp
			,	receipt_id
			from(	select 	mtk.tag_id
				,      	mtk.sku_id
				,      	(select i.batch_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) batch_id
				,      	(select i.expiry_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) expiry_dstamp
				,      	(select i.origin_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) origin_id
				,      	mtk.qty_to_move
				,      	mtk.container_id
				,       mtk.to_container_id
				,      	(select i.condition_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) condition_id
				,      	(select i.receipt_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_dstamp
				,      	(select i.manuf_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) manuf_dstamp
				,      	(select i.receipt_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_id
				from   	dcsdba.move_task		mtk
				where  	mtk.client_id                 	= b_client_id
				and    	mtk.task_id                   	= b_order_id
				and    	(	(	nvl( mtk.pallet_id, '@#')    	= nvl( b_pallet_id, nvl( mtk.pallet_id, '@#'))
						and    	nvl( mtk.container_id, '@#') 	= nvl( b_container_id, nvl( mtk.container_id, '@#'))
						)
						or
						(	nvl( mtk.to_pallet_id, '@#')    	= nvl( b_pallet_id, nvl( mtk.to_pallet_id, '@#'))
						and    	nvl( mtk.to_container_id, '@#') 	= nvl( b_container_id, nvl( mtk.to_container_id, '@#'))
						)
					)
				and    	not exists(	select	1
							from   	dcsdba.shipping_manifest smt
							where  	smt.client_id            = b_client_id
							and    	smt.order_id             = b_order_id
							and    	smt.pallet_id            = mtk.pallet_id
							and    	smt.container_id         = mtk.container_id
						  )
			    )
			group  
			by 	tag_id
			,      	sku_id
			,      	batch_id
			,      	expiry_dstamp
			,      	origin_id
			,	decode(to_container_id,null,container_id,to_container_id)
			,      	condition_id
			,      	receipt_dstamp
			,      	manuf_dstamp
			,      	receipt_id
		;


		-- Fetch serial numbers
		cursor c_snr( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_container_id in varchar2
			    , b_sku_id       in varchar2
			    , b_tag_id       in varchar2
			    )
		is
			select	snr.serial_number
			from   	dcsdba.serial_number snr
			where  	snr.client_id        = b_client_id
			and    	snr.order_id         = b_order_id
			and    	snr.container_id     = b_container_id
			and    	snr.sku_id           = b_sku_id
			and    	snr.tag_id           = b_tag_id
			order  
			by 	snr.serial_number
		;   

		-- 
		r_ocr_dtl	c_ocr_dtl%rowtype;
		r_snr         	c_snr%rowtype;

		l_pallet_id     varchar2(20);
		l_container_id  varchar(20);
		l_count_dtl     number(10) := 0;
		l_snr_prefix    varchar2(20);
		l_count_snr     number(10);
		l_snr_total     varchar2(3999);
		l_snr_line      varchar2(3999);
		l_no_val_chk    integer :=0;
		l_retry	    	number :=0;
		l_qty_chk	c_qty_chk%rowtype;
		l_rtn		varchar2(30) := 'add_sim_lot';
		l_retval	boolean 	:= true;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding SIM_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"is_cont_yn" "'||p_is_cont_yn_i 
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		-- set pallet/container acc. is_cont_yn parameter
		if	p_is_cont_yn_i = g_yes
		then
			l_pallet_id	:= null;
			l_container_id 	:= p_pallet_id_i;
		else
			l_pallet_id    	:= p_pallet_id_i;
			l_container_id 	:= p_container_id_i;
		end if;


		-- Add LOT lines
		<<lot_line_loop>>
		for	r_ocr_dtl in c_ocr_dtl( b_client_id    => p_client_id_i    
					      , b_order_id     => p_order_nr_i
					      , b_pallet_id    => l_pallet_id
					      , b_container_id => l_container_id
					      )
		loop
			l_no_val_chk	:= 1;
			l_count_dtl 	:= l_count_dtl + 1;
			--
			g_add_sim_lot_loop.put('SIM_LOT_SEGMENT_NR',	'Segment SIM / Lot: ' || to_char( p_segment_nr_i) || ' / ' || to_char( l_count_dtl));
			g_add_sim_lot_loop.put('SIM_LOT_TAG_ID',	r_ocr_dtl.tag_id);
			g_add_sim_lot_loop.put('SIM_LOT_SKU_ID',	r_ocr_dtl.sku_id);
			g_add_sim_lot_loop.put('SIM_LOT_BATCH_ID',	r_ocr_dtl.batch_id);
			g_add_sim_lot_loop.put('SIM_LOT_EXPIRY_DATE',	to_char( r_ocr_dtl.expiry_dstamp, 'DD-MM-YYYY'));
			g_add_sim_lot_loop.put('SIM_LOT_EXPIRY_TIME',	to_char( r_ocr_dtl.expiry_dstamp, 'HH24:MI:SS'));
			g_add_sim_lot_loop.put('SIM_LOT_RECEIPT_DATE',	to_char( r_ocr_dtl.receipt_dstamp, 'DD-MM-YYYY'));
			g_add_sim_lot_loop.put('SIM_LOT_RECEIPT_TIME',	to_char( r_ocr_dtl.receipt_dstamp, 'HH24:MI:SS'));
			g_add_sim_lot_loop.put('SIM_LOT_MANUF_DATE',	to_char( r_ocr_dtl.manuf_dstamp, 'DD-MM-YYYY'));
			g_add_sim_lot_loop.put('SIM_LOT_MANUF_TIME',	to_char( r_ocr_dtl.manuf_dstamp, 'HH24:MI:SS'));
			g_add_sim_lot_loop.put('SIM_LOT_ORIGIN_ID',	r_ocr_dtl.origin_id);
			g_add_sim_lot_loop.put('SIM_LOT_CONDITION_ID',	r_ocr_dtl.condition_id);
			g_add_sim_lot_loop.put('SIM_LOT_QTY',	        r_ocr_dtl.qty);
			g_add_sim_lot_loop.put('SIM_LOT_CONTAINER_ID',  r_ocr_dtl.container_id);
		        g_add_sim_lot_list.append(g_add_sim_lot_loop);
                        g_add_sim_lot_loop := pljson();
			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> g_file_name
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Add SIM_LOT_SERIALS'
									   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
													|| '"segment_nr" "'||p_segment_nr_i||'" '
													|| '"is_cont_yn" "'||p_is_cont_yn_i 
									   , p_order_id_i		=> p_order_nr_i
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> l_pallet_id
									   , p_container_id_i		=> l_container_id
									   , p_site_id_i		=> null
									   );
			end if;
			-- Add Serial Numbers
			l_count_snr := 0;
			l_snr_total := null;
			l_snr_line  := null;
			for	r_snr in c_snr( b_client_id    => p_client_id_i
					      , b_order_id     => p_order_nr_i
					      , b_container_id => r_ocr_dtl.container_id
					      , b_sku_id       => r_ocr_dtl.sku_id
					      , b_tag_id       => r_ocr_dtl.tag_id
					      )
			loop
				l_count_snr := l_count_snr + 1;
				l_snr_total := l_snr_line || ', ' || r_snr.serial_number;
				case
				when	length( l_snr_total) > 110
				then
					l_count_snr := 1;
					l_snr_line  := r_snr.serial_number;
					l_snr_total := r_snr.serial_number;
				else
					case	l_count_snr
					when 	1
					then
						l_snr_line  := r_snr.serial_number;
						l_snr_total := r_snr.serial_number;
					else
						l_snr_line  := l_snr_line || ', ' || r_snr.serial_number;
					end case;
				end case;
			end loop; -- SIM LOT SNR loop
                        --
		        g_add_sim_lot.put('SIM_LOT_SERIAL_TOTAL_NRS',	l_snr_line);	  
                        --

		end loop; -- SIM LOT loop

		g_add_sim_lot.put('segment_list', g_add_sim_lot_list);
                g_add_sim_lot_list := pljson_list();
		--dbms_output.put_line(g_add_sim_lot.to_char( true ));
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding SIM_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"is_cont_yn" "'||p_is_cont_yn_i 
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;
		return	true;
        exception
	  when others then
	  return	l_retval;
	end add_sim_lot;

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Create StreamServe Shipment Items block
------------------------------------------------------------------------------------------------
	function add_sim( p_field_prefix_i in  varchar2
		        , p_client_id_i    in  varchar2
		        , p_order_nr_i     in  varchar2
		        , p_pallet_id_i    in  varchar2 := null
		        , p_container_id_i in  varchar2 := null
		        )
	return boolean
	is
		cursor c_ocr( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    )
		is
			select	rownum
			,      	plt.*
			from   	(
				select	distinct
					smt.client_id
				,      	smt.order_id
				,      	nvl( smt.labelled, g_no)                                            			is_cont_yn
				,      	decode( smt.labelled, g_yes, smt.container_id, smt.pallet_id)				pallet_id
				,      	decode( smt.labelled, g_yes, smt.container_type, smt.pallet_config)			pallet_type
				,      	smt.carrier_consignment_id                                          			tracking_nr
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_weight, smt.pallet_weight), 1))	weight
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_depth, smt.pallet_depth), 1))	length
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_width, smt.pallet_width), 1))	width
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_height, smt.pallet_height), 1))	height
				,      	max( nvl( decode( smt.labelled, g_yes, round( ( 
					smt.container_depth * smt.container_width * smt.container_height) / 1000000, 6), 
					round( ( smt.pallet_depth * smt.pallet_width * smt.pallet_height) / 1000000, 6)), 1))	volume
				,      	1                                                                   			cnt
				,      	decode( smt.labelled, g_yes, 1, nvl( smt.transport_boxes, 1))				no_of_boxes
				from   	dcsdba.shipping_manifest            							smt
				where  	smt.client_id                       	= b_client_id
				and    	smt.order_id                        	= b_order_id
				and    	smt.pallet_id                       	= nvl( b_pallet_id, smt.pallet_id)
				and    	smt.container_id                    	= nvl( b_container_id, smt.container_id)
				group  
				by 	smt.client_id
				,      	smt.order_id
				,      	nvl( smt.labelled, g_no)
				,      	decode( smt.labelled, g_yes, smt.container_id, smt.pallet_id)
				,      	decode( smt.labelled, g_yes, smt.container_type, smt.pallet_config)
				,      	smt.carrier_consignment_id
				,      	decode( smt.labelled, g_yes, 1, nvl( smt.transport_boxes, 1))
				union  	-- for pallets which are not 'marshalled' yet
				select 	distinct
					ocr.client_id
				,      	ocr.order_id
				,      	nvl( ocr.labelled, g_no)                                            			is_cont_yn
				,      	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id)				pallet_id
				,      	decode( ocr.labelled, g_yes, nvl(to_container_config, container_config), nvl(to_pallet_config, pallet_config)) pallet_type --jira DBS-5398
				,      	ocr.carrier_consignment_id                                          			tracking_nr
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_weight, ocr.pallet_weight), 1))	weight
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_depth, ocr.pallet_depth), 1))	length
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_width, ocr.pallet_width), 1))	width
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_height, ocr.pallet_height), 1))	height
				,      	max( nvl( decode( ocr.labelled, g_yes, round( ( 
					ocr.container_depth * ocr.container_width * ocr.container_height) / 1000000, 6), 
					round( ( ocr.pallet_depth * ocr.pallet_width * ocr.pallet_height) / 1000000, 6)), 1))	volume
				,      	1                                                                   			cnt
				,      	decode( ocr.labelled, g_yes, 1, nvl( ocr.transport_boxes, 1))				no_of_boxes
				from   	dcsdba.order_container              ocr
				,       dcsdba.move_task mt 
				where  	ocr.client_id                       = b_client_id
				and    	ocr.order_id                        = b_order_id
				and    	ocr.pallet_id                       = nvl( b_pallet_id, ocr.pallet_id)
				and    	ocr.container_id                    = nvl( b_container_id, ocr.container_id)
				and     mt.client_id = ocr.client_id 
				AND     mt.task_id = ocr.order_id 
				AND     ocr.container_id = nvl( mt.to_container_id, mt.container_id)
			        AND     ocr.pallet_id    = nvl( mt.to_pallet_id, mt.pallet_id)
				and    	not exists (	select 1
							from   dcsdba.shipping_manifest smt
							where  smt.client_id            = ocr.client_id
							and    smt.order_id             = ocr.order_id
							and    smt.pallet_id            = ocr.pallet_id
							and    smt.container_id         = ocr.container_id
						   )
				group  
				by 	ocr.client_id
				,      	ocr.order_id
				,      	nvl( ocr.labelled, g_no)
				,      	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id)
				,       decode( ocr.labelled, g_yes, nvl(to_container_config, container_config), nvl(to_pallet_config, pallet_config))
				,      	ocr.carrier_consignment_id
				,      	decode( ocr.labelled, g_yes, 1, nvl( ocr.transport_boxes, 1))
				order  
				by 	client_id
				,      	order_id
				,      	pallet_id) 	plt
		;

		--
		cursor c_pcg( b_client_id in varchar2
			    , b_config_id in varchar2
			    )
		is
			select pcg.client_id              config_client_id
			,      pcg.config_id              config_type
			,      pcg.notes                  config_notes
			,      pcg.pallet_type_group      config_group
			,      ptp.notes                  config_group_notes
			,      nvl( pcg.weight, 0)        config_weight
			,      nvl( pcg.depth, 0)         config_length
			,      nvl( pcg.width, 0)         config_width
			,      nvl( pcg.height, 0)        config_height
			,      (nvl( pcg.depth, 0) * nvl( pcg.width, 0) * nvl( pcg.height, 0)) / 1000000                config_volume
			from   dcsdba.pallet_config       pcg
			,      dcsdba.pallet_type_grp     ptp
			where  pcg.pallet_type_group      = ptp.pallet_type_group (+)
			and    (pcg.client_id = ptp.client_id or ptp.client_id is null)
			and    (pcg.client_id = b_client_id or pcg.client_id is null)
			and    	pcg.config_id = b_config_id
			order  
			by 	pcg.client_id	nulls last
		;

		--
		cursor c_haz( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    )
		is
			select	distinct
				replace(hmt.user_def_type_1, ' ', null) un_code
			from   	dcsdba.shipping_manifest      smt
			,      	dcsdba.sku                    sku
		        ,      	dcsdba.hazmat                 hmt    
		        where  	smt.client_id                 = b_client_id
		        and    	smt.order_id                  = b_order_id
		        and    	(smt.container_id = b_pallet_id or smt.pallet_id = b_pallet_id)
		        and    	smt.client_id                 = sku.client_id
		        and    	smt.sku_id                    = sku.sku_id 
		        and    	sku.hazmat_id                 = hmt.hazmat_id
		        and    	hmt.hazmat_id                 like 'RHS%'
		        union	all
		        select 	distinct
				replace(hmt.user_def_type_1, ' ', null) un_code
		        from   	dcsdba.order_container        ocr
		        ,       dcsdba.move_task              mvt
		        ,      	dcsdba.sku                    sku
		        ,      	dcsdba.hazmat                 hmt    
		        where  	ocr.client_id                 = b_client_id
		        and    	ocr.order_id                  = b_order_id
		        and    	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id) = b_pallet_id
		        and    	ocr.client_id                 = mvt.client_id
		        and    	ocr.pallet_id                 = mvt.pallet_id
		        and    	ocr.container_id              = mvt.container_id
			    and	    mvt.task_id 		          != 'PALLET'
		        and    	mvt.client_id                 = sku.client_id
		        and    	mvt.sku_id                    = sku.sku_id 
		        and    	sku.hazmat_id                 = hmt.hazmat_id
		        and    	hmt.hazmat_id                 like 'RHS%'
		        and    	not exists (	select	1
						from   	dcsdba.shipping_manifest smt
						where  	smt.client_id            = ocr.client_id
						and    	smt.order_id             = ocr.order_id
						and    	smt.pallet_id            = ocr.pallet_id
						and    	smt.container_id         = ocr.container_id
					   )
			order  
			by 	1
		;

		--
		r_ocr         	c_ocr%rowtype;
		r_pcg         	c_pcg%rowtype;
		r_haz         	c_haz%rowtype;

		l_count       	number(10)     := 0;
		l_un_cnt      	number(10)     := 0;
		l_un_total    	varchar2(3999);
		l_un_line     	varchar2(3999);
		l_rtn		varchar2(30)   := 'add_sim';	
		l_retval	boolean	       := false;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding SIM'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		-- Start looping all pallet id's
		for	r_ocr in c_ocr( b_client_id    => p_client_id_i
				      , b_order_id     => p_order_nr_i
				      , b_pallet_id    => p_pallet_id_i
				      , b_container_id => p_container_id_i
				      )
		loop

			l_count := l_count + 1;
			--
			open	c_pcg( b_client_id => r_ocr.client_id
				     , b_config_id => r_ocr.pallet_type
				     );
			fetch 	c_pcg
			into  	r_pcg;
			close 	c_pcg;
			--
		        g_sim_loop.put('SIM_SEGMENT_NR','Segment SIM: '|| to_char( l_count));
			g_sim_loop.put('SIM_ID',to_char( l_count));
			g_sim_loop.put('SIM_SIL_ID','');           
			g_sim_loop.put('SIM_TYPE',r_pcg.config_type);          
/*			g_sim_loop.put('SIM_TYPE_DESC',r_pcg.config_notes);        
			g_sim_loop.put('SIM_GROUP_TYPE',r_pcg.config_group);             
			g_sim_loop.put('SIM_GROUP_TYPE_DESC',r_pcg.config_group_notes);
			g_sim_loop.put('SIM_TYPE_WEIGHT',to_char( r_pcg.config_weight, 'fm999990.90'));        
			g_sim_loop.put('SIM_TYPE_LENGTH',to_char( r_pcg.config_length, 'fm999990.90'));        
			g_sim_loop.put('SIM_TYPE_WIDTH',to_char( r_pcg.config_width, 'fm999990.90'));        
			g_sim_loop.put('SIM_TYPE_HEIGHT',to_char( r_pcg.config_height, 'fm999990.90'));        
			g_sim_loop.put('SIM_TYPE_VOLUME',to_char( r_pcg.config_volume, 'fm999990.90'));        
			g_sim_loop.put('SIM_UNIT_IS_CONT_YN',r_ocr.is_cont_yn);          
			g_sim_loop.put('SIM_UNIT_NR',r_ocr.pallet_id);          
			g_sim_loop.put('SIM_UNIT_NR_MASTER','');   
			g_sim_loop.put('SIM_COLLO_NR',r_ocr.rownum);         
			g_sim_loop.put('SIM_TRACKING_NR',r_ocr.tracking_nr);      
			g_sim_loop.put('SIM_WEIGHT',to_char( r_ocr.weight, 'fm999990.90'));           
			g_sim_loop.put('SIM_LENGTH',to_char( r_ocr.length, 'fm999990.90'));           
			g_sim_loop.put('SIM_WIDTH',to_char( r_ocr.width, 'fm999990.90'));            
			g_sim_loop.put('SIM_HEIGHT',to_char( r_ocr.height, 'fm999990.90'));           
			g_sim_loop.put('SIM_VOLUME',to_char( r_ocr.volume, 'fm999990.90'));           
			g_sim_loop.put('SIM_PIECES',r_ocr.cnt);           
			g_sim_loop.put('SIM_PIECES_PER_ITEM',r_ocr.no_of_boxes);  
			g_sim_loop.put('SIM_DRY_ICE_WEIGHT','');   
			g_sim_loop.put('SIM_UNDG_NUMBER','');      
			g_sim_loop.put('SIM_MAIN_DANGER_CLASS','');
*/
                        g_sim_list.append(g_sim_loop);
                        g_sim_loop := pljson();

			-- Write all UN Codes into 1 line
			l_un_cnt    := 0;
			for 	r_haz in c_haz( b_client_id    => r_ocr.client_id
					              , b_order_id     => r_ocr.order_id
					              , b_pallet_id    => r_ocr.pallet_id  -- can be either container or pallet ID
					              )
			loop
				l_un_cnt   := l_un_cnt + 1;
				l_un_total := l_un_line || ', '|| r_haz.un_code;
				case
				when	length( l_un_total) > 110
				then
					l_un_cnt := 1;
					g_sim.put('SIM_TOTAL_UN_CODES',l_un_line);    
					l_un_line  := r_haz.un_code;
					l_un_total := r_haz.un_code;
				else
					case	l_un_cnt
					when 	1
					then
						l_un_line  := r_haz.un_code;
						l_un_total := r_haz.un_code;
					else
						l_un_line  := l_un_line || ', ' || r_haz.un_code;
					end case;
				end case;
			end loop;
			g_sim.put('SIM_TOTAL_UN_CODES',l_un_line); 

			-- Add SIM LOT lines
		        if add_sim_lot( ''               --p_field_prefix_i
				      , l_count          --p_segment_nr_i
				      , r_ocr.client_id  --p_client_id_i    
				      , r_ocr.order_id   --p_order_nr_i     
				      , r_ocr.pallet_id  --p_pallet_id_i    
				      , p_container_id_i --p_container_id_i 
				      , r_ocr.is_cont_yn --p_is_cont_yn_i
				      )
			then
			  g_sim_list.append(g_add_sim_lot);
			  g_add_sim_lot := pljson();   
			end if;

		end loop;
		g_sim.put('segment_list', g_sim_list);
	        g_sim_list := pljson_list();

		-- add log record

		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i	 => g_print_id
								   , p_file_name_i	 => g_file_name
					 			   , p_source_package_i  => g_pck
					 			   , p_source_routine_i	 => l_rtn
					 			   , p_routine_step_i	 => 'Finished adding SIM'
					 			   , p_code_parameters_i => '"field_prefix" "'||p_field_prefix_i||'" '
					 			   , p_order_id_i	 => p_order_nr_i
						 		   , p_client_id_i	 => p_client_id_i
						 		   , p_pallet_id_i	 => p_pallet_id_i
						 		   , p_container_id_i	 => p_container_id_i
						 		   , p_site_id_i	 => null
						 		   );
		end if;


		return	true;
        exception when others then
	  return	l_retval;
      end add_sim;	

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Create StreamServe Trolley Header block
------------------------------------------------------------------------------------------------
	function add_trl(  p_field_prefix_i in  varchar2
			 , p_site_id_i      in  varchar2
			 , p_list_id_i      in  varchar2
			 , p_user_i         in  varchar2
			 , p_workstation_i  in  varchar2
			 )
	return boolean
	is
		-- Count number of containers on list
		cursor c_cnt( b_list_id varchar2
			    , b_site_id varchar2
		 )
		is
			select  count(distinct to_container_id)
			from    dcsdba.move_task mkt
			where   mkt.list_id = b_list_id
			and     mkt.site_id = b_site_id
		;

		-- Count unique number of carton types
		cursor c_tpe( b_list_id varchar2
			     , b_site_id varchar2
			    )
		is
			select  count(distinct to_container_config)
			from    dcsdba.move_task mkt
			where   mkt.list_id = b_list_id
			and     mkt.site_id = b_site_id
		;

		--
		r_cnt   	number;
		r_tpe   	number;
		l_begin 	varchar2(100);
		l_rtn		varchar2(30) := 'add_trl';
		l_retval	boolean		:= false;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
					 , p_file_name_i		=> g_file_name
					 , p_source_package_i		=> g_pck
					 , p_source_routine_i		=> l_rtn
					 , p_routine_step_i		=> 'Start adding '||p_field_prefix_i||'_HDR'
					 , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"list_id" "'||p_list_id_i||'" '
												|| '"user_id" "'||p_user_i||'" '
												|| '"workstation" "'||p_workstation_i||'" '
					 , p_order_id_i		=> null
					 , p_client_id_i		=> null
					 , p_pallet_id_i		=> null
					 , p_container_id_i		=> null
					 , p_site_id_i		=> p_site_id_i
					 );
		end if;

		open    c_cnt( p_list_id_i, p_site_id_i);
		fetch   c_cnt into r_cnt;
		close   c_cnt;
		open    c_tpe( p_list_id_i, p_site_id_i);
		fetch   c_tpe into r_tpe;
		close   c_tpe;
		--
		l_begin := 'PICK_TROLLEY_EVENT';
		--
		g_add_hdr.put('BEGIN',l_begin);
		g_add_hdr.put('HDR_DATABASE',g_streamserve_wms_db);
		g_add_hdr.put('HDR_APPLICATION_CODE',g_wms);
		g_add_hdr.put('HDR_SITE_ID',upper( p_site_id_i));
		g_add_hdr.put('HDR_USER',upper( p_user_i));
		g_add_hdr.put('HDR_WORKSTATION',upper( p_workstation_i));
		g_add_hdr.put('HDR_LIST_ID',upper( p_list_id_i));
		g_add_hdr.put('HDR_NBR_CONTAINERS',r_cnt);
		g_add_hdr.put('HDR_NBR_CONTAINER_TYPES',r_tpe);
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
					 , p_file_name_i		=> g_file_name
					 , p_source_package_i		=> g_pck
					 , p_source_routine_i		=> l_rtn
					 , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i||'_HDR'
					 , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"list_id" "'||p_list_id_i||'" '
												|| '"user_id" "'||p_user_i||'" '
												|| '"workstation" "'||p_workstation_i||'" '
					 , p_order_id_i		=> null
					 , p_client_id_i		=> null
					 , p_pallet_id_i		=> null
					 , p_container_id_i		=> null
					 , p_site_id_i		=> p_site_id_i
					 );
		end if;
	return l_retval;
	exception when others then
	   return l_retval;
	end add_trl;

-------------------------------------------------------------------------------------------------------------
-- add header info
--
-------------------------------------------------------------------------------------------------------------
	function add_hdr(  p_field_prefix_i in  varchar2
			 , p_site_id_i      in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_owner_id_i     in  varchar2
			 , p_order_id_i     in  varchar2
			 , p_user_i         in  varchar2
			 , p_workstation_i  in  varchar2
			 , p_locality_i     in  varchar2
			 )
		return boolean
	is
		cursor c_ohr( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select	ohr.carrier_id
			,      	ohr.service_level
			from   	dcsdba.order_header ohr
			where  	ohr.client_id = b_client_id
			and    	ohr.order_id  = b_order_id
		;
		--
		r_ohr	c_ohr%rowtype;
		--
		l_begin         varchar2(100);
		l_rtn	        varchar2(30) := 'add_hdr';
		l_retval	boolean 	:= true;
	begin
	        g_add_hdr	:= pljson();
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_field_prefix_i
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" "user_id" "'||p_user_i||'"workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		open	c_ohr( b_client_id => p_client_id_i
			     , b_order_id  => p_order_id_i
			     );
		fetch 	c_ohr
		into  	r_ohr;
		close 	c_ohr;
		--
		l_begin	:= upper( p_client_id_i)|| '_'|| upper( p_owner_id_i)|| '_'|| g_wms|| '_EVENT';
		--	
		g_add_hdr.put('BEGIN',			'');
		g_add_hdr.put('HDR_DATABASE',		g_streamserve_wms_db);
		g_add_hdr.put('HDR_APPLICATION_CODE',	g_wms);
		g_add_hdr.put('HDR_RELATION_NAME_CTM',	upper( p_client_id_i));
		g_add_hdr.put('HDR_SITE_ID',		upper( p_site_id_i));
		g_add_hdr.put('HDR_CLIENT_ID',		upper( p_client_id_i));
		g_add_hdr.put('HDR_OWNER_ID',		upper( p_owner_id_i));
		g_add_hdr.put('HDR_CARRIER_CODE',	upper( r_ohr.carrier_id));
                g_add_hdr.put('HDR_CARRIER_SERVICE',	upper( r_ohr.service_level));
		g_add_hdr.put('HDR_USER',	        upper( p_user_i));
		g_add_hdr.put('HDR_WORKSTATION',	upper( p_workstation_i));
		g_add_hdr.put('HDR_LOCALITY',	        upper( p_locality_i));

		--dbms_output.put_line(g_add_hdr.to_char( true ));

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" "user_id" "'||p_user_i||'"workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;


		return	l_retval;
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
		create_bis_log_record( g_pck||'.'||l_rtn , 'Exception check CNL_ERROR');
		return l_retval;
	end add_hdr;

----------------------------------------------------------------------------------------------------------
--
--
--
----------------------------------------------------------------------------------------------------------

  function add_ptr( p_segment_nr_i        in  number
		  , p_jrp_key_i           in  number
		  , p_template_name_i     in  varchar2
		  , p_ptr_template_name_i in  varchar2
		  , p_ptr_name_i          in  varchar2
		  , p_ptr_unc_path_i      in  varchar2
		  , p_copies_i            in  number
		  , p_print_yn_i          in  varchar2
		  , p_eml_addresses_to_i  in  varchar2
		  , p_eml_addresses_bcc_i in  varchar2
		  , p_email_yn_i          in  varchar2
		  , p_email_attachment_i  in  varchar2
		  , p_email_subject_i     in  varchar2
		  , p_email_message_i     in  varchar2
		  , p_pdf_link_yn_i       in  varchar2
		  , p_pdf_autostore_i     in  varchar2
		  )
	return boolean
	is
		l_begin         varchar2(100);
		l_rtn	        varchar2(30) := 'add_ptr';
		l_retval	boolean 	:= true;
	begin
		-- add log record



		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding PTR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||''||'" '
												|| '"Segment_nr" "'||p_segment_nr_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		g_add_ptr.put('PTR_SEGMENT',		  'Segment Printer: ' || to_char( p_segment_nr_i));
		g_add_ptr.put('PTR_JRP_KEY',		   p_jrp_key_i);
		g_add_ptr.put('PTR_DOC_TYPE',		   p_template_name_i);
		g_add_ptr.put('PTR_MAIN_DOC_TYPE',	   p_ptr_template_name_i);
		g_add_ptr.put('PTR_PRINTER',		   p_ptr_name_i);
		g_add_ptr.put('PTR_PATH',		   p_ptr_unc_path_i);
		g_add_ptr.put('PTR_COPIES',		   to_char( nvl( p_copies_i, 1)));
		g_add_ptr.put('PTR_ACTIVE',	           p_print_yn_i);
                g_add_ptr.put('PTR_EMAIL_TO',	           p_eml_addresses_to_i);
		g_add_ptr.put('PTR_EMAIL_BCC',	           p_eml_addresses_bcc_i);
		g_add_ptr.put('PTR_EMAIL_ACTIVE',	   p_email_yn_i);
		g_add_ptr.put('PTR_EMAIL_ATTACHMENT_NAME', p_email_attachment_i);
                g_add_ptr.put('PTR_EMAIL_SUBJECT',	   p_email_subject_i);
		g_add_ptr.put('PTR_EMAIL_BODY',	           p_email_message_i);
		g_add_ptr.put('PTR_PDF_LINK',	           p_pdf_link_yn_i);
		g_add_ptr.put('PTR_PDF_AUTOSTORE',         p_pdf_autostore_i);		

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding PTR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||''||'" '
												|| '"Segment_nr" "'||p_segment_nr_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
			return	l_retval;
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
		create_bis_log_record( g_pck||'.'||l_rtn , 'Exception check CNL_ERROR' );
		return l_retval;	
	end add_ptr;  

------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Nov-2016
-- Purpose : Create StreamServe Lot block
------------------------------------------------------------------------------------------------
	function add_lot(  p_field_prefix_i in  varchar2 
	                 , p_segment_nr_i   in  number
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 , p_line_id_i      in  number
			 , p_pallet_id_i    in  varchar2 := null
			 , p_container_id_i in  varchar2 := null
			 )
	return boolean
	is
		cursor c_lot( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_line_id      in number
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    )
		is
			select	rowid
			,	smt.tag_id
			,      	smt.sku_id
			,      	smt.batch_id
			,      	smt.expiry_dstamp
			,      	smt.origin_id
			,      	smt.qty_shipped               qty
			,      	smt.container_id
			,      	smt.pallet_id
			,      	smt.condition_id
			,      	smt.receipt_dstamp
			,      	smt.manuf_dstamp
			,      	smt.receipt_id
			from   	dcsdba.shipping_manifest      smt
			where  	smt.client_id                 = b_client_id
			and    	smt.order_id                  = b_order_id
			and    	smt.line_id                   = b_line_id
			and    	(smt.pallet_id = b_pallet_id or b_pallet_id is null)
			and    	(smt.container_id = b_container_id or b_container_id is null)
			union  	-- For pallets which are not 'Marshalled' yet (deleted)
			select 	rowid
			,	mtk.tag_id
			,      	mtk.sku_id
			,      	(select i.batch_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) batch_id
			,      	(select i.expiry_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) expiry_dstamp
			,      	(select i.origin_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) origin_id
			,      	mtk.qty_to_move               qty
			,      	mtk.container_id
			,      	mtk.pallet_id
			,      	(select i.condition_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) condition_id
			,      	(select i.receipt_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_dstamp
			,      	(select i.manuf_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) manuf_dstamp
			,      	(select i.receipt_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_id
			from   	dcsdba.move_task              	mtk
			where  	mtk.client_id                 	= b_client_id
			and    	mtk.task_id                   	= b_order_id
			and    	mtk.line_id                   	= b_line_id
			and    	(mtk.pallet_id = b_pallet_id or b_pallet_id is null)
			and    	(mtk.container_id = b_container_id or b_container_id is null)
			and    	not exists (	select	1
						from   	dcsdba.shipping_manifest smt
						where  	smt.client_id            = b_client_id
						and    smt.order_id             = b_order_id
						and    smt.line_id              = b_line_id
						and    smt.pallet_id            = mtk.pallet_id
					   )
			order  
			by 	container_id	
			,      	tag_id
		;

		--
		r_lot  	c_lot%rowtype;
		l_count number(10) := 0;
		l_rtn	varchar2(30) := 'add_lot';
		l_retval	boolean 	:= true;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		for	r_lot in c_lot( b_client_id    => p_client_id_i
				      , b_order_id     => p_order_nr_i
				      , b_line_id      => p_line_id_i
				      , b_pallet_id    => p_pallet_id_i
				      , b_container_id => p_container_id_i
				      )
		loop
			l_count := l_count +1;
			--
                        g_add_lot_loop.put('LOT_SEGMENT_NR','Segment OLE / Lot: '|| to_char( p_segment_nr_i)|| ' / '|| to_char( l_count));
			g_add_lot_loop.put('LOT_TAG_ID',r_lot.tag_id);      
			g_add_lot_loop.put('LOT_SKU_ID',r_lot.sku_id);                
			g_add_lot_loop.put('LOT_BATCH_ID',r_lot.batch_id);              
			g_add_lot_loop.put('LOT_EXPIRY_DATE',to_char( r_lot.expiry_dstamp, 'DD-MM-YYYY'));           
			g_add_lot_loop.put('LOT_EXPIRY_TIME',to_char( r_lot.expiry_dstamp, 'HH24:MI:SS'));           
			g_add_lot_loop.put('LOT_RECEIPT_ID',r_lot.receipt_id);            
			g_add_lot_loop.put('LOT_RECEIPT_DATE',to_char( r_lot.receipt_dstamp, 'DD-MM-YYYY'));          
			g_add_lot_loop.put('LOT_RECEIPT_TIME',to_char( r_lot.receipt_dstamp, 'HH24:MI:SS'));          
			g_add_lot_loop.put('LOT_MANUF_DATE',to_char( r_lot.manuf_dstamp, 'DD-MM-YYYY'));            
			g_add_lot_loop.put('LOT_MANUF_TIME',to_char( r_lot.manuf_dstamp, 'HH24:MI:SS'));            
			g_add_lot_loop.put('LOT_ORIGIN_ID',r_lot.origin_id);             
			g_add_lot_loop.put('LOT_CONDITION_ID',r_lot.condition_id);          
			g_add_lot_loop.put('LOT_QTY_UPDATE',r_lot.qty);            
			g_add_lot_loop.put('LOT_CONTAINER_ID',r_lot.container_id);  
			g_add_lot_list.append(g_add_lot_loop);
                        g_add_lot_loop := pljson();
		end loop;

                       g_add_lot.put('segment_list', g_add_lot_list);
                       g_add_lot_list := pljson_list();  


		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding OLE_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;
		return l_retval;
	exception when others then
	  return l_retval;
	end add_lot;

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Create StreamServe Serial Number block
------------------------------------------------------------------------------------------------
	function add_snr(  p_field_prefix_i in  varchar2 
	                 , p_segment_nr_i   in  number
			 , p_client_id_i    in  varchar2
			 , p_order_id_i     in  varchar2
			 , p_line_id_i      in  number
			 )
	return boolean 
	is

		cursor c_snr( b_client_id in varchar2
			    , b_order_id  in varchar2
			    , b_line_id   in number
			    )
		is
			select	snr.serial_number
			,      	snr.client_id
			,      	snr.sku_id
			,      	snr.order_id
			,      	snr.line_id
			,      	snr.tag_id
		        ,      	snr.original_tag_id
		        ,      	snr.pick_key
		        ,      	snr.old_pick_key
		        ,      	snr.manifest_key
		        ,      	snr.old_manifest_key
		        ,      	snr.status
		        ,      	snr.supplier_id
		        ,      	snr.site_id
		        ,      	snr.receipt_dstamp
		        ,      	snr.picked_dstamp
		        ,      	snr.shipped_dstamp
		        ,      	snr.uploaded
		        ,      	snr.uploaded_ws2pc_id
		        ,      	snr.uploaded_filename
		        ,      	snr.uploaded_dstamp
		        ,      	snr.repacked
		        ,      	snr.created
		        ,      	snr.screen_mode
		        ,     	snr.station_id
		        ,      	snr.receipt_id
		        ,      	snr.receipt_line_id
		        ,      	snr.kit_sku_id
		        ,      	snr.kit_serial_number
		        ,      	snr.alloc_key
		        ,      	snr.pallet_id
		        ,      	snr.container_id
		        ,       snr.old_pallet_id
		        ,      	snr.old_container_id
		        ,      	snr.tag_adjusted
		        ,      	snr.reused
		        from   	dcsdba.serial_number snr
		        where  	snr.client_id        = b_client_id
		        and    	snr.order_id         = b_order_id
		        and    	snr.line_id          = b_line_id
		        order  	
			by 	snr.serial_number
		;

		--
		r_snr1         	c_snr%rowtype;
		r_snr2         	c_snr%rowtype;

		l_total        	varchar2(3999);
		l_line         	varchar2(3999);
		l_user_field_1 	varchar2(1000);
		l_user_field_2 	varchar2(1000);
		l_user_field_3 	varchar2(1000);
		l_user_field_4 	varchar2(1000);
		l_user_field_5 	varchar2(1000);
		l_cnt          	number(10);
		l_rtn		varchar2(30) := 'add_srn';
		l_retval	boolean 	:= true;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE_SNR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		-- Write all Serials into lines
		l_cnt   := 0;
		for 	r_snr1 in c_snr( b_client_id => p_client_id_i
				       , b_order_id  => p_order_id_i
				       , b_line_id   => p_line_id_i
				       )
		loop
			l_cnt  	:= l_cnt + 1;
			l_total := l_line || ', ' || r_snr1.serial_number;
			case
			when	length( l_total) > 110
			then
				l_cnt := 1;
				l_line  := r_snr1.serial_number;
				l_total := r_snr1.serial_number;
			else
				case	l_cnt
				when 	1
				then
					l_line  := r_snr1.serial_number;
					l_total := r_snr1.serial_number;
				else
					l_line  := l_line || ', ' || r_snr1.serial_number;
				end case;
			end case;
		end loop;

		g_add_snr.put('SERIAL_TOTAL_NRS' ,l_line);

		-- Write separate segments per Serial
		l_cnt  	:= 0;
		for	r_snr2 in c_snr( b_client_id => p_client_id_i
				       , b_order_id  => p_order_id_i
				       , b_line_id   => p_line_id_i
				       )
		loop
			l_cnt := l_cnt + 1;
			g_add_snr_loop.put('SERIAL_SEGMENT_NR','Segment OLE / Serial: ' || to_char( p_segment_nr_i) || ' / ' || to_char( l_cnt));
			g_add_snr_loop.put('SERIAL_NUMBER' ,r_snr2.serial_number);
			g_add_snr_loop.put('SERIAL_TAG_ID' , r_snr2.tag_id);
			g_add_snr_loop.put('SERIAL_SKU_ID', r_snr2.sku_id);
			--dbms_output.put_line(g_add_snr.to_char( true ));
			g_add_snr_list.append(g_add_snr_loop);
                        g_add_snr_loop := pljson();
		end loop;
                g_add_snr.put('serial', g_add_snr_list);
                g_add_snr_list := pljson_list();

             --   dbms_output.put_line(g_add_snr.to_char( true ));     

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding OLE_SNR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
            return true;
	exception
		when	others
		then
			case
			when	c_snr%isopen
			then
			close	c_snr;
			else
				null;
			end case;
return l_retval;
	end add_snr;

------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 27-Sep-2016
-- Purpose : Create StreamServe Order Lines block
------------------------------------------------------------------------------------------------
	function add_ole(  p_field_prefix_i in  varchar2 
	                 , p_segment_nr_i   in  number
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 , p_line_id_i      in  number   := null
			 , p_pallet_id_i    in  varchar2 := null
			 , p_container_id_i in  varchar2 := null
			 )
	return boolean 
	is
		-- Fetch order line details
		cursor c_ole( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_line_id      in number)
		is
			select	ole.*
			from    dcsdba.order_line ole
			where  	ole.client_id      = b_client_id
			and    	ole.order_id       = b_order_id
			and    	ole.line_id        = nvl( b_line_id, ole.line_id)
			order  
			by 	ole.line_id;

		-- Fetch SKU details
		cursor c_sku( b_client_id in varchar2
			    , b_sku_id    in varchar2
			    )
		is
			select 	sku.*
			from   	dcsdba.sku sku
			where  	sku.client_id = b_client_id
			and    	sku.sku_id    = b_sku_id;

		-- Fetch hazmat details
		cursor c_haz( b_hazmat_id in varchar2)
		is
			select	hmt.hazmat_id
			,      	hhs.hazmat_class
			,      	hmt.metapack
			,      	hmt.notes
			,      	replace(hmt.user_def_type_1, ' ', null) user_def_type_1
			,      	hmt.user_def_type_2
			,      	hmt.user_def_type_3
			,      	hmt.user_def_type_4
			,      	hmt.user_def_type_5
			,      	hmt.user_def_type_6
			,     	hmt.user_def_type_7
			,      	hmt.user_def_type_8
			,      	hmt.user_def_chk_1
			,      	hmt.user_def_chk_2
			,      	hmt.user_def_chk_3
			,      	hmt.user_def_chk_4
			,      	hmt.user_def_date_1
			,      	hmt.user_def_date_2
			,      	hmt.user_def_date_3
			,      	hmt.user_def_date_4
			,      	hmt.user_def_num_1
			,      	hmt.user_def_num_2
			,      	hmt.user_def_num_3
			,      	hmt.user_def_num_4
			,      	hmt.user_def_note_1
			,      	hmt.user_def_note_2
			,      	decode(upper(replace(hmt.user_def_type_4,',',null)), '965II', 1, '9651B', 2, '966II', 3, '967II', 4, '968II', 5, '9681B', 6, '969II', 7, '970II', 8, 0) lithium_check_box
			from   	dcsdba.hazmat              hmt
			,      	dcsdba.hazmat_hazmat_class hhs
			where  	hmt.hazmat_id              = b_hazmat_id
			and    	hmt.hazmat_id              = hhs.hazmat_id (+)
		;

		-- Fetch hazmat regulation details
		cursor c_sha( b_hazmat_id in varchar2
			    , b_client_id in varchar2
			    , b_sku_id    in varchar2
			    )
		is
			select 	sha.regulation_id
			,      	sha.hazmat_class
			,      	sha.hazmat_subclass
			,      	sha.classification_code
			,	sha.un_packing_group
			, 	sha.hazmat_labels
			,	sha.transport_category
			,	sha.marine_pollutant
			,	sha.mfag
			,	sha.ems
			,	sha.hazmat_net_weight
			,	sha.hazmat_net_volume
			,	sha.hazmat_net_volume_unit
			,	sha.hazmat_flashpoint
			,	sha.flashpoint_category
			,	sha.wgk_class
			,	sha.ghs_symbol
			,	sha.limited_qty
			,	sha.r_sentence_code
			,	sha.r_sentence_group
			,	sha.r_sentence
			,	sha.proper_shipping_name
			,	sha.additional_shipping_name
			,	sha.un_packaging_code
			,	sha.water_endangerment_class
			,	sha.language
			,	sha.tunnel_code
			from	dcsdba.sku_hazmat_reg sha
			where	sha.sku_id 	= b_sku_id
			and	sha.client_id 	= b_client_id
			and	( sha.hazmat_id  	= b_hazmat_id or sha.hazmat_id is null)	;

		-- select hazmat regulations description
		cursor c_hrn( b_regulation_id in varchar2)
		is
			select	hrn.notes
			from	dcsdba.hazmat_regulation hrn
			where	hrn.regulation_id = b_regulation_id;

		--
		r_ole     	c_ole%rowtype;
		r_sku     	c_sku%rowtype;
		r_haz     	c_haz%rowtype;
		r_sha		c_sha%rowtype;	
		r_hrn		c_hrn%rowtype;	

		l_counter	number := 1;
		l_rtn		varchar2(30) := 'add_ole';
		l_retval	boolean 	:= true;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		open  	c_ole( b_client_id => p_client_id_i
			     , b_order_id  => p_order_nr_i
			     , b_line_id   => p_line_id_i
			     );
		fetch 	c_ole
		into  	r_ole;
		--
		g_ole.put('OLE_SEGMENT_NR','Segment OLE: ' || to_char( p_segment_nr_i));
		g_ole.put('OLE_LINE_ID',to_char( r_ole.line_id));
		g_ole.put('OLE_HOST_LINE_ID',r_ole.host_line_id);
		g_ole.put('OLE_HOST_ORDER_ID',r_ole.host_order_id);
		g_ole.put('OLE_SKU_ID',r_ole.sku_id);
		g_ole.put('OLE_CUSTOMER_SKU_ID',r_ole.customer_sku_id);
		g_ole.put('OLE_CONFIG_ID',r_ole.config_id);
		g_ole.put('OLE_TRACKING_LEVEL',r_ole.tracking_level);
		g_ole.put('OLE_BATCH_ID',r_ole.batch_id);
		g_ole.put('OLE_BATCH_MIXING',r_ole.batch_mixing);
		g_ole.put('OLE_BATCH_ID_SET',r_ole.batch_id_set);
		g_ole.put('OLE_SHELF_LIFE_DAYS',to_char( r_ole.shelf_life_days));
		g_ole.put('OLE_SHELF_LIFE_PERCENT',to_char( r_ole.shelf_life_percent));
		g_ole.put('OLE_ORIGIN_ID',r_ole.origin_id);
		g_ole.put('OLE_CONDITION_ID',r_ole.condition_id);
		g_ole.put('OLE_LOCK_CODE',r_ole.lock_code);
		g_ole.put('OLE_SPEC_CODE',r_ole.spec_code);
		g_ole.put('OLE_QTY_ORDERED',to_char( r_ole.qty_ordered));
		g_ole.put('OLE_QTY_TASKED',to_char( r_ole.qty_tasked));
		g_ole.put('OLE_QTY_PICKED',to_char( r_ole.qty_picked));
		g_ole.put('OLE_QTY_SHIPPED',to_char( r_ole.qty_shipped));
		g_ole.put('OLE_QTY_DELIVERED',to_char( r_ole.qty_delivered));
		g_ole.put('OLE_ALLOCATE',r_ole.allocate);
		g_ole.put('OLE_BACK_ORDERED',r_ole.back_ordered);
		g_ole.put('OLE_KIT_SPLIT',r_ole.kit_split);
		g_ole.put('OLE_DEALLOCATE',r_ole.deallocate);
		g_ole.put('OLE_DISALLOW_MERGE_RULES',r_ole.disallow_merge_rules);
		g_ole.put('OLE_RULE_ID',r_ole.rule_id);
		g_ole.put('OLE_LINE_VALUE',to_char( r_ole.line_value, 'fm999999990.90'));
		g_ole.put('OLE_LINE_VALUE_USER_DEF',r_ole.line_value_user_def);
		g_ole.put('OLE_NOTES',r_ole.notes);
		g_ole.put('OLE_PSFT_INT_LINE',to_char( r_ole.psft_int_line));
		g_ole.put('OLE_PSFT_SCHD_LINE',to_char( r_ole.psft_schd_line));
		g_ole.put('OLE_PSFT_DMND_LINE',to_char( r_ole.psft_dmnd_line));
		g_ole.put('OLE_SAP_PICK_REQ',r_ole.sap_pick_req);
		g_ole.put('OLE_ALLOC_SESSION_ID',to_char( r_ole.alloc_session_id));
		g_ole.put('OLE_ALLOC_STATUS',r_ole.alloc_status);
		g_ole.put('OLE_ORIGINAL_LINE_ID',to_char( r_ole.original_line_id));
		g_ole.put('OLE_CONVERSION_FACOR',to_char( r_ole.conversion_factor));
		g_ole.put('OLE_SUBSTITUTE_FLAG',r_ole.substitute_flag);
		g_ole.put('OLE_TASK_PER_EACH',r_ole.task_per_each);
		g_ole.put('OLE_CATCH_WEIGHT',to_char( r_ole.catch_weight));
		g_ole.put('OLE_USE_PICK_TO_GRID',r_ole.use_pick_to_grid);
		g_ole.put('OLE_IGNORE_WEIGHT_CAPTURE',r_ole.ignore_weight_capture);
		g_ole.put('OLE_STAGE_ROUTE_ID',r_ole.stage_route_id);
		g_ole.put('OLE_QTY_SUBSTITUTED',to_char( r_ole.qty_substituted));
		g_ole.put('OLE_MIN_QTY_ORDERED',to_char( r_ole.min_qty_ordered));
		g_ole.put('OLE_MAX_QTY_ORDERED',to_char( r_ole.max_qty_ordered));
		g_ole.put('OLE_EXPECTED_VOLUME',to_char( r_ole.expected_volume, 'fm999990.90'));
		g_ole.put('OLE_EXPECTED_WEIGHT',to_char( r_ole.expected_weight, 'fm999990.90'));
		g_ole.put('OLE_EXPECTED_VALUE',to_char( r_ole.expected_value, 'fm999990.90'));
		g_ole.put('OLE_CUSTOMER_SKU_DESC1',r_ole.customer_sku_desc1);
		g_ole.put('OLE_CUSTOMER_SKU_DESC2',r_ole.customer_sku_desc2);
		g_ole.put('OLE_PURCHASE_ORDER',r_ole.purchase_order);
		g_ole.put('OLE_PRODUCT_PRICE',to_char( r_ole.product_price, 'fm999990.90'));
		g_ole.put('OLE_PRODUCT_CURRENCY',r_ole.product_currency);
		g_ole.put('OLE_DOCUMENTATION_UNIT',r_ole.documentation_unit);
		g_ole.put('OLE_EXTENDED_PRICE',to_char( r_ole.extended_price, 'fm999990.90'));
		g_ole.put('OLE_TAX_1',to_char( r_ole.tax_1, 'fm999990.90'));
		g_ole.put('OLE_TAX_2',to_char( r_ole.tax_2, 'fm999990.90'));
		g_ole.put('OLE_DOCUMENTATION_TEXT_1',r_ole.documentation_text_1);
		g_ole.put('OLE_SERIAL_NUMBER',r_ole.serial_number);
		g_ole.put('OLE_OWNER_ID',r_ole.owner_id);
		g_ole.put('OLE_CE_RECEIPT_TYPE',r_ole.ce_receipt_type);
		g_ole.put('OLE_CE_COO',r_ole.ce_coo);
		g_ole.put('OLE_V_SN_SCAN','' );
		g_ole.put('OLE_USER_DEF_CHK_1',r_ole.user_def_chk_1);
		g_ole.put('OLE_USER_DEF_CHK_2',r_ole.user_def_chk_2);
		g_ole.put('OLE_USER_DEF_CHK_3',r_ole.user_def_chk_3);
		g_ole.put('OLE_USER_DEF_CHK_4',r_ole.user_def_chk_4);
		g_ole.put('OLE_USER_DEF_DATE_1',to_char( r_ole.user_def_date_1, 'DD-MM-YYYY'));
		g_ole.put('OLE_USER_DEF_TIME_1',to_char( r_ole.user_def_date_1, 'HH24:MI:SS'));
		g_ole.put('OLE_USER_DEF_DATE_2',to_char( r_ole.user_def_date_2, 'DD-MM-YYYY'));
		g_ole.put('OLE_USER_DEF_TIME_2',to_char( r_ole.user_def_date_2, 'HH24:MI:SS'));
		g_ole.put('OLE_USER_DEF_DATE_3',to_char( r_ole.user_def_date_3, 'DD-MM-YYYY'));
		g_ole.put('OLE_USER_DEF_TIME_3',to_char( r_ole.user_def_date_3, 'HH24:MI:SS'));
		g_ole.put('OLE_USER_DEF_DATE_4',to_char( r_ole.user_def_date_4, 'DD-MM-YYYY'));
		g_ole.put('OLE_USER_DEF_TIME_4',to_char( r_ole.user_def_date_4, 'HH24:MI:SS'));
		g_ole.put('OLE_USER_DEF_NUM_1',to_char( r_ole.user_def_num_1, 'fm999999990.9999990'));
		g_ole.put('OLE_USER_DEF_NUM_2',to_char( r_ole.user_def_num_2, 'fm999999990.9999990'));
		g_ole.put('OLE_USER_DEF_NUM_3',to_char( r_ole.user_def_num_3, 'fm999999990.9999990'));
		g_ole.put('OLE_USER_DEF_NUM_4',to_char( r_ole.user_def_num_4, 'fm999999990.9999990'));
		g_ole.put('OLE_USER_DEF_NOTE_1',r_ole.user_def_note_1);
		g_ole.put('OLE_USER_DEF_NOTE_2',r_ole.user_def_note_2);
		g_ole.put('OLE_USER_DEF_TYPE_1',r_ole.user_def_type_1);
		g_ole.put('OLE_USER_DEF_TYPE_2',r_ole.user_def_type_2);
		g_ole.put('OLE_USER_DEF_TYPE_3',r_ole.user_def_type_3);
		g_ole.put('OLE_USER_DEF_TYPE_4',r_ole.user_def_type_4);
		g_ole.put('OLE_USER_DEF_TYPE_5',r_ole.user_def_type_5);
		g_ole.put('OLE_USER_DEF_TYPE_6',r_ole.user_def_type_6);
		g_ole.put('OLE_USER_DEF_TYPE_7',r_ole.user_def_type_7);
		g_ole.put('OLE_USER_DEF_TYPE_8',r_sku.user_def_type_8);
		-- new fields after 2009
		g_ole.put('OLE_KIT_PLAN_ID',r_ole.kit_plan_id);
		g_ole.put('OLE_MASTER_ORDER_ID',r_ole.master_order_id);
		g_ole.put('OLE_MASTER_ORDER_LINE_ID',r_ole.master_order_line_id);
		g_ole.put('OLE_TM_SHIP_LINE_ID',r_ole.tm_ship_line_id);
		g_ole.put('OLE_SOFT_ALLOCATED',r_ole.soft_allocated);
		g_ole.put('OLE_LOCATION_ID',r_ole.location_id);
		g_ole.put('OLE_UNALLOCATABLE',r_ole.unallocatable);
		g_ole.put('OLE_MIN_FULL_PALLET_PERC',r_ole.min_full_pallet_perc);
		g_ole.put('OLE_MAX_FULL_PALLET_PERC',r_ole.max_full_pallet_perc);
		g_ole.put('OLE_FULL_TRACKING_LEVEL_ONLY',r_ole.full_tracking_level_only);
		g_ole.put('OLE_SUBSTITUTE_GRADE',r_ole.substitute_grade);
		g_ole.put('OLE_DISALLOW_SUBSTITUTION',r_ole.disallow_substitution);

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE_SKU'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
												|| '"sku_id" "'||r_ole.sku_id||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		-- Add SKU lines for the current Order Line
		open  	c_sku( b_client_id => r_ole.client_id
			     , b_sku_id    => r_ole.sku_id
			     );
		fetch	c_sku
		into	r_sku;
		--
                g_ole_sku.put('OLE_SKU_EAN',  r_sku.ean);
		g_ole_sku.put('OLE_SKU_UPC',  r_sku.upc);
		g_ole_sku.put('OLE_SKU_DESC',  r_sku.description);
		g_ole_sku.put('OLE_SKU_PRODUCT_GROUP',  r_sku.product_group);
		g_ole_sku.put('OLE_SKU_EACH_HEIGHT',  to_char( r_sku.each_height, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_EACH_WEIGHT',  to_char( r_sku.each_weight, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_EACH_VOLUME',to_char( r_sku.each_weight, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_EACH_VALUE',  to_char( r_sku.each_value, 'fm999990.90'));
		g_ole_sku.put('OLE_SKU_EACH_QTY',  to_char( r_sku.each_quantity, 'fm999990.90'));
		g_ole_sku.put('OLE_SKU_QC_STATUS',  r_sku.qc_status);
		g_ole_sku.put('OLE_SKU_SHELF_LIFE',  to_char( r_sku.shelf_life));
		g_ole_sku.put('OLE_SKU_QC_FREQUENCY',  to_char( r_sku.qc_frequency));
		g_ole_sku.put('OLE_SKU_QC_REC_COUNT',  to_char( r_sku.qc_rec_count));
		g_ole_sku.put('OLE_SKU_SPLIT_LOWEST',  r_sku.split_lowest);
		g_ole_sku.put('OLE_SKU_CONDITION_REQD',  r_sku.condition_reqd);
		g_ole_sku.put('OLE_SKU_EXPIRY_REQD',  r_sku.expiry_reqd);
		g_ole_sku.put('OLE_SKU_ORIGIN_REQD',  r_sku.origin_reqd);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_PACK',  r_sku.serial_at_pack);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_PICK',  r_sku.serial_at_pick);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_RECEIPT',  r_sku.serial_at_receipt);
		g_ole_sku.put('OLE_SKU_SERIAL_RANGE',  r_sku.serial_range);
		g_ole_sku.put('OLE_SKU_SERIAL_FORMAT',  r_sku.serial_format);
		g_ole_sku.put('OLE_SKU_SERIAL_VALID_MERGE',  r_sku.serial_valid_merge);
		g_ole_sku.put('OLE_SKU_SERIAL_NO_REUSE',  r_sku.serial_no_reuse);
		g_ole_sku.put('OLE_SKU_PICK_SEQUENCE',  to_char( r_sku.pick_sequence));
		g_ole_sku.put('OLE_SKU_PICK_COUNT_QTY',  to_char( r_sku.pick_count_qty));
		g_ole_sku.put('OLE_SKU_COUNT_FREQUENCY',  to_char( r_sku.count_frequency));
		g_ole_sku.put('OLE_SKU_COUNT_DSTAMP',  to_char( r_sku.count_dstamp, 'DD-MM-YYYY'));
		g_ole_sku.put('OLE_SKU_COUNT_DSTAMP_TIME',  to_char( r_sku.count_dstamp, 'HH24:MI:SS'));
		g_ole_sku.put('OLE_SKU_COUNT_LIST_ID',  r_sku.count_list_id);
		g_ole_sku.put('OLE_SKU_OAP_WIP_ENABLED',  r_sku.oap_wip_enabled);
		g_ole_sku.put('OLE_SKU_KIT_SKU',  r_sku.kit_sku);
		g_ole_sku.put('OLE_SKU_KIT_SPLIT',  r_sku.kit_split);
		g_ole_sku.put('OLE_SKU_KIT_TRIGGER_QTY',  to_char( r_sku.kit_trigger_qty));
		g_ole_sku.put('OLE_SKU_KIT_QTY_DUE',  to_char( r_sku.kit_qty_due));
		g_ole_sku.put('OLE_SKU_KITTING_LOC_ID',  r_sku.kitting_loc_id);
		g_ole_sku.put('OLE_SKU_ALLOCATION_GROUP',  r_sku.allocation_group);
		g_ole_sku.put('OLE_SKU_PUTAWAY_GROUP',  r_sku.putaway_group);
		g_ole_sku.put('OLE_SKU_ABC_DISABLE',  r_sku.abc_disable);
		g_ole_sku.put('OLE_SKU_HANDLING_CLASS',  r_sku.handling_class);
		g_ole_sku.put('OLE_SKU_OBSOLETE_PRODUCT',  r_sku.obsolete_product);
		g_ole_sku.put('OLE_SKU_NEW_PRODUCT',  r_sku.new_product);
		g_ole_sku.put('OLE_SKU_DISALLOW_UPLOAD',  r_sku.disallow_upload);
		g_ole_sku.put('OLE_SKU_DISALLOW_CROSS_DOCK',  r_sku.disallow_cross_dock);
		g_ole_sku.put('OLE_SKU_MANUF_DSTAMP_REQD',  r_sku.manuf_dstamp_reqd);
		g_ole_sku.put('OLE_SKU_MANUF_DSTAMP_DFLT',  r_sku.manuf_dstamp_dflt);
		g_ole_sku.put('OLE_SKU_MIN_SHELF_LIFE',  to_char( r_sku.min_shelf_life));
		g_ole_sku.put('OLE_SKU_COLOUR',  r_sku.colour);
		g_ole_sku.put('OLE_SKU_SKU_SIZE',  r_sku.sku_size);
		g_ole_sku.put('OLE_SKU_HAZMAT',  r_sku.hazmat);
		g_ole_sku.put('OLE_SKU_HAZMAT_ID',  r_sku.hazmat_id);
		g_ole_sku.put('OLE_SKU_SHIP_SHELF_LIFE',  to_char( r_sku.ship_shelf_life));
		g_ole_sku.put('OLE_SKU_NMFC_NUMBER',  to_char( r_sku.nmfc_number));
		g_ole_sku.put('OLE_SKU_INCUB_RULE',  r_sku.incub_rule);
		g_ole_sku.put('OLE_SKU_INCUB_HOURS',  to_char( r_sku.incub_hours));
		g_ole_sku.put('OLE_SKU_EACH_WIDTH',  to_char( r_sku.each_width, 'fm999990.900'));
		g_ole_sku.put('OLE_SKU_EACH_DEPTH',  to_char( r_sku.each_depth, 'fm999990.900'));
		g_ole_sku.put('OLE_SKU_REORDER_TRIGGER_QTY',  to_char( r_sku.reorder_trigger_qty));
		g_ole_sku.put('OLE_SKU_LOW_TRIGGER_QTY',  to_char( r_sku.low_trigger_qty));
		g_ole_sku.put('OLE_SKU_DISALLOW_MERGE_RULES',  r_sku.disallow_merge_rules);
		g_ole_sku.put('OLE_SKU_PACK_DESPATCH_REPACK',  r_sku.pack_despatch_repack);
		g_ole_sku.put('OLE_SKU_SPEC_ID',  r_sku.spec_id);
		g_ole_sku.put('OLE_SKU_BEAM_UNITS',  to_char( r_sku.beam_units));
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_1',  r_sku.user_def_type_1);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_2',  r_sku.user_def_type_2);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_3',  r_sku.user_def_type_3);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_4',  r_sku.user_def_type_4);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_5',  r_sku.user_def_type_5);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_6',  r_sku.user_def_type_6);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_7',  r_sku.user_def_type_7);
		g_ole_sku.put('OLE_SKU_USER_DEF_TYPE_8',  r_sku.user_def_type_8);
		g_ole_sku.put('OLE_SKU_USER_DEF_CHK_1',  r_sku.user_def_chk_1);
		g_ole_sku.put('OLE_SKU_USER_DEF_CHK_2',  r_sku.user_def_chk_2);
		g_ole_sku.put('OLE_SKU_USER_DEF_CHK_3',  r_sku.user_def_chk_3);
		g_ole_sku.put('OLE_SKU_USER_DEF_CHK_4',  r_sku.user_def_chk_4);
		g_ole_sku.put('OLE_SKU_USER_DEF_DATE_1',  to_char( r_sku.user_def_date_1, 'DD-MM-YYYY'));
		g_ole_sku.put('OLE_SKU_USER_DEF_TIME_1',  to_char( r_sku.user_def_date_1, 'HH24:MI:SS'));
		g_ole_sku.put('OLE_SKU_USER_DEF_DATE_2',  to_char( r_sku.user_def_date_2, 'DD-MM-YYYY'));
		g_ole_sku.put('OLE_SKU_USER_DEF_TIME_2',  to_char( r_sku.user_def_date_2, 'HH24:MI:SS'));
		g_ole_sku.put('OLE_SKU_USER_DEF_DATE_3',  to_char( r_sku.user_def_date_3, 'DD-MM-YYYY'));
		g_ole_sku.put('OLE_SKU_USER_DEF_TIME_3',  to_char( r_sku.user_def_date_3, 'HH24:MI:SS'));
		g_ole_sku.put('OLE_SKU_USER_DEF_DATE_4',  to_char( r_sku.user_def_date_4, 'DD-MM-YYYY'));
		g_ole_sku.put('OLE_SKU_USER_DEF_TIME_4',  to_char( r_sku.user_def_date_4, 'HH24:MI:SS'));
		g_ole_sku.put('OLE_SKU_USER_DEF_NUM_1',  to_char( r_sku.user_def_num_1, 'fm999999990.999990'));
		g_ole_sku.put('OLE_SKU_USER_DEF_NUM_2',  to_char( r_sku.user_def_num_2, 'fm999999990.999990'));
		g_ole_sku.put('OLE_SKU_USER_DEF_NUM_3',  to_char( r_sku.user_def_num_3, 'fm999999990.999990'));
		g_ole_sku.put('OLE_SKU_USER_DEF_NUM_4',  to_char( r_sku.user_def_num_4, 'fm999999990.999990'));
		g_ole_sku.put('OLE_SKU_USER_DEF_NOTE_1',  r_sku.user_def_note_1);
		g_ole_sku.put('OLE_SKU_USER_DEF_NOTE_2',  r_sku.user_def_note_2);
		g_ole_sku.put('OLE_SKU_CE_WAREHOUSE_TYPE',  r_sku.ce_warehouse_type);
		g_ole_sku.put('OLE_SKU_CE_CUSTOMS_EXCISE',  r_sku.ce_customs_excise);
		g_ole_sku.put('OLE_SKU_CE_STANDARD_COST',  to_char( r_sku.ce_standard_cost));
		g_ole_sku.put('OLE_SKU_CE_STANDARD_CURRENCY',  r_sku.ce_standard_currency);
		g_ole_sku.put('OLE_SKU_COUNT_LIST_ID_1',  r_sku.count_list_id_1);
		g_ole_sku.put('OLE_SKU_DISALLOW_CLUSTERING',  r_sku.disallow_clustering);
		g_ole_sku.put('OLE_SKU_MAX_STACK',  to_char( r_sku.max_stack));
		g_ole_sku.put('OLE_SKU_STACK_DESCRIPTION',  r_sku.stack_description);
		g_ole_sku.put('OLE_SKU_STACK_LIMITATION',  to_char( r_sku.stack_limitation));
		g_ole_sku.put('OLE_SKU_CE_DUTY_STAMP',  r_sku.ce_duty_stamp);
		g_ole_sku.put('OLE_SKU_CAPTURE_WEIGHT',  r_sku.capture_weight);
		g_ole_sku.put('OLE_SKU_WEIGH_AT_RECEIPT',r_sku.weigh_at_receipt);
		g_ole_sku.put('OLE_SKU_UPPER_WEIGHT_TOLERANCE',  to_char( r_sku.upper_weight_tolerance));
		g_ole_sku.put('OLE_SKU_LOWER_WEIGHT_TOLERANCE',  to_char( r_sku.lower_weight_tolerance));
		g_ole_sku.put('OLE_SKU_SERIAL_AT_LOADING',  r_sku.serial_at_loading);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_KITTING',  r_sku.serial_at_kitting);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_UNKITTING',  r_sku.serial_at_unkitting);
		g_ole_sku.put('OLE_SKU_ALLOCALG_LOCKING_STN',  r_sku.allocalg_locking_stn);
		g_ole_sku.put('OLE_SKU_PUTALG_LOCKING_STN',  r_sku.putalg_locking_stn);
		g_ole_sku.put('OLE_SKU_CE_COMMODITY_CODE',  r_sku.ce_commodity_code);
		g_ole_sku.put('OLE_SKU_CE_COO',  r_sku.ce_coo);
		g_ole_sku.put('OLE_SKU_CE_CWC',  r_sku.ce_cwc);
		g_ole_sku.put('OLE_SKU_CE_VAT_CODE',  r_sku.ce_vat_code);
		g_ole_sku.put('OLE_SKU_CE_PRODUCT_TYPE',  r_sku.ce_product_type);
		g_ole_sku.put('OLE_SKU_COMMODITY_CODE',  r_sku.commodity_code);
		g_ole_sku.put('OLE_SKU_COMMODITY_DESC',  r_sku.commodity_desc);
		g_ole_sku.put('OLE_SKU_FAMILY_GROUP',  r_sku.family_group);
		g_ole_sku.put('OLE_SKU_BREAKPACK',  r_sku.breakpack);
		g_ole_sku.put('OLE_SKU_CLEARABLE',  r_sku.clearable);
		g_ole_sku.put('OLE_SKU_STAGE_ROUTE_ID',  r_sku.stage_route_id);
		g_ole_sku.put('OLE_SKU_SERIAL_DYNAMIC_RANGE',  r_sku.serial_dynamic_range);
		g_ole_sku.put('OLE_SKU_SERIAL_MAX_RANGE',  to_char( r_sku.serial_max_range));
		g_ole_sku.put('OLE_SKU_MANUFACTURE_AT_REPACK',  r_sku.manufacture_at_repack);
		g_ole_sku.put('OLE_SKU_EXPIRY_AT_REPACK',  r_sku.expiry_at_repack);
		g_ole_sku.put('OLE_SKU_UDF_AT_REPACK',  r_sku.udf_at_repack);
		g_ole_sku.put('OLE_SKU_REPACK_BY_PIECE',  r_sku.repack_by_piece);
		g_ole_sku.put('OLE_SKU_PACKED_HEIGHT',  to_char( r_sku.packed_height, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_PACKED_WIDTH',  to_char( r_sku.packed_width, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_PACKED_DEPTH',  to_char( r_sku.packed_depth, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_PACKED_VOLUME',  to_char( r_sku.packed_volume, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_PACKED_WEIGHT',  to_char( r_sku.packed_weight, 'fm999990.900000'));
		g_ole_sku.put('OLE_SKU_TWO_MAN_LIFT',  r_sku.two_man_lift);
		g_ole_sku.put('OLE_SKU_AWKWARD',  r_sku.awkward);
		g_ole_sku.put('OLE_SKU_DECATALOGUED',  r_sku.decatalogued);
		-- new fields after 2009
		g_ole_sku.put('OLE_SKU_STOCK_CHECK_RULE_ID',  r_sku.stock_check_rule_id);
		g_ole_sku.put('OLE_SKU_UNKITTING_INHERIT',  r_sku.unkitting_inherit);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_STOCK_CHECK',  r_sku.serial_at_stock_check);
		g_ole_sku.put('OLE_SKU_SERIAL_AT_STOCK_ADJUST',  r_sku.serial_at_stock_adjust);
		g_ole_sku.put('OLE_SKU_KIT_SHIP_COMPONENTS',  r_sku.kit_ship_components);
		g_ole_sku.put('OLE_SKU_UNALLOCATABLE',  r_sku.unallocatable);
		g_ole_sku.put('OLE_SKU_BATCH_AT_KITTING',  r_sku.batch_at_kitting);
		g_ole_sku.put('OLE_SKU_BATCH_ID_GENERATION_ALG',  r_sku.batch_id_generation_alg);
		g_ole_sku.put('OLE_SKU_VMI_ALLOW_ALLOCATION',  r_sku.vmi_allow_allocation);
		g_ole_sku.put('OLE_SKU_VMI_ALLOW_REPLENISH',  r_sku.vmi_allow_replenish);
		g_ole_sku.put('OLE_SKU_VMI_ALLOW_MANUAL',  r_sku.vmi_allow_manual);
		g_ole_sku.put('OLE_SKU_VMI_ALLOW_INTERFACED',  r_sku.vmi_allow_interfaced);
		g_ole_sku.put('OLE_SKU_VMI_OVERSTOCK_QTY',  r_sku.vmi_overstock_qty);
		g_ole_sku.put('OLE_SKU_VMI_AGING_DAYS',  r_sku.vmi_aging_days);
		g_ole_sku.put('OLE_SKU_SCRAP_ON_RETURN',  r_sku.scrap_on_return);
		g_ole_sku.put('OLE_SKU_HARMONISED_PRODUCT_CODE',  r_sku.harmonised_product_code);
		g_ole_sku.put('OLE_SKU_TAG_MERGE',  r_sku.tag_merge);
		g_ole_sku.put('OLE_SKU_UPLOADED',  r_sku.uploaded);
		g_ole_sku.put('OLE_SKU_UPLOADED_WS2PC_ID',  r_sku.uploaded_ws2pc_id);
		g_ole_sku.put('OLE_SKU_UPLOADED_FILENAME',  r_sku.uploaded_filename);
		g_ole_sku.put('OLE_SKU_UPLOADED_DSTAMP',  r_sku.uploaded_dstamp);
		g_ole_sku.put('OLE_SKU_CARRIER_PALLET_MIXING',  r_sku.carrier_pallet_mixing);
		g_ole_sku.put('OLE_SKU_SPECIAL_CONTAINER_TYPE',  r_sku.special_container_type);
		g_ole_sku.put('OLE_SKU_DISALLOW_RDT_OVER_PICKING',  r_sku.disallow_rdt_over_picking);
		g_ole_sku.put('OLE_SKU_NO_ALLOC_BACK_ORDER',  r_sku.no_alloc_back_order);
		g_ole_sku.put('OLE_SKU_RETURN_MIN_SHELF_LIFE',  r_sku.return_min_shelf_life);
		g_ole_sku.put('OLE_SKU_WEIGH_AT_GRID_PICK',  r_sku.weigh_at_grid_pick);
		g_ole_sku.put('OLE_SKU_CE_EXCISE_PRODUCT_CODE',  r_sku.ce_excise_product_code);
		g_ole_sku.put('OLE_SKU_CE_DEGREE_PLATO',  r_sku.ce_degree_plato);
		g_ole_sku.put('OLE_SKU_CE_DESIGNATION_ORIGIN',  r_sku.ce_designation_origin);
		g_ole_sku.put('OLE_SKU_CE_DENSITY',  r_sku.ce_density);
		g_ole_sku.put('OLE_SKU_CE_BRAND_NAME',  r_sku.ce_brand_name);
		g_ole_sku.put('OLE_SKU_CE_ALCOHOLIC_STRENGTH',  r_sku.ce_alcoholic_strength);
		g_ole_sku.put('OLE_SKU_CE_FISCAL_MARK',  r_sku.ce_fiscal_mark);
		g_ole_sku.put('OLE_SKU_CE_SIZE_OF_PRODUCER',  r_sku.ce_size_of_producer);
		g_ole_sku.put('OLE_SKU_CE_COMMERCIAL_DESC',  r_sku.ce_commercial_desc);
		g_ole_sku.put('OLE_SKU_SERIAL_NO_OUTBOUND',  r_sku.serial_no_outbound);
		g_ole_sku.put('OLE_SKU_FULL_PALLET_AT_RECEIPT',  r_sku.full_pallet_at_receipt);
		g_ole_sku.put('OLE_SKU_ALWAYS_FULL_PALLET',  r_sku.always_full_pallet);
		g_ole_sku.put('OLE_SKU_SUB_WITHIN_PRODUCT_GRP',  r_sku.sub_within_product_grp);
		g_ole_sku.put('OLE_SKU_SERIAL_CHECK_STRING',  r_sku.serial_check_string);
		g_ole_sku.put('OLE_SKU_CARRIER_PRODUCT_TYPE',  r_sku.carrier_product_type);
		g_ole_sku.put('OLE_SKU_MAX_PACK_CONFIGS',  r_sku.max_pack_configs);
		g_ole_sku.put('OLE_SKU_PARCEL_PACKING_BY_PIECE',  r_sku.parcel_packing_by_piece);

                g_ole.put('sub_segments_sku', g_ole_sku);
		g_ole_sku                    := pljson();
		-- Add Hazmat lines for the current SKU
		if	r_sku.hazmat_id is not null--like 'RHS%' 
		then
			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> g_file_name
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Start adding OLE_HAZ'
									   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
													|| '"segment_nr" "'||p_segment_nr_i||'" '
													|| '"line_id" "'||p_line_id_i||'" '
													|| '"sku_id" "'||r_ole.sku_id||'" '
													|| '"hazmat_id" "'||r_sku.hazmat_id||'" '
									   , p_order_id_i		=> p_order_nr_i
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> p_pallet_id_i
									   , p_container_id_i		=> p_container_id_i
									   , p_site_id_i		=> null
									   );
			end if;
			--
			open	c_haz( b_hazmat_id => r_sku.hazmat_id);
			fetch	c_haz
			into	r_haz;
			--
			g_ole_haz.put( 'OLE_HAZ_HAZMAT_ID', r_haz.hazmat_id);
			g_ole_haz.put( 'OLE_HAZ_HAZMAT_CLASS', r_haz.hazmat_class);
			g_ole_haz.put( 'OLE_HAZ_NOTES', r_haz.notes);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_1', r_haz.user_def_type_1);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_2', r_haz.user_def_type_2);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_3', r_haz.user_def_type_3);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_4', r_haz.user_def_type_4);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_5', r_haz.user_def_type_5);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_6', r_haz.user_def_type_6);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_7', r_haz.user_def_type_7);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TYPE_8', r_haz.user_def_type_8);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_CHK_1', r_haz.user_def_chk_1);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_CHK_2', r_haz.user_def_chk_2);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_CHK_3', r_haz.user_def_chk_3);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_CHK_4', r_haz.user_def_chk_4);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_DATE_1', to_char( r_haz.user_def_date_1, 'dd-mm-yyyy'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TIME_1', to_char( r_haz.user_def_date_1, 'hh24:mi:ss'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_DATE_2', to_char( r_haz.user_def_date_2, 'dd-mm-yyyy'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TIME_2', to_char( r_haz.user_def_date_2, 'hh24:mi:ss'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_DATE_3', to_char( r_haz.user_def_date_3, 'dd-mm-yyyy'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TIME_3', to_char( r_haz.user_def_date_3, 'hh24:mi:ss'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_DATE_4', to_char( r_haz.user_def_date_4, 'dd-mm-yyyy'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_TIME_4', to_char( r_haz.user_def_date_4, 'hh24:mi:ss'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_NUM_1', to_char( r_haz.user_def_num_1, 'fm999999990.099999'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_NUM_2', to_char( r_haz.user_def_num_2, 'fm999999990.099999'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_NUM_3', to_char( r_haz.user_def_num_3, 'fm999999990.099999'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_NUM_4', to_char( r_haz.user_def_num_4, 'fm999999990.099999'));
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_NOTE_1', r_haz.user_def_note_1);
			g_ole_haz.put( 'OLE_HAZ_USER_DEF_NOTE_2', r_haz.user_def_note_2);
			g_ole_haz.put( 'OLE_HAZ_METAPACK', r_haz.metapack);
			g_ole_haz.put( 'OLE_HAZ_UN_CLASS', r_haz.hazmat_class);
			g_ole_haz.put( 'OLE_HAZ_UN_CODE', r_haz.user_def_type_1);
			g_ole_haz.put( 'OLE_HAZ_DG_TYPE', r_haz.user_def_type_2);
			g_ole_haz.put( 'OLE_HAZ_PACKAGE_GROUP', r_haz.user_def_type_3);
			g_ole_haz.put( 'OLE_HAZ_PACKAGE_INSTR', r_haz.user_def_type_4);
			g_ole_haz.put( 'OLE_HAZ_DG_ACCESSIBILITY', r_haz.user_def_type_5);
			g_ole_haz.put( 'OLE_HAZ_DG_CARRIER_DESC', r_haz.user_def_note_1);
			g_ole_haz.put( 'OLE_HAZ_LITHIUM_CHECK_BOX', r_haz.lithium_check_box); 

			g_ole.put('sub_segments_haz', g_ole_haz);
			g_ole_haz                    := pljson();
			close c_haz;

			--
			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> g_file_name
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Start adding OLE_SHA'
									   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
													|| '"segment_nr" "'||p_segment_nr_i||'" '
													|| '"line_id" "'||p_line_id_i||'" '
													|| '"sku_id" "'||r_ole.sku_id||'" '
													|| '"hazmat_id" "'||r_sku.hazmat_id||'" '
									   , p_order_id_i		=> p_order_nr_i
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> p_pallet_id_i
									   , p_container_id_i		=> p_container_id_i
									   , p_site_id_i		=> null
									   );
			end if;

			for	r_sha in c_sha( b_hazmat_id	=> r_sku.hazmat_id
					      , b_client_id	=> r_ole.client_id
					      , b_sku_id	=> r_ole.sku_id
					      )
			loop
				g_ole_sha.put('OLE_SHA_SEGMENT_NR', 'Segment SHA: '|| to_char( l_counter));
				l_counter := l_counter +1; -- For next iteration
				g_ole_sha.put('OLE_SHA_REGULATION_ID', r_sha.regulation_id);
				g_ole_sha.put('OLE_SHA_HAZMAT_CLASS', r_sha.hazmat_class);
				g_ole_sha.put('OLE_SHA_HAZMAT_SUBCLASS', r_sha.hazmat_subclass);
				g_ole_sha.put('OLE_SHA_CLASSIFICATION_CODE', r_sha.classification_code);
				g_ole_sha.put('OLE_SHA_UN_PACKING_GROUP', r_sha.un_packing_group);
				g_ole_sha.put('OLE_SHA_HAZMAT_LABELS', r_sha.hazmat_labels);
				g_ole_sha.put('OLE_SHA_TRANSPORT_CATEGORY', r_sha.transport_category);
				g_ole_sha.put('OLE_SHA_MARINE_POLLUTANT', r_sha.marine_pollutant);
				g_ole_sha.put('OLE_SHA_MFAG', r_sha.mfag);
				g_ole_sha.put('OLE_SHA_EMS', r_sha.ems);
				g_ole_sha.put('OLE_SHA_HAZMAT_NET_WEIGHT', r_sha.hazmat_net_weight);
				g_ole_sha.put('OLE_SHA_HAZMAT_NET_VOLUME', r_sha.hazmat_net_volume);
				g_ole_sha.put('OLE_SHA_HAZMAT_NET_VOLUME_UNIT', r_sha.hazmat_net_volume_unit);
				g_ole_sha.put('OLE_SHA_HAZMAT_FLASHPOINT', r_sha.hazmat_flashpoint);
				g_ole_sha.put('OLE_SHA_FLASHPOINT_CATEGORY', r_sha.flashpoint_category);
				g_ole_sha.put('OLE_SHA_WGK_CLASS', r_sha.wgk_class);
				g_ole_sha.put('OLE_SHA_GHS_SYMBOL', r_sha.ghs_symbol);
				g_ole_sha.put('OLE_SHA_LIMITED_QTY', r_sha.limited_qty);
				g_ole_sha.put('OLE_SHA_R_SENTENCE_CODE', r_sha.r_sentence_code);				
				g_ole_sha.put('OLE_SHA_R_SENTENCE_GROUP', r_sha.r_sentence_group);
				g_ole_sha.put('OLE_SHA_R_SENTENCE', r_sha.r_sentence);
				g_ole_sha.put('OLE_SHA_PROPER_SHIPPING_NAME', r_sha.proper_shipping_name);
				g_ole_sha.put('OLE_SHA_ADDITIONAL_SHIP_NAME', r_sha.additional_shipping_name);
				g_ole_sha.put('OLE_SHA_UN_PACKAGING_CODE', r_sha.un_packaging_code);
				g_ole_sha.put('OLE_SHA_WATER_ENDANGER_CLASS', r_sha.water_endangerment_class);
				g_ole_sha.put('OLE_SHA_LANGUAGE', r_sha.language);
				g_ole_sha.put('OLE_SHA_TUNNEL_CODE', r_sha.tunnel_code);

				if	r_sha.regulation_id is not null
				then
					open	c_hrn(r_sha.regulation_id);
					fetch 	c_hrn
					into	r_hrn;
					close	c_hrn;
					g_ole_sha.put('OLE_SHA_NOTES', r_hrn.notes);
				end if;
			g_ole_sha_list.append(g_ole_sha);
			g_ole_sha             := pljson();
			end loop;
		g_ole.put('sub_segments_sha', g_ole_sha_list);
		g_ole_sha_list               := pljson_list();	
		end if;
		close c_sku;

		if	nvl(r_ole.unallocatable,'N') = 'N'			-- No unallocatbale line
		and	nvl(r_ole.qty_ordered,0) > 0				-- QTY ordered higher than 0
		and	nvl(r_ole.qty_tasked,0) + nvl(r_ole.qty_picked,0) > 0 	-- Not a zero picked order line
		then

			-- Add LOT lines for the current Order Line
			if add_lot ( p_field_prefix_i => p_field_prefix_i
				   , p_segment_nr_i   => p_segment_nr_i
				   , p_client_id_i    => r_ole.client_id
				   , p_order_nr_i     => r_ole.order_id
				   , p_line_id_i      => r_ole.line_id
				   , p_pallet_id_i    => p_pallet_id_i
				   , p_container_id_i => p_container_id_i
				   )
			then
			  g_ole.put('lot_Lines',	g_add_lot);
		          g_add_lot   			:= pljson();
			end if;

			-- Add Serial Lines for the current Order Line
			if add_snr ( p_field_prefix_i => p_field_prefix_i
				   , p_segment_nr_i   => p_segment_nr_i
				   , p_client_id_i    => r_ole.client_id
				   , p_order_id_i     => r_ole.order_id
				   , p_line_id_i      => r_ole.line_id
				  )
			then
			  g_ole.put('serial_Lines',	g_add_snr);
		          g_add_snr    			:= pljson();
			end if;

		end if;
		close c_ole;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding OLE'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;
            return l_retval;
	exception
		when	others
		then
			case 
			when	c_ole%isopen
			then
				close	c_ole;
			when 	c_sku%isopen
			then
			close 	c_sku;
			when 	c_haz%isopen
			then
				close	c_haz;
			else
				null;
			end 	case;
		return l_retval;
	end add_ole;	

------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create StreamServe Move Task block for normal Packlist file
------------------------------------------------------------------------------------------------
	function add_mtk(  p_field_prefix_i in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 )
	return boolean
	is
		cursor c_mtk( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select 	mtka.list_id                    list_id
			,      	mtkb.qty_orders                 qty_orders
			,      	lpad(mtka.seq_num,2,0)          order_seq_num
			,      	lpad(mtka.print_label_id,10,0)  print_label_id
			from   	(
				select  rownum               seq_num
				,       mtk1.client_id       client_id
				,       mtk2.list_id         list_id
				,       mtk1.task_id         task_id
				,       mtk1.print_label_id  print_label_id
				from    dcsdba.move_task     mtk1
				,       dcsdba.move_task     mtk2
				where   mtk1.client_id       = mtk2.client_id
				and     mtk1.list_id         = mtk2.list_id
				and     mtk1.sku_id          = mtk2.sku_id
				and     mtk1.sku_id          = 'DOCUMENT'
				and     mtk1.client_id       = b_client_id
				and     mtk2.task_id         = b_order_id
				order   by mtk1.print_label_id
				)       mtka
			,     	(
				select  mtk.client_id                   client_id
				,       mtk.list_id                     list_id
				,       count(distinct mtk.task_id)     qty_orders
				from    dcsdba.move_task mtk
				where   mtk.client_id   = b_client_id
				group   by mtk.client_id
				,       mtk.list_id
				)       mtkb
			where 	mtka.client_id  = mtkb.client_id
			and   	mtka.list_id    = mtkb.list_id
			and   	mtka.task_id    = b_order_id
		;
		--
		r_mtk	c_mtk%rowtype;
		l_rtn	varchar2(30) := 'add_mtk';
		l_retval	boolean		:= false;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding MTK'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		open	c_mtk( b_client_id => p_client_id_i
			     , b_order_id => p_order_nr_i
			     );
		fetch 	c_mtk
		into  	r_mtk;
		--
		if 	c_mtk%found
		then            
			g_mtk.put('MTK_MAIN_LIST'            ,r_mtk.list_id);
			g_mtk.put('MTK_TOTAL_ORDER_PER_LIST' ,r_mtk.qty_orders);
			g_mtk.put('MTK_ORDER_SEQ_NUM'        ,r_mtk.order_seq_num);
			g_mtk.put('MTK_PRINT_LABEL_ID'       ,r_mtk.print_label_id);
		end if;
		close 	c_mtk;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding MTK'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
	   return l_retval;
	exception
		when others
		then
			case	c_mtk%isopen
			when 	true
			then
				close 	c_mtk;
			else
				null;
			end 	case;
	  return l_retval;
	end add_mtk;

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Create StreamServe Move Task block for Pick Label
------------------------------------------------------------------------------------------------
	function add_mtk_pll( p_field_prefix_i in  varchar2
			    , p_client_id_i    in  varchar2
		            , p_prt_lbl_id_i   in  number
		            )
	return boolean
	is
		-- Fetch move task details
		cursor c_mtk( b_client_id  in varchar2
			    , b_prt_lbl_id in number
			    )
		is
			select	mtk.*
			from   	dcsdba.move_task mtk
			where  	mtk.client_id      = b_client_id
			and    	mtk.print_label_id = b_prt_lbl_id
		;
		--
		r_mtk   c_mtk%rowtype;
		l_rtn	varchar2(30) := 'add_mtk_pll';
		l_retval	boolean		:= false;
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
				 , p_file_name_i		=> g_file_name
				 , p_source_package_i		=> g_pck
				 , p_source_routine_i		=> l_rtn
				 , p_routine_step_i		=> 'Start adding '||p_field_prefix_i||'_MTK'
				 , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"print_label_id" "'||p_prt_lbl_id_i||'" '
				 , p_order_id_i		=> null
				 , p_client_id_i		=> p_client_id_i
				 , p_pallet_id_i		=> null
				 , p_container_id_i		=> null
				 , p_site_id_i		=> null
				 );
		end if;

		open	c_mtk( b_client_id  => p_client_id_i
			     , b_prt_lbl_id => p_prt_lbl_id_i
			     );
		fetch 	c_mtk
		into  	r_mtk;
		if 	c_mtk%found
		then            
			g_mtk_pll.put('MTK_KEY',r_mtk.key);
			g_mtk_pll.put('MTK_FIRST_KEY',r_mtk.first_key);
			g_mtk_pll.put('MTK_TASK_TYPE',r_mtk.task_type);
			g_mtk_pll.put('MTK_TASK_ID',r_mtk.task_id);
			g_mtk_pll.put('MTK_LINE_ID',r_mtk.line_id);
			g_mtk_pll.put('MTK_CLIENT_ID',r_mtk.client_id);
			g_mtk_pll.put('MTK_SKU_ID',r_mtk.sku_id);
			g_mtk_pll.put('MTK_CONFIG_ID',r_mtk.config_id);
			g_mtk_pll.put('MTK_SKU_DESCRIPTION',r_mtk.description);
			g_mtk_pll.put('MTK_TAG_ID',r_mtk.tag_id);
			g_mtk_pll.put('MTK_OLD_TAG_ID',r_mtk.old_tag_id);
			g_mtk_pll.put('MTK_CUSTOMER_ID',r_mtk.customer_id);
			g_mtk_pll.put('MTK_ORIGIN_ID',r_mtk.origin_id);
			g_mtk_pll.put('MTK_CONDITION_ID',r_mtk.condition_id);
			g_mtk_pll.put('MTK_QTY_TO_MOVE',r_mtk.qty_to_move);
			g_mtk_pll.put('MTK_OLD_QTY_TO_MOVE',r_mtk.old_qty_to_move);
			g_mtk_pll.put('MTK_SITE_ID',r_mtk.site_id);
			g_mtk_pll.put('MTK_FROM_LOC_ID',r_mtk.from_loc_id);
			g_mtk_pll.put('MTK_OLD_FROM_LOC_ID',r_mtk.old_from_loc_id);
			g_mtk_pll.put('MTK_TO_LOC_ID',r_mtk.to_loc_id);
			g_mtk_pll.put('MTK_OLD_TO_LOC_ID',r_mtk.old_to_loc_id);
			g_mtk_pll.put('MTK_FINAL_LOC_ID',r_mtk.final_loc_id);
			g_mtk_pll.put('MTK_OWNER_ID',r_mtk.owner_id);
			g_mtk_pll.put('MTK_SEQUENCE',r_mtk.sequence);
			g_mtk_pll.put('MTK_STATUS',r_mtk.status);
			g_mtk_pll.put('MTK_LIST_ID',r_mtk.list_id);
			g_mtk_pll.put('MTK_DSTAMP',to_char( r_mtk.dstamp, 'DD-MM-YYYY'));
			g_mtk_pll.put('MTK_DSTAMP_TIME',to_char( r_mtk.dstamp, 'HH24:MI:SS'));
			g_mtk_pll.put('MTK_START_DSTAMP',to_char( r_mtk.start_dstamp, 'DD-MM-YYYY'));
			g_mtk_pll.put('MTK_START_DSTAMP_TIME',to_char( r_mtk.start_dstamp, 'HH24:MI:SS'));
			g_mtk_pll.put('MTK_FINISH_DSTAMP',to_char( r_mtk.finish_dstamp, 'DD-MM-YYYY'));
			g_mtk_pll.put('MTK_FINISH_DSTAMP_TIME',to_char( r_mtk.finish_dstamp, 'HH24:MI:SS'));
			g_mtk_pll.put('MTK_ORIGINAL_DSTAMP',to_char( r_mtk.original_dstamp, 'DD-MM-YYYY'));
			g_mtk_pll.put('MTK_ORIGINAL_DSTAMP_TIME',to_char( r_mtk.original_dstamp, 'HH24:MI:SS'));
			g_mtk_pll.put('MTK_PRIORITY',r_mtk.priority);
			g_mtk_pll.put('MTK_CONSOL_LINK',r_mtk.consol_link);
			g_mtk_pll.put('MTK_FACE_TYPE',r_mtk.face_type);
			g_mtk_pll.put('MTK_FACE_KEY',r_mtk.face_key);
			g_mtk_pll.put('MTK_WORK_ZONE',r_mtk.work_zone);
			g_mtk_pll.put('MTK_WORK_GROUP',r_mtk.work_group);
			g_mtk_pll.put('MTK_CONSIGNMENT',r_mtk.consignment);
			g_mtk_pll.put('MTK_BOL_ID',r_mtk.bol_id);
			g_mtk_pll.put('MTK_REASON_CODE',r_mtk.reason_code);
			g_mtk_pll.put('MTK_CONTAINER_ID',r_mtk.container_id);
			g_mtk_pll.put('MTK_TO_CONTAINER_ID',r_mtk.to_container_id);
			g_mtk_pll.put('MTK_PALLET_ID',r_mtk.pallet_id);
			g_mtk_pll.put('MTK_TO_PALLET_ID',r_mtk.to_pallet_id);
			g_mtk_pll.put('MTK_TO_PALLET_CONFIG',r_mtk.to_pallet_config);
			g_mtk_pll.put('MTK_TO_PALLET_VOLUME',to_char( r_mtk.to_pallet_volume, 'fm999990.90'));
			g_mtk_pll.put('MTK_TO_PALLET_HEIGHT',to_char( r_mtk.to_pallet_height, 'fm999990.90'));
			g_mtk_pll.put('MTK_TO_PALLET_DEPTH',to_char( r_mtk.to_pallet_depth, 'fm999990.90'));
			g_mtk_pll.put('MTK_TO_PALLET_WIDTH',to_char( r_mtk.to_pallet_width, 'fm999990.90'));
			g_mtk_pll.put('MTK_TO_PALLET_WEIGHT',to_char( r_mtk.to_pallet_weight, 'fm999990.90'));
			g_mtk_pll.put('MTK_PALLET_GROUPED',r_mtk.pallet_grouped);
			g_mtk_pll.put('MTK_PALLET_CONFIG',r_mtk.pallet_config);
			g_mtk_pll.put('MTK_PALLET_VOLUME',to_char( r_mtk.pallet_volume, 'fm999990.90'));
			g_mtk_pll.put('MTK_PALLET_HEIGHT',to_char( r_mtk.pallet_height, 'fm999990.90'));
			g_mtk_pll.put('MTK_PALLET_DEPTH',to_char( r_mtk.pallet_depth, 'fm999990.90'));
			g_mtk_pll.put('MTK_PALLET_WIDTH',to_char( r_mtk.pallet_width, 'fm999990.90'));
			g_mtk_pll.put('MTK_PALLET_WEIGHT',to_char( r_mtk.pallet_weight, 'fm999990.90'));
			g_mtk_pll.put('MTK_USER_ID',r_mtk.user_id);
			g_mtk_pll.put('MTK_STATION_ID',r_mtk.station_id);
			g_mtk_pll.put('MTK_SESSION_TYPE',r_mtk.session_type);
			g_mtk_pll.put('MTK_SUMMARY_RECORD',r_mtk.summary_record);
			g_mtk_pll.put('MTK_REPACK',r_mtk.repack);
			g_mtk_pll.put('MTK_KIT_SKU_ID',r_mtk.kit_sku_id);
			g_mtk_pll.put('MTK_KIT_LINE_ID',r_mtk.kit_line_id);
			g_mtk_pll.put('MTK_KIT_RATIO',r_mtk.kit_ratio);
			g_mtk_pll.put('MTK_KIT_LINK',r_mtk.kit_link);
			g_mtk_pll.put('MTK_DUE_TYPE',r_mtk.due_type);
			g_mtk_pll.put('MTK_DUE_TASK_ID',r_mtk.due_task_id);
			g_mtk_pll.put('MTK_DUE_LINE_ID',r_mtk.due_line_id);
			g_mtk_pll.put('MTK_TRAILER_POSITION',r_mtk.trailer_position);
			g_mtk_pll.put('MTK_CONSOLIDATED_TASK',r_mtk.consolidated_task);
			g_mtk_pll.put('MTK_DISALLOW_TAG_SWAP',r_mtk.disallow_tag_swap);
			g_mtk_pll.put('MTK_CE_UNDER_BOND',r_mtk.ce_under_bond);
			g_mtk_pll.put('MTK_INCREMENT_TIME',r_mtk.increment_time);
			g_mtk_pll.put('MTK_ESTIMATED_TIME',r_mtk.estimated_time);
			g_mtk_pll.put('MTK_UPLOADED_LABOR',r_mtk.uploaded_labor);
			g_mtk_pll.put('MTK_PRINT_LABEL_ID',r_mtk.print_label_id);
			g_mtk_pll.put('MTK_PRINT_LABEL',r_mtk.print_label);
			g_mtk_pll.put('MTK_OLD_STATUS',r_mtk.old_status);
			g_mtk_pll.put('MTK_REPACK_QC_DONE',r_mtk.repack_qc_done);
			g_mtk_pll.put('MTK_OLD_TASK_ID',r_mtk.old_task_id);
			g_mtk_pll.put('MTK_CATCH_WEIGHT',r_mtk.catch_weight);
			g_mtk_pll.put('MTK_MOVED_LOCK_STATUS',r_mtk.moved_lock_status);
			g_mtk_pll.put('MTK_PICK_REALLOC_FLAG',r_mtk.pick_realloc_flag);
			g_mtk_pll.put('MTK_STAGE_ROUTE_ID',r_mtk.stage_route_id);
			g_mtk_pll.put('MTK_STAGE_ROUTE_SEQUENCE',r_mtk.stage_route_sequence);
			g_mtk_pll.put('MTK_LABELLING',r_mtk.labelling);
			g_mtk_pll.put('MTK_PF_CONSOL_LINK',r_mtk.pf_consol_link);
			g_mtk_pll.put('MTK_INV_KEY',r_mtk.inv_key);
			g_mtk_pll.put('MTK_FIRST_PICK',r_mtk.first_pick);
			g_mtk_pll.put('MTK_SERIAL_NUMBER',r_mtk.serial_number);
			g_mtk_pll.put('MTK_LABEL_EXCEPTIONED',r_mtk.label_exceptioned);
			g_mtk_pll.put('MTK_DECONSOLIDATE',r_mtk.deconsolidate);
		end if;

		close	c_mtk;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
				 , p_file_name_i		=> g_file_name
				 , p_source_package_i		=> g_pck
				 , p_source_routine_i		=> l_rtn
				 , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i||'_MTK'
				 , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"print_label_id" "'||p_prt_lbl_id_i||'" '
				 , p_order_id_i		=> null
				 , p_client_id_i		=> p_client_id_i
				 , p_pallet_id_i		=> null
				 , p_container_id_i		=> null
				 , p_site_id_i		=> null
				 );
		end if;
            return l_retval;
	exception
		when	others
		then
			case 
			when	c_mtk%isopen
			then
				close	 c_mtk;
			else
				null;
			end case;
            return l_retval;
	end add_mtk_pll;      

----------------------------------------------------------------------------------------------------------------------
--
--
--
----------------------------------------------------------------------------------------------------------------------
  function  create_pljson_file ( p_site_id_i       in  varchar2
                               , p_client_id_i     in  varchar2
                               , p_owner_id_i      in  varchar2
                               , p_order_id_i      in  varchar2
                               , p_carrier_id_i    in  varchar2  := null
                               , p_pallet_id_i     in  varchar2  := null
                               , p_container_id_i  in  varchar2  := null
                               , p_reprint_yn_i    in  varchar2
                               , p_user_i          in  varchar2
                               , p_workstation_i   in  varchar2
                               , p_locality_i      in  varchar2  := null
                               , p_report_name_i   in  varchar2
                               , p_rtk_key         in  integer
                               , p_pdf_link_i      in  varchar2  := null
                               , p_pdf_autostore_i in  varchar2  := null
                               )
		return boolean
		as
                -- Get order lines
		cursor	c_ole( b_client_id	in varchar2
			     , b_order_id     	in varchar2
			     , b_pallet_id    	in varchar2
			     , b_container_id 	in varchar2
			     )
		is
			-- Get already picked lines without tasks
			select	distinct
				smt.client_id
			,      	smt.order_id
			,      	smt.line_id
			from   	dcsdba.shipping_manifest  smt
			where  	smt.client_id             = b_client_id
			and    	smt.order_id              = b_order_id
			and    	smt.pallet_id             = nvl( b_pallet_id, smt.pallet_id)
			and    	smt.container_id          = nvl( b_container_id, smt.container_id)
			union 
			-- Get tasked lines
			select	distinct
				mtk.client_id
			,     	mtk.task_id               order_id
			,      	mtk.line_id
			from   	dcsdba.move_task          mtk
			where  	mtk.client_id             = b_client_id
			and    	mtk.task_id               = b_order_id
			and    	mtk.pallet_id             = nvl( b_pallet_id, mtk.pallet_id)
			and    	mtk.container_id          = nvl( b_container_id, mtk.container_id)
			-- Get unallocatable and full short lines
			union
			select	distinct
				l.client_id
			,	l.order_id
			,	l.line_id
			from	dcsdba.order_line l
			where	l.order_id		= b_order_id
			and	l.client_id		= b_client_id
			and	(	nvl(l.unallocatable,'N') = 'Y' 
				or	nvl(qty_tasked,0) + nvl(qty_picked,0) = 0
				or	nvl(qty_ordered,0) = 0
				)	
			-- Only select when no specific pallet and/or container id is required
			and	b_pallet_id 		is null
			and	b_container_id 		is null
			order  
			by 	line_id	;




    l_template_name  	varchar2(250);
    l_jrp_key        	number;
    l_eml_ads_list   	varchar2(4000);
    l_pdf_link_name  	varchar2(256);
    l_rtk_command    	varchar2(4000); 
    l_email_yn       	varchar2(1) := g_no;
    l_jrp_count         number(10) := 1;
    L_OLE_COUNT         number(10) := 0;
  begin   
   if	add_hdr	 ( '' -- p_field_prefix_i 
		, p_site_id_i     
		, p_client_id_i   
		, p_owner_id_i    
		, p_order_id_i    
		, p_user_i      
		, p_workstation_i  
		, p_locality_i)
    then
	-- Compound header
	l_body_request.put('header',		g_add_hdr);
	g_add_hdr				:= pljson();
    end if; 

    -- add Move Task segment
    if 	add_mtk ( p_field_prefix_i => ''
		, p_client_id_i    => p_client_id_i
		, p_order_nr_i     => p_order_id_i
		)
     then
	-- Compound header
	l_body_request.put('move_task',	g_mtk);
	g_mtk   			:= pljson();
    end if; 

    if	add_smt( ''            -- p_field_prefix_i 
	       , p_client_id_i -- p_client_id_i   
	       , p_order_id_i  -- p_order_nr_i     
		 ) 
    then
	-- Compound header
	l_body_request.put('sim_lot',	g_add);
	--dbms_output.put_line(l_body_request.to_char ( true ));
	g_add    			:= pljson();
    end if; 
  --  dbms_output.put_line(l_body_request.to_char ( true ));
 -- add Order Line segments incl. Lot and Serial lines
    for r_ole in c_ole ( b_client_id    => p_client_id_i
		       , b_order_id     => p_order_id_i
		       , b_pallet_id    => p_pallet_id_i
		       , b_container_id => p_container_id_i
		       )
    loop
	l_ole_count := l_ole_count + 1;
				--
	if	add_ole('' --p_field_prefix_i => g_plt
	         	, p_segment_nr_i   => l_ole_count
			, p_client_id_i    => r_ole.client_id
			, p_order_nr_i     => r_ole.order_id
			, p_line_id_i      => r_ole.line_id
			, p_pallet_id_i    => p_pallet_id_i
			, p_container_id_i => p_container_id_i
			)
	then
	  g_ole_list.append(g_ole);
          g_ole					:= pljson();
        end if; 
    end loop;
    l_body_request.put('Order_Lines',	g_ole_list);	
    g_ole_list	:= pljson_list();
   -- dbms_output.put_line(l_body_request.to_char ( true ));
    if	add_sim ('' --p_field_prefix_i
		, p_client_id_i    => p_client_id_i
		, p_order_nr_i     => p_order_id_i
		, p_pallet_id_i    => p_pallet_id_i
		, p_container_id_i => p_container_id_i
		)
    then
	-- Compound header
	--dbms_output.put_line(g_sim.to_char( true )); 
	--dbms_output.put_line(l_body_request.to_char( true )); 
	l_body_request.put('add_sim',	g_sim);
	--dbms_output.put_line(l_body_request.to_char( true )); 
	g_sim   			:= pljson();
    end if; 

   -- dbms_output.put_line(l_body_request.to_char ( true ));
    return true;
  end create_pljson_file;

------------------------------------------------------------------------------------------------
-- Author  : 
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
	l_body_req		varchar2(32767) := p_json_body_i.to_char( false);
	l_body_res		clob;
	l_body_text		varchar2(32767);
	l_request		utl_http.req;
	l_response		utl_http.resp;
	l_response_code		varchar2(30);
	l_authenticate_url	varchar2(1000) := cnl_util_pck.get_constant('BIS_AUTHENTICATE_WEBSERVICE_URL');
	l_rtn			varchar2(30) := 'call_webservice_f';
	l_exception_body	pljson	:= pljson();
	l_loop_counter		integer :=1;
	--
begin
	-- add log record
	create_bis_log_record( g_pck||'.'||l_rtn, 'Start preparing HTTP header. Timeout is set to 10 seconds.');
	utl_http.set_transfer_timeout( 5 );
	utl_http.set_proxy(p_proxy_i);
	utl_http.set_wallet(p_wallet_i, p_wallet_password_i);
	utl_http.set_response_error_check( enable => false );
	utl_http.set_detailed_excp_support( enable => true );

	begin
		-- add log record
		create_bis_log_record( g_pck||'.'||l_rtn, 'Begin http request (content-type/json).');
 		-- Iniitiate request	
		l_request := utl_http.begin_request( p_url_i, p_post_get_del_p, utl_http.HTTP_VERSION_1_1 );

  		-- add log record
		create_bis_log_record( g_pck||'.'||l_rtn, 'Content length = '||to_char(length( l_body_req )));
		-- Set request headers
		utl_http.set_header( l_request, 'Content-Type', 'application/json; charset=utf-8' );
		utl_http.set_header( l_request, 'Content-Length', length( l_body_req ) );
		-- Only use authenticate key when not fetching authentication token
--		if	p_url_i != l_authenticate_url
--		then
--			g_authenticate 	:= set_authenticate_key;
--			utl_http.set_header( l_request, 'Authorization', 'Bearer ' || g_authenticate );
--		end if;

		-- add log record
		create_bis_log_record( g_pck||'.'||l_rtn, 'Write request text');

		-- Write request
		utl_http.write_text( l_request, l_body_req );
          	-- Send request and get response
		l_response 		:= utl_http.get_response( r	=> l_request );		
		l_response_code 	:= l_response.status_code;
		p_response_reason_o	:= substr(l_response.reason_phrase,1,4000);

		-- add log record
		create_bis_log_record( g_pck||'.'||l_rtn, ', response code = '|| l_response_code||', response phrase = '|| p_response_reason_o);

		-- add log record
		create_bis_log_record( g_pck||'.'||l_rtn, 'Start Reading response');
                --  p_json_body_i
		-- Read response
		dbms_lob.createtemporary(l_body_res, true);
		begin
			loop
				-- add log record
				create_bis_log_record( g_pck||'.'||l_rtn, 'Read line '|| to_char(l_loop_counter));
				l_loop_counter	:= l_loop_counter + 1;

				--clean temp text 
				l_body_text	:= null;
				-- Read next line
				utl_http.read_text(l_response ,l_body_text, 32766);
				--add line to clob
				dbms_lob.writeappend( l_body_res,length(l_body_text), l_body_text);
				--dbms_output.put_line('l_body_text: '||l_body_text);
			end loop;
		exception
			when utl_http.END_OF_BODY
			then
				-- add log record
				create_bis_log_record( g_pck||'.'||l_rtn, 'Finished reading response.');
		end;			

		-- add log record
		create_bis_log_record( g_pck||'.'||l_rtn, 'return reponse code '|| l_response_code|| '.');
		create_bis_log_record( g_pck||'.'||l_rtn, 'Web service call ended successfuly.');

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
			--dbms_lob.freetemporary(l_body_res);
			if	l_request.http_version != null
			then
				utl_http.end_request( l_request );
			end if;
			if	l_response.http_version != null
			then
				utl_http.end_response( l_response );
			end if;	

			        cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
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
-- Author  : 
-- Purpose : Tracing enabled yes or no
------------------------------------------------------------------------------------------------
function set_bis_tracing_f
	return boolean
is
	l_text_data	dcsdba.system_profile.text_data%type;
	l_retval	boolean;
begin
	select	upper(text_data)
	into	l_text_data
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_CENTIRO_TRACING_BISTRACING'
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
end set_bis_tracing_f;

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Tracing enabled yes or no
------------------------------------------------------------------------------------------------
procedure set_bis_tracing_p
is
begin
	g_tracing	:= set_bis_tracing_f;
end set_bis_tracing_p;


------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Create trace record
------------------------------------------------------------------------------------------------
procedure create_bis_trace_record( p_request_i		in pljson 	default null
				 , p_response_i		in pljson 	default null
				 , p_status_code_i	in varchar2 	default null
				 , p_web_service_name_i	in varchar2
				 , p_key_i		in integer 	default null
				 , p_key_o		out integer
				 )
is
	l_body		clob;
	l_rtn		varchar2(30) := 'create_bis_trace_record';
	l_key		integer;
	pragma		autonomous_transaction;
begin
	set_bis_tracing_p;
	dbms_lob.createtemporary(l_body, false);
	if	g_tracing
	then
		if	p_response_i is null
		then
			p_request_i.to_clob(l_body,  false );
			l_key 	:= cnl_bis_webservice_body_seq1.nextval;
			p_key_o	:= l_key;

			insert
			into	cnl_bis_webservice_body
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

			update	cnl_bis_webservice_body
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

		       cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				        -- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		commit;
end create_bis_trace_record;

------------------------------------------------------------------------------------------------
-- Author  : 
-- Purpose : Get Print Server for StreamServe
------------------------------------------------------------------------------------------------
	function get_print_server
		return varchar2
	is
		l_print_server varchar2(30);
	begin
		select	user_def_type_1
		into   	l_print_server
		from   	dcsdba.system_options
		;
		return l_print_server;
	end get_print_server;

  procedure create_report( p_site_id_i       in  varchar2
			 , p_client_id_i     in  varchar2
                         , p_owner_id_i      in  varchar2
                         , p_order_id_i      in  varchar2
                         , p_carrier_id_i    in  varchar2  := null
                         , p_pallet_id_i     in  varchar2  := null
                         , p_container_id_i  in  varchar2  := null
                         , p_reprint_yn_i    in  varchar2
                         , p_user_i          in  varchar2
                         , p_workstation_i   in  varchar2
                         , p_locality_i      in  varchar2  := null
                         , p_report_name_i   in  varchar2
                         , p_rtk_key         in  integer
                         , p_pdf_link_i      in  varchar2  := null
                         , p_pdf_autostore_i in  varchar2  := null
			 , p_run_task_i      in  dcsdba.run_task%rowtype
                         )
  is

cursor	c_jrp        ( b_report_name	in varchar2
			     , b_site_id     	in varchar2
			     , b_client_id   	in varchar2
			     , b_owner_id    	in varchar2
			     , b_order_id    	in varchar2
			     , b_carrier_id  	in varchar2
			     , b_user_id     	in varchar2
			     , b_station_id  	in varchar2
			     , b_locality    	in varchar2
			     )
		is
			select	jrp.key
			,      	jrp.print_mode
			,      	jrp.report_name
			,      	jrp.template_name
			,      	jrp.site_id
			,      	jrp.client_id
			,      	jrp.owner_id
			,      	jrp.carrier_id
			,      	jrp.user_id
			,      	jrp.station_id
			,      	jrp.customer_id
			,      	jrp.extra_parameters
			,      	jrp.email_enabled
			,      	jrp.email_export_type
			,      	jrp.email_attachment
			,     	jrp.email_subject
			,      	jrp.email_message
			,      	jrp.copies
			,      	jrp.locality
			from   	dcsdba.java_report_map	jrp
			where  	jrp.report_name        	= b_report_name
			and    	(jrp.site_id	= nvl(b_site_id, jrp.site_id) 		or jrp.site_id		is null)
			and    	(jrp.client_id	= nvl(b_client_id, jrp.client_id)	or jrp.client_id        is null)
			and    	(jrp.owner_id	= nvl(b_owner_id, jrp.owner_id)		or jrp.owner_id		is null)
			and    	(jrp.carrier_id	= nvl(b_carrier_id, jrp.carrier_id)	or jrp.carrier_id	is null)
			and    	(jrp.user_id	= nvl(b_user_id, jrp.user_id)		or jrp.user_id		is null)
			and    	(jrp.station_id	= nvl(b_station_id, jrp.station_id)	or jrp.station_id	is null)
			and    	(jrp.locality	= b_locality				or jrp.locality		is null)
			and    	1		= cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i	=> b_client_id
										      , p_order_id_i  	=> b_order_id
										      , p_where_i     	=> nvl( jrp.extra_parameters, '1=1')
										      )
			-- Below is to split documents that must be printed by Jaspersoft.
			and    	instr(lower(nvl(jrp.extra_parameters,'EMPTY')),'jaspersoft') = 0
			order  
			by 	jrp.template_name
			,      	jrp.station_id       	nulls last
			,      	jrp.locality         	nulls last
			,      	jrp.site_id          	nulls last
			,      	jrp.client_id        	nulls last
			,      	jrp.owner_id         	nulls last
			,      	jrp.carrier_id       	nulls last
			,      	jrp.user_id          	nulls last
			,      	jrp.extra_parameters	nulls last
		;

-- Get printers or output method 
	cursor	c_jrt( b_key in number)
		is
			select	rownum
			,      	jrt.key
			,      	jrt.export_type
			,      	jrt.export_target
			,      	jrt.copies
			,      	jrt.template_name
			from   	dcsdba.java_report_export  jrt
			where  	jrt.key = b_key
			order  	
			by 	upper(jrt.export_target)
		;

    l_template_name  	        varchar2(250);
    l_jrp_key             	number;
    l_eml_ads_list   	        varchar2(4000);
    l_pdf_link_name  	        varchar2(256);
    l_rtk_command    	        varchar2(4000); 
    l_email_yn       	        varchar2(1) := g_no;
    l_jrp_count                 number(10) := 1;

    l_report_id 		varchar2(1000);
    l_printers			varchar2(200);
    l_copies 			varchar2(50);
    l_trace_key		        integer;
    l_rtn                       varchar2(100)   := 'create_report';
    l_response_code		varchar2(30);
    l_url                       varchar2(1000)	:= cnl_util_pck.get_constant('BIS_SEEBURGER_END_POINT');
    l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
    l_wallet		        varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
    l_wall_passw	        varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');
    l_response_reason	        varchar2(4000);
    l_print_server              varchar2(30);
    l_prt_template              varchar2(100);
    l_prt_export  	        varchar2(250);
    l_print_server_path         varchar2(250);
  begin 
    case
    when	g_database 	= 'DEVCNLJW'
    then	l_report_id	:= to_char(10000000000+CNL_BIS_REPORT_ID_SEQ1.nextval);
    when	g_database 	= 'TSTCNLJW'
    then	l_report_id	:= to_char(20000000000+CNL_BIS_REPORT_ID_SEQ1.nextval);
    when	g_database 	= 'ACCCNLJW'
    then	l_report_id	:= to_char(30000000000+CNL_BIS_REPORT_ID_SEQ1.nextval);
    when	g_database 	= 'PRDCNLJW'
    then	l_report_id	:= to_char(90000000000+CNL_BIS_REPORT_ID_SEQ1.nextval);
    end case;	  

    -- Add logging
    create_bis_log_record( g_pck||'.'||l_rtn
			 , 'Start report procedure site '
			 ||p_site_id_i
			 ||' from client '
			 ||p_client_id_i
			 ||' from order '
			 ||p_order_id_i
			 ||'.'
			 );

   --print info
for	r_jrp in c_jrp( b_report_name => p_report_name_i -- always with UREPSSVPLT or UREPSSVPLTCON report name
					      , b_site_id     => p_site_id_i
					      , b_client_id   => p_client_id_i
					      , b_owner_id    => p_owner_id_i
					      , b_order_id    => p_order_id_i
					      , b_carrier_id  => p_carrier_id_i
					      , b_user_id     => p_user_i
					      , b_station_id  => p_workstation_i
					      , b_locality    => p_locality_i
					      )
			loop    
    -- dbms_output.put_line('stap 1');
    l_template_name := r_jrp.template_name;
    l_jrp_key   := r_jrp.key;

   l_eml_ads_list	:= cnl_wms_pck.get_jr_email_recipients( p_jrp_key_i    => l_jrp_key
                                                              , p_parameters_i => l_rtk_command); 

   -- set email Y/N
   if 	l_eml_ads_list is not null
   then
        l_email_yn := g_yes;
   else
	l_email_yn := g_no;
   end if;

   if 	p_pdf_link_i is not null
   then
	l_pdf_link_name := upper(nvl(r_jrp.template_name, l_template_name) || '_' || p_pdf_link_i  || '_' || to_char(p_rtk_key)	|| '_' || to_char(nvl(l_jrp_key, 0)));
   else
	l_pdf_link_name	:= null;
   end if;

    l_print_server  := get_print_server;

    for r_jrt in c_jrt(p_rtk_key) loop
      l_prt_template := upper( r_jrt.template_name);
      l_prt_export := r_jrt.export_target;
    end loop;
    l_print_server_path := upper( l_print_server);

    if	add_ptr ( l_jrp_count --p_segment_nr_i
                , l_jrp_key       --p_jrp_key_i           
		, l_template_name --p_template_name_i     
		, l_prt_template --p_ptr_template_name_i 
		, upper( l_prt_export) --p_ptr_name_i          
		, l_print_server_path --p_ptr_unc_path_i      
		, l_copies           
		, g_yes --p_print_yn_i          
		, l_eml_ads_list --p_eml_addresses_to_i 
		, null --p_eml_addresses_bcc_i 
		, l_email_yn --p_email_yn_i          
		, r_jrp.email_attachment --p_email_attachment_i  
		, r_jrp.email_subject --p_email_subject_i     
		, r_jrp.email_message --p_email_message_i     
		, nvl(l_pdf_link_name, g_no) --p_pdf_link_yn_i       
		, nvl(p_pdf_autostore_i, g_no)) --p_pdf_autostore_i)	
     then
--      dbms_output.put_line('l_print_server_path: '||l_print_server_path);
--      dbms_output.put_line(g_add_ptr.to_char(true));
	-- Compound header
	l_body_request.put('print_info',	g_add_ptr);
	g_add_ptr				:= pljson();
	l_jrp_count := l_jrp_count + 1;
    end if;

     -- Fetch shipment
    if	create_pljson_file( p_site_id_i      
                          , p_client_id_i     
                          , p_owner_id_i      
                          , p_order_id_i     
                          , p_carrier_id_i   
                          , p_pallet_id_i     
                          , p_container_id_i 
                          , p_reprint_yn_i   
                          , p_user_i          
                          , p_workstation_i  
                          , p_locality_i     
                          , p_report_name_i   
                          , p_rtk_key        
                          , p_pdf_link_i      
                          , p_pdf_autostore_i 
                          )
    then
	-- Compound streamserver
	--dbms_output.put_line(l_body_request.to_char(true));
	l_report.put('report',			l_body_request);
	--l_body_request				:= pljson();
	--dbms_output.put_line(l_report.to_char ( true ));
    end if;

	-- add web service trace
	create_bis_trace_record( l_body_request, null, null, l_rtn, null, l_trace_key);

	-- Call web service
	l_response_code := call_webservice_f( p_url_i		  => l_url
	                                    , p_proxy_i		  => l_proxy
					    , p_user_name_i	  => null
					    , p_password_i	  => null
					    , p_wallet_i	  => l_wallet
					    , p_wallet_password_i => l_wall_passw
					    , p_post_get_del_p	  => 'POST'
					    , p_json_body_i	  => l_report
					    , p_json_body_o	  => l_body_response
					    , p_response_reason_o => l_response_reason);

	dbms_output.put_line('l_response_code: '||l_response_code);
	dbms_output.put_line('l_response_reason: '||l_response_reason);

	-- Add logging
	create_bis_log_record( g_pck||'.'||l_rtn
			     , 'http response for order ' 
			     || p_order_id_i 
			     || ' from client ' 
			     || p_client_id_i
			     ||' = '
			     || l_response_code 
			     || ', '
			     || nvl(l_response_reason,'N')
			     || '.'
			     );

	-- add web service trace
	create_bis_trace_record( null, l_body_response, l_response_code, l_rtn, l_trace_key, l_trace_key);

end loop;
			-- Add logging
	create_bis_log_record( g_pck||'.'||l_rtn
			     , 'Finished create report procedure for order_id '
			     || p_order_id_i
			     ||' from client '
			     || p_client_id_i
			     ||'. Now continue processing response.'
			     );  
  end;


end cnl_streamserve_PLJSON_pck;