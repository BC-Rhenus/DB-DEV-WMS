CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CTO_PARCEL_PCK" 
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
	-- Name of package used for logging and tracing
	g_pck			varchar2(30)		:= 'cnl_cto_parcel_pck';
	-- Database environment
	g_database		varchar2(10)		:= cnl_cto_pck.fetch_database_f;
	-- run task key
	g_run_task_key		integer;
	-- parcel line attributes
	g_par_line_atts		pljson_list		:= pljson_list();
	-- parcel lines		
	g_parcel_lines		pljson_list		:= pljson_list();
	-- parcel receiver references
	g_par_receiver_refs	pljson			:= pljson();
	-- parcel sender referrences
	g_par_sender_refs	pljson			:= pljson();
	-- parcel attributes
	g_par_atts		pljson_list		:= pljson_list();
	-- list of parcels	
	g_parcels		pljson_list		:= pljson_list();
	-- shipment identifier
	g_shipment_id		dcsdba.order_header.uploaded_ws2pc_id%type;
	-- shipment type
	g_shipment_type		varchar2(20)		:= 'Outbound';
	-- printer type
	g_printer_type		varchar2(20)		:= 'Zebra';
	-- printer name to print to
	g_printer_name		varchar2(200);
	-- copies
	g_nbr_copies		varchar2(50);--integer;
	-- http reponse code
	g_http_response_code	varchar(30);
	-- http response reason	
	g_http_response_reason	varchar2(2000);
	-- multi order y/n
	g_multi_order		varchar2(1) 		:= 'N';
	-- work station id
	g_station_id		dcsdba.workstation.station_id%type;
	-- user id
	g_user_id		dcsdba.application_user.user_id%type;
	-- order_id
	g_order_id		dcsdba.order_header.order_id%type;
	-- pallet or container id
	g_pallet_or_container	varchar2(1); -- P is pallet C is parcel
	-- If printing msut be done
	g_dws			varchar2(1);
	-- shipment already closed require new shipment to be created.
	g_shipment_closed	varchar2(1) := 'N';
	-- New IATA regulation BDS-5898
	g_iata			varchar2(1);
	g_iata_hazmat		dcsdba.hazmat.hazmat_id%type;
	g_iata_sku		dcsdba.sku.sku_id%type;

	type	g_parcel_rec	is record( parcel_id		dcsdba.order_container.pallet_id%type
					 , site_id		dcsdba.site.site_id%type
					 , client_id		dcsdba.client.client_id%type
					 , parcel_height	number
					 , parcel_depth		number
					 , parcel_width		number
					 , parcel_volume	number
					 , parcel_weight	number
					 , parcel_type		dcsdba.order_container.config_id%type
					 , loadingmeter		dcsdba.pallet_config.load_metres%type
					 , netweight		number
					 , trackingnumber	varchar2(50)
					 , trackingNumbersscc	varchar2(50)
					 , typeofgoods		varchar2(50)
					 , typeofpackage	dcsdba.pallet_type_grp.notes%type
					 , consol_link		dcsdba.move_task.consol_link%type
					 , order_id		dcsdba.order_header.order_id%type
					 , shipment_id		varchar2(50)
					 , run_task_key		integer
					 , pallet_or_container	varchar2(10)
					 , pallet_type		varchar2(50)
					 , shp_label		clob
					 , carrier_id		dcsdba.order_header.carrier_id%type
					 , service_level	dcsdba.order_header.service_level%type
					 , tracking_number	varchar2(50)
					 , tracking_url		varchar2(400)
					 , cto_sscc		varchar2(50)
					 , status		varchar(10)
					 , dws			varchar2(1)
					 , pallet_id		varchar2(50)
					 , container_id		varchar2(50)
					 , already_labelled	varchar2(1)
					 , reprinted		varchar2(1)
					 , order_reference	dcsdba.order_header.order_reference%type
					 , purchase_order	dcsdba.order_header.purchase_order%type
					 , shp_label_base64	clob
					 );
	type		g_parcel_tab	is table of g_parcel_rec;
	g_par		g_parcel_rec;
	g_parcel	g_parcel_tab	:= g_parcel_tab();

	type	g_old_par_rec is record( client_id		dcsdba.client.client_id%type
				       , site_id		dcsdba.site.site_id%type
				       , order_id		dcsdba.order_header.order_id%type
				       , shipment_id		cnl_sys.cnl_cto_ship_labels.shipment_id%type
				       , parcel_id		cnl_sys.cnl_cto_ship_labels.parcel_id%type
				       , pallet_id		cnl_sys.cnl_cto_ship_labels.pallet_id%type
				       , container_id		cnl_sys.cnl_cto_ship_labels.container_id%type
				       , pallet_or_container	cnl_sys.cnl_cto_ship_labels.pallet_or_container%type
				       , delete_parcel		varchar2(1)
				       );
	type		g_old_par_tab	is table of g_old_par_rec;
	g_old_parcels	g_old_par_tab	:= g_old_par_tab();

	type	g_printer_rec is record( printer_name	varchar2(200)
				       , copies		number
				       );
	type		g_printers_tab	is table of g_printer_rec;
	g_printers	g_printers_tab	:= g_printers_tab();

-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 06/05/2022
-- Purpose : Log parcel record details at start
------------------------------------------------------------------------------------------------
procedure update_order_master_waybill_p( p_waybill_i		in varchar2
				       , p_client_id_i 		in varchar2
				       , p_shipment_id_i	in number
				       )
is
	l_rtn	varchar2(30) := 'log_parcel_rec_p';
	cursor 	c_trax
	is
		select	count(*)
		from	dcsdba.order_header o
		where	o.client_id = p_client_id_i
		and	o.uploaded_ws2pc_id = p_shipment_id_i
		and	o.trax_id is not null
		and 	o.trax_id in	(
					select 	c.carrier_consignment_id
					from	dcsdba.order_container c
					where	c.order_id = o.order_id
					and	c.client_id = o.client_id
					and	c.carrier_consignment_id is not null
					union
					select 	m.carrier_consignment_id
					from	dcsdba.shipping_manifest m
					where	m.order_id = o.order_id
					and	m.client_id = o.client_id
					and	m.carrier_consignment_id is not null
					)
		;
	l_trax integer;
begin
		open	c_trax;
		fetch 	c_trax into l_trax;
		close 	c_trax;

		if	l_trax = 0
		then
			update	dcsdba.order_header
			set 	trax_id = p_waybill_i
			where	uploaded_ws2pc_id = p_shipment_id_i
			and	client_id = p_client_id_i
			;
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
						  , p_comments_i		=> 'error while updating order header with track and trace'
						  );
end update_order_master_waybill_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 03/03/2022
-- Purpose : Log parcel record details at start
------------------------------------------------------------------------------------------------
procedure log_parcel_rec_p( p_parcel_rec_i	g_parcel_rec)
is
	l_rtn	varchar2(30) := 'log_parcel_rec_p';
	pragma autonomous_transaction;
begin
	insert
	into	cnl_cto_parcel_rec_details
	(	dstamp
	,	parcel_id
	,	site_id	
	,	client_id
	,	parcel_height
	,	parcel_depth
	, 	parcel_width
	, 	parcel_volume
	, 	parcel_weight
	, 	parcel_type
	, 	loadingmeter
	, 	netweight
	, 	trackingnumber
	, 	trackingNumbersscc
	, 	typeofgoods
	, 	typeofpackage
	, 	consol_link
	, 	order_id
	, 	shipment_id
	, 	run_task_key
	, 	pallet_or_container
	,	pallet_type
	, 	shp_label
	, 	carrier_id
	, 	service_level
	, 	tracking_number
	, 	tracking_url
	, 	cto_sscc
	, 	status	
	, 	dws	
	, 	pallet_id
	, 	container_id
	, 	already_labelled
	, 	reprinted	
	, 	order_reference	
	, 	purchase_order	
	, 	shp_label_base64
	)
	values
	(	sysdate
	,	p_parcel_rec_i.parcel_id
	,	p_parcel_rec_i.site_id	
	,	p_parcel_rec_i.client_id
	,	p_parcel_rec_i.parcel_height
	,	p_parcel_rec_i.parcel_depth
	, 	p_parcel_rec_i.parcel_width
	, 	p_parcel_rec_i.parcel_volume
	, 	p_parcel_rec_i.parcel_weight
	, 	p_parcel_rec_i.parcel_type
	, 	p_parcel_rec_i.loadingmeter
	, 	p_parcel_rec_i.netweight
	, 	p_parcel_rec_i.trackingnumber
	, 	p_parcel_rec_i.trackingNumbersscc
	, 	p_parcel_rec_i.typeofgoods
	, 	p_parcel_rec_i.typeofpackage
	, 	p_parcel_rec_i.consol_link
	, 	p_parcel_rec_i.order_id
	, 	p_parcel_rec_i.shipment_id
	, 	p_parcel_rec_i.run_task_key
	, 	p_parcel_rec_i.pallet_or_container
	,	p_parcel_rec_i.pallet_type
	, 	p_parcel_rec_i.shp_label
	, 	p_parcel_rec_i.carrier_id
	, 	p_parcel_rec_i.service_level
	, 	p_parcel_rec_i.tracking_number
	, 	p_parcel_rec_i.tracking_url
	, 	p_parcel_rec_i.cto_sscc
	, 	p_parcel_rec_i.status	
	, 	p_parcel_rec_i.dws	
	, 	p_parcel_rec_i.pallet_id
	, 	p_parcel_rec_i.container_id
	, 	p_parcel_rec_i.already_labelled
	, 	p_parcel_rec_i.reprinted	
	, 	p_parcel_rec_i.order_reference	
	, 	p_parcel_rec_i.purchase_order	
	, 	p_parcel_rec_i.shp_label_base64
	);
	delete 	cnl_cto_parcel_rec_details
	where 	dstamp < sysdate -2;

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
						  , p_comments_i		=> 'error while adding parcel details in pragma autonomous_transcation'					-- Additional comments describing the issue
						  );
end log_parcel_rec_p;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Get zpl label Convert clob to varchar
------------------------------------------------------------------------------------------------
function get_label_file_f( p_shipment_id_i	in varchar2
			 , p_parcel_id_i	in varchar2
			 , p_run_task_key_i 	in integer
			 )
	return clob
is
	l_rtn		varchar2(30) := 'get_label_file_f';
	l_clob		clob;
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetch label for parcel '
						 || p_parcel_id_i
						 || ' from shipment '
						 || p_shipment_id_i
						 || ' from database and add to file.'
						 );

	select	nvl(shp_label_base64, shp_label)
	into	l_clob
	from	cnl_cto_ship_labels
	where	shipment_id 	= p_shipment_id_i
	and	parcel_id	= p_parcel_id_i
	and	run_task_key 	= p_run_task_key_i
	and	rownum =1
	;
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Label file succesfully created.'
						 );
	return l_clob;--l_varchar;
exception
	when NO_DATA_FOUND
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Could not find a label for parcel '
							 || p_parcel_id_i
							 || ' from shipment '
							 || p_shipment_id_i
							 || ' from database and add to file. Generating error label.'
							 );
		return cnl_cto_pck.create_zpl_text_label_f( 'ERROR. Could not find a label associated with parcel_id '|| p_parcel_id_i||'.');
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
		return 'Something else happened';
end get_label_file_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Decode clob bas64
------------------------------------------------------------------------------------------------
function base64decode( p_clob 		clob
		     , p_encode		varchar2 default 'N'
		     )
	return clob
is
	l_blob 		blob;
	l_raw 		raw(32767);
	l_amt 		number 		:= 7700;
	l_offset 	number 		:= 1;
	l_temp 		varchar2(32767);

	l_clob		clob;
	l_varchar 	varchar2(32767);
	l_start 	pls_integer := 1;
	l_buffer 	pls_integer := 32767;
	l_rtn		varchar2(30) := 'base64decode';
begin	
	if	p_encode = 'N'
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Start encoding ZPL code to BASE64 for parcel.');

		begin
			dbms_lob.createtemporary( l_blob, false, dbms_lob.call);
			loop
				dbms_lob.read(p_clob, l_amt, l_offset, l_temp);
				l_offset := l_offset + l_amt;
				l_raw := utl_encode.base64_decode(utl_raw.cast_to_raw(l_temp));
				dbms_lob.append (l_blob, to_blob(l_raw));
			end loop;
		exception
			when NO_DATA_FOUND 
			then
				null;
		end;
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Start decoding BASE64 code to ZPL for parcel.');

		begin
			dbms_lob.createtemporary( l_blob, false, dbms_lob.call);
			loop
				dbms_lob.read(p_clob, l_amt, l_offset, l_temp);
				l_offset := l_offset + l_amt;
				l_raw := utl_encode.base64_encode(utl_raw.cast_to_raw(l_temp));
				dbms_lob.append (l_blob, to_blob(l_raw));
			end loop;
		exception
			when NO_DATA_FOUND 
			then
				null;
		end;
	end if;		
	--
	begin
		dbms_lob.createtemporary(l_clob, true);
		for 	i in 1..ceil(dbms_lob.getlength(l_blob) / l_buffer)
		loop
			l_varchar := utl_raw.cast_to_varchar2(dbms_lob.substr(l_blob, l_buffer, l_start));
			dbms_lob.writeappend(l_clob, length(l_varchar), l_varchar);
			l_start := l_start + l_buffer;
		end loop;
	end;	

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Decoding or encoding finished.');

	return l_clob;
end base64decode;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Parcel line attributes
------------------------------------------------------------------------------------------------
function parcel_line_attributes_f( p_line_id_i		in dcsdba.order_line.line_id%type
				 , p_sku_id_i		in dcsdba.order_line.sku_id%type
				 , p_qty_i		in dcsdba.order_line.qty_shipped%type
				 )
	return boolean
is
	type l_att_rec is record( code	varchar2(30)
			        , value	varchar2(200)
				);
	type l_att_tab is table of l_att_rec;
	l_att 		l_att_tab	:= l_att_tab();

	-- Fetch DG details
	cursor	c_dg
	is
		select	sku.hazmat			hazmat_yn
		,	sku.hazmat_id
		,	hhc.hazmat_class		un_class
		,	hmt.notes 			un_desc
		, 	hmt.user_def_type_1 		un_code
		, 	hmt.user_def_type_2 		cto_type
		, 	hmt.user_def_type_3 		un_pack_grp
		, 	hmt.user_def_type_4 		un_pack_instr
		, 	hmt.user_def_type_5 		un_accessibility
		, 	hmt.user_def_note_1 		cto_carrier_desc
		,	nvl(sku.each_weight,0)
			*
			nvl(p_qty_i,0)			dng_netweight
		from 	dcsdba.sku			sku
		inner
		join 	dcsdba.hazmat 			hmt
		on	hmt.hazmat_id			= sku.hazmat_id
		and 	hmt.user_def_type_1 		is not null -- UN Code
		and 	hmt.user_def_type_2 		is not null -- DG Type
		inner
		join	dcsdba.hazmat_hazmat_class 	hhc
		on	hhc.hazmat_id 			= hmt.hazmat_id
		where	sku.sku_id 			= p_sku_id_i
		and	sku.client_id			= g_par.client_id
		and	sku.hazmat			= 'Y'
	;	

	-- Fetch DG details when new IATA regulation must be applied BDS-5898
	cursor	c_dg_iata
	is
		select	sku.hazmat			hazmat_yn
		,	g_iata_hazmat			hazmat_id
		,	hhc.hazmat_class		un_class
		,	hmt.notes 			un_desc
		, 	hmt.user_def_type_1 		un_code
		, 	hmt.user_def_type_2 		cto_type
		, 	hmt.user_def_type_3 		un_pack_grp
		, 	hmt.user_def_type_4 		un_pack_instr
		, 	hmt.user_def_type_5 		un_accessibility
		, 	hmt.user_def_note_1 		cto_carrier_desc
		,	nvl(sku.each_weight,0)
			*
			nvl(p_qty_i,0)			dng_netweight
		from 	dcsdba.sku			sku
		inner
		join 	dcsdba.hazmat 			hmt
		on	hmt.hazmat_id			= g_iata_hazmat
		and 	hmt.user_def_type_1 		is not null -- UN Code
		and 	hmt.user_def_type_2 		is not null -- DG Type
		inner
		join	dcsdba.hazmat_hazmat_class 	hhc
		on	hhc.hazmat_id 			= g_iata_hazmat
		where	sku.sku_id 			= p_sku_id_i
		and	sku.client_id			= g_par.client_id
		and	sku.hazmat			= 'Y'
	;	

	l_retval	boolean		:= false;
	l_attribute	pljson		:= pljson();
	l_rtn		varchar2(30) 	:= 'parcel_line_attributes_f';
	l_dg		c_dg%rowtype;
	l_un_code	varchar2(50);
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start searching parcel line attributes for line id '
						 || to_char(p_line_id_i)
						 || ' with sku_id '
						 || p_sku_id_i
						 || '.'
						 );

	-- BDS-5898 describes new IATA regulations.
	-- When a parcel contains a DG with hazmat id's like RHSUN3480% or RHSUN3090% and also contains a SKU that has gender F (Female)
	-- The hazmat id to use becomes RHSUN3481P or RHSUN3091P and not the hazmat from the SKU it self.
	if	p_sku_id_i 	= g_iata_sku
	and	g_iata		= 'Y'
	then
		open 	c_dg_iata;
		fetch 	c_dg_iata
		into 	l_dg;
		if	c_dg_iata%notfound 
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Parcel line '
								 || to_char(p_line_id_i)
								 || ' with '
								 || to_char(p_qty_i)
								 || ' of sku id '
								 || p_sku_id_i
								 || ' does not contain DG.'
								 );
		else
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Parcel line '
								 || to_char(p_line_id_i)
								 || ' with '
								 || to_char(p_qty_i)
								 || ' of sku id '
								 || p_sku_id_i
								 || ' contains DG and new IATA regulations are applied.'
								 );

			if	l_dg.un_code is not null
			then
				-- remove space when carrier = tnttel
				if	g_par.carrier_id = 'STD.TNTEL.COM'
				then
					l_un_code := replace(l_dg.un_code,' ','');
				else
					l_un_code := l_dg.un_code;
				end if;

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_UN parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_UN';
				l_att(l_att.count).value	:= l_un_code;--l_dg.un_code;
			end if;

			if	l_dg.cto_carrier_desc is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_DESCRIPTION parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_DESCRIPTION';
				l_att(l_att.count).value	:= l_dg.cto_carrier_desc;
			end if;

			if	l_dg.un_class is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_CLASS parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_CLASS';
				l_att(l_att.count).value	:= l_dg.un_class;
			end if;

			if	l_dg.un_pack_instr is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_PACKAGEINSTRUCTIONS parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_PACKAGEINSTRUCTIONS';
				l_att(l_att.count).value	:= l_dg.un_pack_instr;
			end if;

			if	l_dg.dng_netweight is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Adding DNG_NETWEIGHT attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_NETWEIGHT';
				l_att(l_att.count).value	:= l_dg.dng_netweight;
			end if;

			if	p_qty_i is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_QUANTITY parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_QUANTITY';
				l_att(l_att.count).value	:= p_qty_i;
			end if;
		end if;
		close	c_dg_iata;
	else
		-- fetch DG line attributes
		open 	c_dg;
		fetch 	c_dg 
		into 	l_dg;
		if	c_dg%notfound
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Parcel line '
								 || to_char(p_line_id_i)
								 || ' with '
								 || to_char(p_qty_i)
								 || ' of sku id '
								 || p_sku_id_i
								 || ' does not contain DG.'
								 );
		else
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Parcel line '
								 || to_char(p_line_id_i)
								 || ' with '
								 || to_char(p_qty_i)
								 || ' of sku id '
								 || p_sku_id_i
								 || ' contains DG.'
								 );

			if	l_dg.un_code is not null
			then
				-- remove space when carrier = tnttel
				if	g_par.carrier_id = 'STD.TNTEL.COM'
				then
					l_un_code := replace(l_dg.un_code,' ','');
				else
					l_un_code := l_dg.un_code;
				end if;

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_UN parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_UN';
				l_att(l_att.count).value	:= l_un_code;--l_dg.un_code;
			end if;

			if	l_dg.cto_carrier_desc is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_DESCRIPTION parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_DESCRIPTION';
				l_att(l_att.count).value	:= l_dg.cto_carrier_desc;
			end if;

			if	l_dg.un_class is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_CLASS parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_CLASS';
				l_att(l_att.count).value	:= l_dg.un_class;
			end if;

			if	l_dg.un_pack_instr is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_PACKAGEINSTRUCTIONS parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_PACKAGEINSTRUCTIONS';
				l_att(l_att.count).value	:= l_dg.un_pack_instr;
			end if;

			if	l_dg.dng_netweight is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Adding DNG_NETWEIGHT attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_NETWEIGHT';
				l_att(l_att.count).value	:= l_dg.dng_netweight;
			end if;

			if	p_qty_i is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Creating DNG_QUANTITY parcel line attribute.'
									 );
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_QUANTITY';
				l_att(l_att.count).value	:= p_qty_i;
			end if;
		end if;
		close	c_dg;
	end if;

	-- use g_par as source of information
	if	l_att.count > 0
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Start creating list with '
							 || to_char(l_att.count)
							 || ' parcel line attributes.'
							 );
		l_retval := true;
		g_par_line_atts		:= pljson_list();

		-- Loop true all line attributes
		for	 i in 1..l_att.count
		loop
			-- Clear variable
			l_attribute 	:= pljson();

			-- build attribute
			-- Name of the attribute. Both Code and Value are required if attributes are used
			l_attribute.put('code',		l_att(i).code);
			-- Value of the attribute. Both Code and Value are required if attributes are used
			l_attribute.put('value',	l_att(i).value);

			-- Add attribute to list of attributes
			g_par_line_atts.append(l_attribute);
		end loop;
	end if;

	l_att	:= l_att_tab(); -- clear memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished searching parcel line attributes.'
						 );

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
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR'
							 );
		return l_retval;
end parcel_line_attributes_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Order lines
------------------------------------------------------------------------------------------------
function parcel_lines_f
	return boolean
is
	type l_par_line_rec is record( countryoforigin		varchar2(10)
				     , itemdescription1		dcsdba.sku.description%type
				     , itemdescription2		dcsdba.sku.description%type
				     , productnumber		dcsdba.sku.sku_id%type
				     , quantityshipped		dcsdba.order_line.qty_shipped%type
				     , statisticalnumber	dcsdba.sku.commodity_code%type
				     , unitvalue 		dcsdba.order_line.product_price%type
				     , unitvaluecurrency	dcsdba.order_line.product_currency%type
				     , unitvolume 		dcsdba.sku.each_volume%type
				     , unitvolumeunitofmeasure	varchar2(10)
				     , unitweight 		dcsdba.sku.each_weight%type
				     , unitweightunitofmeasure	varchar2(10)
				     , unitmeasure		dcsdba.sku_config.track_level_1%type
				     , line_id			dcsdba.order_line.line_id%type
				     , hazmat_id		dcsdba.sku.hazmat_id%type
				     , gender			dcsdba.sku.gender%type
				     );
	type l_par_line_tab is table of l_par_line_rec;
	l_par_line		l_par_line_tab	:= l_par_line_tab();

	l_retval			boolean		:= false;
	l_line				pljson		:= pljson();
	l_rtn				varchar2(30) 	:= 'parcel_lines_f';
	l_female			varchar2(1);
	l_iata_hazmat			dcsdba.sku.hazmat_id%type;
	l_iata_sku			dcsdba.sku.sku_id%type;
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start fetching Parcel lines.'
						 );

	select	i.origin_id		countryoforigin
	,	m.description		itemdescription1
	,	null			itemdescription2
	,	m.sku_id		productnumber
	,	sum(i.qty_on_hand)	quantityshipped
	,	s.commodity_code	statisticalnumber
	,	decode(o.export,'Y',decode(o.carrier_id,'STD.UPSREADY.COM',nvl(l.product_price,0.01)),l.product_price)	unitvalue
	,	decode(o.export,'Y',decode(o.carrier_id,'STD.UPSREADY.COM',nvl(l.product_currency,'EUR')),l.product_currency) unitvaluecurrency
	,	s.each_volume 		unitvolume
	,	'm3'			unitvolumeunitofmeasure
	,	s.each_weight		unitweight
	,	'kg'			unitweightunitofmeasure
	,	c.track_level_1		unitmeasure
	, 	m.line_id
	,	s.hazmat_id
	,	s.gender	
	bulk	collect
	into	l_par_line
	from 	dcsdba.move_task 	m
	inner
	join	dcsdba.order_line 	l
	on	l.line_id		= m.line_id
	and	l.order_id		= m.task_id
	and	l.client_id		= m.client_id
	inner
	join	dcsdba.sku 		s
	on	s.sku_id		= m.sku_id
	and	s.client_id		= m.client_id
	left
	join	dcsdba.sku_config 	c
	on	c.config_id		= m.config_id
	and	c.client_id		= m.client_id
	inner
	join	dcsdba.order_header 	o
	on	o.order_id		= m.task_id
	and	o.from_site_id		= m.site_id
	and	o.client_id		= m.client_id
	and	o.uploaded_ws2pc_id	= g_par.shipment_id
	left
	join	dcsdba.inventory	i
	on	i.client_id		= m.client_id
	and	i.site_id		= m.site_id
	and	i.tag_id		= m.tag_id
	and	i.sku_id		= m.sku_id
	and	( i.container_id	= m.to_container_id 	or i.container_id = m.container_id)
	and	( i.pallet_id		= m.to_pallet_id	or i.pallet_id =  m.pallet_id)
	where	m.pallet_id		= g_par.parcel_id
	and	m.site_id		= g_par.site_id
	and	m.client_id		= g_par.client_id
	and	m.consol_link 		= g_par.consol_link
	group
	by	i.origin_id
	,	m.description
	,	null
	,	m.sku_id
	,	s.commodity_code
	,	decode(o.export,'Y',decode(o.carrier_id,'STD.UPSREADY.COM',nvl(l.product_price,0.01)),l.product_price)
	,	decode(o.export,'Y',decode(o.carrier_id,'STD.UPSREADY.COM',nvl(l.product_currency,'EUR')),l.product_currency)
	,	s.each_volume 
	,	'm3'
	,	s.each_weight
	,	'kg'
	,	c.track_level_1
	,	m.line_id
	, 	s.hazmat_id
	,	s.gender
	order
	by	line_id	asc
	;

	-- Check if 
	if	l_par_line.count > 0
	then
		-- Check if parcel contains DG according new IATA regulations
		for 	r in 1..l_par_line.count
		loop
			if	l_par_line(r).gender = 'F'
			then
				-- Register that parcel contains a female SKU
				l_female	:= 'Y';
			end if;
			-- Check if parcel contains any of the hazmat id's affected by the new IATA regulations
			if	l_par_line(r).hazmat_id like 'RHSUN3480%'
			or	l_par_line(r).hazmat_id like 'RHSUN3090%'
			then
				l_iata_hazmat	:= l_par_line(r).hazmat_id;
				l_iata_sku	:= l_par_line(r).productnumber;
			end if;
			-- Set global variaales if IATA regulations apply
			if	l_female = 'Y'
			and	(	l_iata_hazmat like 'RHSUN3480%'
				or	l_iata_hazmat like 'RHSUN3090%'
				)
			then
				g_iata		:= 'Y';
				if	l_iata_hazmat like 'RHSUN3480%'
				then
					g_iata_hazmat	:= 'RHSUN3481P';
				elsif	l_iata_hazmat like 'RHSUN3090%'
				then
					g_iata_hazmat	:= 'RHSUN3091P';
				end if;
				g_iata_sku	:= l_iata_sku;
			end if;
		end loop;

		l_retval 	:= true;
		g_parcel_lines	:= pljson_list();

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Parcel '
							 || g_par.parcel_id
							 || ' contains '
							 || to_char(l_par_line.count)
							 || ' parcel lines. Start creating list with parcel lines.'
							 );

		-- Loop true all line attributes
		for 	i in 1..l_par_line.count
		loop
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Starting creating parcel line for line id '
								 || to_char(l_par_line(i).line_id)
								 || ' with sku id '
								 || l_par_line(i).productnumber
								 || '.'
								 );

			-- Build shipment order line
			l_line					:= pljson();

			-- Customizable codes to be used for customized functionality
			if	parcel_line_attributes_f( p_line_id_i		=> l_par_line(i).line_id
							, p_sku_id_i		=> l_par_line(i).productnumber
							, p_qty_i		=> l_par_line(i).quantityshipped
							)
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_line_attributes_f returned TRUE.');

				l_line.put('attributes',		g_par_line_atts);
				g_par_line_atts				:= pljson_list(); -- clear memory
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_line_attributes_f returned FALSE.');

			end if;

			-- The origin country of the item in question. ISO 3166-1 alpha 2. Mandatory for exports to your customer either on Shipment or Parcel level	
			if 	l_par_line(i).countryoforigin is not null
			then
				l_line.put('countryOfOrigin',		l_par_line(i).countryoforigin);
			end if;

			-- To be used in subsequent carrier integrations as export information regarding items included in shipments. 
			-- Can also be used in printing delivery notes and waybills. 
			-- Mandatory for exports to your customer either on Shipment or Parcel level
			if	l_par_line(i).itemdescription1 is not null
			then
				l_line.put('itemDescription1',		l_par_line(i).itemdescription1);
			end if;
			if	l_par_line(i).itemdescription2 is not null
			then			
				l_line.put('itemDescription2',		l_par_line(i).itemdescription2);
			end if;

			-- Product number of the item in question
			if	l_par_line(i).productnumber is not null
			then			
				l_line.put('productNumber',		l_par_line(i).productnumber);
			end if;

			-- order line QTY shipped
			if	l_par_line(i).quantityshipped is not null
			then			
				l_line.put('quantityShipped', 		l_par_line(i).quantityshipped);
			end if;

			-- Commodty code
			if	l_par_line(i).statisticalnumber is not null
			then			
				l_line.put('statisticalNumber',		l_par_line(i).statisticalnumber);
			end if;

			-- each price
			if	l_par_line(i).unitvalue is not null
			then	
				l_line.put('unitValue', 		l_par_line(i).unitvalue);
			end if;

			-- line currency
			if	l_par_line(i).unitvaluecurrency is not null
			then	
				l_line.put('unitValueCurrency',		l_par_line(i).unitvaluecurrency);
			end if;

			-- each volume
			if	l_par_line(i).unitvolume is not null
			then	
				l_line.put('unitVolume', 		l_par_line(i).unitvolume);
			end if;

			-- m3
			if	l_par_line(i).unitvolumeunitofmeasure is not null
			then	
				l_line.put('unitVolumeUnitOfMeasure',	l_par_line(i).unitvolumeunitofmeasure);
			end if;

			-- Each weight
			if	l_par_line(i).unitweight is not null
			then	
				l_line.put('unitWeight', 		l_par_line(i).unitweight);
			end if;

			-- kg
			if	l_par_line(i).unitweightunitofmeasure is not null
			then	
				l_line.put('unitWeightUnitOfMeasure',	l_par_line(i).unitweightunitofmeasure);
			end if;

			-- Related to how you sell the product, for example '22meter hose' or 'packet'. Connected to QuantityShipped
			if	l_par_line(i).unitmeasure is not null
			then	
				l_line.put('unitMeasure',		l_par_line(i).unitmeasure); 
			end if;

			-- Add order line to list of order lines
			g_parcel_lines.append(l_line);
		end loop;
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment order lines'
						 );

	l_par_line	:= l_par_line_tab(); -- clear memory
	g_iata		:= 'N';
	g_iata_hazmat	:= null;
	g_iata_sku	:= null;

	return	l_retval;
exception
	when NO_DATA_FOUND
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No parcel line data found for parcel '
							 ||g_par.parcel_id
							 ||'.'
							 );
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Finished fetching shipment order lines'
							 );
		return l_retval;
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
		return l_retval;
end parcel_lines_f;

-----------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment parcel Receive References 
------------------------------------------------------------------------------------------------
function parcel_receiver_references_f
	return boolean
is
	l_retval	boolean		:= false;
	l_rtn		varchar2(30) 	:= 'parcel_receiver_references_f';
begin
	-- user g_par as information source

	g_par_receiver_refs	:= pljson();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching parcel receiver references for parcel '||g_par.parcel_id||'.'
						 );

	-- build parcel receiver

	-- When parcel contains multiple order we can't use a single order order reference
	if 	g_par.order_reference is not null
	and	g_multi_order	= 'N'
	then
		l_retval	:= true;
		g_par_receiver_refs.put('receiverReference1',		g_par.order_reference);
	end if;

	-- When parcel contains multiple order we can't use a single order purchase order
	if	g_par.purchase_order is not null
	and	g_multi_order	= 'N'
	then
		l_retval	:= true;
		g_par_receiver_refs.put('receiverReference2',		g_par.purchase_order);
	end if;

	if	1=2
	then
		l_retval	:= true;
		g_par_receiver_refs.put('receiverReference3',		'Example reference1');
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcel receiver references for parcel '||g_par.parcel_id||'.'
						 );

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
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR'
							 );
		return l_retval;
end parcel_receiver_references_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment parcel Sender references 
------------------------------------------------------------------------------------------------
function parcel_sender_references_f
	return boolean
is
	l_retval	boolean		:= false;
	l_rtn		varchar2(30) 	:= 'parcel_sender_references_f';
begin
	-- use g_par as information source

	g_par_sender_refs	:= pljson();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching parcel Sender references for parcel '||g_par.parcel_id||'.'
						 );

	-- Build parcel sender

	-- When multi order parcel we can't use single order order id
	if	g_par.order_id is not null
	and	g_multi_order = 'N'
	then
		l_retval	:= true;
		g_par_sender_refs.put('senderReference1',	g_par.order_id);
	end if;

	-- Shipent id
	if 	g_par.shipment_id is not null
	then
		l_retval	:= true;
		g_par_sender_refs.put('senderReference2',	g_par.shipment_id);
	end if;

	if	1=2
	then
		l_retval	:= true;
		g_par_sender_refs.put('senderReference3',	'example reference3');
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcel Sender references for parcel '||g_par.parcel_id||'.'
						 );

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
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR'
							 );
		return l_retval;
end parcel_sender_references_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Parcel line attributes
------------------------------------------------------------------------------------------------
function parcel_attributes_f
	return boolean
is
	type l_att_rec is record( code	varchar2(30)
			        , value	varchar2(200)
				);
	type l_att_tab is table of l_att_rec;
	l_att 		l_att_tab	:= l_att_tab();
	l_retval	boolean		:= false;
	l_attribute	pljson		:= pljson();
	l_rtn		varchar2(30) 	:= 'parcel_attributes_f';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start searching for parcel attributes.'
						 );

	-- Use all info from g_par
	if	l_att.count > 0
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Found '
							 || to_char(l_att.count)
							 || ' parcel attributes. Start creating list with parcel attributes.'
							 );
		l_retval 	:= true;
		g_par_atts	:= pljson_list();

		-- Loop true all line attributes
		for	 i in 1..l_att.count
		loop
			-- Clear variable
			l_attribute 	:= pljson();

			-- build attribute
			-- Name of the attribute. Both Code and Value are required if attributes are used
			l_attribute.put('code',		l_att(i).code);
			-- Value of the attribute. Both Code and Value are required if attributes are used
			l_attribute.put('value',	l_att(i).value);

			-- Add attribute to list of attributes
			g_par_atts.append(l_attribute);
		end loop;
	end if;

	l_att	:= l_att_tab(); -- clear memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished searching parcel attributes.'
						 );

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
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Exception check CNL_ERROR'
							 );
		return l_retval;
end parcel_attributes_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Figure out if it is a pallet or a parcel (e.g. pallet_id vs container_id to use as parcel identfier
------------------------------------------------------------------------------------------------
procedure pallet_or_container_p( p_pallet_id_i		dcsdba.move_task.pallet_id%type 		default null
			       , p_container_id_i	dcsdba.move_task.container_id%type		default null
			       , p_order_id_i		dcsdba.order_header.order_id%type		default null
			       , p_shipment_id_i	dcsdba.order_header.uploaded_ws2pc_id%type
			       , p_site_id_i		dcsdba.site.site_id%type
			       , p_client_id_i		dcsdba.client.client_id%type
			       )
is
	l_cnt		integer;
	l_rtn		varchar2(30) := 'pallet_or_container_p';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start working out if pallet or container id must be used as parcel identidier.'
						 );
	if	p_pallet_id_i 		is null
	and	p_container_id_i	is not null
	then
		g_pallet_or_container 	:= 'C';
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Only container id provided so using container id(s) as parcel indentifiers.'
							 );
	elsif	p_pallet_id_i 		is not null
	and	p_container_id_i	is null
	then
		g_pallet_or_container 	:= 'P';
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Only pallet id provided so using pallet id(s) as parcel indentifiers.'
							 );
	elsif	p_pallet_id_i 		is null
	and	p_container_id_i	is null
	then
		g_pallet_or_container 	:= 'P';
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No pallet or container id specified so using pallet id(s) as parcel indentifier.'
							 );
	elsif	p_pallet_id_i 		= p_container_id_i
	then
		g_pallet_or_container	:= 'C';
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Pallet id and container id are the same so using container id as parcel identifier.'
							 );
	else	-- Pallet id is different then container id. 
		-- Now we need to check if the pallet already contains more containers. 
		-- If it does then we assume a shipping label is asked for a multi container pallet.
		-- If it does not we assume a label is requested for the first containet on that pallet
		select	count(distinct nvl(m.to_container_id, m.container_id))
		into	l_cnt
		from	dcsdba.move_task m
		where	( to_pallet_id 	= p_pallet_id_i or pallet_id	= p_pallet_id_i)
		and	m.client_id	= p_client_id_i
		and	m.site_id	= p_site_id_i
		and	m.task_id 	!= 'PALLET'
		;
		if	l_cnt	= 1
		then
			g_pallet_or_container	:= 'C';
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Pallet and container id are different but pallet only contains one container so assuming label is required for the container. Using container id as parcel id.'
								 );
		else
			select	count(*)
			into	l_cnt
			from	dcsdba.order_container 
			where	pallet_id	= p_pallet_id_i
			and	labelled 	= 'Y'
			;
			if	l_cnt	= 0
			then
				g_pallet_or_container	:= 'P';
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Pallet contains multiple not labelled container id''s so using pallet id as parcel identifier.'
									 );
			else
				g_pallet_or_container	:= 'C';
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Pallet contains one or more containers already labelled container id''s so using pallet id as parcel identifier.'
									 );
			end if;
		end if;	
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
end pallet_or_container_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment
------------------------------------------------------------------------------------------------
function parcels_f( p_site_id_i		in dcsdba.site.site_id%type
		  , p_client_id_i    	in dcsdba.client.client_id%type
		  , p_order_id_i	in dcsdba.order_header.order_id%type 		default null
		  , p_pallet_id_i	in dcsdba.order_container.pallet_id%type	default null
		  , p_container_id_i	in dcsdba.order_container.container_id%type	default null
		  )
	return boolean
is
	cursor c_existing_label( b_parcel_id	varchar2)
	is
		select 	l.*
		from	cnl_sys.cnl_cto_ship_labels l
		where	l.client_id 	= p_client_id_i
		and	l.site_id	= p_site_id_i
		and	l.parcel_id	= b_parcel_id
		and	l.shipment_id	= g_shipment_id
		and	l.status 	in ('Created','Reprint','Error')
		order
		by	l.creation_dstamp desc
	;

	l_retval		boolean		:= false;
	l_parcel		pljson		:= pljson();
	l_rtn			varchar2(30) 	:= 'parcels_f';
	l_existing_parcel	cnl_sys.cnl_cto_ship_labels%rowtype;
	l_new_label		varchar2(1) 	:= 'Y';
	l_mvt_chk		integer;
	l_mvt_chk_cntr		integer 	:= 0;
	l_wait_time		number;
	l_cnt_chk		integer		:= 0;
	l_par_chk		varchar2(50);
begin
	-- First ensure move tasks are processed by WMS. Else data will be incorrect.
	<<move_task_chk_loop>>
	while l_mvt_chk_cntr < 200
	loop
		-- Increase loop counter by 1 loop will automatically stop at counter 200
		l_mvt_chk_cntr := l_mvt_chk_cntr+1;

		-- Search for any move task at status complete. (Complete means not yet processed)
		select	count(*)
		into	l_mvt_chk
		from	dcsdba.move_task m
		where	(	m.pallet_id 		= nvl(p_pallet_id_i,'XXX')
			or	m.container_id 		= nvl(p_container_id_i,'XXX')
			or	m.to_pallet_id 		= nvl(p_pallet_id_i,'XXX')
			or	m.to_container_id 	= nvl(p_container_id_i,'XXX')
			or	m.task_id 		= nvl(p_order_id_i,'XXX')
			)
		and 	m.status 	= 'Complete'
		and	(m.client_id 	= p_client_id_i or p_client_id_i is null)
		and	(m.site_id 	= p_site_id_i or p_site_id_i is null)
		;

		-- When move tasks are found with status complete lop will start new iteration. At specific nbr of iterations wait time is added to this routine.
		if	l_mvt_chk > 0
		then
			if	l_mvt_chk_cntr in (25, 50, 75, 100, 125, 150, 175, 199)
			then
				case l_mvt_chk_cntr
				when 25 then l_wait_time := 0.1;
				when 50 then l_wait_time := 0.2;
				when 75 then l_wait_time := 0.4;
				when 100 then l_wait_time := 0.8;
				when 125 then l_wait_time := 1;
				when 150 then l_wait_time := 1.4;
				when 175 then l_wait_time := 1.8;
				when 199 then l_wait_time := 2;
				else l_wait_time := 0.1;
				end case;

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Waiting '||to_char(l_wait_time)||' second (max 8 seconds) for move task daemon to finish processing tasks attempt nr '||to_char(l_mvt_chk_cntr)||'.'
									 ||' Site id '||nvl(p_site_id_i,'null')
									 ||', client '||nvl(p_client_id_i,'null')
									 ||', order_id '||nvl(p_order_id_i,'null')
									 ||', pallet '||nvl(p_pallet_id_i,'null')
									 ||', container '||nvl(p_container_id_i,'null')
									 );			
				dbms_lock.sleep(l_wait_time);
			else	
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Waiting for move task daemon to finish processing tasks attempt nr '||to_char(l_mvt_chk_cntr)||'.'
									 ||' Site id '||nvl(p_site_id_i,'null')
									 ||', client '||nvl(p_client_id_i,'null')
									 ||', order_id '||nvl(p_order_id_i,'null')
									 ||', pallet '||nvl(p_pallet_id_i,'null')
									 ||', container '||nvl(p_container_id_i,'null')
									 );			
			end if;
		end if;

		-- When no move tasks with status complete are found tasks are processed or are bieng processed 
		exit when l_mvt_chk = 0;
	end loop;

	-- First work out if parcel identifiers should be container id or pallet id
	pallet_or_container_p( p_pallet_id_i	=> p_pallet_id_i
			     , p_container_id_i	=> p_container_id_i
			     , p_order_id_i	=> p_order_id_i
			     , p_shipment_id_i	=> g_shipment_id
			     , p_site_id_i	=> p_site_id_i
			     , p_client_id_i	=> p_client_id_i
			     );

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start gathering ship units (parcels) information'
						 );

	-- When move tasks are getting processed there is still a small window where the parcel fetch can return duplicate records of the same parcel.
	-- In order to prevent this a check is build in.
	-- Howevere if after 200 iterations the fetch still returns duplicate records the code will continue with errors at some point. This to prevent long stalled processes.
	<<fetch_par_loop>>
	while 	l_cnt_chk < 200
	loop
		-- Clear temp table
		g_parcel.delete; --Clear table

		l_cnt_chk := l_cnt_chk +1;

		-- First fetch all parcels + parcel details that require labels
		select 	distinct 
			decode(g_pallet_or_container,'C',nvl(mt.to_container_id, mt.container_id),nvl(mt.to_pallet_id,mt.pallet_id))				parcel_id
		, 	p_site_id_i																site_id
		, 	p_client_id_i																client_id
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_height, 	oc.pallet_height),	nvl(oc.pallet_height,	oc.container_height)) 	parcel_height
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_depth, 	oc.pallet_depth), 	nvl(oc.pallet_depth,  	oc.container_depth))	parcel_depth
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_width, 	oc.pallet_width), 	nvl(oc.pallet_width, 	oc.container_width))	parcel_width
		, 	decode(g_pallet_or_container,'C', (	nvl(oc.container_width,nvl(oc.pallet_width,1))
							  * 	nvl(oc.container_depth,nvl(oc.pallet_depth,0))
							  * 	nvl(oc.container_height,nvl(oc.pallet_height,0))
							  )
							, oc.pallet_volume)											parcel_volume
		,	decode(g_pallet_or_container,'C', nvl(oc.container_weight, 	oc.pallet_weight), 	nvl(oc.pallet_weight, 	oc.container_weight))	parcel_weight
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_type, 	oc.config_id), 		nvl(oc.config_id, 	oc.container_type)) 	parcel_type
		, 	pt.load_metres																loadingmeter
		, 	sum(sk.each_weight * mt.qty_to_move)													netweight
		,	null																	trackingnumber
		,	null																	trackingNumbersscc
		, 	nvl(cl.user_def_type_4, 'Consumer Products') 												typeofgoods
		--,	pt.pallet_type_group															pallet_type_group
		,       ptg.notes																pallet_type_group --BDS-6555
		,	mt.consol_link																consol_link
		,	p_order_id_i																order_id
		,	g_shipment_id																shipment_id
		,	g_run_task_key																run_task_key
		,	g_pallet_or_container	 														pallet_or_container
		,	pt.config_id																pallet_type
		,	null																	shp_label
		,	oh.carrier_id																carrier_id
		,	oh.service_level															service_level
		,	null																	tracking_number
		,	null																	tracking_url
		,	null																	cto_sscc
		,	null																	status
		,	g_dws																	dws
		,	p_pallet_id_i																pallet_id
		,	p_container_id_i															container_id
		,	decode(oc.labelled,'Y',oc.labelled,decode(oc.pallet_labelled,'Y','Y','N'))								already_labelled
		,	'N'																	reprinted
		,	oh.order_reference															order_reference
		,	oh.purchase_order															purchase_order
		,	null
		bulk	collect 
		into	g_parcel
		from	dcsdba.move_task mt
		inner
		join	dcsdba.order_container oc
		on	oc.order_id 	= mt.task_id
		and	( oc.pallet_id = mt.to_pallet_id or oc.pallet_id = mt.pallet_id)
		and	( oc.container_id = mt.to_container_id or oc.container_id = mt.container_id)
		and	oc.client_id	= mt.client_id
		inner
		join	dcsdba.order_header oh
		on	oh.order_id 		= oc.order_id
		and	oh.client_id		= oc.client_id 
		left
		join	dcsdba.pallet_config pt
		on	pt.config_id 	= decode(oc.labelled, 'Y', nvl(oc.container_type,oc.config_id), decode(pallet_labelled, 'Y', nvl(oc.config_id,oc.container_type), nvl(oc.container_type, oc.config_id)))
		and	pt.client_id	= mt.client_id
		inner
		join	dcsdba.client cl
		on	cl.client_id = mt.client_id
		inner
		join	dcsdba.sku sk
		on	sk.sku_id	= mt.sku_id
		and	sk.client_id	= mt.client_id
		inner
		join    dcsdba.pallet_type_grp ptg
		on      ptg.pallet_type_group = pt.pallet_type_group
		where	mt.client_id	= p_client_id_i
		and	mt.site_id	= p_site_id_i
		and	( 	( 	mt.task_id	= p_order_id_i 
				and	(	(	nvl(mt.to_pallet_id, mt.pallet_id) = p_pallet_id_i 
						or 	p_pallet_id_i is null
						)
					and	(	nvl(mt.to_container_id, mt.container_id) = p_container_id_i 
						or 	p_container_id_i is null
						)
					)
				)
			or	(	p_order_id_i 	is null
				and	( 	nvl(mt.to_pallet_id, mt.pallet_id) = p_pallet_id_i 
					or 	p_pallet_id_i is null
					)
				and	( 	nvl(mt.to_container_id, mt.container_id)= p_container_id_i 
					or 	p_container_id_i is null
					)
				and	mt.task_id	in	(	select	o.order_id
									from	dcsdba.order_header o
									where	o.uploaded_ws2pc_id 	= g_shipment_id
									and	o.client_id		= mt.client_id
									and	o.from_site_id		= mt.site_id
								)
				)
			)
		group
		by	decode(g_pallet_or_container,'C',nvl(mt.to_container_id, mt.container_id),nvl(mt.to_pallet_id,mt.pallet_id))
		, 	p_site_id_i
		, 	p_client_id_i
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_height, 	oc.pallet_height),	nvl(oc.pallet_height,	oc.container_height))
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_depth, 	oc.pallet_depth), 	nvl(oc.pallet_depth,  	oc.container_depth))
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_width, 	oc.pallet_width), 	nvl(oc.pallet_width, 	oc.container_width))
		, 	decode(g_pallet_or_container,'C', (	nvl(oc.container_width,nvl(oc.pallet_width,1))
							  * 	nvl(oc.container_depth,nvl(oc.pallet_depth,0))
							  * 	nvl(oc.container_height,nvl(oc.pallet_height,0))
							  )
							, oc.pallet_volume)
		,	decode(g_pallet_or_container,'C', nvl(oc.container_weight, 	oc.pallet_weight), 	nvl(oc.pallet_weight, 	oc.container_weight))
		, 	decode(g_pallet_or_container,'C', nvl(oc.container_type, 	oc.config_id), 		nvl(oc.config_id, 	oc.container_type))
		, 	pt.load_metres
		,	null
		,	null
		, 	nvl(cl.user_def_type_4, 'Consumer Products')
		,	ptg.notes   --pallet_type_group --BDS-6555
		,	mt.consol_link
		,	p_order_id_i
		,	g_shipment_id
		,	g_run_task_key
		,	g_pallet_or_container
		,	pt.config_id
		,	null
		,	oh.carrier_id
		,	oh.service_level
		,	null
		,	null
		,	null
		,	null
		,	g_dws
		,	p_pallet_id_i
		,	p_container_id_i
		,	decode(oc.labelled,'Y',oc.labelled,decode(oc.pallet_labelled,'Y','Y','N'))
		,	oh.order_reference
		,	oh.purchase_order
		order 
		by	decode(g_pallet_or_container,'C',nvl(mt.to_container_id, mt.container_id),nvl(mt.to_pallet_id,mt.pallet_id))
		, 	p_site_id_i
		, 	p_client_id_i
		;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Found ' || g_parcel.count || ' parcels.'
							 );
		-- Check if fetch did not returned same parcel twice
		if	g_parcel.count > 1
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Found more then one parcel. Starting check for duplicates.'
								 );

			for i in 1..g_parcel.count
			loop
				if	l_par_chk is null
				then
					l_par_chk := g_parcel(i).parcel_id;
				else
					if	l_par_chk = g_parcel(i).parcel_id
					then				
						-- Found two records for same parcel query must be run again.
						if	l_cnt_chk in (25, 50, 75, 100, 125, 150, 175, 199)
						then
							case l_cnt_chk
							when 25 then l_wait_time := 0.1;
							when 50 then l_wait_time := 0.2;
							when 75 then l_wait_time := 0.4;
							when 100 then l_wait_time := 0.8;
							when 125 then l_wait_time := 1;
							when 150 then l_wait_time := 1.4;
							when 175 then l_wait_time := 1.8;
							when 199 then l_wait_time := 2;
							else l_wait_time := 0.1;
							end case;

							-- Add logging
							cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
												 , 'Fetch parcel returned duplicate records. Retry in '||to_char(l_wait_time)||' seconds (max 8 seconds) attempt nr '||to_char(l_cnt_chk)||'.'
												 ||' Site id '||nvl(p_site_id_i,'null')
												 ||', client '||nvl(p_client_id_i,'null')
												 ||', order_id '||nvl(p_order_id_i,'null')
												 ||', pallet '||nvl(p_pallet_id_i,'null')
												 ||', container '||nvl(p_container_id_i,'null')
												 );			
							dbms_lock.sleep(l_wait_time);
						end if;
					end if;
				end if;
			end loop;
		else
			exit fetch_par_loop;
		end if;
	end loop;		

	-- Save captured parcel details from query
	for 	i in 1..g_parcel.count
	loop
		-- Insert record in cnl_cto_parcel_rec_details
		log_parcel_rec_p(g_parcel(i));

		-- add monitoring log record
		cnl_sys.cnl_cto_pck.print_monitoring_log_record( p_run_task_key_i		=> g_run_task_key
							       , p_add_or_update_i		=> 'U'
							       , p_parcel_id_i			=> g_parcel(i).parcel_id
							       , p_shipment_id_i		=> g_shipment_id
							       , p_order_id_i			=> p_order_id_i
							       , p_client_id_i			=> p_client_id_i
							       , p_run_task_creation_i		=> null
							       , p_procedure_start_i		=> null
							       , p_parcel_details_fetched_i	=> sysdate
							       , p_call_webservice_i		=> null
							       , p_webservice_response_i	=> null
							       , p_update_wms_i			=> null
							       , p_send_to_printer_i		=> null
							       , p_finished_i			=> null
							       );		
	end loop;

	for	i in 1..g_parcel.count
	loop
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Starting with parcel '
							 || g_parcel(i).parcel_id
							 || '.'
							 );

		if	g_parcel(i).pallet_type is null
		then
			-- generate error label. A pallet type is used that does not exist for this client
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Non existing pallet type is used for parcel '
								 || g_parcel(i).parcel_id
								 || '. Create error label for each printer assigned.'
								 );		
			for 	i in 1..g_printers.count
			loop
				begin
					insert
					into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
								   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
								   , update_dstamp, printer_name, dws, copies, shp_label_base64
								   )
					values
					(	nvl(p_client_id_i,'NOCLIENT')
					,	nvl(p_site_id_i,'NOSITE')
					,	p_order_id_i
					,	null
					,	nvl(g_parcel(i).parcel_id,'NOPARCELID')
					,	p_pallet_id_i
					,	p_container_id_i
					,	null
					,	null
					,	null
					,	null
					,	null
					,	g_run_task_key
					,	null
					,	null
					,	cnl_cto_pck.create_zpl_text_label_f( 'An invalid pallet or container type has been used for pallet id '
										  || p_pallet_id_i
										  || ' and container id '
										  || p_container_id_i
										  || '.'
										  )
					,	null
					,	null
					,	null
					,	null
					,	null
					,	sysdate
					,	'Error'
					,	null
					,	g_printers(i).printer_name
					,	g_dws
					,	g_printers(i).copies
					,	base64decode( cnl_cto_pck.create_zpl_text_label_f( 'An invalid pallet or container type has been used for pallet id '
											 	 || p_pallet_id_i
												 || ' and container id '
												 || p_container_id_i
												 || '.'
												 )
							    , 'Y')
					);
				exception
					when others
					then
						cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
										  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
										  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
										  , p_package_name_i		=> g_pck				-- Package name the error occured
										  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
										  , p_routine_parameters_i	=> null					-- list of all parameters involved
										  , p_comments_i		=> 'Error inserting an error label'     -- Additional comments describing the issue
										  );
						-- Add logging
						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
											 , 'Exception check CNL_ERROR'
											 );						
				end;
			end loop;
		else
			-- It is possible that a reprint is requested In that case check if a label already exists and then copy with new run task key and status 'Reprint'.
			if	g_parcel(i).already_labelled = 'Y'
			then
				open	c_existing_label(g_parcel(i).parcel_id);
				fetch	c_existing_label
				into	l_existing_parcel;
				if	c_existing_label%found
				then
					if	l_existing_parcel.status = 'Error'
					then
						update	cnl_cto_ship_labels
						set 	status 		= 'Cancelled'
						where	parcel_id 	= g_parcel(i).parcel_id
						and	status 		= 'Error'
						;

						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
											 , 'This Parcel '
											 || g_parcel(i).parcel_id
											 ||' Already exists but received an error label. Error label is cancelled and a new request will be done for shipment '
											 || g_parcel(i).shipment_id
											 ||'.'
											 );

						cnl_cto_cancel_shp_or_par_pck.cancel_parcel_p	( p_client_id_i		=> g_parcel(i).client_id
												, p_site_id_i		=> g_parcel(i).site_id
												, p_shipment_id_i	=> g_parcel(i).shipment_id
												, p_parcel_id_i		=> g_parcel(i).parcel_id
												, p_canc_shp_if_last_i	=> 'N'
												);

						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
											 , 'Starting cancel parcel routine for parcel '
											 || g_parcel(i).parcel_id
											 ||' from shipment '
											 || g_parcel(i).shipment_id
											 ||'.'
											 );
					else
						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
											 , 'This Parcel '
											 || g_parcel(i).parcel_id
											 ||' Already exists checking if anything has changed to teh parcel details. If not a reprint will be printed else a cancel and recreate for shipment '
											 || g_parcel(i).shipment_id
											 ||'.'
											 );
						if	nvl(g_parcel(i).parcel_height,0)	!= nvl(l_existing_parcel.parcel_height,0)
						or	nvl(g_parcel(i).parcel_depth,0)		!= nvl(l_existing_parcel.parcel_depth,0)
						or	nvl(g_parcel(i).parcel_width,0)		!= nvl(l_existing_parcel.parcel_width,0)
						or	nvl(g_parcel(i).parcel_volume,0)	!= nvl(l_existing_parcel.parcel_volume,0)
						or	nvl(g_parcel(i).parcel_weight,0)	!= nvl(l_existing_parcel.parcel_weight,0)
						or	nvl(g_parcel(i).pallet_type,'X')	!= nvl(l_existing_parcel.pallet_type,'X')
						then
							cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
												 , 'Parcel '
												 || g_parcel(i).parcel_id
												 ||' has received new parcel details like other dims and weights or new package type. Start cancel original parcel and start recreate for '
												 || g_parcel(i).shipment_id
												 ||'.'
												 );
							cnl_sys.cnl_cto_cancel_shp_or_par_pck.cancel_parcel_p	( p_client_id_i		=> g_parcel(i).client_id
														, p_site_id_i		=> g_parcel(i).site_id
														, p_shipment_id_i	=> g_parcel(i).shipment_id
														, p_parcel_id_i		=> g_parcel(i).parcel_id
														, p_canc_shp_if_last_i	=> 'N'
														);
						else
							-- Add logging
							cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
												 , 'This is a reprint. Parcel '
												 || g_parcel(i).parcel_id
												 ||' Already exists. Copy existing label to new print run for shipment '
												 || g_parcel(i).shipment_id
												 ||'.'
												 );
							for i in 1..g_printers.count
							loop
								insert
								into	cnl_sys.cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
												   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
												   , update_dstamp, printer_name, dws, copies, shp_label_base64
												   )
								values
								(	l_existing_parcel.client_id
								,	l_existing_parcel.site_id
								,	l_existing_parcel.order_id
								,	g_shipment_id
								,	l_existing_parcel.parcel_id
								,	l_existing_parcel.pallet_id
								,	l_existing_parcel.container_id
								,	l_existing_parcel.parcel_height
								,	l_existing_parcel.parcel_width
								,	l_existing_parcel.parcel_depth
								,	l_existing_parcel.parcel_volume
								,	l_existing_parcel.parcel_weight
								,	g_run_task_key
								,	l_existing_parcel.pallet_or_container
								,	l_existing_parcel.pallet_type
								,	l_existing_parcel.shp_label
								,	l_existing_parcel.carrier_id
								,	l_existing_parcel.service_level
								,	l_existing_parcel.tracking_number
								,	l_existing_parcel.tracking_url
								,	l_existing_parcel.cto_sscc
								,	sysdate
								,	'Reprint'
								,	null
								,	g_printers(i).printer_name
								,	g_dws
								,	g_printers(i).copies
								, 	base64decode(l_existing_parcel.shp_label,'Y')
								);	
							end loop;

							-- No creation of a new label is required
							l_new_label		:= 'N';
							-- Flag record as reprinted
							g_parcel(i).reprinted	:= 'Y';
							-- Continue loop
							continue;
						end if;	
					end if;
				end if;
				close c_existing_label;
			end if;

			-- If not a reprint continue creating lael
			if	l_new_label = 'Y'
			then
				-- When no data is found exception is raised else continues automatically
				l_retval	:= true;

				if	g_parcel(i).pallet_or_container = 'C'
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Add parcel id for container id '
										 || g_parcel(i).parcel_id
										 ||' to shipment '
										 || g_parcel(i).shipment_id
										 ||'.'
										 );
				else
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Add parcel id for pallet id '
										 || g_parcel(i).parcel_id
										 ||' to shipment '
										 || g_parcel(i).shipment_id
										 ||'.'
										 );
				end if;

				-- Add parcel record to g_par
				g_par	:= g_parcel(i);

				-- fetch parcel attributes
				if	parcel_attributes_f
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_attributes_f returned TRUE.');

					l_parcel.put('attributes',	g_par_atts);
					g_par_atts			:= pljson_list();
				else
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_attributes_f returned FALSE.');
				end if;

				-- parcel height
				if	g_parcel(i).parcel_height is not null
				then
					l_parcel.put('height',		g_parcel(i).parcel_height);
					l_parcel.put('heightUnitOfMeasure','m');
				end if;

				-- pallet depth
				if	g_parcel(i).parcel_depth is not null
				then
					l_parcel.put('length',		g_parcel(i).parcel_depth);
					l_parcel.put('lengthUnitOfMeasure','m');
				end if;

				-- A loading metre corresponds to one linear meter of loading space in a truck. This is used as a calculation unit for goods that cannot be stacked or when stacking of or on top of these goods is not allowed, compensating for any lost volume
				if	g_parcel(i).loadingmeter is not null
				then
					l_parcel.put('loadingMeter',	g_parcel(i).loadingmeter);
				end if;

				-- The net weight of the content without its package, used as a secondary weight for reports or simliar. Typically not communicated to the carrier unless required. Uses same unit of measure as
				if	g_parcel(i).netweight is not null
				then
					l_parcel.put('netWeight',	g_parcel(i).netweight);
				end if;

				-- A unique identifier provided by the source system. Typically a system reference for linking parcels between the system, not a customer reference. The parcel identifier is used in other messages as a key identifier
				if	g_parcel(i).parcel_id is not null
				then
					l_parcel.put('externalParcelIdentifier',	g_parcel(i).parcel_id);
				end if;

				-- Centiro normally generates the tracking number for the shipment, but if the source system generates the tracking number it should be sent here. The tracking number is the carrier tracking number, which is the main barcode on the shipping label and the key identifier in all communication with the carrier
				if	g_parcel(i).trackingnumber is not null
				then
					l_parcel.put('trackingNumber',	g_parcel(i).trackingnumber);
				end if;

				-- The tracking number SSCC is a specific type of tracking range using the SSCC format as specified and provided by GS1 and should only be populated in the cases where the source system is responsible for carrier tracking number generation. https://www.gs1.org/serial-shipping-container-code-sscc
				if	g_parcel(i).trackingnumbersscc is not null
				then
					l_parcel.put('trackingNumberSSCC',	g_parcel(i).trackingnumbersscc);
				end if;

				-- The type of goods sent in the package. This value is normally mandated by the carrier to be present and is expected to be a generalization of the content
				if	g_parcel(i).typeofgoods is not null
				then
					l_parcel.put('typeOfGoods',	g_parcel(i).typeofgoods);
				end if;

				-- The package type used. This value will be translated for carriers that requires it. For example if the package type is a pallet, or half-pallet or if the carrier have specific packaging material that is being used.
				if	g_parcel(i).typeofpackage is not null
				then
					l_parcel.put('typeOfPackage',	g_parcel(i).typeofpackage);
				end if;

				-- The volume of the individual parcel
				if	g_parcel(i).parcel_volume is not null
				then
					l_parcel.put('volume',	g_parcel(i).parcel_volume);
					l_parcel.put('volumeUnitOfMeasure',	'm3');
				end if;

				-- The weight of the individual parcel
				if	g_parcel(i).parcel_weight is not null
				then
					l_parcel.put('weight',	g_parcel(i).parcel_weight);
					l_parcel.put('weightUnitOfMeasure',	'kg');
				end if;

				-- The width of the individual parcel
				if	g_parcel(i).parcel_width is not null
				then
					l_parcel.put('width',	g_parcel(i).parcel_width);
					l_parcel.put('widthUnitOfMeasure',	'm');
				end if;

				-- parcel receiver referremeces
				if	parcel_receiver_references_f
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_receiver_references_f returned TRUE.');

					l_parcel.put('receiverReferences',	g_par_receiver_refs);
					g_par_receiver_refs	:= pljson();
				else
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_receiver_references_f returned FALSE.');
				end if;

				-- parcel sender referremeces
				if	parcel_sender_references_f
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_sender_references_f returned TRUE.');

					l_parcel.put('senderReferences',	g_par_sender_refs);
					g_par_sender_refs	:= pljson();
				else
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_sender_references_f returned FALSE.');
				end if;

				-- parcel sender referremeces
				if 	g_parcel(i).carrier_id != 'RHENUSROAD.NL'
				then
					if	parcel_lines_f		
					then
						-- Add logging
						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_lines_f returned TRUE.');

						l_parcel.put('orderLines',	g_parcel_lines);
						g_parcel_lines			:= pljson_list();
					else
						cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcel_lines_f returned FALSE.');				
					end if;
				else
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'For Carrier Rhenus Road no Parcel lines are added.');				
				end if;
				g_parcels.append(l_parcel);
			end if;
		end if;
	end loop;

	-- Clear for memory
	g_par := null;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcels'
									 );		
	return l_retval;
exception
	when NO_DATA_FOUND
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No ship units found'
							 );
		return l_retval;
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
	return l_retval;
end parcels_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Update WMS and CNL_SYS with reponse data
------------------------------------------------------------------------------------------------
procedure update_wms_p 
is
	l_rtn		varchar2(30) := 'update_wms_p';
	l_raw		raw(32767);
	l_tmp		varchar2(32767);
	l_clob		clob;
	l_blob		blob;
	l_length	number;
	l_pos		integer := 1;
	l_loops		integer := 1;
	l_mod		integer	:= 1;
	l_printer	varchar2(200);
begin
	if 	g_shipment_closed = 'N'
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Decode '
							 || to_char(g_parcel.count)
							 || ' labels from base64.'
							 );

		-- decode base64
		for	i in 1..g_parcel.count
		loop
			if	g_parcel(i).status != 'Error'
			then
				g_parcel(i).shp_label	:= base64decode(	g_parcel(i).shp_label, 'N');
			end if;
		end loop;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Insert labels, tracking numbers, tracking url and other parcel related data into cnl_cto_ship_labels.'
							 );

		-- Save labels to DB
		for i in 1..g_printers.count
		loop
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Start looping printers. Printer found = '
								 || g_printers(i).printer_name
								 || '.'
								 );
			l_printer	:= g_printers(i).printer_name;

			for 	i in 1..g_parcel.count
			loop	
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Inserting label for printer '
									 || l_printer
									 || '. Parameters: '
									 || g_parcel(i).client_id
									 ||', '
									 ||g_parcel(i).site_id
									 ||', '	
									 ||g_parcel(i).parcel_id
									 ||', '
									 ||to_char(g_parcel(i).run_task_key)
									 ||', '
									 ||l_printer
									 );
				insert
				into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
							   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
							   , update_dstamp, printer_name, dws, copies, shp_label_base64
							   )
				values
				(	nvl(g_parcel(i).client_id,'NOCLIENT')
				,	nvl(g_parcel(i).site_id,'NOSITE') 
				,	g_parcel(i).order_id
				,	g_parcel(i).shipment_id
				,	nvl(g_parcel(i).parcel_id,'NOPARCELID')
				,	g_parcel(i).pallet_id
				,	g_parcel(i).container_id
				,	g_parcel(i).parcel_height
				,	g_parcel(i).parcel_width
				,	g_parcel(i).parcel_depth
				,	g_parcel(i).parcel_volume
				,	g_parcel(i).parcel_weight
				,	g_parcel(i).run_task_key
				,	g_parcel(i).pallet_or_container
				,	g_parcel(i).pallet_type
				,	g_parcel(i).shp_label
				,	g_parcel(i).carrier_id
				,	g_parcel(i).service_level
				,	g_parcel(i).tracking_number
				,	g_parcel(i).tracking_url
				,	g_parcel(i).cto_sscc
				,	sysdate
				,	g_parcel(i).status
				,	null
				,	nvl(l_printer,'NOPRINTER')--g_printers(i).printer_name
				,	g_parcel(i).dws
				,	g_printers(i).copies
				,	g_parcel(i).shp_label_base64
				);
			end loop;
		end loop;
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Update order container data with tracking number.'
							 );

		-- Insert tracking number in WMS
		forall i in 1..g_parcel.count
			update	dcsdba.order_container o
			set	o.carrier_consignment_id 	= g_parcel(i).tracking_number
			where	o.client_id 			= g_parcel(i).client_id
			and	(	
				(	g_parcel(i).pallet_or_container 	= 'C'
				and	g_parcel(i).parcel_id 			= o.container_id
				)
				or
				(	g_parcel(i).pallet_or_container 	= 'P'
				and	g_parcel(i).parcel_id 			= o.pallet_id
				)
				)
			and	(	o.order_id 				= g_parcel(i).order_id
				or	g_parcel(i).order_id 			is null
				)
			;
			-- Set tracking number in order?
		for i in 1..g_parcel.count		
		loop
			update_order_master_waybill_p( p_waybill_i		=> g_parcel(i).tracking_number
						     , p_client_id_i 		=> g_parcel(i).client_id
						     , p_shipment_id_i		=> g_parcel(i).shipment_id
						     );
		end loop;
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
end update_wms_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Process addshipment response
------------------------------------------------------------------------------------------------
procedure process_addparcel_response_p( p_json_string_i	in pljson)
is
	cursor c_ord( b_container_id	dcsdba.order_container.container_id%type
		    , b_pallet_id	dcsdba.order_container.pallet_id%type default null
		    )
	is
		select	order_id
		from	dcsdba.order_container
		where	container_id = b_container_id
		and	(pallet_id = b_pallet_id or b_pallet_id is null)
	;

	l_rtn			varchar2(30) 	:= 'process_addparcel_response_p';
	l_label			clob;
	l_url			varchar2(400);
	l_tracking_number	varchar2(50);
	l_sscc			varchar2(50);
	l_parcel_id		varchar2(50);
	l_list_of_labels	pljson_list	:= pljson_list();
	l_pljson_label		pljson		:= pljson();
	l_ready			integer		:= 0;
	l_varchar		varchar2(32767);
	l_transform		clob;
	l_container_id		varchar2(50);
	l_result 		integer;

begin
	if	g_http_response_code	= '200'
	then	
		-- Extract labels
		l_list_of_labels		:= pljson_ext.get_json_list(p_json_string_i, 'labels');

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Found '
							 || to_char(l_list_of_labels.count)
							 || ' labels in response. Start loop true each label.'
							 );

		-- loop true all labels
		for 	i in 1..l_list_of_labels.count
		loop
			-- Fetch label from list
			l_pljson_label		:= pljson(l_list_of_labels.get(i));
			l_label			:= l_pljson_label.get_clob('content');
			l_url			:= pljson_ext.get_string(pljson_ext.get_json(l_pljson_label, 'parcel'), 'parcelTrackingURL');
			l_tracking_number 	:= pljson_ext.get_string(pljson_ext.get_json(l_pljson_label, 'parcel'), 'parcelTrackingNumber');
			l_sscc			:= pljson_ext.get_string(pljson_ext.get_json(l_pljson_label, 'parcel'), 'parcelSSCCNumber');
			l_parcel_id		:= pljson_ext.get_string(pljson_ext.get_json(l_pljson_label, 'parcel'), 'externalParcelIdentifier');
			for 	p in 1..g_parcel.count
			loop
				if	g_parcel(p).parcel_id = l_parcel_id
				then
					g_parcel(p).shp_label			:= l_label;
					g_parcel(p).tracking_number		:= l_tracking_number;
					g_parcel(p).tracking_url		:= l_url;
					g_parcel(p).cto_sscc			:= l_sscc;
					g_parcel(p).status			:= 'Created';
					g_parcel(p).shp_label_base64		:= l_label;
				end if;
			end loop;		
		end loop;

		for 	p in 1..g_parcel.count
		loop	
			if	nvl(l_container_id,'X') != g_parcel(p).container_id 
			then
				l_container_id := g_parcel(p).container_id;

				if 	g_parcel(p).order_id is null
				and	l_container_id is not null
				then
					open	c_ord( l_container_id
						     , g_parcel(p).pallet_id
						     );
					fetch 	c_ord
					into	g_parcel(p).order_id;
					close	c_ord;
				end if;

				if	nvl(g_dws,'N') != 'Y'
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Create run task for container label for container '
										 || l_container_id
										 || ' from order '
										 || g_parcel(p).order_id
										 || '.'
										 );

					l_result := dcsdba.libruntask.createruntask( stationid             => g_station_id
										   , userid                => g_user_id
										   , commandtext           => '"SSV_PLT_CON" "lp" "J" "1" '
													   || '"site_id" "'        || g_parcel(p).site_id
													   || '" "client_id" "'    || g_parcel(p).client_id
													   || '" "order_id" "'     || g_parcel(p).order_id
													   || '" "container_id" "' || g_parcel(p).container_id
													   || '"'
										   , nametext              => 'UREPSSVPLTCON'
										   , siteid                => g_parcel(p).site_id
										   , tmplanguage           => 'EN_GB'
										   , p_javareport          => 'Y'
										   , p_archive             => 'N'
										   , p_runlight            => null
										   , p_serverinstance      => null
										   , p_priority            => null
										   , p_timezonename        => 'Europe/Amsterdam'
										   , p_archiveignorescreen => null
										   , p_archiverestrictuser => null
										   , p_clientid            => g_parcel(p).client_id
										   , p_emailrecipients     => null
										   , p_masterkey           => null
										   , p_usedbtimezone       => 'N'
										   , p_nlscalendar         => 'Gregorian'
										   , p_emailattachment     => null
										   , p_emailsubject        => null
										   , p_emailmessage        => null
										   );
				end if;
			end if;
		end loop;
		l_container_id := null;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Process response finished.'
							 );

	else	
		if	pljson_ext.get_string(p_json_string_i, 'message') = 'Shipment is closed, cannot add parcel(s)'
		then
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Shipment already closed. A new shipment must be created.'
								 );
			g_shipment_closed := 'Y';
		else
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Error reponse. Create error label text.'
								 );

			if 	pljson_ext.get_string(p_json_string_i, 'message') = 'The request is invalid.'
			then
				begin
					-- Create error label
					l_label := cnl_cto_pck.create_zpl_text_label_f( 'Centiro web service call error Message: '
										     || p_json_string_i.to_char( false )
										     )
										     ;				
				exception
					when others 
					then
						-- Create error label
						l_label := cnl_cto_pck.create_zpl_text_label_f( 'Centiro web service call error Message: The request is invalid and error text not available');
				end;

			else
				-- Create error label
				l_label := cnl_cto_pck.create_zpl_text_label_f( 'Centiro web service call error Message: '
									     || pljson_ext.get_string(p_json_string_i, 'message')
									     );
			end if;

			for 	p in 1..g_parcel.count
			loop
				g_parcel(p).shp_label			:= l_label;
				g_parcel(p).status			:= 'Error';
				g_parcel(p).shp_label_base64		:= base64decode(l_label, 'Y');
			end loop;	

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Process response finished.'
								 );
		end if;
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
end process_addparcel_response_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : add parcels. If parcel id is not filled shipment id must be provided and all parcels will be added
------------------------------------------------------------------------------------------------
procedure add_print_parcels_p( p_site_id_i	in dcsdba.site.site_id%type
			     , p_client_id_i    in dcsdba.client.client_id%type
			     , p_order_id_i	in dcsdba.order_header.order_id%type 		default null
			     , p_pallet_id_i	in dcsdba.order_container.pallet_id%type	default null
			     , p_container_id_i	in dcsdba.order_container.container_id%type	default null
			     , p_shipment_id_i	in dcsdba.order_header.uploaded_ws2pc_id%type	default null
			     , p_rtk_key_i	in dcsdba.run_task.key%type
			     , p_printer_i      in varchar2
			     , p_copies_i       in varchar2--number 
			     , p_dws_i		in varchar2
			     , p_station_id_i	in dcsdba.workstation.station_id%type
			     , p_shp_closed_o	out varchar2
			     )
is	
	cursor c_usr
	is
		select 	user_id 
		from	dcsdba.run_task
		where	key = p_rtk_key_i
	;

	l_url			varchar2(1000)	:= cnl_util_pck.get_constant('CTO_ADDPARCEL_WEBSERVICE_URL');
	l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_wallet		varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw		varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');

	l_body_request		pljson		:= pljson();
	l_response_code		varchar2(30);
	l_response_reason	varchar2(4000);
	l_body_response		pljson		:= pljson();
	l_parcels		pljson_list	:= pljson_list();
	l_trace_key		integer;
	l_rtn			varchar2(30) 	:= 'add_print_parcels_p';
	l_single		integer 	:= 1; -- 1 is singel parcel 0 is multi parcel
	l_printers		varchar2(200);
	l_copies		varchar2(50);
	--
	e_no_printer		exception;
	e_no_shipment_id	exception;
	pragma			exception_init(e_no_printer, -20001);
	pragma			exception_init(e_no_shipment_id, -20002);
begin
	-- raise exception when we can't print
	if	p_printer_i 	is null
	and	p_dws_i		= 'N'
	then		
		raise_application_error(-20001,'No printer could be found');
		-- Raise exception we can't print
	end if;

	-- Raise error when we don't have the shipment id	
	if	p_shipment_id_i	is null
	then
		raise_application_error(-20002,'No shipment id could be found');	
		--raise e_no_shipment_id;
		-- Create error label
	end if;

	-- Fetch user id for container label
	open	c_usr;
	fetch 	c_usr
	into	g_user_id;
	close	c_usr;

	-- Fetch all already existing parcels for shipment to check if one has been replaced by a new one.
	begin
		select	client_id
		,	site_id
		,	order_id
		,	shipment_id
		,	parcel_id
		,	pallet_id
		,	container_id
		,	pallet_or_container
		,	nvl(delete_parcel,'Y') delete_parcel
		bulk 	collect 
		into	g_old_parcels
		from	(
			select	l.client_id
			, 	l.site_id
			, 	l.order_id
			, 	l.shipment_id
			, 	l.parcel_id
			, 	l.pallet_id
			, 	l.container_id
			,	l.pallet_or_container
			,	decode(o.labelled,'Y','N',decode(o.pallet_labelled,'Y','N',decode(m.labelled,'Y','N',decode(m.pallet_labelled,'Y','N','Y')))) delete_parcel
			from	cnl_sys.cnl_cto_ship_labels l
			left	
			join	dcsdba.order_container o
			on	o.client_id	= l.client_id
			and	(	(	o.container_id		= l.parcel_id
					and	l.pallet_or_container 	= 'C'
					)
				or	(	o.pallet_id		= l.parcel_id
					and	l.pallet_or_container 	= 'P'
					)
				)
			left	
			join	dcsdba.shipping_manifest m
			on	m.client_id	= l.client_id
			and	m.site_id	= l.site_id
			and	(	(	m.container_id		= l.parcel_id
					and	l.pallet_or_container 	= 'C'
					)
				or	(	m.pallet_id		= l.parcel_id
					and	l.pallet_or_container 	= 'P'
					)
				)
			where	l.shipment_id 	= p_shipment_id_i
			and	l.status 	!= 'Cancelled'
--			and	l.status 	!= 'Error'
			)
		;
	exception
		when NO_DATA_FOUND
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'There are no parcels to Cancel for shipment '
								 || to_char(p_shipment_id_i)
								 || '.'
								 );
		when others
		then
			null;
	end;	

	-- Check if old parcels must be cancelled
	for 	i in 1..g_old_parcels.count
	loop	
		if	g_old_parcels(i).delete_parcel = 'Y'
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Parcel '
								 || g_old_parcels(i).parcel_id
								 || ' from shipment '
								 || to_char(p_shipment_id_i)
								 || ' no longer exist. Start canceling parcel.'
								 );
			-- Cancel parcel
			cnl_sys.cnl_cto_cancel_shp_or_par_pck.cancel_parcel_p( p_client_id_i		=> g_old_parcels(i).client_id
									     , p_site_id_i		=> g_old_parcels(i).site_id
									     , p_shipment_id_i		=> g_old_parcels(i).shipment_id
									     , p_parcel_id_i		=> g_old_parcels(i).parcel_id
									     , p_canc_shp_if_last_i	=> 'N'
									     );
			update	cnl_cto_ship_labels
			set	status 		= 'Cancelled'
			where	client_id 	= g_old_parcels(i).client_id
			and	site_id		= g_old_parcels(i).site_id
			and	shipment_id 	= g_old_parcels(i).shipment_id
			and	parcel_id	= g_old_parcels(i).parcel_id
			;
		end if;
	end loop;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start adding parcel(s) for '
						 || 'p_site_id_i => "'|| p_site_id_i
						 || '", p_client_id_i => "'|| p_client_id_i
						 || '", p_order_id_i => "'|| p_order_id_i
						 || '", p_pallet_id => "'|| p_pallet_id_i
						 || '", p_container_id_i => "'||p_container_id_i
						 || '", p_shipment_id_i	=> "'||p_shipment_id_i
						 || '", p_printer_i => "'||p_printer_i
						 || '", p_copies_i => "'||p_copies_i
						 ||'".'
						 );

	-- create table of printers
	l_printers 	:= replace(p_printer_i,' ');
	l_copies	:= replace(p_copies_i,' ');

	if	l_printers is null
	and	p_dws_i = 'Y'
	then
		l_printers	:= 'DWSPRINTER';
		l_copies	:= 1;
	end if;

	while	l_printers is not null
	loop
		if 	instr(l_printers,',') > 0
		then
			g_printers.extend;
			g_printers(g_printers.count).printer_name	:= substr(l_printers,1,instr(l_printers,',')-1);
			g_printers(g_printers.count).copies		:= to_number(substr(l_copies,1,instr(l_copies,',')-1));

			l_printers	:= substr(l_printers,instr(l_printers,',')+1);
			l_copies 	:= substr(l_copies,instr(l_copies,',')+1);
		else
			g_printers.extend;
			g_printers(g_printers.count).printer_name	:= l_printers;
			g_printers(g_printers.count).copies		:= l_copies;

			l_printers 	:= null;
			l_copies 	:= null;
		end if;
	end loop;

	g_shipment_id	:= p_shipment_id_i;
	g_run_task_key	:= p_rtk_key_i;
	g_dws		:= p_dws_i;
	g_station_id	:= p_station_id_i;

	-- Identifier for shipment, communicated with Centiro
	l_body_request.put('shipmentIdentifier',	to_char(g_shipment_id));

	-- Shipment type, used in combination with identifier if not unique
	if	g_shipment_type is not null
	then
		l_body_request.put('shipmentType',	g_shipment_type);
	end if;

	-- parcels
	if	parcels_f( p_site_id_i		=> p_site_id_i
			 , p_client_id_i    	=> p_client_id_i
			 , p_order_id_i		=> p_order_id_i
			 , p_pallet_id_i	=> p_pallet_id_i
			 , p_container_id_i	=> p_container_id_i
			 )
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn, 'Function parcels_f returned TRUE.');

		l_body_request.put('parcels',			g_parcels);
		g_parcels					:= pljson_list();

		-- Type of label
		if	g_printer_type is not null
		then
			l_body_request.put('printerType',		g_printer_type);
		end if;

		-- add web service tace
		cnl_cto_pck.create_cto_trace_record( l_body_request, null, null, l_rtn, null, l_trace_key);

		-- add monitoring log record
		cnl_sys.cnl_cto_pck.print_monitoring_log_record( p_run_task_key_i			=> g_run_task_key
					   , p_add_or_update_i			=> 'U'
					   , p_parcel_id_i			=> null
					   , p_shipment_id_i			=> null
					   , p_order_id_i			=> null
					   , p_client_id_i			=> null
					   , p_run_task_creation_i		=> null
					   , p_procedure_start_i		=> null
					   , p_parcel_details_fetched_i		=> null
					   , p_call_webservice_i		=> sysdate
					   , p_webservice_response_i		=> null
					   , p_update_wms_i			=> null
					   , p_send_to_printer_i		=> null
					   , p_finished_i			=> null
					   );		

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

		-- add monitoring log record
		cnl_sys.cnl_cto_pck.print_monitoring_log_record( p_run_task_key_i			=> g_run_task_key
					   , p_add_or_update_i			=> 'U'
					   , p_parcel_id_i			=> null
					   , p_shipment_id_i			=> null
					   , p_order_id_i			=> null
					   , p_client_id_i			=> null
					   , p_run_task_creation_i		=> null
					   , p_procedure_start_i		=> null
					   , p_parcel_details_fetched_i		=> null
					   , p_call_webservice_i		=> null
					   , p_webservice_response_i		=> sysdate
					   , p_update_wms_i			=> null
					   , p_send_to_printer_i		=> null
					   , p_finished_i			=> null
					   );		
		-- Process response
		process_addparcel_response_p( l_body_response);

		-- add monitoring log record
		cnl_sys.cnl_cto_pck.print_monitoring_log_record( p_run_task_key_i			=> g_run_task_key
					   , p_add_or_update_i			=> 'U'
					   , p_parcel_id_i			=> null
					   , p_shipment_id_i			=> null
					   , p_order_id_i			=> null
					   , p_client_id_i			=> null
					   , p_run_task_creation_i		=> null
					   , p_procedure_start_i		=> null
					   , p_parcel_details_fetched_i		=> null
					   , p_call_webservice_i		=> null
					   , p_webservice_response_i		=> null
					   , p_update_wms_i			=> sysdate
					   , p_send_to_printer_i		=> null
					   , p_finished_i			=> null
					   );		

		-- Update tables
		update_wms_p;
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No parcel or other parcels found. No web servcie call required.'
							 );
	end if;
	p_shp_closed_o	:= g_shipment_closed;

exception
	when e_no_shipment_id
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No shipment id for '
							 || 'p_site_id_i => "'|| p_site_id_i
							 || '", p_client_id_i => "'|| p_client_id_i
							 || '", p_order_id_i => "'|| p_order_id_i
							 || '", p_pallet_id => "'|| p_pallet_id_i
							 || '", p_container_id_i => "'||p_container_id_i
							 || '", p_shipment_id_i	=> "'||p_shipment_id_i
							 || '", p_run_task_key_i => "'||p_rtk_key_i
							 || '", p_printer_i => "'||p_printer_i
							 || '", p_copies_i => "'||p_copies_i
							 ||'".'
							 );
		for 	i in 1..g_printers.count
		loop
			insert
			into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
						   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
						   , update_dstamp, printer_name, dws, copies, shp_label_base64
						   )
			values
			(	nvl(p_client_id_i,'NOCLIENT')
			,	nvl(p_site_id_i,'NOSITE')
			,	p_order_id_i
			,	null
			,	'NOPARCELID'
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
			,	cnl_cto_pck.create_zpl_text_label_f( 'Order '
								  || nvl(p_order_id_i,'s')
								  || ' from client '
								  || p_client_id_i
								  || ' on pallet '
								  || p_pallet_id_i
								  || ' and/or container id '
								  || p_container_id_i
								  || ' does not have a centiro shipment id'
								  )
			,	null
			,	null
			,	null
			,	null
			,	null
			,	sysdate
			,	'Error'
			,	null
			,	nvl(g_printers(i).printer_name,'NOPRINTERID')
			,	p_dws_i
			,	g_printers(i).copies
			,	base64decode( cnl_cto_pck.create_zpl_text_label_f( 'Order '
										 || nvl(p_order_id_i,'s')
										 || ' from client '
										 || p_client_id_i
										 || ' on pallet '
										 || p_pallet_id_i
										 || ' and/or container id '
										 || p_container_id_i
										 || ' does not have a centiro shipment id'
										 )
					   , 'Y')
			);
		end loop;
	when e_no_printer
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No printer for '
							 || 'p_site_id_i => "'|| p_site_id_i
							 || '", p_client_id_i => "'|| p_client_id_i
							 || '", p_order_id_i => "'|| p_order_id_i
							 || '", p_pallet_id => "'|| p_pallet_id_i
							 || '", p_container_id_i => "'||p_container_id_i
							 || '", p_shipment_id_i	=> "'||p_shipment_id_i
							 || '", p_run_task_key_i => "'||p_rtk_key_i
							 || '", p_printer_i => "'||p_printer_i
							 || '", p_copies_i => "'||p_copies_i
							 ||'".'
							 );
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> g_pck				-- Package name the error occured
						  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> null					-- list of all parameters involved
						  , p_comments_i		=> 'A printer is mandatory for printing a shipping label' -- Additional comments describing the issue
						  );
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

end add_print_parcels_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : initialization
------------------------------------------------------------------------------------------------
begin
	null;
end cnl_cto_parcel_pck;