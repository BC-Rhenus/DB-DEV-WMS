CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CTO_ADDSHIPMENT_PCK" 
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
	g_pck		varchar2(30) 			:= 'cnl_cto_addshipment_pck';

	-- Order record type.
	type g_ord_rec 	is record( from_site_id			dcsdba.order_header.from_site_id%type
				 , client_id			dcsdba.order_header.client_id%type
				 , order_id			dcsdba.order_header.order_id%type
				 , order_type			dcsdba.order_header.order_type%type
				 , order_reference		dcsdba.order_header.order_reference%type
				 , purchase_order		dcsdba.order_header.purchase_order%type
				 , shipment_id			dcsdba.order_header.uploaded_ws2pc_id%type
				 , mpack_consignment		dcsdba.order_header.mpack_consignment%type
				 , status			dcsdba.order_header.status%type

				 , status_reason_code		dcsdba.order_header.status_reason_code%type
				 , instructions			dcsdba.order_header.instructions%type
				 , saved_instructions		dcsdba.order_header.instructions%type
				 , new_instructions		dcsdba.order_header.instructions%type
				 , order_volume			dcsdba.order_header.order_volume%type
				 , order_weight			dcsdba.order_header.order_weight%type

				 , dispatch_method		dcsdba.order_header.dispatch_method%type
				 , user_def_type_1		dcsdba.order_header.user_def_type_1%type
				 , carrier_id			dcsdba.order_header.carrier_id%type
				 , service_level		dcsdba.order_header.service_level%type
				 , delivery_point		dcsdba.order_header.delivery_point%type

				 , creation_date		dcsdba.order_header.creation_date%type
				 , order_date			dcsdba.order_header.order_date%type
				 , ship_by_date			dcsdba.order_header.ship_by_date%type
				 , deliver_by_date		dcsdba.order_header.deliver_by_date%type

				 , export			dcsdba.order_header.export%type
				 , tod				dcsdba.order_header.tod%type
				 , tod_place			dcsdba.order_header.tod_place%type

				 , freight_charges		dcsdba.order_header.freight_charges%type
				 , inv_total_1			dcsdba.order_header.inv_total_1%type
				 , inv_currency			dcsdba.order_header.inv_currency%type
				 , cod				dcsdba.order_header.cod%type
				 , cod_value			dcsdba.order_header.cod_value%type
				 , cod_currency			dcsdba.order_header.cod_currency%type
				 , cod_type			dcsdba.order_header.cod_type%type

				 , seller_name			dcsdba.order_header.seller_name%type
				 , seller_phone			dcsdba.order_header.seller_phone%type
				 , ce_eu_type			dcsdba.country.ce_eu_type%type
				 , freight_terms		dcsdba.order_header.freight_terms%type 	--Transport_account 
				 , letter_of_credit		dcsdba.order_header.letter_of_credit%type --taxduties_account

				 , del_customer_id		dcsdba.order_header.customer_id%type
				 , del_vat_number		dcsdba.order_header.vat_number%type
				 , del_contact			dcsdba.order_header.contact%type
				 , del_contact_phone		dcsdba.order_header.contact_phone%type
				 , del_contact_mobile		dcsdba.order_header.contact_mobile%type
				 , del_contact_fax		dcsdba.order_header.contact_fax%type
				 , del_contact_email		dcsdba.order_header.contact_email%type
				 , del_name			dcsdba.order_header.name%type
				 , del_address1			dcsdba.order_header.address1%type
				 , del_address2			dcsdba.order_header.address2%type
				 , del_town			dcsdba.order_header.town%type
				 , del_county			dcsdba.order_header.county%type
				 , del_postcode			dcsdba.order_header.postcode%type
				 , del_country			dcsdba.order_header.country%type

				 , hub_address_id		dcsdba.order_header.hub_address_id%type
				 , hub_carrier_id		dcsdba.order_header.hub_carrier_id%type
				 , hub_service_level		dcsdba.order_header.hub_service_level%type
				 , hub_vat_number		dcsdba.order_header.hub_vat_number%type
				 , hub_contact			dcsdba.order_header.hub_contact%type
				 , hub_contact_phone		dcsdba.order_header.hub_contact_phone%type
				 , hub_contact_mobile		dcsdba.order_header.hub_contact_mobile%type
				 , hub_contact_fax		dcsdba.order_header.hub_contact_fax%type
				 , hub_contact_email		dcsdba.order_header.hub_contact_email%type
				 , hub_name			dcsdba.order_header.hub_name%type
				 , hub_address1			dcsdba.order_header.hub_address1%type
				 , hub_address2			dcsdba.order_header.hub_address2%type
				 , hub_town			dcsdba.order_header.hub_town%type
				 , hub_county			dcsdba.order_header.hub_county%type
				 , hub_postcode			dcsdba.order_header.hub_postcode%type
				 , hub_country			dcsdba.order_header.hub_country%type

				 , rd_address_id		dcsdba.address.address_id%type
				 , rd_vat_number		dcsdba.address.vat_number%type
				 , rd_contact			dcsdba.address.contact%type
				 , rd_contact_phone		dcsdba.address.contact_phone%type
				 , rd_contact_mobile		dcsdba.address.contact_mobile%type
				 , rd_contact_fax		dcsdba.address.contact_fax%type
				 , rd_contact_email		dcsdba.address.contact_email%type
				 , rd_name			dcsdba.address.name%type
				 , rd_address1			dcsdba.address.address1%type
				 , rd_address2			dcsdba.address.address2%type
				 , rd_town			dcsdba.address.town%type
				 , rd_county			dcsdba.address.county%type
				 , rd_postcode			dcsdba.address.postcode%type
				 , rd_country			dcsdba.address.country%type

				 , huv_address_id		dcsdba.address.address_id%type
				 , huv_vat_number		dcsdba.address.vat_number%type
				 , huv_contact			dcsdba.address.contact%type
				 , huv_contact_phone		dcsdba.address.contact_phone%type
				 , huv_contact_mobile		dcsdba.address.contact_mobile%type
				 , huv_contact_fax		dcsdba.address.contact_fax%type
				 , huv_contact_email		dcsdba.address.contact_email%type
				 , huv_name			dcsdba.address.name%type
				 , huv_address1			dcsdba.address.address1%type
				 , huv_address2			dcsdba.address.address2%type
				 , huv_town			dcsdba.address.town%type
				 , huv_county			dcsdba.address.county%type
				 , huv_postcode			dcsdba.address.postcode%type
				 , huv_country			dcsdba.address.country%type

				 , inv_address_id		dcsdba.order_header.hub_address_id%type
				 , inv_vat_number		dcsdba.order_header.hub_vat_number%type
				 , inv_contact			dcsdba.order_header.hub_contact%type
				 , inv_contact_phone		dcsdba.order_header.hub_contact_phone%type
				 , inv_contact_mobile		dcsdba.order_header.hub_contact_mobile%type
				 , inv_contact_fax		dcsdba.order_header.hub_contact_fax%type
				 , inv_contact_email		dcsdba.order_header.hub_contact_email%type
				 , inv_name			dcsdba.order_header.hub_name%type
				 , inv_address1			dcsdba.order_header.hub_address1%type
				 , inv_address2			dcsdba.order_header.hub_address2%type
				 , inv_town			dcsdba.order_header.hub_town%type
				 , inv_county			dcsdba.order_header.hub_county%type
				 , inv_postcode			dcsdba.order_header.hub_postcode%type
				 , inv_country			dcsdba.order_header.hub_country%type

				 , sid_address_id		dcsdba.address.address_id%type
				 , sid_vat_number		dcsdba.address.vat_number%type
				 , sid_contact			dcsdba.address.contact%type
				 , sid_contact_phone		dcsdba.address.contact_phone%type
				 , sid_contact_mobile		dcsdba.address.contact_mobile%type
				 , sid_contact_fax		dcsdba.address.contact_fax%type
				 , sid_contact_email		dcsdba.address.contact_email%type
				 , sid_name			dcsdba.address.name%type
				 , sid_address1			dcsdba.address.address1%type
				 , sid_address2			dcsdba.address.address2%type
				 , sid_town			dcsdba.address.town%type
				 , sid_county			dcsdba.address.county%type
				 , sid_postcode			dcsdba.address.postcode%type
				 , sid_country			dcsdba.address.country%type

				 , cid_address_id		dcsdba.address.address_id%type
				 , cid_vat_number		dcsdba.address.vat_number%type
				 , cid_contact			dcsdba.address.contact%type
				 , cid_contact_phone		dcsdba.address.contact_phone%type
				 , cid_contact_mobile		dcsdba.address.contact_mobile%type
				 , cid_contact_fax		dcsdba.address.contact_fax%type
				 , cid_contact_email		dcsdba.address.contact_email%type
				 , cid_name			dcsdba.address.name%type
				 , cid_address1			dcsdba.address.address1%type
				 , cid_address2			dcsdba.address.address2%type
				 , cid_town			dcsdba.address.town%type
				 , cid_county			dcsdba.address.county%type
				 , cid_postcode			dcsdba.address.postcode%type
				 , cid_country			dcsdba.address.country%type

				 , http_response_code		varchar2(30)
				 , http_response_reason		varchar2(2000)
				 , cancel_cto_shipment		varchar2(1)
				 , consol_order			varchar(1)
				 , consol_id			varchar2(20)
				 , nacex_copies			dcsdba.order_header.tax_amount_5%type
				 , has_error			varchar2(1)
				 , addons			varchar2(400)
				 );
	-- Table with order record types
	type g_ord_tab 	is table of g_ord_rec;
	-- Table 
	g_ord			g_ord_tab	:= g_ord_tab();
	-- order record
	g_order_header		g_ord_rec;

	-- Order lines
	type g_orl_rec is record( countryoforigin		varchar2(10)
	                        , itemdescription1		dcsdba.sku.description%type
	                        , itemdescription2		dcsdba.sku.description%type -- customer_sku_desc1
				, line_id			dcsdba.order_line.line_id%type
				, productnumber			dcsdba.sku.sku_id%type
				, quantityshipped		dcsdba.order_line.qty_shipped%type
				, statisticalnumber		dcsdba.sku.commodity_code%type
				, totalvolume			dcsdba.sku.each_volume%type
				, totalvolumeunitofmeasure	varchar2(10)
				, totalweight			dcsdba.sku.each_weight%type
				, totalweightunitofmeasure	varchar2(10)
				, totalvalue			dcsdba.order_line.line_value%type
				, currency			dcsdba.order_line.product_currency%type
				, unitvalue 			dcsdba.order_line.product_price%type
				, unitvolume 			dcsdba.sku.each_volume%type
				, unitvolumeunitofmeasure	varchar2(10)
				, unitweight 			dcsdba.sku.each_weight%type
				, unitweightunitofmeasure	varchar2(10)
				, unitmeasure			dcsdba.sku_config.track_level_1%type
				, hazmat_id			dcsdba.sku.hazmat_id%type
				, hazmat_notes			dcsdba.hazmat.notes%type
				, hazmat_regulation_id		dcsdba.hazmat_regulation_notes.regulation_id%type
				, hrn_r_sentence		dcsdba.hazmat_regulation_notes.r_sentence%type
				, hrn_proper_shipping_name	dcsdba.hazmat_regulation_notes.proper_shipping_name%type
				, hrn_additional_shipping_name	dcsdba.hazmat_regulation_notes.additional_shipping_name%type
				, hr_notes			dcsdba.hazmat_regulation.notes%type
				, hazmat_class			dcsdba.sku_hazmat_reg.hazmat_class%type
				, hazmat_subclass		dcsdba.sku_hazmat_reg.hazmat_subclass%type
				);
	type g_orl_tab is table of g_orl_rec;
	g_orl			g_orl_tab	:= g_orl_tab();

	g_line			g_orl_rec;
	-- Parcels
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

	g_database		varchar2(10)	:= cnl_cto_pck.fetch_database_f;
	g_cs_force		varchar2(1) 	:= 'N';
	g_shipment		pljson		:= pljson();
	g_shp_sender_refs	pljson		:= pljson();
	g_shp_receiver_refs	pljson		:= pljson();
	g_parcel_atts		pljson_list	:= pljson_list();
	g_par_sender_refs	pljson		:= pljson();
	g_par_receiver_refs	pljson		:= pljson();
	g_par_line_atts		pljson_list	:= pljson_list();
	g_par_lines		pljson_list	:= pljson_list();
	g_shp_parcels		pljson_list	:= pljson_list();
	g_shp_lines		pljson_list	:= pljson_list();
	g_shp_line_atts		pljson_list	:= pljson_list();
	g_shp_custom_dates	pljson_list	:= pljson_list();
	g_shp_dates		pljson		:= pljson();
	g_shp_customs		pljson		:= pljson();
	g_shp_car_serv_val	pljson_list	:= pljson_list();
	g_shp_car_atts		pljson_list	:= pljson_list();
	g_shp_atts		pljson_list	:= pljson_list();
	g_shp_addresses		pljson_list	:= pljson_list();
	g_shp_closed		varchar2(1)	:= 'N';
	g_dif_carrier		varchar2(1)	:= 'N';
	g_dif_carrier_id	varchar2(50);
	g_dif_service_lvl	varchar2(50);
	g_ok			varchar2(1)	:= 'Y';

	-- Used for forced carrier update 
	g_carrier_update	varchar2(1)	:= 'N';
	g_client_id		dcsdba.client.client_id%type;
	g_old_shipment_id	dcsdba.order_header.uploaded_ws2pc_id%type;
	g_printer		varchar2(100);
	g_order_id		dcsdba.order_header.order_id%type;
	g_rtk_key		dcsdba.run_task.key%type;

	-- New IATA regulation BDS-5898
	g_iata			varchar2(1);
	g_iata_hazmat		dcsdba.hazmat.hazmat_id%type;
	g_iata_sku		dcsdba.sku.sku_id%type;

--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 10/Jun/2022
-- Purpose : Add accessory
------------------------------------------------------------------------------------------------
procedure add_order_accesorials_p( p_addons_i		in varchar2
				 , p_client_id_i	in varchar2
				 , p_order_id_i		in varchar2
				 )
is
	type l_addon_rec 	is record ( addon varchar2(50));
	type l_addon_tab	is table of l_addon_rec;
	l_addons		l_addon_tab  := l_addon_tab();
	l_addon			l_addon_rec;

	l_rtn		varchar2(30) := 'add_order_accesorials_p';
	l_string	varchar2(400);
	l_chk		number := 0;
begin
	l_string := upper(p_addons_i);

	<<addon_loop>>
	loop
		-- exit loop when no more value
		if 	length(l_string) = 0
		or	l_string is null
		or 	l_string in (';',';;')
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'No more addons to add for '
								 || p_order_id_i
								 || ' from client '
								 || p_client_id_i
								 || '.'
								 );
			exit addon_loop;
		end if;

		-- Only one (more) value
		if	instr(l_string,';') = 0
		then
			l_addons.extend;
			l_addons(l_addons.count).addon		:= l_string;
			exit addon_loop;			
		-- Multple values
		else
			l_addons.extend;
			l_addons(l_addons.count).addon		:= substr(l_string,1,instr(l_string,';')-1);
			l_string := substr(l_string,instr(l_string,';')+1);
		end if;
	end loop;
	-- check if addon already exist if not add it to WMS
	for i in 1..l_addons.count
	loop
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Check if addon '
							 || l_addons(i).addon
							 || ' already exists for order '
							 || p_order_id_i
							 || ' from client '
							 || p_client_id_i
							 || '.'
							 );

		select	count(*)
		into	l_chk
		from	dcsdba.order_accessory
		where	client_id 	= p_client_id_i
		and	order_id 	= p_order_id_i
		and	accessorial 	= l_addons(i).addon
		;

		if	l_chk = 0
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Addon '
								 || l_addons(i).addon
								 || ' does not exist so now adding it to WMS ordre accessorial for '
								 || p_order_id_i
								 || ' from client '
								 || p_client_id_i
								 || '.'
								 );

			-- add accesoorial to WMS
			insert
			into	dcsdba.order_accessory
			values	
			(	p_client_id_i
			,	p_order_id_i
			,	l_addons(i).addon
			);
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
						  , p_routine_parameters_i	=> p_order_id_i||' '||p_client_id_i||' '||p_addons_i					-- list of all parameters involved
						  , p_comments_i		=> 'error adding order accesorials.'
						  );
end add_order_accesorials_p;

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
-- Author  : M. Swinkels 31/08/2021
-- Purpose : Check for existing parcels
------------------------------------------------------------------------------------------------
function parcels_exist_f( p_shipment_id_i	dcsdba.order_header.uploaded_ws2pc_id%type
			, p_site_id_i		dcsdba.site.site_id%type
			, p_client_id_i		dcsdba.client.client_id%type
			)
	return integer
is
	cursor c_parcels
	is
		select	(	select	count(*)
				from	dcsdba.order_container o
				where	(	o.labelled		= 'Y'
					or	o.pallet_labelled	= 'Y'
					)
				and	o.order_id	in (	select	r.order_id
								from	dcsdba.order_header r
								where	r.order_id = o.order_id
								and	r.uploaded_ws2pc_id	= p_shipment_id_i
								and	r.from_site_id		= p_site_id_i
								and	r.client_id		= p_client_id_i
							   )
			)
		+
			(	select	count(*)
				from	dcsdba.shipping_manifest m
				where	(	m.labelled		= 'Y'
					or	m.pallet_labelled	= 'Y'
					)
				and	m.order_id	in (	select	r.order_id
								from	dcsdba.order_header r
								where	r.order_id = m.order_id
								and	r.uploaded_ws2pc_id	= p_shipment_id_i
								and	r.from_site_id		= p_site_id_i
								and	r.client_id		= p_client_id_i
							   )
			)
		from	dual
	;

	r_prc	integer;
	l_rtn	varchar2(30) := 'parcels_exist_f';
begin
	open	c_parcels;
	fetch	c_parcels
	into	r_prc;
	close	c_parcels;
	if	r_prc > 0
	then
		return 1;
	else
		return 0;
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
		Return 0;
end parcels_exist_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 25/04/2021
-- Purpose : Fetch orders for add shipment
------------------------------------------------------------------------------------------------
procedure error_checks_p( p_routine_i	in varchar2)
is
	l_rtn		varchar2(30) 	:= 'error_checks_p';
begin

	for i in 1..g_ord.count
	loop
		if	p_routine_i = 'add_shipment_p'
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Start checking for errors for order id '||g_ord(i).order_id||'.'
								 );

			if	g_ord(i).del_name is null
			then
				g_ord(i).has_error 			:= 'Y';
				g_ord(i).new_instructions 		:= 'CTOMSG# Delivery address name is required';
				g_ord(i).status_reason_code		:= 'CSERROR';
				if	g_cs_force = 'N'
				then
					g_ord(i).carrier_id		:= 'ERROR';
					g_ord(i).service_level		:= 'ERROR';
				end if;
			elsif	g_ord(i).del_address1 is null
			then
				g_ord(i).has_error := 'Y';
				g_ord(i).new_instructions := 'CTOMSG# Delivery address street name is required in address 1';
				g_ord(i).status_reason_code	:= 'CSERROR';			
				if	g_cs_force = 'N'
				then
					g_ord(i).carrier_id		:= 'ERROR';
					g_ord(i).service_level		:= 'ERROR';
				end if;
			elsif	g_ord(i).del_town is null
			then
				g_ord(i).has_error := 'Y';
				g_ord(i).new_instructions := 'CTOMSG# Delivery address town is required';
				g_ord(i).status_reason_code	:= 'CSERROR';			
				if	g_cs_force = 'N'
				then
					g_ord(i).carrier_id		:= 'ERROR';
					g_ord(i).service_level		:= 'ERROR';
				end if;
			end if;	

			if	g_ord(i).hub_carrier_id = 'OLH'
			then
				if	g_ord(i).hub_town is null
				then
					g_ord(i).has_error		:= 'Y';
					g_ord(i).new_instructions 	:= 'CTOMSG# HUB Town is required for Return to address';
					g_ord(i).status_reason_code	:= 'CSERROR';			
					if	g_cs_force = 'N'
					then
						g_ord(i).carrier_id		:= 'ERROR';
						g_ord(i).service_level		:= 'ERROR';
					end if;
				elsif	g_ord(i).hub_country is null
				then
					g_ord(i).has_error		:= 'Y';
					g_ord(i).new_instructions 	:= 'CTOMSG# HUB country is required for Return to address';
					g_ord(i).status_reason_code	:= 'CSERROR';			
					if	g_cs_force = 'N'
					then
						g_ord(i).carrier_id		:= 'ERROR';
						g_ord(i).service_level		:= 'ERROR';
					end if;
				elsif	g_ord(i).hub_address1 is null
				then
					g_ord(i).has_error		:= 'Y';
					g_ord(i).new_instructions 	:= 'CTOMSG# HUB Address 1 is required for Return to address';
					g_ord(i).status_reason_code	:= 'CSERROR';			
					if	g_cs_force = 'N'
					then
						g_ord(i).carrier_id		:= 'ERROR';
						g_ord(i).service_level		:= 'ERROR';
					end if;
				elsif	g_ord(i).hub_postcode is null
				then
					g_ord(i).has_error		:= 'Y';
					g_ord(i).new_instructions 	:= 'CTOMSG# HUB Postcode is required for Return to address';
					g_ord(i).status_reason_code	:= 'CSERROR';			
					if	g_cs_force = 'N'
					then
						g_ord(i).carrier_id		:= 'ERROR';
						g_ord(i).service_level		:= 'ERROR';
					end if;
				end if;
			end if;
		end if;

		if	g_ord(i).has_error = 'Y'
		then
			g_ord(i).mpack_consignment := null;
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Order id '||g_ord(i).order_id||' contains errors. Processing this order is stopped.'
								 );
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
end error_checks_P;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 25/04/2021
-- Purpose : Fetch orders for add shipment
------------------------------------------------------------------------------------------------
procedure save_original_instructions_p
is
	l_rtn	varchar2(30) 	:= 'save_original_instructions_p';
begin
	for i in 1..g_ord.count
	loop
		if	g_ord(i).has_error = 'Y'
		then
			-- It is possible instructions are already saved in previous runs.
			-- and that the original instructions have been modified by the operator.
			-- This will require the saved instructions to be modified as well.
			if	g_ord(i).saved_instructions	is not null
			and	g_ord(i).instructions		is not null
			and	g_ord(i).instructions		!= g_ord(i).saved_instructions
			and	g_ord(i).instructions		not like 'CTOMSG#%'
			then
				update	cnl_sys.cnl_ohr_instructions
				set	instructions	= g_ord(i).instructions
				where	order_id 	= g_ord(i).order_id
				and	client_id	= g_ord(i).client_id
				and	site_id		= g_ord(i).from_site_id
				;
			-- If no instructions are saved yet and the order contains an instruction that is not an error message it must be saved.
			elsif	g_ord(i).saved_instructions	is null
			and	g_ord(i).instructions		is not null
			and	g_ord(i).instructions		not like 'CTOMSG#%'
			then
				insert
				into	cnl_sys.cnl_ohr_instructions
				(	site_id	
				,	client_id
				,	order_id
				,	instructions
				)
				values
				(	g_ord(i).from_site_id
				,	g_ord(i).client_id
				,	g_ord(i).order_id
				,	g_ord(i).instructions
				)
				;
			end if;
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
end save_original_instructions_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Convert ISO3 to ISO2 country code
------------------------------------------------------------------------------------------------
function iso2_country( p_iso3_i varchar2)
	return dcsdba.country.iso2_id%type
is
	l_retval	dcsdba.country.iso2_id%type;
begin
	select	iso2_id
	into	l_retval
	from	dcsdba.country
	where	iso3_id = p_iso3_i
	;
	return l_retval;
exception 
	when NO_DATA_FOUND
	then
		return 'XX';
end iso2_country;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Sender references 
------------------------------------------------------------------------------------------------
function shipment_sender_references_f
	return boolean
is
	l_retval	boolean		:= false;
	l_rtn		varchar2(30) 	:= 'shipment_sender_references_f';
begin

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment Sender references'
						 );

	g_shp_sender_refs	:= pljson();

	if	g_order_header.consol_order = 'N'
	then
		if	g_order_header.order_id is not null
		then
			l_retval := true;
			g_shp_sender_refs.put('senderReference1',	g_order_header.order_id);
		end if;
	else
		if	g_order_header.consol_id is not null
		then
			l_retval := true;
			g_shp_sender_refs.put('senderReference1',	g_order_header.consol_id);
		end if;
	end if;	

	if	g_order_header.shipment_id is not null
	then
		l_retval := true;
		g_shp_sender_refs.put('senderReference2',	g_order_header.shipment_id);
	end if;

	if	1=2
	then
		l_retval := true;
		g_shp_sender_refs.put('senderReference3',		'Example value 1');
	end if;

	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment Sender references'
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
end shipment_sender_references_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Receiver References 
------------------------------------------------------------------------------------------------
function shipment_receiver_references_f
	return boolean
is
	l_retval	boolean 	:= false;
	l_rtn		varchar2(30) 	:= 'shipment_receiver_references_f';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment receiver references'
						 );

	g_shp_receiver_refs	:= pljson();

	-- build receiver
	-- Receiver reference 1, 2 and 3. 
	-- For example order number can be sent here This information will only be provided to the carrier if it's additional fields for such information exists for the carrier. 
	-- In most cases not communicated or printed on label.
	if	g_order_header.consol_order = 'N'
	then
		if	g_order_header.order_reference is not null
		then
			l_retval 	:= true;
			g_shp_receiver_refs.put('receiverReference1',	g_order_header.order_reference);
		end if;

		if	g_order_header.purchase_order is not null
		then
			l_retval 	:= true;
			g_shp_receiver_refs.put('receiverReference2',	g_order_header.purchase_order);
		end if;

		if	1=2
		then
			l_retval 	:= true;
			g_shp_receiver_refs.put('receiverReference3',		'Example value 1');
		end if;
	else
		if	g_order_header.consol_id is not null
		then
			l_retval 	:= true;
			g_shp_receiver_refs.put('receiverReference1',	g_order_header.consol_id);
		end if;
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment receiver references'
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
end shipment_receiver_references_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Parcel attributes
------------------------------------------------------------------------------------------------
function parcel_attributes_f( p_parcel_id_i	in dcsdba.order_container.container_id%type)
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
						 , 'Fetching Parcel attributes'
						 );
	if	l_att.count > 0
	then
		g_parcel_atts	:= pljson_list();
		l_retval	:= true;

		-- Loop true all parcel attributes
		for 	i in 1..l_att.count
		loop
			-- Clear variable
			l_attribute	:= pljson();

			-- Build attribute	
			-- Name of the attribute. Both Code and Value are required if attributes are used
			l_attribute.put('code',		l_att(i).code);
			-- Value of the attribute. Both Code and Value are required if attributes are used
			l_attribute.put('value',	l_att(i).value);

			-- add attribute to attribute list
			g_parcel_atts.append(l_attribute);
		end loop;
	end if;

	l_att	:= l_att_tab(); -- clear memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching Parcel attributes'
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
-- Purpose : Shipment parcel Sender references 
------------------------------------------------------------------------------------------------
function parcel_sender_references_f( p_parcel_id_i	in dcsdba.order_container.container_id%type)
	return boolean
is
	l_retval	boolean		:= false;
	l_rtn		varchar2(30) 	:= 'parcel_sender_references_f';
begin
	g_par_sender_refs	:= pljson();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching parcel Sender references for parcel '||p_parcel_id_i||'.'
						 );

	-- Build parcel sender
	-- Sender reference 1, 2 and 3. 
	-- For example order number can be sent here This information will only be provided to the carrier if it's additional fields for such information exists for the carrier.
	-- In most cases not communicated or printed on label.
	if	1=2
	then
		l_retval	:= true;
		g_par_sender_refs.put('senderReferences',	'example reference1');
	end if;
	if 	1=2
	then
		l_retval	:= true;
		g_par_sender_refs.put('senderReferences',	'example reference2');
	end if;
	if	1=2
	then
		l_retval	:= true;
		g_par_sender_refs.put('senderReferences',	'example reference3');
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcel Sender references for parcel '||p_parcel_id_i||'.'
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
-- Purpose : Shipment parcel Receive References 
------------------------------------------------------------------------------------------------
function parcel_receiver_references_f( p_parcel_id_i	in dcsdba.order_container.container_id%type)
	return boolean
is
	l_retval	boolean		:= false;
	l_rtn		varchar2(30) 	:= 'parcel_receiver_references_f';
begin
	g_par_receiver_refs	:= pljson();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching parcel receiver references for parcel '||p_parcel_id_i||'.'
						 );

	-- build parcel receiver
	-- Receiver reference 1, 2 and 3. 
	-- For example order number can be sent here This information will only be provided to the carrier if it's additional fields for such information exists for the carrier. 
	-- In most cases not communicated or printed on label.
	if 	1=2
	then
		l_retval	:= true;
		g_par_receiver_refs.put('receiverReference1',		'Example reference1');
	end if;
	if	1=2
	then
		l_retval	:= true;
		g_par_receiver_refs.put('receiverReference2',		'Example reference1');
	end if;
	if	1=2
	then
		l_retval	:= true;
		g_par_receiver_refs.put('receiverReference3',		'Example reference1');
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcel receiver references for parcel '||p_parcel_id_i||'.'
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
-- Purpose : Shipment parcel line attributes
------------------------------------------------------------------------------------------------
function parcel_line_atts_f( p_parcel_id_i	in dcsdba.order_container.container_id%type
			   , p_line_id_i	in dcsdba.order_line.line_id%type
			   , p_sku_id_i		in dcsdba.sku.sku_id%type
			   , p_client_id_i	in dcsdba.client.client_id%type
			   , p_site_id_i	in dcsdba.site.site_id%type
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
		and	sku.client_id			= p_client_id_i
		and	sku.hazmat			= 'Y'
		order
		by	sku.hazmat_id
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

	l_retval		boolean		:= false;
	l_parcel_line_att	pljson		:= pljson();
	l_rtn			varchar2(30) 	:= 'parcel_line_atts_f';
	l_dg			c_dg%rowtype;
	l_un_code		varchar2(50);

begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching parcel line attributes for line id '||p_line_id_i|| ' in parcel '||p_parcel_id_i||'.'
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
		if	c_dg_iata%found 
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Line id '
								 || p_line_id_i
								 || ' in parcel '
								 || p_parcel_id_i
								 ||' contains DG. Adding DG attributes.'
								 );

			if	l_dg.un_code is not null
			then
				if	g_order_header.carrier_id = 'STD.TNTEL.COM'
				then
					l_un_code := replace(l_dg.un_code,' ','');
				else
					l_un_code := l_dg.un_code;
				end if;

				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_UN';
				l_att(l_att.count).value	:= l_un_code;--l_dg.un_code;
			end if;
			if	l_dg.cto_carrier_desc is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_DESCRIPTION';
				l_att(l_att.count).value	:= l_dg.cto_carrier_desc;
			end if;
			if	l_dg.un_class is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_CLASS';
				l_att(l_att.count).value	:= l_dg.un_class;
			end if;
			if	l_dg.un_pack_grp is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_PACKAGEGROUP';
				l_att(l_att.count).value	:= l_dg.un_pack_grp;
			end if;
			if	l_dg.un_pack_instr is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_PACKAGEINSTRUCTIONS';
				l_att(l_att.count).value	:= l_dg.un_pack_instr;
			end if;
			if	l_dg.dng_netweight is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_NETWEIGHT';
				l_att(l_att.count).value	:= l_dg.dng_netweight;
			end if;
			if	p_qty_i is not null
			then
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
		if	c_dg%found
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Line id '
								 || p_line_id_i
								 || ' in parcel '
								 || p_parcel_id_i
								 ||' contains DG. Adding DG attributes.'
								 );

			if	l_dg.un_code is not null
			then
				if	g_order_header.carrier_id = 'STD.TNTEL.COM'
				then
					l_un_code := replace(l_dg.un_code,' ','');
				else
					l_un_code := l_dg.un_code;
				end if;

				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_UN';
				l_att(l_att.count).value	:= l_un_code;--l_dg.un_code;
			end if;
			if	l_dg.cto_carrier_desc is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_DESCRIPTION';
				l_att(l_att.count).value	:= l_dg.cto_carrier_desc;
			end if;
			if	l_dg.un_class is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_CLASS';
				l_att(l_att.count).value	:= l_dg.un_class;
			end if;
			if	l_dg.un_pack_grp is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_PACKAGEGROUP';
				l_att(l_att.count).value	:= l_dg.un_pack_grp;
			end if;
			if	l_dg.un_pack_instr is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_PACKAGEINSTRUCTIONS';
				l_att(l_att.count).value	:= l_dg.un_pack_instr;
			end if;
			if	l_dg.dng_netweight is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_NETWEIGHT';
				l_att(l_att.count).value	:= l_dg.dng_netweight;
			end if;
			if	p_qty_i is not null
			then
				l_att.extend;
				l_att(l_att.count).code		:= 'DNG_QUANTITY';
				l_att(l_att.count).value	:= p_qty_i;
			end if;
		end if;
		close	c_dg;
	end if;

	if	l_att.count > 0
	then
		g_par_line_atts	:= pljson_list();
		l_retval	:= true;
		-- Loop true all parcel line attributes
		for 	i in 1..l_att.count
		loop
			-- clear variable
			l_parcel_line_att	:= pljson();

			-- Build parcel line	
			-- Name of the attribute. Both Code and Value are required if attributes are used
			l_parcel_line_att.put('code',		l_att(i).code);
			-- Value of the attribute. Both Code and Value are required if attributes are used
			l_parcel_line_att.put('value',		l_att(i).value);

			-- Add parcel line to parcel line list
			g_par_line_atts.append(l_parcel_line_att);
		end loop;
	end if;

	l_att	:= l_att_tab(); -- clear memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcel line attributes for line id '||p_line_id_i|| ' in parcel '||p_parcel_id_i||'.'
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
end parcel_line_atts_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment parcel lines
------------------------------------------------------------------------------------------------
function parcel_lines_f( p_parcel_id_i	in dcsdba.order_container.container_id%type 
		       , p_client_id_i	in dcsdba.client.client_id%type
		       , p_site_id_i	in dcsdba.site.site_id%type
		       )
	return boolean
is
	type l_par_line_rec is record( line_id			dcsdba.order_line.line_id%type
				     , countryoforigin		varchar2(10)
				     , itemDescription1		dcsdba.sku.description%type
				     , itemDescription2		dcsdba.sku.description%type
				     , productNumber		dcsdba.sku.sku_id%type
				     , quantityShipped		dcsdba.inventory.qty_on_hand%type
				     , statisticalNumber	dcsdba.sku.commodity_code%type
				     , unitValue		number
				     , unitValueCurrency	varchar2(10)
				     , unitVolume		dcsdba.sku.each_volume%type
				     , unitVolumeUnitOfMeasure	varchar2(10)
				     , unitWeight		dcsdba.sku.each_weight%type
				     , unitWeightUnitOfMeasure	varchar2(10)
				     , unitMeasure		varchar2(20)
				     , hazmat_id		dcsdba.sku.hazmat_id%type
				     , gender			dcsdba.sku.gender%type
				     );
	type l_par_line_tab is table of l_par_line_rec;
	l_par_line		l_par_line_tab	:= l_par_line_tab();

	l_retval		boolean		:= false;
	l_parcel_line		pljson		:= pljson();
	l_parcel_id		dcsdba.order_container.container_id%type;
	l_parcel_line_id	dcsdba.order_line.line_id%type;
	l_rtn			varchar2(30) 	:= 'parcel_lines_f';
	l_female		varchar2(1);
	l_iata_hazmat		dcsdba.sku.hazmat_id%type;
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching parcel lines for parcel '
						 || p_parcel_id_i
						 || ' from client '
						 || p_client_id_i
						 || ' in site id '
						 || p_site_id_i
						 || ' using shipment id '
						 || g_order_header.shipment_id
						 || '.'
						 );
	select	m.line_id
	,	i.origin_id		countryoforigin
	,	m.description		itemdescription1
	,	null			itemdescription2
	,	m.sku_id		productnumber
	,	sum(i.qty_on_hand)	quantityshipped
	,	s.commodity_code	statisticalnumber
	,	l.product_price		unitvalue
	,	l.product_currency	unitvaluecurrency
	,	s.each_volume 		unitvolume
	,	'm3'			unitvolumeunitofmeasure
	,	s.each_weight		unitweight
	,	'kg'			unitweightunitofmeasure
	,	c.track_level_1		unitmeasure
	,	s.gender
	,	s.hazmat_id
	bulk	collect
	into	l_par_line
	from 	dcsdba.move_task 	m
	-- For product price and currency
	inner
	join	dcsdba.order_line 	l
	on	l.line_id		= m.line_id
	and	l.order_id		= m.task_id
	and	l.client_id		= m.client_id
	-- For commodity code, each volume and weight
	inner
	join	dcsdba.sku 		s
	on	s.sku_id		= m.sku_id
	and	s.client_id		= m.client_id
	-- For lowest tracking level e.g. unit of measure
	left
	join	dcsdba.sku_config 	c
	on	c.config_id		= m.config_id
	and	c.client_id		= m.client_id
	-- To ensure we only fetch lines from this order
	inner
	join	dcsdba.order_header 	o
	on	o.order_id		= m.task_id
	and	o.from_site_id		= m.site_id
	and	o.client_id		= m.client_id
	and	o.uploaded_ws2pc_id	= g_old_shipment_id--g_order_header.shipment_id
	-- Origin id, and qty on hand
	left
	join	dcsdba.inventory 	i
	on	i.client_id		= m.client_id
	and	i.site_id		= m.site_id
	and	i.tag_id		= m.tag_id
	and	i.sku_id		= m.sku_id
	and	( i.container_id 	= m.to_container_id or i.container_id = m.container_id)
	and	( i.pallet_id 		= m.to_pallet_id or i.pallet_id = m.pallet_id)
	where	(	( m.to_pallet_id	= p_parcel_id_i or m.pallet_id = p_parcel_id_i)
		or	( m.to_container_id	= p_parcel_id_i or m.container_id = p_parcel_id_i)
		)
	and	m.site_id		= p_site_id_i
	and	m.client_id		= p_client_id_i
	group
	by	i.origin_id
	,	m.description
	,	null
	,	m.sku_id
	,	s.commodity_code
	,	l.product_price
	,	l.product_currency
	,	s.each_volume 
	,	'm3'
	,	s.each_weight
	,	'kg'
	,	c.track_level_1
	,	m.line_id
	,	s.gender
	,	s.hazmat_id
	order
	by	line_id	asc
	;


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
				g_iata_sku	:= l_par_line(r).productnumber;
			end if;
		end loop;

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Found '
							 || to_char(l_par_line.count)
							 || 'inventory records inside '
							 || p_parcel_id_i
							 ||'. Start building json list with lines.'
							 );

		l_retval	:= true;
		g_par_lines	:= pljson_list();

		-- build parcel line
		for 	i in 1..l_par_line.count
		loop
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Start adding first inventory record to the list. Line id '
								 || l_par_line(i).line_id
								 || ', SKU '
								 || l_par_line(i).productnumber
								 || ', client '
								 || p_client_id_i
								 || ', site '
								 || p_site_id_i
								 || ', qty '
								 || to_char( l_par_line(i).quantityshipped)
								 || '.'
								 );

			-- clear variable
			l_parcel_line		:= pljson();

			-- fetch parcel line attributes
			if 	parcel_line_atts_f( p_parcel_id_i
						  , l_par_line(i).line_id
						  , l_par_line(i).productnumber
						  , p_client_id_i
						  , p_site_id_i
						  , l_par_line(i).quantityshipped
						  )
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_line_atts_f returned TRUE.'
									 );

				l_parcel_line.put('attributes',			g_par_line_atts);
				g_par_line_atts					:= pljson_list(); -- clear memory
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_line_atts_f returned FALSE.'
									 );
			end if;

			-- The origin country of the item in question.ISO 3166-1 alpha 2. Mandatory for exports to your customer either on Shipment or Parcel level
			if	l_par_line(i).countryoforigin is not null
			then
				l_parcel_line.put('countryOfOrigin',		l_par_line(i).countryoforigin);
			end if;

			-- To be used in subsequent carrier integrations as export information regarding items included in shipments. 
			-- Can also be used in printing delivery notes and waybills. 
			-- Mandatory for exports to your customer either on Shipment or Parcel level.
			if	l_par_line(i).itemDescription1 is not null
			then
				l_parcel_line.put('itemDescription1',		l_par_line(i).itemDescription1);
			end if;
			if	l_par_line(i).itemDescription2 is not null
			then
				l_parcel_line.put('itemDescription',		l_par_line(i).itemDescription2);
			end if;

			-- Product number of the item in question.
			if	l_par_line(i).productNumber is not null
			then
				l_parcel_line.put('productNumber',		l_par_line(i).productNumber);
			end if;

			-- Number of units of the given item that is being shipped. Mandatory for exports to your customer either on Shipment or Parcel level
			if	l_par_line(i).quantityShipped is not null
			then
				l_parcel_line.put('quantityShipped',		l_par_line(i).quantityShipped);
			end if;

			-- Commodity code
			if	l_par_line(i).statisticalNumber is not null
			then
				l_parcel_line.put('statisticalNumber',		l_par_line(i).statisticalNumber);
			end if;

			-- Value of a single item
			if	l_par_line(i).unitValue is not null
			then
				l_parcel_line.put('unitValue',			l_par_line(i).unitValue);
			end if;

			-- Currency of UnitValue
			if	l_par_line(i).unitValueCurrency is not null
			then
				l_parcel_line.put('unitValueCurrency',		l_par_line(i).unitValueCurrency);
			end if;

			-- Volume of a single item
			if	l_par_line(i).unitVolume is not null
			then
				l_parcel_line.put('unitVolume',		 	l_par_line(i).unitVolume);
			end if;

			-- m3
			if	l_par_line(i).unitVolumeUnitOfMeasure is not null
			then
				l_parcel_line.put('unitVolumeUnitOfMeasure',	l_par_line(i).unitVolumeUnitOfMeasure);
			end if;

			-- Volume of a single item
			if	l_par_line(i).unitWeight is not null
			then
				l_parcel_line.put('unitWeight',			l_par_line(i).unitWeight);
			end if;

			-- kg
			if	l_par_line(i).unitWeightUnitOfMeasure is not null
			then
				l_parcel_line.put('unitWeightUnitOfMeasure',	l_par_line(i).unitWeightUnitOfMeasure);
			end if;

			-- lowest tracking level
			if	l_par_line(i).unitMeasure is not null
			then
				l_parcel_line.put('unitMeasure',		l_par_line(i).unitMeasure);
			end if;

			-- Add parcel line to parcel line list
			g_par_lines.append(l_parcel_line);
		end loop;		
	end if;

	l_par_line	:= l_par_line_tab(); -- clear memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching parcel lines for parcel '||p_parcel_id_i||'.'
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
end parcel_lines_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Parcels 
------------------------------------------------------------------------------------------------
function shipment_parcels_f
	return boolean
is

	-- Fetch all parcel information that must be reprinted
	cursor	c_recreate_parcels( b_shipment_id	dcsdba.order_header.uploaded_ws2pc_id%type)
	is
		select	distinct 
			decode(o.labelled,'Y',o.container_id,o.pallet_id) 			parcel_id
		,	o.client_id
		,	g_order_header.from_site_id						site_id
		,	o.order_id
		,	g_order_header.shipment_id						shipment_id
		,	o.pallet_id
		,	o.container_id
		,	decode(o.labelled,'Y',nvl(o.container_height,0),nvl(o.pallet_height,0))	parcel_height
		,	decode(o.labelled,'Y',nvl(o.container_width,0),nvl(o.pallet_width,0))	parcel_width
		,	decode(o.labelled,'Y',nvl(o.container_depth,0),nvl(o.pallet_depth,0))	parcel_depth
		,	decode(o.labelled,'Y',nvl(o.container_depth,nvl(o.pallet_depth,1))*
					      nvl(o.container_width,nvl(o.pallet_width,0))*
					      nvl(o.container_height,nvl(o.pallet_height,0))
					     ,nvl(o.pallet_volume,0))				parcel_volume
		,	decode(o.labelled,'Y',nvl(o.container_weight,0),nvl(o.pallet_weight,0)) parcel_weight
		,	decode(o.labelled,'Y','C','P')						pallet_or_container
		,	decode(o.labelled,'Y',nvl(o.container_type,o.config_id),o.config_id)	pallet_type
		, 	nvl(cl.user_def_type_4, 'Consumer Products') 				type_of_goods
		,	ptg.notes                                                               pallet_type_group  --BDS-6555
		,	sum(sk.each_weight * m.qty_to_move)					net_weight
		from	dcsdba.order_container o
		inner
		join	dcsdba.client cl
		on	cl.client_id	= o.client_id
		left
		join	dcsdba.pallet_config p
		on	p.client_id	= o.client_id
		and	p.config_id	= decode(o.labelled,'Y',o.container_type,o.config_id)
		inner
		join	dcsdba.move_task m
		on	m.client_id	= o.client_id
		and	m.site_id	= g_order_header.from_site_id
		and	nvl(m.to_pallet_id,m.pallet_id) 	= o.pallet_id
		and	nvl(m.to_container_id,m.container_id) 	= o.container_id
		and	m.task_id	= o.order_id
		inner
		join	dcsdba.sku sk
		on	sk.client_id	= m.client_id
		and	sk.sku_id	= m.sku_id
		inner
		join    dcsdba.pallet_type_grp ptg
		on      ptg.pallet_type_group = p.pallet_type_group
		where	o.order_id	in 	(
						select	h.order_id
						from	dcsdba.order_header h
						where	h.uploaded_ws2pc_id	= b_shipment_id
						and	h.from_site_id		= g_order_header.from_site_id
						and	h.client_id		= g_order_header.client_id
						)
		group
		by	decode(o.labelled,'Y',o.container_id,o.pallet_id)
		,	o.client_id
		,	g_order_header.from_site_id
		,	o.order_id
		,	g_order_header.shipment_id
		,	o.pallet_id
		,	o.container_id
		,	decode(o.labelled,'Y',nvl(o.container_height,0),nvl(o.pallet_height,0))
		,	decode(o.labelled,'Y',nvl(o.container_width,0),nvl(o.pallet_width,0))
		,	decode(o.labelled,'Y',nvl(o.container_depth,0),nvl(o.pallet_depth,0))
		,	decode(o.labelled,'Y',nvl(o.container_depth,nvl(o.pallet_depth,1))*
					      nvl(o.container_width,nvl(o.pallet_width,0))*
					      nvl(o.container_height,nvl(o.pallet_height,0))
					     ,nvl(o.pallet_volume,0))
		,	decode(o.labelled,'Y',nvl(o.container_weight,0),nvl(o.pallet_weight,0))
		,	decode(o.labelled,'Y','C','P')					
		,	decode(o.labelled,'Y',nvl(o.container_type,o.config_id),o.config_id)
		, 	nvl(cl.user_def_type_4, 'Consumer Products') 				
		,	ptg.notes   --BDS-6555
		union
		select	distinct 
			decode(s.labelled,'Y',s.container_id,s.pallet_id) 			parcel_id
		,	s.client_id
		,	g_order_header.from_site_id						site_id
		,	s.order_id
		,	g_order_header.shipment_id						shipment_id
		,	s.pallet_id
		,	s.container_id
		,	decode(s.labelled,'Y',nvl(s.container_height,0),nvl(s.pallet_height,0))	parcel_height
		,	decode(s.labelled,'Y',nvl(s.container_width,0),nvl(s.pallet_width,0))	parcel_width
		,	decode(s.labelled,'Y',nvl(s.container_depth,0),nvl(s.pallet_depth,0))	parcel_depth
		,	decode(s.labelled,'Y',nvl(s.container_depth,nvl(s.pallet_depth,1))*
					      nvl(s.container_width,nvl(s.pallet_width,0))*
					      nvl(s.container_height,nvl(s.pallet_height,0))
					     ,nvl(s.pallet_volume,0))				parcel_volume
		,	decode(s.labelled,'Y',nvl(s.container_weight,0),nvl(s.pallet_weight,0)) parcel_weight
		,	decode(s.labelled,'Y','C','P')						pallet_or_container
		,	decode(s.labelled,'Y',nvl(s.container_type,s.config_id),s.config_id)	pallet_type
		, 	nvl(cl.user_def_type_4, 'Consumer Products') 				type_of_goods
		,	ptg.notes                                                               pallet_type_group  --BDS-6555
		,	sum(sk.each_weight * m.qty_to_move)					net_weight
		from	dcsdba.shipping_manifest s
		inner
		join	dcsdba.client cl
		on	cl.client_id	= s.client_id
		left
		join	dcsdba.pallet_config p
		on	p.client_id	= s.client_id
		and	p.config_id	= decode(s.labelled,'Y',s.container_type,s.config_id)
		inner
		join	dcsdba.move_task m
		on	m.client_id	= s.client_id
		and	m.site_id	= s.site_id
		and	nvl(m.to_pallet_id,m.pallet_id) 	= s.pallet_id
		and	nvl(m.to_container_id,m.container_id) 	= s.container_id
		and	m.task_id	= s.order_id
		inner
		join	dcsdba.sku sk
		on	sk.client_id	= m.client_id
		and	sk.sku_id	= m.sku_id
		inner
		join    dcsdba.pallet_type_grp ptg
		on      ptg.pallet_type_group = p.pallet_type_group
		where	s.order_id	in 	(
						select	h.order_id
						from	dcsdba.order_header h
						where	h.uploaded_ws2pc_id	= b_shipment_id
						and	h.from_site_id		= g_order_header.from_site_id
						and	h.client_id		= g_order_header.client_id
						)
		group
		by	decode(s.labelled,'Y',s.container_id,s.pallet_id)
		,	s.client_id
		,	g_order_header.from_site_id
		,	s.order_id
		,	g_order_header.shipment_id
		,	s.pallet_id
		,	s.container_id
		,	decode(s.labelled,'Y',nvl(s.container_height,0),nvl(s.pallet_height,0))
		,	decode(s.labelled,'Y',nvl(s.container_width,0),nvl(s.pallet_width,0))
		,	decode(s.labelled,'Y',nvl(s.container_depth,0),nvl(s.pallet_depth,0))
		,	decode(s.labelled,'Y',nvl(s.container_depth,nvl(s.pallet_depth,1))*
					      nvl(s.container_width,nvl(s.pallet_width,0))*
					      nvl(s.container_height,nvl(s.pallet_height,0))
					     ,nvl(s.pallet_volume,0))
		,	decode(s.labelled,'Y',nvl(s.container_weight,0),nvl(s.pallet_weight,0))
		,	decode(s.labelled,'Y','C','P')					
		,	decode(s.labelled,'Y',nvl(s.container_type,s.config_id),s.config_id)
		, 	nvl(cl.user_def_type_4, 'Consumer Products') 				
		,	ptg.notes   --BDS-6555
		order
		by	parcel_id
	;

	l_retval		boolean		:= false;
	l_parcel		pljson		:= pljson();
	l_parcel_id		dcsdba.order_container.container_id%type;
	l_receiver_ref		pljson		:= pljson();
	l_sender_ref		pljson		:= pljson();
	l_parcel_lines		pljson_list	:= pljson_list();
	l_rtn			varchar2(30) 	:= 'parcels_f';
	l_val			integer;
	l_previous_parcel	varchar2(30);
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment parcels'
						 );
	if	g_carrier_update = 'Y'
	then
		<<recreate_parcel_loop>>
		for	i in c_recreate_parcels( g_old_shipment_id)
		loop
			if	l_previous_parcel is null
			or	l_previous_parcel != i.parcel_id
			then
				g_parcel.extend;
				g_parcel(g_parcel.count).parcel_id			:= i.parcel_id;
				g_parcel(g_parcel.count).site_id			:= g_order_header.from_site_id;
				g_parcel(g_parcel.count).client_id			:= g_order_header.client_id;
				g_parcel(g_parcel.count).parcel_height			:= i.parcel_height;
				g_parcel(g_parcel.count).parcel_depth			:= i.parcel_depth;
				g_parcel(g_parcel.count).parcel_width			:= i.parcel_width;
				g_parcel(g_parcel.count).parcel_volume			:= i.parcel_volume;
				g_parcel(g_parcel.count).parcel_weight			:= i.parcel_weight;
				g_parcel(g_parcel.count).parcel_type			:= i.pallet_type;
				g_parcel(g_parcel.count).netweight			:= i.net_weight;
				g_parcel(g_parcel.count).typeofgoods			:= i.type_of_goods;
				g_parcel(g_parcel.count).typeofpackage			:= i.pallet_type_group;
				g_parcel(g_parcel.count).order_id			:= g_order_header.order_id;
				g_parcel(g_parcel.count).shipment_id			:= g_order_header.shipment_id;
				g_parcel(g_parcel.count).run_task_key			:= g_rtk_key;
				g_parcel(g_parcel.count).pallet_or_container		:= i.pallet_or_container;
				g_parcel(g_parcel.count).pallet_type			:= i.pallet_type;
				g_parcel(g_parcel.count).carrier_id			:= g_order_header.carrier_id;
				g_parcel(g_parcel.count).service_level			:= g_order_header.service_level;
				g_parcel(g_parcel.count).status				:= 'Created';
				g_parcel(g_parcel.count).dws				:= 'N';
				g_parcel(g_parcel.count).pallet_id			:= i.pallet_id;
				g_parcel(g_parcel.count).container_id			:= i.container_id;
			end if;
			l_previous_parcel 	:= i.parcel_id;
		end  loop;
	end if;

	if	g_parcel.count > 0
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Found ' || to_char(g_parcel.count) || ' parcels for order ' || g_order_header.order_id ||'.'
							 );

		l_retval 	:= true;
		g_shp_parcels	:= pljson_list();

		-- build parcel	
		for 	i in 1..g_parcel.count
		loop
			l_parcel_id := g_parcel(i).parcel_id;

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Starting with parcel '||l_parcel_id||'.'
								 );

			-- Clean all variable from previous loop run
			l_parcel		:= pljson();

			-- parcel attributes
			if	parcel_attributes_f(l_parcel_id)
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_attributes_f returned TRUE.'
									 );
				l_parcel.put('attributes', 		g_parcel_atts);
				g_parcel_atts				:= pljson_list(); -- Clear memory
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_attributes_f returned FALSE.'
									 );
			end if;

			-- Parcel height
			l_parcel.put('height',			nvl(g_parcel(i).parcel_height,0.01));
			l_parcel.put('heightUnitOfMeasure',	'm');

			-- pallet length
			l_parcel.put('length', 			nvl(g_parcel(i).parcel_depth,0.01));
			l_parcel.put('lengthUnitOfMeasure',	'm');

			-- A loading metre corresponds to one linear meter of loading space in a truck. This is used as a calculation unit for goods that cannot be stacked or when stacking of or on top of these goods is not allowed, compensating for any lost volume
			if	g_parcel(i).loadingmeter is not null
			then
				l_parcel.put('loadingMeter', 		g_parcel(i).loadingMeter);
			end if;

			-- The net weight of the content without its package, used as a secondary weight for reports or simliar. Typically not communicated to the carrier unless required. 
			l_parcel.put('netWeight', 		nvl(g_parcel(i).netweight,0.01));

			-- A unique identifier provided by the source system. Typically a system reference for linking parcels between the system, not a customer reference. 
			-- The parcel identifier is used in other messages as a key identifier
			l_parcel.put('externalParcelIdentifier',g_parcel(i).parcel_id);

			-- Centiro normally generates the tracking number for the shipment, but if the source system generates the tracking number it should be sent here.
			-- The tracking number is the carrier tracking number, which is the main barcode on the shipping label and the key identifier in all communication with the carrier
			if	g_parcel(i).trackingnumber is not null
			then
				l_parcel.put('trackingNumber',		g_parcel(i).trackingnumber);
			end if;

			-- The tracking number SSCC is a specific type of tracking range using the SSCC format as specified and provided by GS1 and should only be populated in_
			-- the cases where the source system is responsible for carrier tracking number generation. 
			-- https://www.gs1.org/serial-shipping-container-code-sscc
			if	g_parcel(i).trackingnumbersscc is not null
			then
				l_parcel.put('trackingNumberSSCC',	g_parcel(i).trackingnumbersscc);
			end if;

			-- The type of goods sent in the package. This value is normally mandated by the carrier to be present and is expected to be a generalization of the content
			if	g_parcel(i).typeofgoods is not null
			then
				l_parcel.put('typeOfGoods',		g_parcel(i).typeofgoods);
			end if;

			-- The package type used. This value will be translated for carriers that requires it. 
			-- For example if the package type is a pallet, or half-pallet or if the carrier have specific packaging material that is being used.
			-- In WMS operators can select pallet types from other clients but then have to manually add the dims and weights. This causes the query to not return any value.
			-- Centiro requires a value therefore when this operators mistake is made we return a default value CARTON.
			l_parcel.put('typeOfPackage',		nvl(g_parcel(i).typeofpackage,'CARTON'));

			-- palle volume
			l_parcel.put('volume', 			nvl(g_parcel(i).parcel_volume,0.01));
			l_parcel.put('volumeUnitOfMeasure',	'm3');

			-- pallet weight
			l_parcel.put('weight', 			nvl(g_parcel(i).parcel_weight,0.01));
			l_parcel.put('weightUnitOfMeasure',	'kg');

			-- pallet width
			l_parcel.put('width', 			nvl(g_parcel(i).parcel_width,0.01));
			l_parcel.put('widthUnitOfMeasure',	'm');

			-- fetch parcel receiver references
			if	parcel_receiver_references_f(l_parcel_id)
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_receiver_references_f returned TRUE.'
									 );

				l_parcel.put('receiverReferences',	g_par_receiver_refs);
				g_par_receiver_refs			:= pljson(); -- Clear memory
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_receiver_references_f returned FALSE.'
									 );

			end if;

			-- Fetch parcel sender referrences
			if 	parcel_sender_references_f(l_parcel_id)
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_sender_references_f returned TRUE.'
									 );

				l_parcel.put('senderReferences', 	g_par_sender_refs);
				g_par_sender_refs			:= pljson();-- Clear memory
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_receiver_references_f returned FALSE.'
									 );
			end if;

			-- Add logging
			if	g_order_header.carrier_id = 'RHENUSROAD.NL'
			then
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'For Rhenus road no parcel lines are added.'
									 );
			end if;

			-- Fetch parcel content lines
			if	parcel_lines_f( l_parcel_id
					      , g_order_header.client_id
					      , g_order_header.from_site_id
					      )
			and	nvl(g_order_header.carrier_id,'X') != 'RHENUSROAD.NL'
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_lines_f returned TRUE.'
									 );

				l_parcel.put('orderLines', 		g_par_lines);
				g_par_lines				:= pljson_list(); -- clear memory
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Function parcel_lines_f returned FALSE.'
									 );
			end if;

			-- Add parcel to list of parcels
			g_shp_parcels.append(l_parcel);
		end loop;

	end if;	

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment parcels'
						 );
	return l_retval;
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
end shipment_parcels_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Order line attributes
------------------------------------------------------------------------------------------------
function shipment_line_attributes_f
	return boolean
is
	type l_att_rec is record( code	varchar2(30)
			        , value	varchar2(200)
				);
	type l_att_tab is table of l_att_rec;
	l_att 		l_att_tab	:= l_att_tab();

	l_retval	boolean	:= false;
	l_attribute	pljson		:= pljson();
	l_rtn		varchar2(30) 	:= 'shipment_line_attributes_f';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment order lines attributes for line id '||g_line.line_id||'.'
						 );
	if	g_line.hazmat_id	is not null
	then
		l_att.extend;
		l_att(l_att.count).code		:= 'HAZMAT_ID';
		l_att(l_att.count).value	:= g_line.hazmat_id;
	end if;

	if	l_att.count > 0
	then
		l_retval := true;
		g_shp_line_atts		:= pljson_list();

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
			g_shp_line_atts.append(l_attribute);
		end loop;
	end if;

	l_att	:= l_att_tab(); -- clear memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment order lines attributes for line id '||g_line.line_id||'.'
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
end shipment_line_attributes_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Order lines
------------------------------------------------------------------------------------------------
function shipment_order_lines_f
	return boolean
is
	cursor	c_lines
	is
		select	null	countryoforigin
		,	s.description
		,	l.customer_sku_desc1
		,	l.line_id
		,	l.sku_id
		,	null	quantityshipped
		,	s.commodity_code
		,	null	totalvolume
		,	null	totalvolumeunitofmeasure
		,	null	totalweight
		,	null	totalweightunitofmeasure
		,	null	totalvalue
		,	l.product_currency
		,	l.product_price
		,	s.each_volume
		,	'm3'	unitvolumeunitofmeasure
		,	s.each_weight
		,	'kg'	unitweightunitofmeasure
		,	(	select 	p.track_level_1
				from	dcsdba.sku_config p
				where	p.config_id	= (	select	c.config_id
								from	dcsdba.sku_sku_config c
								where	c.client_id 	= l.client_id
								and	c.sku_id	= l.sku_id
								and	rownum 		= 1
							   )
				and	p.client_id	= l.client_id
			) track_level_1
		,	s.hazmat_id
		,	h.notes				hazmat_notes
		,	n.regulation_id			hazmat_regulation_id
		,	n.r_sentence			hrn_r_sentence
		,	n.proper_shipping_name 		hrn_proper_shipping_name
		, 	n.additional_shipping_name	hrn_additional_shipping_name
		,	t.notes				hr_notes
		,	r.hazmat_class
		,	r.hazmat_subclass
		from	dcsdba.order_line l
		inner
		join	dcsdba.sku s
		on	s.sku_id	= l.sku_id
		and	s.client_id	= l.client_id
		left
		join	dcsdba.hazmat h
		on	s.hazmat_id	= h.hazmat_id
		left
		join	dcsdba.hazmat_regulation_notes n
		on	n.sku_id	= s.sku_id
		and	n.client_id	= s.client_id
		left
		join	dcsdba.hazmat_regulation t
		on	t.regulation_id	= n.regulation_id
		left
		join	dcsdba.sku_hazmat_reg r
		on	r.sku_id	= s.sku_id
		and	r.hazmat_id	= s.hazmat_id
		and	r.client_id	= s.client_id
		where	l.order_id	= g_order_header.order_id
		and	l.client_id	= g_order_header.client_id
		and	(	l.unallocatable	= 'N'
			or 	l.unallocatable	= null
			)
	;

	l_retval			boolean		:= false;
	l_line				pljson		:= pljson();
	l_rtn				varchar2(30) 	:= 'shipment_order_lines_f';
begin
	if	g_order_header.consol_order = 'N'
	then
		open	c_lines;
		fetch	c_lines
		bulk	collect
		into	g_orl;
		close	c_lines;
-- Requested to remove order line segment.
/*
		if	g_orl.count > 0
		then
			l_retval 	:= true;
			g_shp_lines	:= pljson_list();

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Fetching shipment order lines'
								 );

			-- Loop true all line attributes
			for 	i in 1..g_orl.count
			loop
				-- Build shipment order line
				l_line					:= pljson();
				g_line					:= g_orl(i);
				-- Customizable codes to be used for customised functionality
				if	shipment_line_attributes_f
				then
					l_line.put('attributes',		g_shp_line_atts);
					g_shp_line_atts				:= pljson_list(); -- clear memory
				end if;

				-- The origin country of the item in question. ISO 3166-1 alpha 2. Mandatory for exports to your customer either on Shipment or Parcel level	
				if 	g_orl(i).countryoforigin is not null
				then
					l_line.put('countryOfOrigin',		g_orl(i).countryoforigin);
				end if;

				-- To be used in subsequent carrier integrations as export information regarding items included in shipments. 
				-- Can also be used in printing delivery notes and waybills. 
				-- Mandatory for exports to your customer either on Shipment or Parcel level
				if	g_orl(i).itemdescription1 is not null
				then
					l_line.put('itemDescription1',		g_orl(i).itemdescription1);
				end if;
				if	g_orl(i).itemdescription2 is not null
				then			
					l_line.put('itemDescription2',		g_orl(i).itemdescription2);
				end if;

				-- Product number of the item in question
				if	g_orl(i).productnumber is not null
				then			
					l_line.put('productNumber',		g_orl(i).productnumber);
				end if;

				-- order line QTY shipped
				if	g_orl(i).quantityshipped is not null
				then			
					l_line.put('quantityShipped', 		g_orl(i).quantityshipped);
				end if;

				-- Commodty code
				if	g_orl(i).statisticalnumber is not null
				then			
					l_line.put('statisticalNumber',		g_orl(i).statisticalnumber);
				end if;

				-- total line volume
				if	g_orl(i).totalvolume is not null
				then	
					l_line.put('totalVolume', 		g_orl(i).totalvolume);
				end if;

				-- m3
				if	g_orl(i).totalvolumeunitofmeasure is not null
				then	
					l_line.put('totalVolumeUnitOfMeasure',	g_orl(i).totalvolumeunitofmeasure);
				end if;

				-- total line weight
				if	g_orl(i).totalweight is not null
				then	
					l_line.put('totalWeight', 		g_orl(i).totalweight);
				end if;

				-- kg
				if	g_orl(i).totalweightunitofmeasure is not null
				then	
					l_line.put('totalWeightUnitOfMeasure',	g_orl(i).totalweightunitofmeasure);
				end if;

				-- total line value
				if	g_orl(i).totalvalue is not null
				then	
					l_line.put('totalValue', 		g_orl(i).totalvalue);
				end if;

				-- line currency
				if	g_orl(i).currency is not null
				then	
					l_line.put('currency',			g_orl(i).currency);
				end if;

				-- each price
				if	g_orl(i).unitvalue is not null
				then	
					l_line.put('unitValue', 		g_orl(i).unitvalue);
				end if;

				-- each volume
				if	g_orl(i).unitvolume is not null
				then	
					l_line.put('unitVolume', 		g_orl(i).unitvolume);
				end if;

				-- m3
				if	g_orl(i).unitvolumeunitofmeasure is not null
				then	
					l_line.put('unitVolumeUnitOfMeasure',	g_orl(i).unitvolumeunitofmeasure);
				end if;

				-- Each weight
				if	g_orl(i).unitweight is not null
				then	
					l_line.put('unitWeight', 		g_orl(i).unitweight);
				end if;

				-- kg
				if	g_orl(i).unitweightunitofmeasure is not null
				then	
					l_line.put('unitWeightUnitOfMeasure',	g_orl(i).unitweightunitofmeasure);
				end if;

				-- Related to how you sell the product, for example '22meter hose' or 'packet'. Connected to QuantityShipped
				if	g_orl(i).unitmeasure is not null
				then	
					l_line.put('unitMeasure',		g_orl(i).unitmeasure); 
				end if;

				-- Add order line to list of order lines
				g_shp_lines.append(l_line);
			end loop;
		end if;

		g_orl	:= g_orl_tab(); -- clear memory

		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Finished fetching shipment order lines'
							 );
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Consol order so can''t add lines to shipment.'
							 );		
*/	end if;

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
end shipment_order_lines_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Dates custom dates
------------------------------------------------------------------------------------------------
function shipment_custom_dates_f
	return boolean
is
	type l_dates_rec is record ( dstamp		varchar2(50)
				   , description	varchar2(50)
				   );
	type l_dates_tab is table of l_dates_rec;
	l_dates				l_dates_tab	:= l_dates_tab();

	l_retval			boolean		:= false;
	l_custom_date			pljson		:= pljson();
	l_rtn				varchar2(30) 	:= 'shipment_custom_dates_f';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment dates custom Dates'
						 );

	if	g_order_header.consol_order = 'N'
	then
		-- Add order creation date
		l_dates.extend;
		l_dates(l_dates.count).dstamp		:= to_char(g_order_header.creation_date,'yyyy-mm-dd')||'T'||to_char(g_order_header.creation_date,'hh24:mi:ss')||'.0000000+02:00';
		l_dates(l_dates.count).description	:= 'WMS order creation date';

		-- Add order order date
		l_dates.extend;
		l_dates(l_dates.count).dstamp		:= to_char(g_order_header.order_date,'yyyy-mm-dd')||'T'||to_char(g_order_header.order_date,'hh24:mi:ss')||'.0000000+02:00';
		l_dates(l_dates.count).description	:= 'WMS order order date';
	end if;

	if	l_dates.count > 0
	then
		l_retval		:= true;
		g_shp_custom_dates	:= pljson_list();

		for 	i in l_dates.first..l_dates.last
		loop
			-- clear date variable
			l_custom_date	:= pljson();

			-- Build custom date
			l_custom_date.put('code',	l_dates(i).description);
			l_custom_date.put('value',	l_dates(i).dstamp);

			-- Add custom date to list of custom dates
			g_shp_custom_dates.append(l_custom_date);      
		end loop;
	end if;

	l_dates		:= l_dates_tab(); -- clear memory
	-- Clear temp table	
	l_dates	:= l_dates_tab();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment dates custom Dates'
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
end shipment_custom_dates_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Dates
------------------------------------------------------------------------------------------------
function shipment_dates_f
	return boolean
is
	l_date				varchar2(50);
	l_retval			boolean		:= false;
	l_rtn				varchar2(30) 	:= 'shipment_dates_f';
begin
	g_shp_dates	:= pljson();
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment dates'
						 );

	-- Build shipment dates

	-- The dispatch date from consignor location. Date ISO 8601
	if	g_order_header.ship_by_date is null
	or	g_order_header.ship_by_date < sysdate
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Ship by date is null or in the past ('
							 ||to_char(nvl(g_order_header.ship_by_date,sysdate),'YYYYMMDDHH24MISS')
							 ||') using sysdate as dispatch date ('
							 ||to_char(sysdate,'yyyy-mm-dd')||'T'||to_char(sysdate,'hh24:mi:ss')||'.0000000+02:00'
							 ||').'
							 );
		l_date		:= to_char(sysdate,'yyyy-mm-dd')||'T'||to_char(sysdate,'hh24:mi:ss')||'.0000000+02:00';
		l_retval	:= true;
		g_shp_dates.put('dispatchDate',		l_date);
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Ship by date ('
							 || to_char(g_order_header.ship_by_date,'YYYYMMDDHH24MISS')
							 ||') in the future. Set as dispatch date ('
							 || to_char(g_order_header.ship_by_date,'yyyy-mm-dd')||'T'||to_char(g_order_header.ship_by_date,'hh24:mi:ss')||'.0000000+02:00'
							 ||').'
							 );
		l_date		:= to_char(g_order_header.ship_by_date,'yyyy-mm-dd')||'T'||to_char(g_order_header.ship_by_date,'hh24:mi:ss')||'.0000000+02:00';
		l_retval	:= true;
		g_shp_dates.put('dispatchDate',		l_date);

	end if;

	-- The planned dispatch date from consignor location. Date ISO 8601
	if	1=2
	then
		l_retval	:= true;
		g_shp_dates.put('dispatchDatePlanned',	'2021-04-22T12:15:42.6920441+02:00');
	end if;
	-- Estimated time of arrival (ETA). Date ISO 8601
	if	1=2
	then
		l_retval	:= true;
		g_shp_dates.put('eta',			'2021-04-22T12:15:42.6920441+02:00');
	end if;

	-- Customer promised date of arrival.Date ISO 8601
	if	g_order_header.deliver_by_date is not null
	and	g_order_header.deliver_by_date > sysdate
	then
		l_date		:= to_char(g_order_header.deliver_by_date,'yyyy-mm-dd')||'T'||to_char(g_order_header.deliver_by_date,'hh24:mi:ss')||'.0000000+02:00';		
		l_retval	:= true;
		g_shp_dates.put('promisedDate',		l_date);
	end if;

	-- Defined time window of delivery Date ISO 8601. Needed if a service which require a time window should be used
	if	1=2
	then
		l_retval	:= true;
		g_shp_dates.put('timeWindowDeliveryFrom','2021-04-22T12:15:42.6920441+02:00');
	end if;
	-- Defined time window of delivery Date ISO 8601. Needed if a service which require a time window should be used
	if	1=2
	then
		l_retval	:= true;
		g_shp_dates.put('timeWindowDeliveryTo',	'2021-04-22T12:15:42.6920441+02:00');
	end if;
	-- Defined time window of pickup.Date ISO 8601
	if	1=2
	then
		l_retval	:= true;
		g_shp_dates.put('timeWindowPickupFrom',	'2021-04-22T12:15:42.6920441+02:00');
	end if;
	-- Defined time window of pickup.Date ISO 8601
	if	1=2
	then
		l_retval	:= true;
		g_shp_dates.put('timeWindowPickupTo',	'2021-04-22T12:15:42.6920441+02:00');
	end if;

	-- Fetch list of custom dates
	if	shipment_custom_dates_f
	then
		l_retval	:= true;
		g_shp_dates.put('customDate',		g_shp_custom_dates);
		g_shp_custom_dates			:= pljson_list(); -- clean memory
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment dates'
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
end shipment_dates_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment Customs
------------------------------------------------------------------------------------------------
function shipment_customs_f
	return boolean
is
	l_retval			boolean		:= false;
	l_rtn				varchar2(30) 	:= 'shipment_customs_f';
begin
	g_shp_customs	:= pljson();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching shipment customs'
						 );

	-- Build customs

	-- Specifies the value of the entire shipment. This value is used for customs clearance purposes. Required for international/cross-border shipments. Mandatory for exports
	if	g_order_header.export = 'Y'
	or	g_order_header.del_country in ('GBR','MCO')
	then
		if	g_order_header.consol_order	= 'N'
		then
			if	g_order_header.inv_total_1 is not null
			then
				l_retval	:= true;
				g_shp_customs.put('shipmentValue',		g_order_header.inv_total_1);
			end if;

			--Specifies the currency used for the shipment value. This value is used for customs clearance purposes. Required for international/cross-border shipments. Mandatory for exports
			if	g_order_header.inv_currency is not null
			then
				l_retval	:= true;
				g_shp_customs.put('shipmentValueCurrency',	nvl(g_order_header.inv_currency,'EUR'));
			end if;
		end if;
		-- This field is used differently depending on shipment origin, but it specifies a customs number required to export the shipment. 
		-- In the US this equals the AES code required for shipments with a value higher than $2 500
		if 	1=2
		then
			l_retval	:= true;
			g_shp_customs.put('exportLicenseNumber',	'sample string 3');
		end if;

		-- TOD
		if	g_order_header.tod is not null
		then
			l_retval	:= true;
			g_shp_customs.put('termsOfDelivery',		g_order_header.tod);
		end if;

		-- TOD place
		if 	g_order_header.tod_place is not null
		then
			l_retval	:= true;
			g_shp_customs.put('termsOfDeliveryLocation',	g_order_header.tod_place);
		end if;

		-- The reason for exporting a shipment which will be used for customs clearance purposes. Mandatory for exports
		if 	1=2
		then
			l_retval	:= true;
			g_shp_customs.put('exportReasonCode',	'sample string 6');
		end if;

		-- If the export reason code other is used, an export reason must be provided. Mandatory for exports
		if	1=2
		then
			l_retval	:= true;
			g_shp_customs.put('exportReason',		'sample string 7');
		end if;
	else
		-- TOD
		if	g_order_header.tod is not null
		then
			l_retval	:= true;
			g_shp_customs.put('termsOfDelivery',		g_order_header.tod);
		end if;

		-- TOD place
		if 	g_order_header.tod_place is not null
		then
			l_retval	:= true;
			g_shp_customs.put('termsOfDeliveryLocation',	g_order_header.tod_place);
		end if;

	end if;
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching shipment customs'
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
end shipment_customs_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment carrier service attribute values
------------------------------------------------------------------------------------------------
function shipment_carr_serv_att_val_f
	return boolean
is
	type l_att_rec 			is record( name		varchar2(30)
						 , value	varchar2(60)
						 );
	type l_att_tab			is table of l_att_rec;
	l_att				l_att_tab	:= l_att_tab();

	l_retval			boolean		:= false;
	l_value				pljson		:= pljson();
	l_rtn				varchar2(30) 	:= 'shipment_carr_serv_att_val_f';
begin

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching carrier service attribute values.'
						 );

	if	l_att.count > 0
	then
		l_retval		:= true;
		g_shp_car_serv_val	:= pljson_list();

		for 	i in 1..l_att.count
		loop
			-- Clear variable
			l_value	:= pljson();

			-- Build carrier service attribute value

			-- Name of the attribute. Both Code and Value are required if attributes are used
			l_value.put('name',		l_att(i).name);

			-- Value of the attribute. Both Code and Value are required if attributes are use
			l_value.put('value',		l_att(i).value);

			-- Add value to list of values
			g_shp_car_serv_val.append(l_value);
		end loop;
	end if;

	-- clear temp table
	l_att	:= l_att_tab();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching carrier service attribute values.'
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
end shipment_carr_serv_att_val_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment carrier service attributes
------------------------------------------------------------------------------------------------
function shipment_carr_service_atts_f
	return boolean
is
	l_nbr_attributes		integer		:= 0;
	l_retval			boolean		:= false;
	l_carrier_service_att		pljson		:= pljson();		
	l_rtn				varchar2(30) 	:= 'shipment_carr_service_atts_f';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching carrier service attributes.'
						 );
	if	l_nbr_attributes > 0
	then
		g_shp_car_atts	:= pljson_list();

		for 	i in 1..l_nbr_attributes
		loop
			if	shipment_carr_serv_att_val_f
			then
				l_retval	:= true;
				l_carrier_service_att.put('carrierServiceAttributeValues',	g_shp_car_serv_val);
				g_shp_car_serv_val						:= pljson_list();
				g_shp_car_atts.append(l_carrier_service_att);
			end if;
		end loop;
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching carrier service attributes.'
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
end shipment_carr_service_atts_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment attributes
------------------------------------------------------------------------------------------------
function Shipment_attributes_f
	return boolean
is
	type l_att_rec		is record( code 	varchar2(30)
					 , value	varchar2(60)
					 );
	type l_att_tab		is table of l_att_rec;
	l_att			l_att_tab	:= l_att_tab();

	cursor c_dg
	is
		select 	distinct
			decode(	hmt.user_def_type_2, 'HZ', 0, 'LB', 1, 'LQ', 2, 'EQ', 3, 4)	dg_sorter
		, 	hmt.user_def_type_2 cto_type
		,	substr(hmt.user_def_type_4,1,3) carrier_lb_addon
		from 	dcsdba.order_line ole
		inner
		join 	dcsdba.sku sku
		on 	sku.sku_id 		= ole.sku_id
		and 	sku.client_id 		= ole.client_id
		and 	sku.hazmat 		= 'Y'
		and 	sku.hazmat_id 		is not null
		inner
		join 	dcsdba.hazmat hmt
		on 	hmt.hazmat_id 		= hmt.hazmat_id
		and 	hmt.user_def_type_1 	is not null -- UN Code
		and 	hmt.user_def_type_2 	is not null -- DG Type
		and 	ole.client_id 		= g_order_header.client_id
		and 	ole.order_id 		= g_order_header.order_id
	        and     sku.hazmat_id           = hmt.hazmat_id  --cmd-297
		order
		by 	dg_sorter asc
	;	

	cursor c_ord_acc
	is
		select	o.accessorial
		from	dcsdba.order_accessory o
		where	o.order_id 	= g_order_header.order_id
		and	o.client_id	= g_order_header.client_id
	;

	l_retval		boolean		:= false;
	l_shipment_att		pljson		:= pljson();
	l_rtn			varchar2(30) 	:= 'Shipment_attributes_f';
	l_dg			c_dg%rowtype;
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Fetching Shipment attributes.'
						 );

	-- add order type used for ACSS
	if	g_order_header.order_type is not null
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add ORDER_TYPE attribute.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'ORDER_TYPE';
		l_att(l_att.count).value	:= g_order_header.order_type;
	end if;

	-- Add dispatch_method as attribute used for ACSS
	if	g_order_header.dispatch_method is not null
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add DISPATCH_METHOD attribute.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'DISPATCH_METHOD';
		l_att(l_att.count).value	:= g_order_header.dispatch_method;
	end if;

	-- Add user_def_type_1 regarding ACSS
	if 	g_order_header.user_def_type_1 is not null
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add ODH_UDT_1 attribute.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'ODH_UDT_1'; 
		l_att(l_att.count).value	:= g_order_header.user_def_type_1;
	end if;

	-- add COD attribute
	if	g_order_header.cod = 'Y'
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add ADDONSERVICE attribute.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'ADDONSERVICE'; 
		l_att(l_att.count).value	:= 'COD';
	end if;

	-- RUN VALIDATION FOR WHEN NO ACSS is USED
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Add RUNVALIDATION attribute.'
						 );
	l_att.extend;
	l_att(l_att.count).code		:= 'RUNVALIDATION'; 
	l_att(l_att.count).value	:= 'TRUE';

	-- Add source system
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Add source system attribute.'
						 );
	-- An on going discussion about removing a validation in Centiro is not yet finished therefore we still need to use hardcoded values setup in Centiro.
	if	g_database = 'DEVCNLJW'
	then
		l_att.extend;
		l_att(l_att.count).code		:= 'SourceSystem'; 
		l_att(l_att.count).value	:= 'DEVJDA2016';
	elsif	g_database = 'TSTCNLJW'
	then
		l_att.extend;
		l_att(l_att.count).code		:= 'SourceSystem'; 
		l_att(l_att.count).value	:= 'TSTJDA2016';
	elsif	g_database = 'ACCCNLJW'
	then
		l_att.extend;
		l_att(l_att.count).code		:= 'SourceSystem'; 
		l_att(l_att.count).value	:= 'ACCJDA2016';
	elsif	g_database = 'PRDCNLJW'
	then
		l_att.extend;
		l_att(l_att.count).code		:= 'SourceSystem'; 
		l_att(l_att.count).value	:= 'PRDJDA2016';
	end if;

	-- Add Site id
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Add Site id attribute.'
						 );
	l_att.extend;
	l_att(l_att.count).code		:= 'SiteId'; 
	l_att(l_att.count).value	:= g_order_header.from_site_id;

	-- Add Shipment id
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Add Shipment id attribute.'
						 );
	l_att.extend;
	l_att(l_att.count).code		:= 'ShipmentId'; 
	l_att(l_att.count).value	:= g_order_header.shipment_id;

	-- Add Client id
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Add Client id attribute.'
						 );
	l_att.extend;
	l_att(l_att.count).code		:= 'CLIENTID'; 
	l_att(l_att.count).value	:= g_order_header.client_id||'@'||g_order_header.from_site_id;


	-- cursor is opened and will select first value from the results set. The set is ordered asc so highest number comes first.
	open	c_dg;
	fetch 	c_dg
	into	l_dg;
	if	c_dg%found 
	and	l_dg.dg_sorter in (0,1,2,3)
	and	l_dg.cto_type is not null
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add ADDONSERVICE, DNG, SERVICE_ATTRIBUTE attributes.'
							 );
		-- Add user_def_type_1 regarding ACSS
		l_att.extend;
		l_att(l_att.count).code		:= 'ADDONSERVICE';
		l_att(l_att.count).value	:= l_dg.cto_type;

		l_att.extend;
		l_att(l_att.count).code		:= 'DNG';
		l_att(l_att.count).value	:= 'TRUE';

		l_att.extend;
		l_att(l_att.count).code		:= 'SERVICE_ATTRIBUTE';
		l_att(l_att.count).value	:= l_dg.cto_type;

		if	l_dg.dg_sorter = 1
		then
			l_att.extend;
			l_att(l_att.count).code		:= 'CARRIER_LB_ADDON';
			l_att(l_att.count).value	:= l_dg.carrier_lb_addon;			
		end if;
	end if;
	close	c_dg;

	-- Add manual addeed order accessory
	for 	i in c_ord_acc
	loop
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add attributes from order attributes in WMS.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'ADDONSERVICE';
		l_att(l_att.count).value	:= i.accessorial;		
	end loop;

	-- Fixed attributes for when carrier Nacex is selected via ACSS. Centiro will add the attribute only when Nacex is selected as a carrier.
	if	nvl(g_order_header.nacex_copies,1) > 4
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add NACEX_CRW "R" attribute.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'NACEX_CRW';
		l_att(l_att.count).value	:= 'R';
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Add NACEX_CRW "D" attribute.'
							 );
		l_att.extend;
		l_att(l_att.count).code		:= 'NACEX_CRW';
		l_att(l_att.count).value	:= 'D';
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Add NACEX_CopiesSPL attribute.'
						 );

	l_att.extend;
	l_att(l_att.count).code		:= 'NACEX_CopiesSPL';
	l_att(l_att.count).value	:= to_char(nvl(g_order_header.nacex_copies,1));


	-- Build shipment attribute
	if	l_att.count > 0
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Creating list of attributes.'
							 );

		l_retval	:= true;
		g_shp_atts	:= pljson_list();

		-- Loop true all parcel line attributes
		for 	i in 1..l_att.count
		loop
			-- CLear variable
			l_shipment_att	:= pljson();

			-- Attribute name
			if	l_att(i).code is not null
			then
				l_shipment_att.put('code',	l_att(i).code);
			end if;
			-- Attribute value
			if	l_att(i).value is not null
			then
				l_shipment_att.put('value',	l_att(i).value);
			end if;

			-- Add attribute to list of attributes
			g_shp_atts.append(l_shipment_att);
		end loop;
	end if;

	l_att	:= l_att_tab(); -- clean memory

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching Shipment attributes.'
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
end Shipment_attributes_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment addresses
------------------------------------------------------------------------------------------------
function Shipment_addresses_f
	return boolean
is
	-- Record type to pre fetch all address details from all areas
	type address_rec 	is record( additional_adr_info1		varchar2(80)
					 , additional_adr_info2		varchar2(80)
					 , cto_address_type		varchar2(20)
					 , address_id			dcsdba.address.address_id%type
					 , contact_mobile		dcsdba.address.contact_mobile%type
					 , town				dcsdba.address.town%type
					 , contact_name			dcsdba.address.name%type
					 , carr_account_nbr		varchar2(30)
					 , contact_email		dcsdba.address.contact_email%type
					 , contact_fax			dcsdba.address.contact_fax%type
					 , iso2countrycode		dcsdba.country.iso2_id%type
					 , name				dcsdba.address.name%type
					 , contact_phone		dcsdba.address.contact_phone%type
					 , residential			boolean
					 , salutation			varchar2(20)
					 , state			varchar2(60)			
					 , streetaddress		varchar2(200)
					 , streetaddresshousenumber	varchar2(10)
					 , vat_number			dcsdba.address.vat_number%type
					 , zipcode			dcsdba.address.postcode%type
					 , longitude			number
					 , latitude			number
					 );
	-- Table type with adrress records
	type address_tab 	is table of address_rec;
	-- Address table variable
	l_address		address_tab	:= address_tab();		

	l_retval		boolean		:= false;
	l_shipment_address	pljson		:= pljson();
	l_rtn			varchar2(30) 	:= 'Shipment_addresses_f';
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Adding receiver address.'
						 );
	-- Receiver address
	l_address.extend;
	l_address(l_address.count).additional_adr_info1		:= g_order_header.del_address2;
	l_address(l_address.count).additional_adr_info2		:= null;
	l_address(l_address.count).cto_address_type		:= 'Receiver';
	l_address(l_address.count).address_id			:= g_order_header.del_customer_id;
	l_address(l_address.count).contact_mobile		:= g_order_header.del_contact_mobile;
	l_address(l_address.count).town				:= g_order_header.del_town;
	l_address(l_address.count).contact_name			:= g_order_header.del_contact;
	l_address(l_address.count).carr_account_nbr		:= null;
	l_address(l_address.count).contact_email		:= g_order_header.del_contact_email;
	l_address(l_address.count).contact_fax			:= g_order_header.del_contact_fax;
	l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.del_country);
	l_address(l_address.count).name				:= nvl(g_order_header.del_name,'NO NAME');
	l_address(l_address.count).contact_phone		:= g_order_header.del_contact_phone;
	l_address(l_address.count).residential			:= null;
	l_address(l_address.count).salutation			:= null;
	l_address(l_address.count).state			:= g_order_header.del_county;
	l_address(l_address.count).streetaddress		:= g_order_header.del_address1;
	l_address(l_address.count).streetaddresshousenumber	:= null;
	l_address(l_address.count).vat_number			:= g_order_header.del_vat_number;
	l_address(l_address.count).zipcode			:= g_order_header.del_postcode;
	l_address(l_address.count).longitude			:= null;
	l_address(l_address.count).latitude			:= null;

	-- sender address
	if	g_order_header.hub_carrier_id in ('OLL')
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Adding Sender address.'
							 );
		l_address.extend;
		l_address(l_address.count).additional_adr_info1		:= g_order_header.hub_address2;
		l_address(l_address.count).additional_adr_info2		:= null;
		l_address(l_address.count).cto_address_type		:= 'Sender';
		l_address(l_address.count).address_id			:= g_order_header.hub_address_id;
		l_address(l_address.count).contact_mobile		:= g_order_header.hub_contact_mobile;
		l_address(l_address.count).town				:= g_order_header.hub_town;
		l_address(l_address.count).contact_name			:= g_order_header.hub_contact;
		l_address(l_address.count).carr_account_nbr		:= null;
		l_address(l_address.count).contact_email		:= g_order_header.hub_contact_email;
		l_address(l_address.count).contact_fax			:= g_order_header.hub_contact_fax;
		l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.hub_country);
		l_address(l_address.count).name				:= nvl(g_order_header.hub_name,'NO NAME');
		l_address(l_address.count).contact_phone		:= g_order_header.hub_contact_phone;
		l_address(l_address.count).residential			:= null;
		l_address(l_address.count).salutation			:= null;
		l_address(l_address.count).state			:= g_order_header.hub_county;
		l_address(l_address.count).streetaddress		:= g_order_header.hub_address1;
		l_address(l_address.count).streetaddresshousenumber	:= null;
		l_address(l_address.count).vat_number			:= g_order_header.hub_vat_number;
		l_address(l_address.count).zipcode			:= g_order_header.hub_postcode;
		l_address(l_address.count).longitude			:= null;
		l_address(l_address.count).latitude			:= null;
	end if;

	-- ReturnTo address
	if	g_order_header.hub_carrier_id in ('OLH')
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Adding return to address.'
							 );
		l_address.extend;
		l_address(l_address.count).additional_adr_info1		:= g_order_header.hub_address2;
		l_address(l_address.count).additional_adr_info2		:= null;
		l_address(l_address.count).cto_address_type		:= 'ReturnTo';
		l_address(l_address.count).address_id			:= g_order_header.hub_address_id;
		l_address(l_address.count).contact_mobile		:= g_order_header.hub_contact_mobile;
		l_address(l_address.count).town				:= g_order_header.hub_town;
		l_address(l_address.count).contact_name			:= g_order_header.hub_contact;
		l_address(l_address.count).carr_account_nbr		:= null;
		l_address(l_address.count).contact_email		:= g_order_header.hub_contact_email;
		l_address(l_address.count).contact_fax			:= g_order_header.hub_contact_fax;
		l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.hub_country);
		l_address(l_address.count).name				:= nvl(g_order_header.hub_name,'NO NAME');
		l_address(l_address.count).contact_phone		:= g_order_header.hub_contact_phone;
		l_address(l_address.count).residential			:= null;
		l_address(l_address.count).salutation			:= null;
		l_address(l_address.count).state			:= g_order_header.hub_county;
		l_address(l_address.count).streetaddress		:= g_order_header.hub_address1;
		l_address(l_address.count).streetaddresshousenumber	:= null;
		l_address(l_address.count).vat_number			:= g_order_header.hub_vat_number;
		l_address(l_address.count).zipcode			:= g_order_header.hub_postcode;
		l_address(l_address.count).longitude			:= null;
		l_address(l_address.count).latitude			:= null;
	end if;

	-- collectionpoint address
	if	g_order_header.hub_carrier_id = 'COL'
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Adding CollectionPoint address.'
							 );
		l_address.extend;
		l_address(l_address.count).additional_adr_info1		:= g_order_header.hub_address2;
		l_address(l_address.count).additional_adr_info2		:= null;
		l_address(l_address.count).cto_address_type		:= 'CollectionPoint';
		l_address(l_address.count).address_id			:= g_order_header.hub_address_id;
		l_address(l_address.count).contact_mobile		:= g_order_header.hub_contact_mobile;
		l_address(l_address.count).town				:= g_order_header.hub_town;
		l_address(l_address.count).contact_name			:= g_order_header.hub_contact;
		l_address(l_address.count).carr_account_nbr		:= null;
		l_address(l_address.count).contact_email		:= g_order_header.hub_contact_email;
		l_address(l_address.count).contact_fax			:= g_order_header.hub_contact_fax;
		l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.hub_country);
		l_address(l_address.count).name				:= nvl(g_order_header.hub_name,'NO NAME');
		l_address(l_address.count).contact_phone		:= g_order_header.hub_contact_phone;
		l_address(l_address.count).residential			:= null;
		l_address(l_address.count).salutation			:= null;
		l_address(l_address.count).state			:= g_order_header.hub_county;
		l_address(l_address.count).streetaddress		:= g_order_header.hub_address1;
		l_address(l_address.count).streetaddresshousenumber	:= null;
		l_address(l_address.count).vat_number			:= g_order_header.hub_vat_number;
		l_address(l_address.count).zipcode			:= g_order_header.hub_postcode;
		l_address(l_address.count).longitude			:= null;
		l_address(l_address.count).latitude			:= null;
	end if;

	-- transportpayer address
	if	(	g_order_header.freight_charges in ('collect','3rd party')
		or	g_order_header.freight_terms in ('CC','CB','COL','BB','BP','BC')
		)
	and	g_order_header.hub_vat_number is not null
	then
		if	(	g_order_header.freight_charges 	= '3rd party'
			or	g_order_header.freight_terms in ('BB','BP','BC')
			)
		and	g_order_header.huv_address_id 	is not null		
		then
			if	g_order_header.huv_town is not null
			and	g_order_header.huv_country is not null
			and	g_order_header.huv_address1 is not null
			and	g_order_header.huv_postcode is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Adding TransportPayer address.'
									 );

				l_address.extend;
				l_address(l_address.count).additional_adr_info1		:= g_order_header.huv_address2;
				l_address(l_address.count).additional_adr_info2		:= null;
				l_address(l_address.count).cto_address_type		:= 'TransportPayer';
				l_address(l_address.count).address_id			:= g_order_header.huv_address_id;
				l_address(l_address.count).contact_mobile		:= g_order_header.huv_contact_mobile;
				l_address(l_address.count).town				:= g_order_header.huv_town;
				l_address(l_address.count).contact_name			:= g_order_header.huv_contact;
				l_address(l_address.count).carr_account_nbr		:= g_order_header.hub_vat_number;
				l_address(l_address.count).contact_email		:= g_order_header.huv_contact_email;
				l_address(l_address.count).contact_fax			:= g_order_header.huv_contact_fax;
				l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.huv_country);
				l_address(l_address.count).name				:= nvl(g_order_header.huv_name,'NO NAME');
				l_address(l_address.count).contact_phone		:= g_order_header.huv_contact_phone;
				l_address(l_address.count).residential			:= null;
				l_address(l_address.count).salutation			:= null;
				l_address(l_address.count).state			:= g_order_header.huv_county;
				l_address(l_address.count).streetaddress		:= g_order_header.huv_address1;
				l_address(l_address.count).streetaddresshousenumber	:= null;
				l_address(l_address.count).vat_number			:= g_order_header.huv_vat_number;
				l_address(l_address.count).zipcode			:= g_order_header.huv_postcode;
				l_address(l_address.count).longitude			:= null;
				l_address(l_address.count).latitude			:= null;
			end if;
		elsif	(	g_order_header.freight_charges 	= 'collect'
			or	g_order_header.freight_terms in ('CC','CB','COL')
			)
		then
			if	g_order_header.del_town is not null
			and	g_order_header.del_country is not null
			and	g_order_header.del_address1 is not null
			and	g_order_header.del_postcode is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Adding TransportPayer address.'
									 );

				l_address.extend;
				l_address(l_address.count).additional_adr_info1		:= g_order_header.del_address2;
				l_address(l_address.count).additional_adr_info2		:= null;
				l_address(l_address.count).cto_address_type		:= 'TransportPayer';
				l_address(l_address.count).address_id			:= g_order_header.del_customer_id;
				l_address(l_address.count).contact_mobile		:= g_order_header.del_contact_mobile;
				l_address(l_address.count).town				:= g_order_header.del_town;
				l_address(l_address.count).contact_name			:= g_order_header.del_contact;
				l_address(l_address.count).carr_account_nbr		:= g_order_header.hub_vat_number;
				l_address(l_address.count).contact_email		:= g_order_header.del_contact_email;
				l_address(l_address.count).contact_fax			:= g_order_header.del_contact_fax;
				l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.del_country);
				l_address(l_address.count).name				:= nvl(g_order_header.del_name,'NO NAME');
				l_address(l_address.count).contact_phone		:= g_order_header.del_contact_phone;
				l_address(l_address.count).residential			:= null;
				l_address(l_address.count).salutation			:= null;
				l_address(l_address.count).state			:= g_order_header.del_county;
				l_address(l_address.count).streetaddress		:= g_order_header.del_address1;
				l_address(l_address.count).streetaddresshousenumber	:= null;
				l_address(l_address.count).vat_number			:= g_order_header.del_vat_number;
				l_address(l_address.count).zipcode			:= g_order_header.del_postcode;
				l_address(l_address.count).longitude			:= null;
				l_address(l_address.count).latitude			:= null;
			end if;
		end if;
	end if;

	-- tax and duty payer address
	if	(	g_order_header.freight_charges in ('collect','3rd party')
		or	g_order_header.freight_terms in ('CC','CB','COL','BB','BP','BC')
		)
	and	(	g_order_header.ce_eu_type != 'EU'
		or	g_order_header.ce_eu_type is null
		)
	and	g_order_header.letter_of_credit is not null
	then
		if	(	g_order_header.freight_charges 	= '3rd party'
			or	g_order_header.freight_terms in ('BB','BP','BC')
			)
		and	g_order_header.huv_address_id 	is not null		
		then
			if	g_order_header.huv_town is not null
			and	g_order_header.huv_country is not null
			and	g_order_header.huv_address1 is not null
			and	g_order_header.huv_postcode is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Adding TaxAndDutiesPayer address.'
									 );

				l_address.extend;
				l_address(l_address.count).additional_adr_info1		:= g_order_header.huv_address2;
				l_address(l_address.count).additional_adr_info2		:= null;
				l_address(l_address.count).cto_address_type		:= 'TaxAndDutiesPayer';
				l_address(l_address.count).address_id			:= g_order_header.huv_address_id;
				l_address(l_address.count).contact_mobile		:= g_order_header.huv_contact_mobile;
				l_address(l_address.count).town				:= g_order_header.huv_town;
				l_address(l_address.count).contact_name			:= g_order_header.huv_contact;
				l_address(l_address.count).carr_account_nbr		:= g_order_header.letter_of_credit;
				l_address(l_address.count).contact_email		:= g_order_header.huv_contact_email;
				l_address(l_address.count).contact_fax			:= g_order_header.huv_contact_fax;
				l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.huv_country);
				l_address(l_address.count).name				:= nvl(g_order_header.huv_name,'NO NAME');
				l_address(l_address.count).contact_phone		:= g_order_header.huv_contact_phone;
				l_address(l_address.count).residential			:= null;
				l_address(l_address.count).salutation			:= null;
				l_address(l_address.count).state			:= g_order_header.huv_county;
				l_address(l_address.count).streetaddress		:= g_order_header.huv_address1;
				l_address(l_address.count).streetaddresshousenumber	:= null;
				l_address(l_address.count).vat_number			:= g_order_header.huv_vat_number;
				l_address(l_address.count).zipcode			:= g_order_header.huv_postcode;
				l_address(l_address.count).longitude			:= null;
				l_address(l_address.count).latitude			:= null;
			end if;
		elsif	(	g_order_header.freight_charges 	= 'collect'
			or	g_order_header.freight_terms in ('CC','CB','COL')
			)
		then
			if	g_order_header.huv_town is not null
			and	g_order_header.huv_country is not null
			and	g_order_header.huv_address1 is not null
			and	g_order_header.huv_postcode is not null
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Adding TaxAndDutiesPayer address.'
									 );

				l_address.extend;
				l_address(l_address.count).additional_adr_info1		:= g_order_header.del_address2;
				l_address(l_address.count).additional_adr_info2		:= null;
				l_address(l_address.count).cto_address_type		:= 'TaxAndDutiesPayer';
				l_address(l_address.count).address_id			:= g_order_header.del_customer_id;
				l_address(l_address.count).contact_mobile		:= g_order_header.del_contact_mobile;
				l_address(l_address.count).town				:= g_order_header.del_town;
				l_address(l_address.count).contact_name			:= g_order_header.del_contact;
				l_address(l_address.count).carr_account_nbr		:= g_order_header.letter_of_credit;
				l_address(l_address.count).contact_email		:= g_order_header.del_contact_email;
				l_address(l_address.count).contact_fax			:= g_order_header.del_contact_fax;
				l_address(l_address.count).iso2countrycode		:= iso2_country(g_order_header.del_country);
				l_address(l_address.count).name				:= nvl(g_order_header.del_name,'NO NAME');
				l_address(l_address.count).contact_phone		:= g_order_header.del_contact_phone;
				l_address(l_address.count).residential			:= null;
				l_address(l_address.count).salutation			:= null;
				l_address(l_address.count).state			:= g_order_header.del_county;
				l_address(l_address.count).streetaddress		:= g_order_header.del_address1;
				l_address(l_address.count).streetaddresshousenumber	:= null;
				l_address(l_address.count).vat_number			:= g_order_header.del_vat_number;
				l_address(l_address.count).zipcode			:= g_order_header.del_postcode;
				l_address(l_address.count).longitude			:= null;
				l_address(l_address.count).latitude			:= null;
			end if;
		end if;
	end if;



	if	l_address.count > 0
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Create list of addresses.'
							 );
		l_retval		:= true;
		g_shp_addresses		:= pljson_list();

		-- Build address
		for 	i in 1..l_address.count
		loop
			-- clear variable
			l_shipment_address	:= pljson();

			-- Additional address information
			if	l_address(i).additional_adr_info1 is not null
			then
				l_shipment_address.put('additionalAddressInformation1',		l_address(i).additional_adr_info1);
			end if;

			-- Additional address information
			if	l_address(i).additional_adr_info2 is not null
			then
				l_shipment_address.put('additionalAddressInformation2',		l_address(i).additional_adr_info2);
			end if;

			-- Sender, receiver , buyer etc. .etc.
			l_shipment_address.put('addressType',			l_address(i).cto_address_type); -- See list in routine description

			-- Customer number connected to the AddressType, for example the Receiver's or Senders customer number. Not to be confused with carrier account number
			if	l_address(i).address_id is not null
			then
				l_shipment_address.put('externalAddressIdentifier',		l_address(i).address_id);
			end if;

			-- Contact cell phone number
			if	l_address(i).contact_mobile is not null
			then
				l_shipment_address.put('cellPhone',		l_address(i).contact_mobile);
			end if;

			-- Name of city Required segment
			l_shipment_address.put('city',			l_address(i).town);

			-- Name of contact person
			if	l_address(i).contact_name is not null
			then
				l_shipment_address.put('contactName',				l_address(i).contact_name);
			end if;

			-- Carrier account number connected to this address
			if	l_address(i).carr_account_nbr is not null
			then
				l_shipment_address.put('carrierAccountNumber',			l_address(i).carr_account_nbr);
			end if;

			-- Contact email address
			if	l_address(i).contact_email is not null
			then
				l_shipment_address.put('email',					l_address(i).contact_email);
			end if;

			-- Contact fax number
			if	l_address(i).contact_fax is not null
			then
				l_shipment_address.put('fax',					l_address(i).contact_fax);
			end if;

			-- Country code, two characters. ISO 3166-1 alpha 2 Required segment
			l_shipment_address.put('isoCountry',			l_address(i).iso2countrycode);

			-- Name of person or company
			if	l_address(i).name is not null
			then
				l_shipment_address.put('name',				l_address(i).name);
			end if;

			-- Contact phone number
			if	l_address(i).contact_phone is not null
			then
				l_shipment_address.put('phone',					l_address(i).contact_phone);
			end if;

			-- Indicates if address is residential or commercial
			--l_shipment_address.put('residential',		l_address(i).residential);

			-- If needed title to address either or see
			if	l_address(i).salutation is not null
			then
				l_shipment_address.put('salutation',		l_address(i).salutation);
			end if;

			-- Subdivision code, two characters. ISO 3166-1 alpha 2
			if	l_address(i).state is not null
			then
				l_shipment_address.put('state',			l_address(i).state);
			end if;

			-- Street address Required segment
			l_shipment_address.put('streetAddress',		l_address(i).streetaddress);

			-- Can also be provided in StreetAddress if a split is not desirable
			if	l_address(i).streetaddresshousenumber is not null
			then
				l_shipment_address.put('streetAddressHouseNumber',		l_address(i).streetaddresshousenumber);
			end if;

			-- Value added tax identification number
			if	l_address(i).vat_number is not null
			then
				l_shipment_address.put('vatNumber',			l_address(i).vat_number);
			end if;

			-- Zip code Required segment
			l_shipment_address.put('zipCode',			l_address(i).zipcode);

			-- No CTO description
			if	l_address(i).longitude is not null
			then
				l_shipment_address.put('longitude',			l_address(i).longitude);
			end if;

			-- No CTO description
			if	l_address(i).latitude is not null
			then
				l_shipment_address.put('latitude',			l_address(i).latitude);
			end if;

			-- Add address to list of addresses
			g_shp_addresses.append(l_shipment_address);
		end loop;
	end if;

	-- clear temp address table
	l_address	:= address_tab();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching Shipment addresses.'
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
end Shipment_addresses_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Shipment
------------------------------------------------------------------------------------------------
function Shipment_f
	return boolean
is
	l_retval		boolean 	:= true;
	l_rtn			varchar2(30) 	:= 'Shipment_f';
	l_site			dcsdba.site.site_id%type;
begin
	g_shipment	:= pljson();

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Start building Shipment JSON details for order '
						 || g_order_header.order_id
						 || ' from client '
						 || g_order_header.client_id
						 || '.'
						 );

	-- Build shipment

	-- Set carrier when carrier is froced from order
	if	g_cs_force = 'Y'
	and	g_shp_closed = 'N'
	then
		g_shipment.put('carrier',			g_order_header.carrier_id);
		g_shipment.put('carrierService',		g_order_header.service_level);
	end if;

	-- If carrier supports it, these instructions will be communicated to them concerning the delivery location of the shipment in for example labels and EDI
	if	g_order_header.instructions is not null
	then
		if	g_order_header.consol_order = 'N'
		then
			g_shipment.put('instructionDelivery',		g_order_header.instructions);
		end if;
	end if;

	--g_shipment.put('instructionDelivery2',	'sample string 4');
	--g_shipment.put('instructionDelivery3',	'sample string 5');
	--g_shipment.put('instructionPickup',		'sample string 6');
	--g_shipment.put('loadingMeter',		7.1);	

	-- ACSS when carrier selection is required
	if	g_cs_force = 'N'
	or	g_shp_closed = 'Y'
	then
		g_shipment.put('modeOfTransport',		'ACSS');
	end if;

	--g_shipment.put('numberOfEURPallets',		9);
	--g_shipment.put('routeNumber',			'sample string 10');
	--g_shipment.put('routeNumberTripNo',		'sample string 11');

	-- Sender is client.site@rhenus.com
	-- Translate site id GBMIK01 to GBASF02 as legacy issue
	if 	g_order_header.from_site_id = 'GBMIK01'
	then
		l_site := 'GBASF02';
	else
		l_site := g_order_header.from_site_id;
	end if;
	g_shipment.put('senderCode',			g_order_header.client_id||'.'||l_site||'@rhenus.com');

	g_shipment.put('externalShipmentIdentifier',	g_order_header.shipment_id);
	g_shipment.put('shipmentType',			'Outbound'); -- 0 = Outbound, 1 = Return, 2 = Inbound, 3 = Master

	--g_shipment.put('trackingNumber',			'sample string 14');

	-- Billing code
	if 	lower(g_order_header.freight_charges) = '3rd party'
	and	g_order_header.hub_vat_number is not null
	then
		g_shipment.put('billingCode',	'B');

	elsif	lower(g_order_header.freight_charges) = 'collect'
	and	g_order_header.hub_vat_number is not null
	then
		g_shipment.put('billingCode',	'C');	

	elsif	(	lower(g_order_header.freight_charges) = 'prepaid' 
		or 	g_order_header.freight_charges is null
		)
	and	g_order_header.hub_vat_number is null
	and	g_order_header.export = 'Y'
	and 	g_cs_force = 'N'
	then
		g_shipment.put('billingCode',	'PP');

	elsif	(	g_order_header.freight_charges is null
		and	g_order_header.hub_vat_number is not null
		)
	or	(	(	lower(g_order_header.freight_charges) = 'prepaid' 
			or 	g_order_header.freight_charges is null
			)
		and	g_cs_force = 'Y'
		)
	then
		g_shipment.put('billingCode',	nvl(g_order_header.freight_terms,'P'));
	else
		g_shipment.put('billingCode',	'P');
	end if;

	-- transport payer customer number --BDS-6574
	/*
        if	g_order_header.freight_charges in ('collect','3rd party')
	then
		if	g_order_header.hub_vat_number is not null
		then
			g_shipment.put('transportPayerCustomerNumber',	g_order_header.hub_vat_number); -- HUB VAT number
		end if;
	end if;
        */
	if      g_order_header.hub_carrier_id = 'TP'
		and	g_order_header.cod_type is not null
		and	g_order_header.hub_service_level = 'TP'
	then
			g_shipment.put('transportPayerCustomerNumber',	g_order_header.cod_type); -- HUB VAT number
	end if;


	-- Order volume
	if	g_order_header.order_volume is not null
	then
		if	g_order_header.order_volume < 0.01
		then
			g_shipment.put('volume',			0.01);
			g_shipment.put('volumeUnitOfMeasure',		'm3'); -- 0 = m3 (cubic meter), 1 = cm3 (cubic centimeter), 2 = cmm3 (cubic milimeter), 3 = in3 (cubic inch), 4 = ft3 (cubic feet), 5 = dm3 (cubic decimeter)
		else
			g_shipment.put('volume',			g_order_header.order_volume);
			g_shipment.put('volumeUnitOfMeasure',		'm3'); -- 0 = m3 (cubic meter), 1 = cm3 (cubic centimeter), 2 = cmm3 (cubic milimeter), 3 = in3 (cubic inch), 4 = ft3 (cubic feet), 5 = dm3 (cubic decimeter)
		end if;
	end if;

	if	g_order_header.order_weight is not null
	then
		if	g_order_header.order_weight < 0.01
		then
			g_shipment.put('weight', 			0.01);
			g_shipment.put('weightUnitOfMeasure',		'kg'); -- 0 = Kg (kilogram), 1 = g (gram) , 2 = oz (Ounce), 3 = lbs (pound)
		else
			g_shipment.put('weight', 			g_order_header.order_weight);
			g_shipment.put('weightUnitOfMeasure',		'kg'); -- 0 = Kg (kilogram), 1 = g (gram) , 2 = oz (Ounce), 3 = lbs (pound)
		end if;
	end if;

	-- Addresses connected to the shipment
	if	Shipment_addresses_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function Shipment_addresses_f returned TRUE.'
							 );
		g_shipment.put('addresses',		g_shp_addresses);
		g_shp_addresses				:= pljson_list();
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function Shipment_addresses_f returned FALSE.'
							 );	
	end if;

	-- Customizable codes to be used in case of standard functionality not having sufficient support for operations
	if	Shipment_attributes_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function Shipment_attributes_f returned TRUE.'
							 );
		g_shipment.put('attributes',		g_shp_atts);
		g_shp_atts				:= pljson_list();
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function Shipment_attributes_f returned FALSE.'
							 );
	end if;

	-- Optional services that can typically be applied for multiple different carrier services, such as saturday delivery or refridgerated
	if	shipment_carr_service_atts_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_carr_service_atts_f returned TRUE.'
							 );
		g_shipment.put('carrierServiceAttributes',g_shp_car_atts);
		g_shp_car_atts				:= pljson_list(); -- Clean memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_carr_service_atts_f returned FALSE.'
							 );
	end if;

	-- Used to provide information needed for export shipments
	if	shipment_customs_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_customs_f returned TRUE.'
							 );
		g_shipment.put('customs',		g_shp_customs);
		g_shp_customs				:= pljson(); -- Clean memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_customs_f returned FALSE.'
							 );

	end if;

	-- Dates connected to a shipment
	if	shipment_dates_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_dates_f returned TRUE.'
							 );
		g_shipment.put('dates',			g_shp_dates);
		g_shp_dates				:= pljson(); -- Clean memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_dates_f returned FALSE.'
							 );
	end if;

	-- Orderlines connected to the shipment
	if	shipment_order_lines_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_order_lines_f returned TRUE.'
							 );
		g_shipment.put('orderLines',		g_shp_lines);
		g_shp_lines				:= pljson_list(); -- Clean memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_order_lines_f returned FALSE.'
							 );
	end if;

	-- Parcels connected to the shipment
	if	shipment_parcels_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_parcels_f returned TRUE.'
							 );
		g_shipment.put('parcels',		g_shp_parcels);
		g_shp_parcels				:= pljson_list(); -- Clear memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_parcels_f returned FALSE.'
							 );
	end if;

	-- Shipment receiver referrences
	if	shipment_receiver_references_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_receiver_references_f returned TRUE.'
							 );
		g_shipment.put('receiverReferences',	g_shp_receiver_refs);
		g_shp_receiver_refs			:= pljson(); -- clear memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_receiver_references_f returned FALSE.'
							 );
	end if;

	-- Shipment sender references
	if	shipment_sender_references_f
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_sender_references_f returned TRUE.'
							 );
		g_shipment.put('senderReferences',	g_shp_sender_refs);
		g_shp_sender_refs			:= pljson(); -- clear memory
	else
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Function shipment_sender_references_f returned FALSE.'
							 );
	end if;

	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 , 'Finished fetching Shipment.'
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
end Shipment_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Update WMS order using direct API
------------------------------------------------------------------------------------------------
procedure update_wms_order_p 
is
	cursor	c_if_error(b_err varchar2)
	is
		select	text
		from	dcsdba.language_text
		where	label 		= b_err
		and	language	= 'EN_GB'
	;

	l_rtn		varchar2(30) := 'update_wms_order_p';
	l_err		integer := 1; -- 1 = OK, 0 = Error
	l_err_code	varchar2(20);
	l_err_desc	dcsdba.language_text.text%type;
	l_instructions	dcsdba.order_header.instructions%type;
	l_itl_status	integer;
	l_tmp_notes	varchar2(200);
begin
	if 	g_shp_closed = 'Y'
	and	g_order_header.http_response_code	= '200'
	and	g_dif_carrier = 'N'
	then
		for	i in 1..g_ord.count
		loop	-- Only an update with new shipment id is needed.
			update	dcsdba.order_header
			set 	uploaded_ws2pc_id	= g_ord(i).shipment_id
			where	from_site_id		= g_ord(i).from_site_id
			and	order_id		= g_ord(i).order_id
			and	client_id		= g_ord(i).client_id
			;

			-- When BBX an update of the shipment is required
			if	g_ord(i).service_level = 'BBX'
			then
				insert
				into	cnl_sys.cnl_cto_shipments_to_update
				(	site_id
				,	client_id
				,	order_id
				,	shipment_id
				)
				values
				(	g_ord(i).from_site_id
				,	g_ord(i).client_id
				,	g_ord(i).order_id
				,	g_ord(i).shipment_id
				);
			end if;
		end loop;		
	elsif	g_shp_closed = 'N'
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Start order update loop for all orders in current collection.'
							 );
		if	g_parcel.count > 0
		then
			for i in 1..g_parcel.count
			loop
				insert
				into	cnl_sys.cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
							   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
							   , update_dstamp, printer_name, dws, copies, shp_label_base64
							   )
				values
				(	g_parcel(i).client_id
				,	g_parcel(i).site_id
				,	g_parcel(i).order_id
				,	g_parcel(i).shipment_id
				,	g_parcel(i).parcel_id
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
				,	g_printer
				,	g_parcel(i).dws
				,	1
				,	g_parcel(i).shp_label_base64
				);
				-- Set tracking number in order container
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
				-- Set tracking number in shipping_manifest
				update	dcsdba.shipping_manifest s
				set	s.carrier_consignment_id 	= g_parcel(i).tracking_number
				where	s.client_id 			= g_parcel(i).client_id
				and	(	
					(	g_parcel(i).pallet_or_container 	= 'C'
					and	g_parcel(i).parcel_id 			= s.container_id
					)
					or
					(	g_parcel(i).pallet_or_container 	= 'P'
					and	g_parcel(i).parcel_id 			= s.pallet_id
					)
					)
				and	(	s.order_id 				= g_parcel(i).order_id
					or	g_parcel(i).order_id 			is null
					)
				;		
				-- Update header trax id
				update_order_master_waybill_p( p_waybill_i		=> g_parcel(i).tracking_number
							     , p_client_id_i 		=> g_parcel(i).client_id
							     , p_shipment_id_i		=> g_parcel(i).shipment_id
							     );			
			end loop;
		end if;

		-- Start looping and call direct order API
		for	i in 1..g_ord.count
		loop
			if	g_carrier_update = 'Y'
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Forced carrier update. Can''t update order using interface therefore do a direct update on order header. '
									 || g_ord(i).order_id
									 || ' from client '
									 || g_ord(i).client_id
									 ||'.'
									 );
				update	dcsdba.order_header o
				set	o.delivery_point	= g_ord(i).delivery_point
				, 	o.instructions		= g_ord(i).new_instructions
				,	o.status_reason_code	= g_ord(i).status_reason_code
				,	o.mpack_consignment	= g_ord(i).mpack_consignment
				where	o.client_id 		= g_ord(i).client_id
				and	o.from_site_id 		= g_ord(i).from_site_id
				and	o.order_id 		= g_ord(i).order_id
				;
			else
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Update order using direct order API interface for order '
									 || g_ord(i).order_id
									 || ' from client '
									 || g_ord(i).client_id
									 ||'.'
									 );
				-- Initialise Session
				dcsdba.libsession.InitialiseSession( userid		=> 'CENTIRO'
								   , groupid		=> null
								   , stationid		=> 'Automatic'
								   , wksgroupid		=> null
								   );
				-- Update order					 
				l_err := dcsdba.libmergeorder.directorderheader( p_mergeerror		=> l_err_code
									       , p_toupdatecols		=> ':client_id::order_id::from_site_id::delivery_point::instructions::carrier_id::service_level::status_reason_code::time_zone_name::nls_calendar::mpack_consignment:'
									       , p_mergeaction      	=> 'U'
									       , p_clientid         	=> g_ord(i).client_id
									       , p_orderid          	=> g_ord(i).order_id
									       , p_fromsiteid       	=> g_ord(i).from_site_id
									       , p_deliverypoint    	=> g_ord(i).delivery_point
									       , p_instructions     	=> g_ord(i).new_instructions
									       , p_carrierid        	=> g_ord(i).carrier_id
									       , p_servicelevel     	=> g_ord(i).service_level
									       , p_statusreasoncode 	=> g_ord(i).status_reason_code
									       , p_timezonename     	=> 'Europe/Amsterdam'
									       , p_mpackconsignment	=> g_ord(i).mpack_consignment 
									      );

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Direct order API reponse for order '
									 || g_ord(i).order_id
									 || ' from client '
									 || g_ord(i).client_id
									 ||'.'
									 || 'API returned '
									 || l_err
									 || ', with error code '
									 || l_err_code
									 || '.'
									 );
			end if;

			if	l_err = 0
			then
				-- Fetch IF error code description from language text
				open	c_if_error( l_err_code );
				fetch	c_if_error
				into	l_err_desc;
				close	c_if_error;

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Order update raised an interface error for '
									 || g_ord(i).order_id
									 || ' from client '
									 || g_ord(i).client_id
									 ||'. IF error :'
									 || l_err_code 
									 || ', '
									 || l_err_desc
									 || '.'
									 );
				-- Construct instructions
				if	l_err_code	= 'IF2453'
				then
					l_instructions	:= substr( 'CTOMSG# Carrier '
								 || g_ord(i).carrier_id
								 || ' and sevice level '
								 || g_ord(i).service_level
								 || ' combination does not exist for this client'
								 ,1,180
								 );
				elsif	l_err_code 	= 'Put any new error code here'
				then
					l_instructions	:= substr('CTOMSG# construct any new error message here',1,180);
				else
					l_instructions	:= substr('CTOMSG# An undefined WMS interface error was raised during order update. IF code '|| l_err_code
																		     || ': '
																		     || l_err_desc
																		     ,1,180);
				end if;

				-- Set order details as if it was a carrier selection error
				g_ord(i).cancel_cto_shipment	:= 'N';
				if	g_cs_force		= 'N'
				then	
					g_ord(i).carrier_id		:= 'ERROR';
					g_ord(i).service_level		:= 'ERROR';
				end if;
				g_ord(i).status_reason_code	:= 'CSERROR';
				g_ord(i).new_instructions	:= l_instructions;
				g_ord(i).has_error		:= 'Y';

				-- Save original instructions for later 
				if	g_ord(i).saved_instructions	is not null
				and	g_ord(i).instructions		is not null
				and	g_ord(i).instructions		!= g_ord(i).saved_instructions
				and	g_ord(i).instructions		not like 'CTOMSG#%'
				then
					update	cnl_sys.cnl_ohr_instructions
					set	instructions	= g_ord(i).instructions
					where	order_id 	= g_ord(i).order_id
					and	client_id	= g_ord(i).client_id
					and	site_id		= g_ord(i).from_site_id
					;
				elsif	g_ord(i).saved_instructions	is null
				and	g_ord(i).instructions		is not null
				and	g_ord(i).instructions		not like 'CTOMSG#%'
				then
					insert
					into	cnl_sys.cnl_ohr_instructions
					(	site_id	
					,	client_id
					,	order_id
					,	instructions
					)
					values
					(	g_ord(i).from_site_id
					,	g_ord(i).client_id
					,	g_ord(i).order_id
					,	g_ord(i).instructions
					)
					;
				end if;

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Execute a direct update because API failed. '
									 || 'Even when ACSS was successful order is now set to error and CTO shipment will be cancelled. Order id '
									 || g_ord(i).order_id
									 || ' from client '
									 || g_ord(i).client_id
									 || '.'
									 );

				-- Direct update order header without API
				update	dcsdba.order_header
				set 	uploaded_ws2pc_id	= null
				,	carrier_id 		= g_ord(i).carrier_id
				,	service_level		= g_ord(i).service_level
				,	status_reason_code	= g_ord(i).status_reason_code
				,	instructions		= g_ord(i).new_instructions
				,	mpack_consignment	= g_ord(i).mpack_consignment
				where	from_site_id		= g_ord(i).from_site_id
				and	order_id		= g_ord(i).order_id
				and	client_id		= g_ord(i).client_id
				;

				-- When no interface update was raised set shipment id. This field is not part of direct API therefore must be inserted directly
				update	dcsdba.order_header
				set 	uploaded_ws2pc_id	= g_ord(i).shipment_id
				where	from_site_id		= g_ord(i).from_site_id
				and	order_id		= g_ord(i).order_id
				and	client_id		= g_ord(i).client_id
				;

				-- Add order status change ITL to show status reason change
				cnl_cto_pck.create_itl_p( p_status		=> l_itl_status
							, p_code 		=> 'Order Status'
							, p_updateqty		=> 0
							, p_clientid 		=> g_ord(i).client_id
							, p_referenceid 	=> g_ord(i).order_id
							, p_stationid 		=> 'Automatic'
							, p_userid 		=> 'Centiro'
							, p_tmpnotes 		=> 'CSPENDING --> '||g_ord(i).status_reason_code
							, p_siteid		=> g_ord(i).from_site_id
							, p_ownerid 		=> g_ord(i).client_id
							, p_customerid 		=> g_ord(i).del_customer_id
							, p_fromstatus 		=> 'Hold'
							, p_tostatus 		=> 'Hold'
							, p_tagid		=> null
							, p_tolocation		=> null
							, p_extranotes 		=> substr(g_ord(i).new_instructions,1,80)
							);
			else
				-- When no interface update was raised set shipment id. This field is not part of direct API therefore must be inserted directly
				update	dcsdba.order_header
				set 	uploaded_ws2pc_id	= g_ord(i).shipment_id
				where	from_site_id		= g_ord(i).from_site_id
				and	order_id		= g_ord(i).order_id
				and	client_id		= g_ord(i).client_id
				;

				-- When BBX an update of the shipment is required after it was created
				if	g_ord(i).service_level = 'BBX'
				then
					insert
					into	cnl_sys.cnl_cto_shipments_to_update
					(	site_id
					,	client_id
					,	order_id
					,	shipment_id
					)
					values
					(	g_ord(i).from_site_id
					,	g_ord(i).client_id
					,	g_ord(i).order_id
					,	g_ord(i).shipment_id
					);
				end if;

				-- Add order status change ITL to show status reason change
				if	g_carrier_update = 'Y'
				then
					l_tmp_notes := 'CSPENDING --> '||g_ord(i).status_reason_code||' Forced carrier update.';
				else
					l_tmp_notes := 'CSPENDING --> '||g_ord(i).status_reason_code;
				end if;

				cnl_cto_pck.create_itl_p( p_status		=> l_itl_status
							, p_code 		=> 'Order Status'
							, p_updateqty		=> 0
							, p_clientid 		=> g_ord(i).client_id
							, p_referenceid 	=> g_ord(i).order_id
							, p_stationid 		=> 'Automatic'
							, p_userid 		=> 'Centiro'
							, p_tmpnotes 		=> l_tmp_notes
							, p_siteid		=> g_ord(i).from_site_id
							, p_ownerid 		=> g_ord(i).client_id
							, p_customerid 		=> g_ord(i).del_customer_id
							, p_fromstatus 		=> 'Hold'
							, p_tostatus 		=> 'Hold'
							, p_tagid		=> null
							, p_tolocation		=> null
							, p_extranotes 		=> substr(g_ord(i).new_instructions,1,80)
							);

				if	g_ord(i).addons is not null
				then
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Start adding addons to order_accessorial for order '
										 || g_ord(i).order_id
										 || ' from client '
										 || g_ord(i).client_id
										 || '.'
										 );

					add_order_accesorials_p	( p_addons_i	=> g_ord(i).addons
								, p_client_id_i	=> g_ord(i).client_id
								, p_order_id_i	=> g_ord(i).order_id
								);
				end if;
			end if;
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
end update_wms_order_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Emergency authenication key fetch
------------------------------------------------------------------------------------------------
procedure emergency_authenticate_key
is
	pragma autonomous_transaction;
begin
	cnl_cto_pck.fetch_authenticate_key_p;
end emergency_authenticate_key;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Process addshipment response
------------------------------------------------------------------------------------------------
procedure process_addshipment_response_p( p_json_string_i	in pljson)
is
	l_service_level		dcsdba.order_header.service_level%type;
	l_rtn			varchar2(30) 	:= 'process_addshipment_response_p';
	l_carrier_id		dcsdba.order_header.carrier_id%type;
	l_instructions		dcsdba.order_header.instructions%type;
	l_authenticate_key      cnl_sys.cnl_cto_authenticate_key.authenticate_key%type;
	l_list_of_labels	pljson_list	:= pljson_list();
	l_list_of_parcels	pljson_list	:= pljson_list();
	l_list_of_attributes	pljson_list	:= pljson_list();
	l_pljson_label		pljson		:= pljson();
	l_pljson_parcel		pljson		:= pljson();
	l_pljson_attribute	pljson		:= pljson();
	l_hub_code		varchar2(20);
	l_addons		varchar2(400);
	l_label			clob;
	l_label_base64		clob;
	l_parcel_id		varchar2(50);
	l_url			varchar2(400);
	l_tracking_number	varchar2(50);
	l_sscc			varchar2(50);
	l_ready			integer		:= 0;
	l_varchar		varchar2(32767);
	l_transform		clob;

begin 
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn ,'start');

	if	g_order_header.http_response_code	= '200'
	then
		-- fetch shipment attributes
		l_list_of_attributes	:= pljson_ext.get_json_list(pljson_ext.get_json(p_json_string_i, 'shipment'), 'attributes');
		-- loop attributes to find hub code and carrier service addons
		<<attribute_loop>>
		for i in 1..l_list_of_attributes.count
		loop
			-- fetch attribute from list
			l_pljson_attribute	:= pljson(l_list_of_attributes.get(i));
			-- Check if attribute is the hub code
			if	pljson_ext.get_string(l_pljson_attribute, 'code') = 'FirstHub'
			then
				-- Get hub code value
				l_hub_code	:= pljson_ext.get_string(l_pljson_attribute, 'value');
				g_order_header.delivery_point	:= l_hub_code;
			elsif	pljson_ext.get_string(l_pljson_attribute, 'code') = 'ADDONSERVICES'
			then
				-- Get hub code value
				l_addons	:= pljson_ext.get_string(l_pljson_attribute, 'value');

				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Found the following addons for order '
									 || g_order_header.order_id
									 || ' to add in order accesorials. addons: '
									 || l_addons
									 || '.'
									 );
				g_order_header.addons := l_addons;
			else
				continue attribute_loop;
			end if;
		end loop;

		if	g_carrier_update = 'Y'
		then
			-- Fetch labaldataarray from shipment segment
			l_list_of_labels	:= pljson_ext.get_json_list(pljson_ext.get_json(p_json_string_i, 'shipment'), 'labelDataArray');
			l_list_of_parcels	:= pljson_ext.get_json_list(pljson_ext.get_json(p_json_string_i, 'shipment'), 'parcels');

			if 	l_list_of_labels.count	> 0
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Found '
									 || to_char(l_list_of_labels.count)
									 || ' labels in response. Start loop true each label.'
									 );
			end if;

			for 	i in 1..l_list_of_labels.count
			loop
				-- Fetch first label from list of labels
				l_pljson_label	:= pljson(l_list_of_labels.get(i));
				l_label 	:= l_pljson_label.get_clob('data');
				l_label_base64	:= l_label;

				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Label extracted now decode Base64.'
									|| '.'
									);
				l_label 	:= cnl_sys.cnl_cto_parcel_pck.base64decode(l_label,'N');

				l_parcel_id	:= pljson_ext.get_string(l_pljson_label, 'externalParcelIdentifier');

				for i in 1..l_list_of_parcels.count
				loop
					l_pljson_parcel	:= pljson(l_list_of_parcels.get(i));
					if	pljson_ext.get_string(l_pljson_parcel, 'externalParcelIdentifier') = l_parcel_id
					then
						l_url			:= pljson_ext.get_string(l_pljson_parcel, 'parcelTrackingURL');
						l_tracking_number	:= pljson_ext.get_string(l_pljson_parcel, 'parcelTrackingNumber');
						l_sscc			:= pljson_ext.get_string(l_pljson_parcel, 'parcelSSCCNumber');
					else
						continue;
					end if;
				end loop;

				for i in 1..g_parcel.count
				loop
					if	g_parcel(i).parcel_id = l_parcel_id
					then
						g_parcel(i).shp_label 		:= l_label;
						g_parcel(i).tracking_number	:= l_tracking_number;
						g_parcel(i).cto_sscc		:= l_sscc;
						g_parcel(i).tracking_url	:= l_url;
						g_parcel(i).shp_label_base64	:= l_label_base64;
					else
						continue;
					end if;
				end loop;
			end loop;
		end if;

		-- extrac tcarrier and service
		l_carrier_id				:= pljson_ext.get_string(pljson_ext.get_json(p_json_string_i, 'shipment'), 'carrier');
		l_service_level				:= pljson_ext.get_string(pljson_ext.get_json(p_json_string_i, 'shipment'), 'carrierService');
		g_dif_carrier_id 			:= l_carrier_id;
		g_dif_service_lvl			:= l_service_level;

		-- When a second shipment is created because the initial shipment is closed
		-- We must check if the new shipment has the same carrier.
		-- If not cancel new shipment and send error mail
		if 	g_shp_closed = 'Y'
		and	(upper(g_order_header.carrier_id) 	!= upper(l_carrier_id) or upper(g_order_header.service_level) 	!= upper(l_service_level))
		then
			g_dif_carrier	:= 'Y';
		end if;

		if	g_cs_force = 'N'
		then
			g_order_header.carrier_id		:= upper(l_carrier_id);
			g_order_header.service_level		:= upper(l_service_level);
		end if;

		-- set new order instructions
		if	g_shp_closed = 'N'
		then
			if	g_order_header.instructions		is null
			and	g_order_header.saved_instructions	is not null
			and	g_order_header.saved_instructions	not like 'CTOMSG#%'
			then
				g_order_header.new_instructions	:= g_order_header.saved_instructions;
			elsif	g_order_header.instructions		is not null
			and	g_order_header.instructions		like 'CTOMSG#%'
			and	g_order_header.saved_instructions	is not null
			and	g_order_header.saved_instructions	not like 'CTOMSG#%'
			then
				g_order_header.new_instructions	:= g_order_header.saved_instructions;
			elsif	g_order_header.instructions		is not null
			and	g_order_header.instructions		not like 'CTOMSG#%'
			then
				g_order_header.new_instructions	:= g_order_header.instructions;
			end if;

			-- Set status reason code	
			g_order_header.status_reason_code	:= 'CSSUCCESS';

			-- Clear mpack consignment (used for Acss run)
			g_order_header.mpack_consignment	:= null;
		end if;
	else	
		if	g_shp_closed = 'N'
		then
			-- Extract error message
			l_instructions				:= substr('CTOMSG# '||pljson_ext.get_string(p_json_string_i, 'message'),1,180);
			if	l_instructions	like 'CTOMSG# Unauthorized%'
			then
				if	g_cs_force = 'N'
				then
					-- Authenticate key is not valid so we must generate a new key. Set this order back to original state 
					g_order_header.carrier_id 		:= null;
					g_order_header.service_level		:= null;
					g_order_header.status_reason_code	:= 'CSREQUIRED'; -- Reset for next run
				else
					g_order_header.status_reason_code	:= 'CSFORCED'; -- Reset for next run
				end if;

				g_order_header.new_instructions		:= g_order_header.instructions; -- Keep orinal value
				g_order_header.shipment_id		:= null;
				g_order_header.mpack_consignment	:= null;

				-- Fetch new authenticate key
				emergency_authenticate_key;

			elsif	g_order_header.http_response_code	in ('1009','1010')
			then
				-- add log record
				if	g_order_header.http_response_code = '1009'
				then
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Time out while starting web service call for order '
										 || g_order_header.order_id
										 || ' at site '
										 || g_order_header.from_site_id
										 || ' from client '
										 || g_order_header.client_id
										 || '. Order is reset for another try.'
										 );
				else
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Exception while executing web service call. Order has been reset to try again. For more details see CNL_ERROR.'
										 );
				end if;	
				-- Authenticate key is not valid so we must generate a new key. Set this order back to original state 
				if	g_cs_force = 'N'
				then
					g_order_header.carrier_id 		:= null;
					g_order_header.service_level		:= null;
					g_order_header.status_reason_code	:= 'CSREQUIRED'; -- Reset for next run
				else
					g_order_header.status_reason_code	:= 'CSFORCED'; -- Reset for next run
				end if;

				g_order_header.new_instructions		:= g_order_header.instructions; -- Keep orinal value
				g_order_header.shipment_id		:= null;
				g_order_header.mpack_consignment	:= null;

			else
				g_order_header.carrier_id		:= 'ERROR';
				g_order_header.service_level		:= 'ERROR';
				g_order_header.status_reason_code	:= 'CSERROR';
				g_order_header.cancel_cto_shipment	:= 'N';
				g_order_header.shipment_id		:= null;			
				g_order_header.mpack_consignment	:= null;

				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn ,': CSERROR Undefined');


				g_order_header.new_instructions		:= l_instructions;
				-- Save or update original instructions
				if	g_order_header.saved_instructions	is not null
				and	g_order_header.instructions		is not null
				and	g_order_header.instructions		!= g_order_header.saved_instructions
				and	g_order_header.instructions		not like 'CTOMSG#%'
				then
					update	cnl_sys.cnl_ohr_instructions
					set	instructions	= g_order_header.instructions
					where	order_id 	= g_order_header.order_id
					and	client_id	= g_order_header.client_id
					and	site_id		= g_order_header.from_site_id
					;
				elsif	g_order_header.saved_instructions	is null
				and	g_order_header.instructions		is not null
				and	g_order_header.instructions		not like 'CTOMSG#%'
				then
					insert
					into	cnl_sys.cnl_ohr_instructions
					(	site_id	
					,	client_id
					,	order_id
					,	instructions
					)
					values
					(	g_order_header.from_site_id
					,	g_order_header.client_id
					,	g_order_header.order_id
					,	g_order_header.instructions
					)
					;
				end if;
			end if;
		else
			-- Creating additional shipment failed while the initial shipment is already closed.
			-- Return N as not succesfull so the code can now generate an error label.
			g_ok := 'N';
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
end process_addshipment_response_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Add Shipment
------------------------------------------------------------------------------------------------
procedure cancel_shipment_p( p_shipment_id_i	in dcsdba.order_header.uploaded_ws2pc_id%type
			   , p_result_o		out varchar2
			   )
is
	l_rtn		varchar2(30) 	:= 'cancel_shipment_p';
	l_result 	varchar2(50);	
begin
		cnl_sys.cnl_cto_cancel_shp_or_par_pck.cancel_shipment_p( p_shipment_id_i 	=> to_char(p_shipment_id_i)
								       , p_result_o		=> l_result
								       );
		p_result_o	:= l_result;

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
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Add Shipment
------------------------------------------------------------------------------------------------
procedure add_shipment_p
is
	l_url			varchar2(1000)	:= cnl_util_pck.get_constant('CTO_ADDSHIPMENT_WEBSERVICE_URL');
	l_proxy			varchar2(50)	:= cnl_util_pck.get_constant('PROXY_SERVER');
	l_wallet		varchar2(400)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PATH');
	l_wall_passw		varchar2(50)	:= cnl_util_pck.get_constant('ORACLE_WALLET_PASSW');

	l_body_request		pljson		:= pljson();
	l_response_code		varchar2(30);
	l_response_reason	varchar2(4000);
	l_body_response		pljson		:= pljson();
	l_shipment		pljson		:= pljson();
	l_trace_key		integer;
	l_rtn			varchar2(30) 	:= 'add_shipment_p';
	l_shipment_id		varchar2(20);
begin
	-- Run some error checks first 
	if	g_shp_closed = 'N'
	then
		error_checks_p(l_rtn);
	end if;

	-- Set unique shipment id based on environment
	for	i in 1..g_ord.count
	loop
		-- When order contains an error it is skipped.
		if	g_ord(i).has_error = 'Y'
		then
			continue;
		-- When order does not contain any errors
		else
			-- Generate shipment id based on environment
			case
			when	g_database 	= 'DEVCNLJW'
			then	g_ord(i).shipment_id	:= to_char(10000000000+cnl_cto_shp_id_seq1.nextval);
			when	g_database 	= 'TSTCNLJW'
			then	g_ord(i).shipment_id	:= to_char(20000000000+cnl_cto_shp_id_seq1.nextval);
			when	g_database 	= 'ACCCNLJW'
			then	g_ord(i).shipment_id	:= to_char(30000000000+cnl_cto_shp_id_seq1.nextval);
			when	g_database 	= 'PRDCNLJW'
			then	g_ord(i).shipment_id	:= to_char(90000000000+cnl_cto_shp_id_seq1.nextval);
			end case;	

			-- Set current order as global variable
			g_order_header	:= g_ord(i);

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Start add shipment procedure for order_id '
								 || g_order_header.order_id
								 || ' from client '
								 || g_order_header.client_id
								 || ' with shipment id '
								 || to_char(g_order_header.shipment_id)
								 || '.'
								 );

			-- Build Json body
			--If set to "true", skips responding w shipment (faster)
			l_body_request.put('omitResponseContent',	false);
			-- Creates a return shipment when set to true
			l_body_request.put('createReturnShipment',	false);
			-- Not to be used	
			l_body_request.put('Rating',			false);
			-- Not to be used
			l_body_request.put('leadTimeCalculation',	false);
			-- not to be used?!	
			l_body_request.put('printerType',		'Zebra');
			-- not to be used
			l_body_request.put('freightPriceCalculation',	false);

			-- Build shipment body
			if	shipment_f
			then
				-- Compound Shipment
				l_body_request.put('shipment',			g_shipment);
				g_shipment					:= pljson();
			end if;

			-- add web service trace and store key for later update
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

			g_order_header.http_response_code 	:= l_response_code;
			g_order_header.http_response_reason 	:= l_response_reason;

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'http response for order ' 
								 || g_order_header.order_id 
								 || ' from client ' 
								 || g_order_header.client_id 
								 ||' = '
								 || l_response_code 
								 || ', '
								 || nvl(l_response_reason,'N')
								 || '.'
								 );

			-- add web service trace
			cnl_cto_pck.create_cto_trace_record( null, l_body_response, l_response_code, l_rtn, l_trace_key, l_trace_key);

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Finished add shipment procedure for order_id '
								 ||g_order_header.order_id
								 ||' from client '
								 ||g_order_header.client_id
								 ||'. Now continue processing response.'
								 );
			-- Start process response	
			process_addshipment_response_p( l_body_response);

			-- Update temp table
			g_ord(i).carrier_id		:= g_order_header.carrier_id; 
			g_ord(i).service_level		:= g_order_header.service_level;
			g_ord(i).new_instructions	:= g_order_header.new_instructions;
			g_ord(i).status_reason_code	:= g_order_header.status_reason_code;
			g_ord(i).http_response_code	:= g_order_header.http_response_code;
			g_ord(i).http_response_reason	:= g_order_header.http_response_reason;
			g_ord(i).cancel_cto_shipment	:= g_order_header.cancel_cto_shipment;
			g_ord(i).shipment_id		:= g_order_header.shipment_id;
			g_ord(i).mpack_consignment	:= g_order_header.mpack_consignment;
			g_ord(i).delivery_point		:= g_order_header.delivery_point;
			g_ord(i).addons			:= g_order_header.addons;
		end if;
	end loop;

	-- Start updating WMS order
	update_wms_order_p;

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

end add_shipment_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 25/04/2021
-- Purpose : Fetch orders for add shipment
------------------------------------------------------------------------------------------------
procedure fetch_orders_addship_p( p_site_id_i 	dcsdba.site.site_id%type
				, p_cs_force_i	varchar2 default 'N'
				)
is
	cursor c_orders( b_run_id	varchar2)
	is
		select	o.from_site_id
		, 	o.client_id
		,	o.order_id
		,	o.order_type
		, 	o.order_reference
		, 	o.purchase_order
		, 	o.uploaded_ws2pc_id	shipment_id
		,	o.mpack_consignment

		,	o.status
		, 	o.status_reason_code
		, 	o.instructions
		,	i.instructions		saved_instructions
		,	null			new_instructions
		,	o.order_volume
		,	o.order_weight

		, 	o.dispatch_method
		, 	o.user_def_type_1
		, 	o.carrier_id
		, 	o.service_level
		, 	o.delivery_point

		, 	o.creation_date
		, 	o.order_date
		, 	o.ship_by_date
		,	o.deliver_by_date

		,	o.export
		, 	o.tod
		, 	o.tod_place

		, 	lower(o.freight_charges) freight_charges
		, 	decode(o.export,'Y',nvl(o.inv_total_1,0.01),decode(o.country,'GBR',nvl(o.inv_total_1,0.01),'MCO',nvl(o.inv_total_1,0.01),o.inv_total_1)) inv_total_1
		, 	decode(o.export,'Y',nvl(o.inv_currency,'EUR'),decode(o.country,'GBR',nvl(o.inv_currency,'EUR'),'MCO',nvl(o.inv_currency,'EUR'),o.inv_currency)) inv_currency
		,	o.cod
		,	o.cod_value
		,	o.cod_currency
		,	o.cod_type

		,	o.seller_name
		,	o.seller_phone
		,	y.ce_eu_type	
		,	o.freight_terms		
		,	o.letter_of_credit

		--	delicery address
		,	o.customer_id		del_customer_id
		, 	o.vat_number		del_vat_number
		, 	o.contact		del_contact
		, 	o.contact_phone		del_contact_phone
		, 	o.contact_mobile	del_contact_mobile
		, 	o.contact_fax		del_contact_fax
		, 	o.contact_email		del_contact_email
		, 	o.name			del_name
		, 	o.address1		del_address1
		,	o.address2		del_address2
		, 	o.town			del_town
		,	o.county		del_country
		, 	o.postcode		del_postcode
		, 	o.country		del_county

		-- hub address
		,	o.hub_address_id
		,	o.hub_carrier_id
		,	o.hub_service_level
		, 	o.hub_vat_number
		,	o.hub_contact
		,	o.hub_contact_phone
		,	o.hub_contact_mobile
		,	o.hub_contact_fax
		,	o.hub_contact_email
		,	o.hub_name
		,	o.hub_address1
		,	o.hub_address2
		,	o.hub_town
		,	o.hub_county
		,	o.hub_postcode
		,	o.hub_country

		-- rd address
		,	rd.address_id		rd_address_id
		, 	rd.vat_number 		rd_vat_number
		,	rd.contact		rd_contact
		,	rd.contact_phone	rd_contact_phone
		,	rd.contact_mobile	rd_contact_mobile
		,	rd.contact_fax		rd_contact_fax
		,	rd.contact_email	rd_contact_email
		,	rd.name			rd_name
		,	rd.address1		rd_address1
		,	rd.address2		rd_address2
		,	rd.town			rd_town
		,	rd.county		rd_county
		,	rd.postcode		rd_postcode
		,	rd.country		rd_country

		-- huv address
		,	huv.address_id		huv_address_id
		, 	huv.vat_number 		huv_vat_number
		,	huv.contact		huv_contact
		,	huv.contact_phone	huv_contact_phone
		,	huv.contact_mobile	huv_contact_mobile
		,	huv.contact_fax		huv_contact_fax
		,	huv.contact_email	huv_contact_email
		,	huv.name		huv_name
		,	huv.address1		huv_address1
		,	huv.address2		huv_address2
		,	huv.town		huv_town
		,	huv.county		huv_county
		,	huv.postcode		huv_postcode
		,	huv.country		huv_country

		-- inv address
		,	o.inv_address_id
		, 	o.inv_vat_number
		,	o.inv_contact
		,	o.inv_contact_phone
		,	o.inv_contact_mobile
		,	o.inv_contact_fax
		,	o.inv_contact_email
		,	o.inv_name
		,	o.inv_address1
		,	o.inv_address2
		,	o.inv_town
		,	o.inv_county
		,	o.inv_postcode
		,	o.inv_country

		-- sid address
		,	s.address_id		sid_address_id
		, 	s.vat_number		sid_vat_number
		,	s.contact		sid_contact
		,	s.contact_phone		sid_contact_phone
		, 	s.contact_mobile	sid_contact_mobile
		, 	s.contact_fax		sid_contact_fax
		, 	s.contact_email		sid_contact_email
		, 	s.name			sid_name
		, 	s.address1		sid_address1
		,	s.address2		sid_address2
		, 	s.town			sid_town
		,	s.county		sid_county
		, 	s.postcode		sid_postcode
		, 	s.country		sid_country

		-- cid address
		,	c.address_id		cid_address_id
		, 	c.vat_number		cid_vat_number
		,	c.contact		cid_contact
		,	c.contact_phone		cid_contact_phone
		, 	c.contact_mobile	cid_contact_mobile
		, 	c.contact_fax		cid_contact_fax
		, 	c.contact_email		cid_contact_email
		, 	c.name			cid_name
		, 	c.address1		cid_address1
		,	c.address2		cid_address2
		, 	c.town			cid_town
		,	c.county		cid_county
		, 	c.postcode		cid_postcode
		, 	c.country		cid_country

		, 	null 			http_response_code
		,	null			http_response_reason
		,	'N'			cancel_cto_shipment
		,	decode(substr(o.consignment,1,4),'CNSL','Y','N')			consol_order
		,	decode(substr(o.consignment,1,4),'CNSL',consignment,null)		consol_id
		,	o.tax_amount_5		nacex_copies
		,	'N'			has_error
		,	null			addons
		from	dcsdba.order_header o
		inner
		join	dcsdba.country  y
		on	y.iso3_id		= o.country 
		left
		join	cnl_sys.cnl_ohr_instructions i
		on 	i.site_id		= o.from_site_id
		and	i.client_id		= o.client_id
		and	i.order_id		= o.order_id
		left 
		join	dcsdba.address rd
		on	rd.address_id 		= o.hub_address_id
		and	rd.address_type 	= '3rdParty'
		and	rd.client_id		= o.client_id
		left 
		join	dcsdba.address huv
		on	huv.address_id		= o.hub_vat_number
		and	huv.address_type 	= '3rdParty'
		and	huv.client_id		= o.client_id
		left
		join	dcsdba.address s
		on	s.address_id 		= o.sid_number
		and	s.client_id		= o.client_id
		left
		join	dcsdba.address c
		on	c.address_id 		= o.sid_number
		and	c.client_id		= o.client_id
		where	o.from_site_id 		= p_site_id_i
		and	o.mpack_consignment	= b_run_id
		and	o.status 		not in ('Complete','Cancelled','Delivered','Shipped')
		order
		by	o.creation_date 	asc
	;

	l_cnt				integer 	:= 0;
	l_rtn				varchar2(30) 	:= 'fetch_orders_addship_p';
	l_ins				integer		:= 0;
	l_run_id_key			integer		:= cto_saas_run_id_seq1.nextval;
	l_run_id			varchar2(30)	:= 'RUNNBR-'||to_char(l_run_id_key);
	l_limit				integer		:= 5;
	l_tmp_notes			varchar2(80);
	l_itl_status			integer;
	l_result			varchar2(50);
	l_status_reason_code		dcsdba.order_header.status_reason_code%type;
	l_from_status_reason_code	dcsdba.order_header.status_reason_code%type;
begin
	-- update all orders that will be processed during this run with unique run id and set status reason code to CSPENDING	
	-- Two situations exist.
	-- 1 Forced carrier update. This means a shipment already exists including parcels.
	-- 2 Forced carrier or regular carrier selection for new shipments.

	-- Initialise Session so auditing and users in transactions are shown as below
	dcsdba.libsession.InitialiseSession( userid		=> 'CENTIRO'
					   , groupid		=> null
					   , stationid		=> 'Automatic'
					   , wksgroupid		=> null
					   );

	-- When forced carrier update is true
	if	g_carrier_update = 'Y' 
	then
		l_status_reason_code	:= 'CSPENDING';

		-- Forced carrier update is always one order to process
		update	dcsdba.order_header o
		set 	o.status_reason_code 	= l_status_reason_code
		,	o.mpack_consignment 	= l_run_id
		where	o.mpack_consignment	is null
		and	o.client_id 		= g_client_id
		and	o.client_id		in 	(	
							select	c.client_id
							from	dcsdba.client_group_clients c
							where	c.client_group = 'CTOSAAS'
							)
		and	(	(	o.order_id		= g_order_id
				and	o.uploaded_ws2pc_id	= g_old_shipment_id
				or	(	g_order_id 		is null
					and	o.uploaded_ws2pc_id 	= g_old_shipment_id
					or	(	g_old_shipment_id	is null
						and	o.order_id		= g_order_id
						)
					)
				)
			)
		and	o.status_reason_code	= 'CSFORCED'
		and	o.status 		= 'Hold'
		and	o.from_site_id		= p_site_id_i
		;
	-- When forced carrier update is not true
	else
		l_status_reason_code	:= 'CSPENDING';

		-- normal processing orders that have no labels attached
		update	dcsdba.order_header o
		set	o.status_reason_code 	= l_status_reason_code
		,	o.mpack_consignment 	= l_run_id
		where	o.mpack_consignment	is null
		and	o.client_id		in 	(	
							select	c.client_id
							from	dcsdba.client_group_clients c
							where	c.client_group = 'CTOSAAS'
							)
		and	o.status_reason_code	= decode(p_cs_force_i,'Y','CSFORCED','CSREQUIRED')
		and	o.status 		= 'Hold'
		and	o.from_site_id		= p_site_id_i
		and	(	-- check for any existing labelled pallets or containers
			select	count(*)
			from	dcsdba.order_container c
			where	c.order_id 	= o.order_id 
			and	c.client_id	= o.client_id
			and	(	c.labelled 		= 'Y'
				or	c.pallet_labelled	= 'Y'
				)
			) = 0
		and	(	-- check for any existing labelled pallets or containers
			select	count(*)
			from	dcsdba.shipping_manifest s
			where	s.order_id 	= o.order_id 
			and	s.client_id	= o.client_id
			and	(	s.labelled 		= 'Y'
				or	s.pallet_labelled	= 'Y'
				)
			) = 0
		;
	end if;

	-- Set status reason code variables for creating ITL transactions.
	-- When ACSS is not required. (Not forced carrier update)
	if	p_cs_force_i = 'Y'
	then	
		l_status_reason_code		:= 'CSPENDING';
		l_from_status_reason_code	:= 'CSFORCED';

		-- When forced carrier update is true
		if	g_carrier_update = 'Y'
		then
			l_tmp_notes			:= 'CSFORCED --> CSPENDING (Forced carrier update)';
		-- when forced carrier update is not true
		else
			l_tmp_notes			:= 'CSFORCED --> CSPENDING';
		end if;

		g_cs_force			:= 'Y';
	-- When ACSS is required
	else
		l_status_reason_code		:= 'CSPENDING';
		l_from_status_reason_code	:= 'CSREQUIRED';
		l_tmp_notes			:= 'CSREQUIRED --> CSPENDING';
	end if;

	-- Open orders cursor
	open	c_orders( l_run_id );

	--Loop cursor records max 5
	loop
		fetch	c_orders
		bulk	collect
		into	g_ord
		limit 	l_limit;
		exit 
		when	g_ord.count = 0;

		if	g_ord.count > 0
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Start fetching orders for add shipment. Run id = '
								 || l_run_id
								 ||'.'
								 );

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Found '||to_char(g_ord.count)||' orders to process. Run id = '
								 || l_run_id
								 ||'.'
								 );
		end if;

		-- Add order status change ITL to show status reason change
		for 	i in 1..g_ord.count
		loop
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Create ITL transaction for order '
								 || g_ord(i).order_id
								 || ' from status reason '
								 || l_from_status_reason_code
								 || ' to status reason '
								 || l_status_reason_code
								 || '. run id = '
								 || l_run_id
								 || '.'
								 );

			l_itl_status	:= null;

			-- Add ITL transaction for status reason code update
			cnl_cto_pck.create_itl_p( p_status		=> l_itl_status
						, p_code 		=> 'Order Status'
						, p_updateqty		=> 0
						, p_clientid 		=> g_ord(i).client_id
						, p_referenceid 	=> g_ord(i).order_id
						, p_stationid 		=> 'Automatic'
						, p_userid 		=> 'Centiro'
						, p_tmpnotes 		=> l_tmp_notes
						, p_siteid		=> p_site_id_i
						, p_ownerid 		=> g_ord(i).client_id
						, p_customerid 		=> g_ord(i).del_customer_id
						, p_fromstatus 		=> g_ord(i).status
						, p_tostatus 		=> g_ord(i).status
						, p_tagid		=> null
						, p_tolocation		=> null
						, p_extranotes		=> 'Centiro ACSS and/or add shipment is started,'
						);
		end loop;	

		-- Cancel shipments
		for	i in 1..g_ord.count
		loop
			-- When forced carrier update is true
			if	g_carrier_update = 'Y'
			then
				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Forced carrier update requires the old shipment '
									 || g_old_shipment_id
									 || ' to be cancelled first. Run id '
									 || l_run_id
									 ||'.'
									 );
				cancel_shipment_p( p_shipment_id_i	=> g_old_shipment_id
						 , p_result_o		=> l_result
						 );
				-- It is possible that a shipment does not exist anymore.
				-- This will generate an error 400 but without description.
				-- We can't see if a shipment cancel error was raised because it does not exist or because another reason.
				-- Therefore when an error occures we simply continue as if nothing happened.
				-- Note that this could cause another issue if a shipment does exist containing the same parcels. 
				-- NO SOLUTION HAS BEEN FOUND YET.

				-- When cancellation was succesfull
				if 	l_result = '200'
				then
					null;
				-- When cancellation failed
				else
					-- Add logging
					cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
										 , 'Adding error label to tell operator shipment '
										 || g_old_shipment_id
										 || ' was not deleted from Centiro. It could be because it does not exist or some other thing went wrong.'
										 );

					-- Insert error label telling operator that the old shipment was possibly not deleted
					insert
					into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
								   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
								   , update_dstamp, printer_name, dws, copies, shp_label_base64
								   )
					values
					(	g_ord(i).client_id
					,	g_ord(i).from_site_id
					,	g_ord(i).order_id
					,	'NOSHIPMENTID'
					,	'NOPARCELID'
					,	null
					,	null
					,	null
					,	null
					,	null
					,	null
					,	null
					,	g_rtk_key
					,	null
					,	null
					,	cnl_cto_pck.create_zpl_text_label_f( 'Failed to cancel the previous shipment '
										   || g_old_shipment_id
										   || ' from order id '
										   || g_ord(i).order_id
										   ||' in Centiro. Process continues but contact CS about the cancelation issue.'
										   )
					,	null
					,	null
					,	null
					,	null
					,	null
					,	sysdate
					,	'Error'
					,	null
					,	g_printer
					,	'N'
					,	1
					,	cnl_cto_parcel_pck.base64decode( cnl_cto_pck.create_zpl_text_label_f( 'Failed to cancel the previous shipment '
														    || g_old_shipment_id
														    || ' from order id '
														    || g_ord(i).order_id
														    ||' in Centiro. Process continues but contact CS about the cancelation issue.'
														    )
									       , 'Y')
					);

				end if;
			-- When forced carrier update is not true
			else
				if	g_ord(i).shipment_id is not null
				then
					cancel_shipment_p( p_shipment_id_i	=> g_ord(i).shipment_id
							 , p_result_o		=> l_result
							 );
					-- It is possible that a shipment does not exist anymore.
					-- This will generate an error 400 but without description.
					-- We can't see if a shipment cancel error was raised because it does not exist or because another reason.
					-- Therefore when an error occures we simply continue as if nothing happened.
					-- At this point no actions have ben defined when there was an error during cancellation
					if 	l_result = '200'
					then
						null;
					else
						null;
					end if;
				end if;
			end if;
		end loop;

		-- Start add shipment for the set orders captured
		add_shipment_p;

		-- Commit after every 5 orders
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							  , 'Commit add shipment for run id '
							  || l_run_id
							  || 'for '
							  || to_char(g_ord.count)
							  || ' Orders.'
							  );
		commit;
	end loop;

	close 	c_orders;
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
		rollback;
end fetch_orders_addship_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 25/04/2021
-- Purpose : Fetch orders for add shipment
------------------------------------------------------------------------------------------------
procedure Shipment_closed_p( p_shipment_id_i 	in dcsdba.order_header.uploaded_ws2pc_id%type
			   , p_order_id_i	in dcsdba.order_header.order_id%type
			   , p_client_id_i	in dcsdba.order_header.client_id%type
			   , p_dif_carrier_o	out varchar2
			   , p_carrier_id_o	out varchar2
			   , p_service_level_o	out varchar2
			   , p_new_shp_id_o	out number
			   , p_ok_o		out varchar2
			   )
is
	cursor c_orders
	is
		select	o.from_site_id
		, 	o.client_id
		,	o.order_id
		,	o.order_type
		, 	o.order_reference
		, 	o.purchase_order
		, 	null	--o.uploaded_ws2pc_id	shipment_id
		,	o.mpack_consignment

		,	o.status
		, 	o.status_reason_code
		, 	o.instructions
		,	null			saved_instructions
		,	i.instructions		new_instructions
		,	o.order_volume
		,	o.order_weight

		, 	o.dispatch_method
		, 	o.user_def_type_1
		, 	o.carrier_id
		, 	o.service_level
		, 	o.delivery_point

		, 	o.creation_date
		, 	o.order_date
		, 	o.ship_by_date
		,	o.deliver_by_date

		,	o.export
		, 	o.tod
		, 	o.tod_place

		, 	lower(o.freight_charges) freight_charges
		, 	decode(o.export,'Y',nvl(o.inv_total_1,0.01),decode(o.country,'GBR',nvl(o.inv_total_1,0.01),'MCO',nvl(o.inv_total_1,0.01),o.inv_total_1)) inv_total_1
		, 	decode(o.export,'Y',nvl(o.inv_currency,'EUR'),decode(o.country,'GBR',nvl(o.inv_currency,'EUR'),'MCO',nvl(o.inv_currency,'EUR'),o.inv_currency)) inv_currency
		,	o.cod
		,	o.cod_value
		,	o.cod_currency
		,	o.cod_type

		,	o.seller_name
		,	o.seller_phone
		,	y.ce_eu_type	
		,	o.freight_terms		
		,	o.letter_of_credit

		--	delicery address
		,	o.customer_id		del_customer_id
		, 	o.vat_number		del_vat_number
		, 	o.contact		del_contact
		, 	o.contact_phone		del_contact_phone
		, 	o.contact_mobile	del_contact_mobile
		, 	o.contact_fax		del_contact_fax
		, 	o.contact_email		del_contact_email
		, 	o.name			del_name
		, 	o.address1		del_address1
		,	o.address2		del_address2
		, 	o.town			del_town
		,	o.county		del_country
		, 	o.postcode		del_postcode
		, 	o.country		del_county

		-- hub address
		,	o.hub_address_id
		,	o.hub_carrier_id
		,	o.hub_service_level
		, 	o.hub_vat_number
		,	o.hub_contact
		,	o.hub_contact_phone
		,	o.hub_contact_mobile
		,	o.hub_contact_fax
		,	o.hub_contact_email
		,	o.hub_name
		,	o.hub_address1
		,	o.hub_address2
		,	o.hub_town
		,	o.hub_county
		,	o.hub_postcode
		,	o.hub_country

		-- rd address
		,	rd.address_id		rd_address_id
		, 	rd.vat_number 		rd_vat_number
		,	rd.contact		rd_contact
		,	rd.contact_phone	rd_contact_phone
		,	rd.contact_mobile	rd_contact_mobile
		,	rd.contact_fax		rd_contact_fax
		,	rd.contact_email	rd_contact_email
		,	rd.name			rd_name
		,	rd.address1		rd_address1
		,	rd.address2		rd_address2
		,	rd.town			rd_town
		,	rd.county		rd_county
		,	rd.postcode		rd_postcode
		,	rd.country		rd_country

		-- huv address
		,	huv.address_id		huv_address_id
		, 	huv.vat_number 		huv_vat_number
		,	huv.contact		huv_contact
		,	huv.contact_phone	huv_contact_phone
		,	huv.contact_mobile	huv_contact_mobile
		,	huv.contact_fax		huv_contact_fax
		,	huv.contact_email	huv_contact_email
		,	huv.name		huv_name
		,	huv.address1		huv_address1
		,	huv.address2		huv_address2
		,	huv.town		huv_town
		,	huv.county		huv_county
		,	huv.postcode		huv_postcode
		,	huv.country		huv_country

		-- inv address
		,	o.inv_address_id
		, 	o.inv_vat_number
		,	o.inv_contact
		,	o.inv_contact_phone
		,	o.inv_contact_mobile
		,	o.inv_contact_fax
		,	o.inv_contact_email
		,	o.inv_name
		,	o.inv_address1
		,	o.inv_address2
		,	o.inv_town
		,	o.inv_county
		,	o.inv_postcode
		,	o.inv_country

		-- sid address
		,	s.address_id		sid_address_id
		, 	s.vat_number		sid_vat_number
		,	s.contact		sid_contact
		,	s.contact_phone		sid_contact_phone
		, 	s.contact_mobile	sid_contact_mobile
		, 	s.contact_fax		sid_contact_fax
		, 	s.contact_email		sid_contact_email
		, 	s.name			sid_name
		, 	s.address1		sid_address1
		,	s.address2		sid_address2
		, 	s.town			sid_town
		,	s.county		sid_county
		, 	s.postcode		sid_postcode
		, 	s.country		sid_country

		-- cid address
		,	c.address_id		cid_address_id
		, 	c.vat_number		cid_vat_number
		,	c.contact		cid_contact
		,	c.contact_phone		cid_contact_phone
		, 	c.contact_mobile	cid_contact_mobile
		, 	c.contact_fax		cid_contact_fax
		, 	c.contact_email		cid_contact_email
		, 	c.name			cid_name
		, 	c.address1		cid_address1
		,	c.address2		cid_address2
		, 	c.town			cid_town
		,	c.county		cid_county
		, 	c.postcode		cid_postcode
		, 	c.country		cid_country

		, 	null 			http_response_code
		,	null			http_response_reason
		,	'N'			cancel_cto_shipment
		,	decode(substr(o.consignment,1,4),'CNSL','Y','N')			consol_order
		,	decode(substr(o.consignment,1,4),'CNSL',consignment,null)		consol_id
		,	o.tax_amount_5		nacex_copies
		,	'N'			has_error
		,	null			addons
		from	dcsdba.order_header o
		inner
		join	dcsdba.country  y
		on	y.iso3_id		= o.country 
		left
		join	cnl_sys.cnl_ohr_instructions i
		on 	i.site_id		= o.from_site_id
		and	i.client_id		= o.client_id
		and	i.order_id		= o.order_id
		left 
		join	dcsdba.address rd
		on	rd.address_id 		= o.hub_address_id
		and	rd.address_type 	= '3rdParty'
		and	rd.client_id		= o.client_id
		left 
		join	dcsdba.address huv
		on	huv.address_id		= o.hub_vat_number
		and	huv.address_type 	= '3rdParty'
		and	huv.client_id		= o.client_id
		left
		join	dcsdba.address s
		on	s.address_id 		= o.sid_number
		and	s.client_id		= o.client_id
		left
		join	dcsdba.address c
		on	c.address_id 		= o.sid_number
		and	c.client_id		= o.client_id
		where	o.uploaded_ws2pc_id 	= p_shipment_id_i
		and	o.client_id		= p_client_id_i
		and	o.order_id		= p_order_id_i
		order
		by	o.creation_date 	asc
	;

	l_cnt				integer 	:= 0;
	l_rtn				varchar2(30) 	:= 'Shipment_closed_p';
	l_ins				integer		:= 0;
	l_run_id_key			integer		:= cto_saas_run_id_seq1.nextval;
	l_run_id			varchar2(30)	:= 'RUNNBR-'||to_char(l_run_id_key);
	l_limit				integer		:= 5;
	l_tmp_notes			varchar2(80);
	l_itl_status			integer;
	l_result			varchar2(50);
	l_status_reason_code		dcsdba.order_header.status_reason_code%type;
	l_from_status_reason_code	dcsdba.order_header.status_reason_code%type;
begin
	-- Initialise Session so auditing and users in transactions are shown as below
	dcsdba.libsession.InitialiseSession( userid		=> 'CENTIRO'
					   , groupid		=> null
					   , stationid		=> 'Automatic'
					   , wksgroupid		=> null
					   );

	g_shp_closed	:= 'Y';
	g_cs_force 	:= 'Y';

	-- Open orders cursor
	open	c_orders;

	--Loop cursor records max 5
	loop
		fetch	c_orders
		bulk	collect
		into	g_ord
		limit 	l_limit;
		exit 
		when	g_ord.count = 0;

		if	g_ord.count > 0
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Start fetching order for closed shipment '||to_char(p_shipment_id_i)
								 ||'.'
								 );

			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Found '||to_char(g_ord.count)||' orders. '
								 ||'.'
								 );
		end if;

		-- Start add shipment for the set orders captured
		add_shipment_p;

		-- Commit after every 5 orders
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							  , 'Commit add shipment for run id '
							  || l_run_id
							  || 'for '
							  || to_char(g_ord.count)
							  || ' Orders.'
							  );
		commit;
	end loop;

	p_ok_o			:= g_ok;
	p_new_shp_id_o		:= g_order_header.shipment_id;
	p_carrier_id_o		:= g_dif_carrier_id;
	p_service_level_o	:= g_dif_service_lvl;
	p_dif_carrier_o		:= g_dif_carrier;

	if	g_dif_carrier = 'Y'
	and	g_ok = 'Y'
	then
		cancel_shipment_p( p_shipment_id_i	=> g_order_header.shipment_id
				 , p_result_o		=> l_result
				 );
	end if;

	close 	c_orders;
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
		rollback;
end Shipment_closed_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 25/04/2021
-- Purpose : Fetch orders for add shipment
------------------------------------------------------------------------------------------------
procedure force_carrier_update_p( p_site_id_i 		dcsdba.site.site_id%type
			        , p_client_id_i		dcsdba.client.client_id%type
			        , p_printer_i		varchar2
			        , p_order_id_i		dcsdba.order_header.order_id%type		default null
			        , p_shipment_id_i	dcsdba.order_header.uploaded_ws2pc_id%type	default null
			        , p_rtk_key_i		dcsdba.run_task.key%type
			        )
is
	cursor	c_shipment_id
	is
		select	o.uploaded_ws2pc_id
		from	dcsdba.order_header o
		where	o.order_id 	= p_order_id_i
		and	o.client_id	= p_client_id_i
		and	o.from_site_id	= p_site_id_i
	;

	cursor	c_label_check( b_order_id 	varchar2
			     , b_client_id	varchar2
			     )
	is
		select 	sum(tot)
		from	(
			select 	count(*) tot
			from	dcsdba.order_container
			where	order_id = b_order_id
			and	client_id = b_client_id
			and	(labelled = 'Y' or pallet_labelled = 'Y')
			union
			select 	count(*) tot
			from	dcsdba.shipping_manifest
			where	order_id = b_order_id
			and	client_id = b_client_id
			and	(labelled = 'Y' or pallet_labelled = 'Y')
			)
	;

	l_rtn			varchar2(30) 	:= 'force_carrier_update_p';
	l_shipment_id		dcsdba.order_header.uploaded_ws2pc_id%type;
	l_cancel_result		varchar2(30);

	l_error_label		clob;
	e_missing_parameters	exception;
	e_cancel_shp_failed	exception;
	l_labels 		integer	:= 1;
begin
	-- Add logging
	cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
						 ,  'Starting a forced carrier and service update for existing shipment at site "'
						 || p_site_id_i
						 || '" for client "'
						 || p_client_id_i
						 || '" using order id "'
						 || p_order_id_i
						 || '" or shipment id "'
						 || p_shipment_id_i
						 || '". Labels to be printed at this printer "'
						 || p_printer_i
						 || '" using run task "'
						 || p_rtk_key_i
						 || '".'
						 );

	-- Check if one of the required parameters is in place
	if	p_order_id_i 	is null
	and	p_shipment_id_i is null
	then
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'No order or shipment id defined during forced carrier update for run task ' ||p_rtk_key_i||'.'
							 );	
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 , 'Create error label for run task ' ||p_rtk_key_i||'.'
							 );	

		-- Insert error label telling operator no order and/or shipment id entered
		insert
		into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
					   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
					   , update_dstamp, printer_name, dws, copies, shp_label_base64
					   )
		values
		(	p_client_id_i
		,	p_site_id_i
		,	'NOORDERID'
		,	'NOSHIPMENTID'
		,	'NOPARCELID'
		,	null
		,	null
		,	null
		,	null
		,	null
		,	null
		,	null
		,	p_rtk_key_i
		,	null
		,	null
		,	cnl_cto_pck.create_zpl_text_label_f( 'No order id and shipment id where entered while running the update carrier report. Please try again.')
		,	null
		,	null
		,	null
		,	null
		,	null
		,	sysdate
		,	'Error'
		,	null
		,	p_printer_i
		,	'N'
		,	1
		, 	cnl_cto_parcel_pck.base64decode( cnl_cto_pck.create_zpl_text_label_f( 'No order id and shipment id where entered while running the update carrier report. Please try again.'), 'Y')
		);
	else
		-- Fetch current shipment id if not send by operator
		if	p_shipment_id_i is null
		then
			-- Add logging
			cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
								 , 'Shipment id not provided by operator. Start fetching shipment id from order '
								 || p_order_id_i
								 ||'.'
								 );	

			-- Fetch shipment id
			open 	c_shipment_id;
			fetch	c_shipment_id
			into	l_shipment_id;
			if	c_shipment_id%notfound
			then
				close	c_shipment_id;

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'No shipment id found for order '||p_order_id_i||'. Create error label'
									 );	

				-- Insert error label telling operator no shipment id found for order
				insert
				into	cnl_cto_ship_labels( client_id, site_id, order_id, shipment_id, parcel_id, pallet_id, container_id, parcel_height, parcel_width, parcel_depth, parcel_volume, parcel_weight
							   , run_task_key, pallet_or_container, pallet_type, shp_label, carrier_id, service_level, tracking_number, tracking_url, cto_sscc, creation_dstamp, status
							   , update_dstamp, printer_name, dws, copies, shp_label_base64
							   )
				values
				(	p_client_id_i
				,	p_site_id_i
				,	p_order_id_i
				,	'NOSHIPMENTID'
				,	'NOPARCELID'
				,	null
				,	null
				,	null
				,	null
				,	null
				,	null
				,	null
				,	p_rtk_key_i
				,	null
				,	null
				,	cnl_cto_pck.create_zpl_text_label_f( 'No shipment id exists for order '
									   ||p_order_id_i
									   ||'. Check if the order actually exists in Centiro. If not use CSFORCED or CSREQUIRED.'
									   )
				,	null
				,	null
				,	null
				,	null
				,	null
				,	sysdate
				,	'Error'
				,	null
				,	p_printer_i
				,	'N'
				,	1
				,	cnl_cto_parcel_pck.base64decode( cnl_cto_pck.create_zpl_text_label_f( 'No shipment id exists for order '||p_order_id_i||'. Check if the order actually exists in Centiro. If not use CSFORCED or CSREQUIRED.'), 'Y')
				);

			else
				close	c_shipment_id;			

				-- Add logging
				cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
									 , 'Found shipment id '
									 || l_shipment_id
									 || ' from order '
									 || p_order_id_i
									 ||'.'
									 );	
			end if;
		else
			l_shipment_id := p_shipment_id_i;		
		end if;

		-- Global variables to hold the old shipment id needed to update tables this variable will change the behavior of the add shipment procedures
		g_carrier_update	:= 'Y';
		g_old_shipment_id	:= l_shipment_id;
		g_printer		:= p_printer_i;
		g_client_id		:= p_client_id_i;
		g_order_id		:= p_order_id_i;
		g_rtk_key		:= p_rtk_key_i;

		-- check if shipment has labels
		if	p_order_id_i is not null
		and 	p_client_id_i is not null
		then
			open	c_label_check(p_order_id_i, p_client_id_i);
			fetch 	c_label_check into l_labels;
			close 	c_label_check;
		end if;

		if	l_labels > 0
		then
			-- Start fetching order details
			fetch_orders_addship_p( p_site_id_i 	=> p_site_id_i
					      , p_cs_force_i	=> 'Y'
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
						  , p_routine_parameters_i	=> 'order '
										|| p_order_id_i
										|| ' client ' 
										|| p_client_id_i
										|| ' site id '
										|| p_site_id_i				-- list of all parameters involved
						  , p_comments_i		=> 'Could not find a shipment id for order'					-- Additional comments describing the issue
						  );
		-- Add logging
		cnl_sys.cnl_cto_pck.create_cto_log_record( g_pck||'.'||l_rtn
							 ,  'Unhandled exception during forced carrier update at site "'
							 || p_site_id_i
							 || '" and for client "'
							 || p_client_id_i
							 || '" using order id "'
							 || p_order_id_i
							 || '" or shipment id "'
							 || p_shipment_id_i
							 || '". Labels to be printed at this printer "'
							 || p_printer_i
							 || '" using run task "'
							 || p_rtk_key_i
							 || '". Check CNL_ERROR.'
							 );
end force_carrier_update_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels 23/04/2021
-- Purpose : Innitialize 
------------------------------------------------------------------------------------------------
begin
	null;
end cnl_cto_addshipment_pck;