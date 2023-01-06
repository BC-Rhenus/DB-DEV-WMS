CREATE OR REPLACE PROCEDURE "CNL_SYS"."UPLOAD_TEST_ORDERS_P" (p_test_key_i integer)
is
	o_key integer;
	cursor c_ord
	is
	select	 client_id,order_id,
	 order_type,work_order_type,status,move_task_status,priority,ship_dock,work_group,consignment,delivery_point,load_sequence,from_site_id,to_site_id,owner_id,
	 customer_id,order_date,ship_by_date,deliver_by_date,purchase_order,contact,contact_phone,contact_mobile,contact_fax,contact_email,name,address1,address2,town,county,postcode,
	 country,instructions,repack,carrier_id,dispatch_method,service_level,fastest_carrier,cheapest_carrier,inv_address_id,inv_contact,inv_contact_phone,inv_contact_mobile,inv_contact_fax,
	 inv_contact_email,inv_name,inv_address1,inv_address2,inv_town,inv_county,inv_postcode,inv_country,psft_dmnd_srce,psft_order_id,site_replen,cid_number,sid_number,location_number,
	 freight_charges,disallow_merge_rules,export,soh_id,repack_loc_id,user_def_type_1,user_def_type_2,user_def_type_3,user_def_type_4,user_def_type_5,user_def_type_6,user_def_type_7,user_def_type_8,
	 user_def_chk_1,user_def_chk_2,user_def_chk_3,user_def_chk_4,user_def_date_1,user_def_date_2,user_def_date_3,user_def_date_4,user_def_num_1,user_def_num_2,user_def_num_3,user_def_num_4,
	 user_def_note_1,user_def_note_2,ce_reason_code,ce_reason_notes,ce_order_type,ce_customs_customer,ce_excise_customer,ce_instructions,delivered_dstamp,signatory,route_id,
	 cross_dock_to_site,web_service_alloc_immed,web_service_alloc_clean,disallow_short_ship,hub_address_id,hub_contact,hub_contact_phone,hub_contact_mobile,hub_contact_fax,
	 hub_contact_email,hub_name,hub_address1,hub_address2,hub_town,hub_county,hub_postcode,hub_country,hub_carrier_id,hub_service_level,status_reason_code,stage_route_id,
	 single_order_sortation,force_single_carrier,expected_volume,expected_weight,expected_value,tod,tod_place,language,seller_name,seller_phone,documentation_text_1,documentation_text_2,
	 documentation_text_3,cod,cod_value,cod_currency,cod_type,vat_number,inv_vat_number,hub_vat_number,print_invoice,inv_reference,inv_dstamp,inv_currency,letter_of_credit,
	 payment_terms,subtotal_1,subtotal_2,subtotal_3,subtotal_4,freight_cost,freight_terms,insurance_cost,misc_charges,discount,other_fee,inv_total_1,inv_total_2,inv_total_3,inv_total_4,
	 tax_rate_1,tax_basis_1,tax_amount_1,tax_rate_2,tax_basis_2,tax_amount_2,tax_rate_3,tax_basis_3,tax_amount_3,tax_rate_4,tax_basis_4,tax_amount_4,tax_rate_5,tax_basis_5,
	 tax_amount_5,order_reference,collective_mode,collective_sequence,start_by_date,metapack_carrier_pre,shipment_group,freight_currency,ncts,gln,hub_gln,inv_gln,allow_pallet_pick,
	 split_shipping_units,vol_pck_sscc_label,allocation_priority,trax_use_hub_addr,mpack_consignment,mpack_nominated_dstamp,direct_to_store,vol_ctr_label_format,retailer_id,
	 carrier_bags,session_time_zone_name,time_zone_name,nls_calendar,client_group
	from	cnl_sys.cnl_test_if_order_header
	where	test_key = p_test_key_i
	;
	cursor c_lin(b_ord varchar2)
	is
	select	client_id,order_id,line_id,host_order_id,host_line_id,sku_id,customer_sku_id,config_id,tracking_level,batch_id,batch_mixing,shelf_life_days,shelf_life_percent,origin_id,
	condition_id,lock_code,spec_code,qty_ordered,allocate,back_ordered,kit_split,deallocate,notes,psft_int_line,psft_schd_line,psft_dmnd_line,sap_pick_req,disallow_merge_rules,
	line_value,rule_id,soh_id,user_def_type_1,user_def_type_2,user_def_type_3,user_def_type_4,user_def_type_5,user_def_type_6,user_def_type_7,user_def_type_8,user_def_chk_1,
	user_def_chk_2,user_def_chk_3,user_def_chk_4,user_def_date_1,user_def_date_2,user_def_date_3,user_def_date_4,user_def_num_1,user_def_num_2,user_def_num_3,user_def_num_4,
	user_def_note_1,user_def_note_2,task_per_each,use_pick_to_grid,ignore_weight_capture,stage_route_id,min_qty_ordered,max_qty_ordered,expected_volume,expected_weight,
	expected_value,customer_sku_desc1,customer_sku_desc2,purchase_order,product_price,product_currency,documentation_unit,extended_price,tax_1,tax_2,documentation_text_1,
	serial_number,owner_id,collective_mode,collective_sequence,ce_receipt_type,ce_coo,kit_plan_id,location_id,unallocatable,min_full_pallet_perc,max_full_pallet_perc,
	full_tracking_level_only,substitute_grade,disallow_substitution,session_time_zone_name,time_zone_name,nls_calendar,client_group,
	'A','Pending',null,sysdate
	from	cnl_sys.cnl_test_if_order_line
	where	test_key = p_test_key_i
	and	order_id = b_ord;
	--
	l_order varchar2(20);
begin
	for i in c_ord
	loop
		dbms_output.put_line(i.order_id);
		l_order := i.order_id||cnl_if_order_id_seq1.nextval;
		dbms_output.put_line(l_order);
		insert into dcsdba.interface_order_header
		(key,client_id,order_id,order_type,work_order_type,status,move_task_status,priority,ship_dock,work_group,consignment,delivery_point,load_sequence,from_site_id,to_site_id,owner_id,
		 customer_id,order_date,ship_by_date,deliver_by_date,purchase_order,contact,contact_phone,contact_mobile,contact_fax,contact_email,name,address1,address2,town,county,postcode,
		 country,instructions,repack,carrier_id,dispatch_method,service_level,fastest_carrier,cheapest_carrier,inv_address_id,inv_contact,inv_contact_phone,inv_contact_mobile,inv_contact_fax,
		 inv_contact_email,inv_name,inv_address1,inv_address2,inv_town,inv_county,inv_postcode,inv_country,psft_dmnd_srce,psft_order_id,site_replen,cid_number,sid_number,location_number,
		 freight_charges,disallow_merge_rules,export,soh_id,repack_loc_id,user_def_type_1,user_def_type_2,user_def_type_3,user_def_type_4,user_def_type_5,user_def_type_6,user_def_type_7,user_def_type_8,
		 user_def_chk_1,user_def_chk_2,user_def_chk_3,user_def_chk_4,user_def_date_1,user_def_date_2,user_def_date_3,user_def_date_4,user_def_num_1,user_def_num_2,user_def_num_3,user_def_num_4,
		 user_def_note_1,user_def_note_2,ce_reason_code,ce_reason_notes,ce_order_type,ce_customs_customer,ce_excise_customer,ce_instructions,delivered_dstamp,signatory,route_id,
		 cross_dock_to_site,web_service_alloc_immed,web_service_alloc_clean,disallow_short_ship,hub_address_id,hub_contact,hub_contact_phone,hub_contact_mobile,hub_contact_fax,
		 hub_contact_email,hub_name,hub_address1,hub_address2,hub_town,hub_county,hub_postcode,hub_country,hub_carrier_id,hub_service_level,status_reason_code,stage_route_id,
		 single_order_sortation,force_single_carrier,expected_volume,expected_weight,expected_value,tod,tod_place,language,seller_name,seller_phone,documentation_text_1,documentation_text_2,
		 documentation_text_3,cod,cod_value,cod_currency,cod_type,vat_number,inv_vat_number,hub_vat_number,print_invoice,inv_reference,inv_dstamp,inv_currency,letter_of_credit,
		 payment_terms,subtotal_1,subtotal_2,subtotal_3,subtotal_4,freight_cost,freight_terms,insurance_cost,misc_charges,discount,other_fee,inv_total_1,inv_total_2,inv_total_3,inv_total_4,
		 tax_rate_1,tax_basis_1,tax_amount_1,tax_rate_2,tax_basis_2,tax_amount_2,tax_rate_3,tax_basis_3,tax_amount_3,tax_rate_4,tax_basis_4,tax_amount_4,tax_rate_5,tax_basis_5,
		 tax_amount_5,order_reference,collective_mode,collective_sequence,start_by_date,metapack_carrier_pre,shipment_group,freight_currency,ncts,gln,hub_gln,inv_gln,allow_pallet_pick,
		 split_shipping_units,vol_pck_sscc_label,allocation_priority,trax_use_hub_addr,mpack_consignment,mpack_nominated_dstamp,direct_to_store,vol_ctr_label_format,retailer_id,
		 carrier_bags,session_time_zone_name,time_zone_name,nls_calendar,client_group,merge_action,merge_status,merge_error,merge_dstamp)
		 values(dcsdba.if_oh_pk_seq.nextval,
		 i.client_id,
		 l_order,
		 i.order_type,i.work_order_type,i.status,i.move_task_status,i.priority,i.ship_dock,i.work_group,i.consignment,i.delivery_point,i.load_sequence,i.from_site_id,i.to_site_id,
		 i.owner_id,i.customer_id,i.order_date,i.ship_by_date,i.deliver_by_date,i.purchase_order,i.contact,i.contact_phone,i.contact_mobile,i.contact_fax,i.contact_email,i.name,
		 i.address1,i.address2,i.town,i.county,i.postcode,i.country,i.instructions,i.repack,i.carrier_id,i.dispatch_method,i.service_level,i.fastest_carrier,i.cheapest_carrier,
		 i.inv_address_id,i.inv_contact,i.inv_contact_phone,i.inv_contact_mobile,i.inv_contact_fax,i.inv_contact_email,i.inv_name,i.inv_address1,i.inv_address2,i.inv_town,
		 i.inv_county,i.inv_postcode,i.inv_country,i.psft_dmnd_srce,i.psft_order_id,i.site_replen,i.cid_number,i.sid_number,i.location_number,i.freight_charges,i.disallow_merge_rules,
		 i.export,i.soh_id,i.repack_loc_id,i.user_def_type_1,i.user_def_type_2,i.user_def_type_3,i.user_def_type_4,i.user_def_type_5,i.user_def_type_6,i.user_def_type_7,i.user_def_type_8,
		 i.user_def_chk_1,i.user_def_chk_2,i.user_def_chk_3,i.user_def_chk_4,i.user_def_date_1,i.user_def_date_2,i.user_def_date_3,i.user_def_date_4,i.user_def_num_1,i.user_def_num_2,
		 i.user_def_num_3,i.user_def_num_4,i.user_def_note_1,i.user_def_note_2,i.ce_reason_code,i.ce_reason_notes,i.ce_order_type,i.ce_customs_customer,i.ce_excise_customer,i.ce_instructions,
		 i.delivered_dstamp,i.signatory,i.route_id,i.cross_dock_to_site,i.web_service_alloc_immed,i.web_service_alloc_clean,i.disallow_short_ship,i.hub_address_id,i.hub_contact,
		 i.hub_contact_phone,i.hub_contact_mobile,i.hub_contact_fax,i.hub_contact_email,i.hub_name,i.hub_address1,i.hub_address2,i.hub_town,i.hub_county,i.hub_postcode,
		 i.hub_country,i.hub_carrier_id,i.hub_service_level,i.status_reason_code,i.stage_route_id,i.single_order_sortation,i.force_single_carrier,i.expected_volume,i.expected_weight,i.expected_value,i.tod,i.tod_place,i.language,
		 i.seller_name,i.seller_phone,i.documentation_text_1,i.documentation_text_2,i.documentation_text_3,i.cod,i.cod_value,i.cod_currency,i.cod_type,i.vat_number,
		 i.inv_vat_number,i.hub_vat_number,i.print_invoice,i.inv_reference,i.inv_dstamp,i.inv_currency,i.letter_of_credit,i.payment_terms,i.subtotal_1,i.subtotal_2,i.subtotal_3,
		 i.subtotal_4,i.freight_cost,i.freight_terms,i.insurance_cost,i.misc_charges,i.discount,i.other_fee,i.inv_total_1,i.inv_total_2,i.inv_total_3,i.inv_total_4,i.tax_rate_1,i.tax_basis_1,
		 i.tax_amount_1,i.tax_rate_2,i.tax_basis_2,i.tax_amount_2,i.tax_rate_3,i.tax_basis_3,i.tax_amount_3,i.tax_rate_4,i.tax_basis_4,i.tax_amount_4,i.tax_rate_5,i.tax_basis_5,i.tax_amount_5,
		 i.order_reference,i.collective_mode,i.collective_sequence,i.start_by_date,i.metapack_carrier_pre,i.shipment_group,i.freight_currency,i.ncts,i.gln,i.hub_gln,i.inv_gln,i.allow_pallet_pick,
		 i.split_shipping_units,i.vol_pck_sscc_label,i.allocation_priority,i.trax_use_hub_addr,i.mpack_consignment,i.mpack_nominated_dstamp,i.direct_to_store,i.vol_ctr_label_format,i.retailer_id,
		 i.carrier_bags,i.session_time_zone_name,i.time_zone_name,i.nls_calendar,i.client_group,
		 'A','Pending',null,sysdate)
		 ;
		--		
		for r in c_lin(i.order_id)
		loop
			insert into dcsdba.interface_order_line
			(key,client_id,order_id,line_id,host_order_id,host_line_id,sku_id,customer_sku_id,config_id,tracking_level,batch_id,batch_mixing,shelf_life_days,shelf_life_percent,origin_id,
			condition_id,lock_code,spec_code,qty_ordered,allocate,back_ordered,kit_split,deallocate,notes,psft_int_line,psft_schd_line,psft_dmnd_line,sap_pick_req,disallow_merge_rules,
			line_value,rule_id,soh_id,user_def_type_1,user_def_type_2,user_def_type_3,user_def_type_4,user_def_type_5,user_def_type_6,user_def_type_7,user_def_type_8,user_def_chk_1,
			user_def_chk_2,user_def_chk_3,user_def_chk_4,user_def_date_1,user_def_date_2,user_def_date_3,user_def_date_4,user_def_num_1,user_def_num_2,user_def_num_3,user_def_num_4,
			user_def_note_1,user_def_note_2,task_per_each,use_pick_to_grid,ignore_weight_capture,stage_route_id,min_qty_ordered,max_qty_ordered,expected_volume,expected_weight,
			expected_value,customer_sku_desc1,customer_sku_desc2,purchase_order,product_price,product_currency,documentation_unit,extended_price,tax_1,tax_2,documentation_text_1,
			serial_number,owner_id,collective_mode,collective_sequence,ce_receipt_type,ce_coo,kit_plan_id,location_id,unallocatable,min_full_pallet_perc,max_full_pallet_perc,
			full_tracking_level_only,substitute_grade,disallow_substitution,session_time_zone_name,time_zone_name,nls_calendar,client_group,merge_action,merge_status,
			merge_error,merge_dstamp)
			values(dcsdba.if_ol_pk_seq.nextval,r.client_id,l_order,r.line_id,r.host_order_id,r.host_line_id,r.sku_id,r.customer_sku_id,r.config_id,r.tracking_level,r.batch_id,
			r.batch_mixing,r.shelf_life_days,r.shelf_life_percent,r.origin_id,r.condition_id,r.lock_code,r.spec_code,r.qty_ordered,r.allocate,r.back_ordered,r.kit_split,
			r.deallocate,r.notes,r.psft_int_line,r.psft_schd_line,r.psft_dmnd_line,r.sap_pick_req,r.disallow_merge_rules,r.line_value,r.rule_id,r.soh_id,r.user_def_type_1,
			r.user_def_type_2,r.user_def_type_3,r.user_def_type_4,r.user_def_type_5,r.user_def_type_6,r.user_def_type_7,r.user_def_type_8,r.user_def_chk_1,r.user_def_chk_2,
			r.user_def_chk_3,r.user_def_chk_4,r.user_def_date_1,r.user_def_date_2,r.user_def_date_3,r.user_def_date_4,r.user_def_num_1,r.user_def_num_2,r.user_def_num_3,
			r.user_def_num_4,r.user_def_note_1,r.user_def_note_2,r.task_per_each,r.use_pick_to_grid,r.ignore_weight_capture,r.stage_route_id,r.min_qty_ordered,r.max_qty_ordered,
			r.expected_volume,r.expected_weight,r.expected_value,r.customer_sku_desc1,r.customer_sku_desc2,r.purchase_order,r.product_price,r.product_currency,r.documentation_unit,
			r.extended_price,r.tax_1,r.tax_2,r.documentation_text_1,r.serial_number,r.owner_id,r.collective_mode,r.collective_sequence,r.ce_receipt_type,r.ce_coo,
			r.kit_plan_id,r.location_id,r.unallocatable,r.min_full_pallet_perc,r.max_full_pallet_perc,r.full_tracking_level_only,r.substitute_grade,r.disallow_substitution,r.session_time_zone_name,
			r.time_zone_name,r.nls_calendar,r.client_group,
			'A','Pending',null,sysdate)
			;
		end loop;
	end loop;
	commit;
end;