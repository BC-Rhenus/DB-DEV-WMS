CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CLIENT_SPECIFICS_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: WMS functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
-- Global variables
	g_pck	varchar2(30) := 'cnl_client_specifics_pck';
------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels
-- Purpose : Create relocates to merge inventory records
------------------------------------------------------------------------------------------------
procedure return_relocate_p( p_client_id_i 	in dcsdba.client.client_id%type
			   , p_shelf_zone_i	in dcsdba.location.zone_1%type
			   , p_shelf_sub1_i	in dcsdba.location.subzone_1%type
			   , p_shelf_oversub1_i in dcsdba.location.subzone_1%type
			   , p_shelf_retzone_i	in dcsdba.location.zone_1%type
			   , p_batch_mix_i	varchar2
			   )
is
	cursor	c_inv_select
	is
		select	i.tag_id
		,	i.sku_id
		,	nvl(s.packed_volume, nvl(s.each_volume,0)) * i.qty_on_hand required_volume
		,	i.location_id
		,	i.batch_id
		,	i.condition_id
		,	i.client_id
		,	i.site_id
		,	i.qty_on_hand
		from 	dcsdba.inventory 	i
		inner 
		join 	dcsdba.location		l 
		on 	l.location_id		= i.location_id 
		and 	l.site_id		= i.site_id
		and	l.subzone_1		= p_shelf_sub1_i
		and 	l.lock_status 		in ('UnLocked','InLocked')
		and	l.zone_1		= p_shelf_retzone_i
		inner 
		join 	dcsdba.sku 		s 
		on 	i.sku_id		= s.sku_id 
		and 	i.client_id		= s.client_id
		where 	i.client_id		= p_client_id_i
		and 	not exists	(	select	1 
						from	dcsdba.inventory i2
						inner 
						join 	dcsdba.location l2 
						on 	i2.location_id			= l2.location_id 
						and	i2.site_id			= l2.site_id
						and 	l2.zone_1			= p_shelf_retzone_i
						and 	nvl(l2.subzone_1,'0') 		= nvl(p_shelf_sub1_i,'0')
						where 	i2.site_id			= i.site_id
						and 	i2.client_id			= i.client_id
						and 	substr(i2.location_id,4,3) 	= substr(l.location_id,4,3)
						and 	i2.condition_id 		is not null
					)
		and 	not exists 	(	select	1 
						from 	dcsdba.move_task	m
						where 	m.site_id		= i.site_id
						and 	m.client_id		= i.client_id
						and 	m.tag_id		= i.tag_id
						and 	m.from_loc_id		= i.location_id
						and	m.task_type		in ('M','R')
					)
	;
	--
	cursor	c_to_loc( b_sku_id		dcsdba.sku.sku_id%type
			, b_site_id		dcsdba.site.site_id%type
			, b_batch_id		dcsdba.batch.batch_id%type
			, b_condition_id 	dcsdba.condition_code.condition_id%type
			, b_subzone_1		dcsdba.location.subzone_1%type
			, b_required_volume	dcsdba.location.volume%type
			, b_exclude_loc		dcsdba.location.location_id%type
			)
	is
		select	l.location_id 
		from 	dcsdba.location	l
		inner 
		join 	dcsdba.inventory i
		on 	i.location_id	= l.location_id 
		and 	i.site_id	= l.site_id
		and 	i.sku_id	= b_sku_id
		and	i.client_id 	= p_client_id_i					
		and	(	nvl(p_batch_mix_i,'N') = 'Y'
			or 	(	nvl(p_batch_mix_i,'N')	= 'N'
				and 	nvl(i.batch_id,'N')	= nvl(b_batch_id,'N')
				)
			)
		and 	nvl(i.condition_id,'N')	= nvl(b_condition_id,'N')
		where 	l.site_id	= b_site_id
		and 	l.zone_1	= p_shelf_zone_i
		and 	l.subzone_1	= b_subzone_1
		and 	l.lock_status 	in ('UnLocked','OutLocked')
		and 	(((l.volume/100)*85) - (nvl(l.alloc_volume,0))) >= b_required_volume
		and	l.location_id 	!= b_exclude_loc
		and	rownum 		= 1
	;
	--

	l_to_loc_option_1	dcsdba.location.location_id%type;
	l_to_loc_option_2	dcsdba.location.location_id%type;
	l_to_location		dcsdba.location.location_id%type;
	l_merge_error		varchar2(20);
	l_find_location		varchar2(1) := 'N';
	l_retval		integer;
begin
	<<inventory_loop>>
	for 	i in c_inv_select
	loop
		-- Select preferred to location for relocation
		open 	c_to_loc( i.sku_id
				, i.site_id
				, i.batch_id
				, i.condition_id
				, p_shelf_sub1_i
				, i.required_volume
				, i.location_id
				);
		fetch	c_to_loc
		into	l_to_loc_option_1;
		close 	c_to_loc;
		-- Select secondary to location for relocation
		open 	c_to_loc( i.sku_id
				, i.site_id
				, i.batch_id
				, i.condition_id
				, p_shelf_oversub1_i
				, i.required_volume
				, i.location_id
				);
		fetch	c_to_loc
		into	l_to_loc_option_2;
		close 	c_to_loc;
		--
		if	l_to_loc_option_1 is not null
		then
			l_to_location 	:= l_to_loc_option_1;
		elsif	l_to_loc_option_1 is not null
		then
			l_to_location 	:= l_to_loc_option_2;
		else
			l_to_location	:= null;
			l_find_location	:= 'Y';
		end if;
		-- Create relocate using API.
		l_retval	:= dcsdba.libmergeinvmove.directinventorymove	( p_mergeerror		=> l_merge_error
										, p_toupdatecols	=> null
										, p_mergeaction		=> 'A'
										, p_tolocid		=> l_to_location
										, p_tagid		=> i.tag_id
										, p_skuid		=> i.sku_id
										, p_palletid		=> null
										, p_fromlocid		=> i.location_id
										, p_clientid		=> i.client_id
										, p_siteid		=> i.site_id
										, p_quantity		=> i.qty_on_hand
										, p_moveifallocated	=> 'Y'
										, p_movetaskstatus	=> 'Released'
										, p_findlocation	=> l_find_location
										, p_disallowtagswap	=> 'N'
										, p_timezonename	=> null
										);
		commit;		
		--
		l_to_loc_option_1 	:= null;
		l_to_loc_option_2 	:= null;
	end loop; -- inventory_loop
exception
	when others
	then
		null;
end return_relocate_p;
------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels
-- Purpose : Workarround to fix an issue with orders that go back to in progress after they  got shipped.
/*
* CRP-105 requests to set status back to either Shipped, cancelled or Released based on certain conditions 
------------------------------------------------------------------------------------------------*/
procedure set_order_back_to_shipped_p( p_client_id_i 	dcsdba.client.client_id%type
				     , p_site_id_i	dcsdba.site.site_id%type
				     )
is
	type key_rec is record (itl_key integer);
	type key_tab is table of key_rec;
	t_keys	key_tab := key_tab();

	cursor c_ord
	is
		select	odh.order_id
		,	odh.disallow_short_ship
		,	sum(	(
				select	sum(odl.qty_tasked+odl.qty_picked) s
				from 	dcsdba.order_line odl 
				where	odl.order_id = odh.order_id
				and	odl.client_id = p_client_id_i
			) 	)summary
		from	dcsdba.order_header odh
		where	odh.status 		= 'In Progress'
		and	odh.client_id		= p_client_id_i
		and	odh.from_site_id	= p_site_id_i
		and	odh.order_id in	(	select	distinct 
							o.order_id
						from	dcsdba.order_header o
						inner
						join	dcsdba.inventory_transaction i
						on	i.reference_id	= o.order_id
						and	i.client_id	= o.client_id
						and	i.site_id	= o.from_site_id
						and	i.code 		= 'Order Status'
						and	i.from_status	= 'Shipped'
						and	i.to_status 	= 'In Progress'
						where	o.status 	= 'In Progress'
						and	o.client_id	= p_client_id_i
						and	o.from_site_id	= p_site_id_i
					)
		group 
		by 	odh.order_id
		,	odh.disallow_short_ship
	;
	l_status	dcsdba.order_header.status%type;
	l_notes 	dcsdba.inventory_transaction.notes%type;
	l_key 		integer;
begin
	for i in c_ord
	loop
		if	nvl(i.summary,0) > 0
		then
			l_status 	:= 'Shipped';
			l_notes 	:= 'In Progress --> Shipped';
		elsif	i.disallow_short_ship is null
		then
			l_status 	:= 'Cancelled';
			l_notes		:= 'In Progress --> Cancelled';
		elsif	i.disallow_short_ship is not null
		then
			l_status 	:= 'Released';
			l_notes		:= 'In Progress --> Released';
		else
			l_status 	:= 'Shipped';
			l_notes		:= 'In Progress --> Shipped';
		end if;

		l_key := dcsdba.inventory_transaction_pk_seq.nextval;

		t_keys.extend;
		t_keys(t_keys.count).itl_key		:= l_key;

		-- Insert new shipped transaction	
		insert
		into	dcsdba.inventory_transaction
		select	l_key key
		,	i.code
		,	i.site_id
		,	i.from_site_id
		,	i.to_site_id
		,	i.from_loc_id
		,	i.to_loc_id
		,	i.final_loc_id
		,	i.ship_dock
		,	i.dock_door_id
		,	i.owner_id
		,	i.client_id
		,	i.sku_id
		,	i.config_id
		,	i.tag_id
		,	i.container_id
		,	i.pallet_id
		,	i.batch_id
		,	i.qc_status
		,	i.expiry_dstamp
		,	i.manuf_dstamp
		,	i.origin_id
		,	i.condition_id
		,	i.spec_code
		,	i.lock_status
		,	i.list_id
		,	sysdate 			dstamp
		,	i.work_group
		,	i.consignment
		,	i.supplier_id
		,	i.reference_id
		,	i.line_id
		,	i.reason_id
		,	'Automatic'			station_id
		,	'CNL_SYS' 			user_id
		,	i.group_id
		,	i.shift
		,	i.update_qty
		,	i.original_qty
		,	null				uploaded
		,	i.uploaded_ws2pc_id
		,	null 				uploaded_filename
		,	null				uploaded_dstamp
		,	i.uploaded_ab
		,	i.uploaded_tm
		,	i.uploaded_vview
		,	i.session_type
		,	i.summary_record
		,	i.elapsed_time
		,	i.estimated_time
		,	i.sap_idoc_type
		,	i.sap_tid
		,	i.task_category
		,	i.sampling_type
		,	i.job_id
		,	i.manning
		,	i.job_unit
		,	i.complete_dstamp
		,	i.grn
		,	l_notes 			notes --'In Progress --> Shipped' 	notes
		,	i.user_def_type_1
		,	i.user_def_type_2
		,	i.user_def_type_3
		,	i.user_def_type_4
		,	i.user_def_type_5
		,	i.user_def_type_6
		,	i.user_def_type_7
		,	i.user_def_type_8
		,	i.user_def_chk_1
		,	i.user_def_chk_2
		,	i.user_def_chk_3
		,	i.user_def_chk_4
		,	i.user_def_date_1
		,	i.user_def_date_2
		,	i.user_def_date_3
		,	i.user_def_date_4
		,	i.user_def_num_1
		,	i.user_def_num_2
		,	i.user_def_num_3
		,	i.user_def_num_4
		,	i.user_def_note_1
		,	i.user_def_note_2
		,	i.old_user_def_type_1
		,	i.old_user_def_type_2
		,	i.old_user_def_type_3
		,	i.old_user_def_type_4
		,	i.old_user_def_type_5
		,	i.old_user_def_type_6
		,	i.old_user_def_type_7
		,	i.old_user_def_type_8
		,	i.old_user_def_chk_1
		,	i.old_user_def_chk_2
		,	i.old_user_def_chk_3
		,	i.old_user_def_chk_4
		,	i.old_user_def_date_1
		,	i.old_user_def_date_2
		,	i.old_user_def_date_3
		,	i.old_user_def_date_4
		,	i.old_user_def_num_1
		,	i.old_user_def_num_2
		,	i.old_user_def_num_3
		,	i.old_user_def_num_4
		,	i.old_user_def_note_1
		,	i.old_user_def_note_2
		,	i.ce_orig_rotation_id
		,	i.ce_rotation_id
		,	i.ce_consignment_id
		,	i.ce_receipt_type
		,	i.ce_originator
		,	i.ce_originator_reference
		,	i.ce_coo
		,	i.ce_cwc
		,	i.ce_ucr
		,	i.ce_under_bond
		,	i.ce_document_dstamp
		,	i.ce_colli_count
		,	i.ce_colli_count_expected
		,	i.ce_seals_ok
		,	i.uploaded_customs
		,	i.uploaded_labor
		,	i.print_label_id
		,	i.lock_code
		,	i.asn_id
		,	i.customer_id
		,	i.ce_duty_stamp
		,	i.pallet_grouped
		,	i.consol_link
		,	i.job_site_id
		,	i.job_client_id
		,	i.tracking_level
		,	i.extra_notes
		,	i.stage_route_id
		,	i.stage_route_sequence
		,	i.pf_consol_link
		,	i.master_pah_id
		,	i.master_pal_id
		,	null --i.archived
		,	i.shipment_number
		,	i.customer_shipment_number
		,	i.pallet_config
		,	i.container_type
		,	i.ce_avail_status
		,	'In Progress' 			from_status
		,	l_status			to_status--'Shipped' 			to_status
		,	i.kit_plan_id
		,	i.plan_sequence
		,	i.master_order_id
		,	i.master_order_line_id
		,	i.ce_invoice_number
		,	i.rdt_user_mode
		,	i.labor_assignment
		,	i.grid_pick
		,	i.labor_grid_sequence
		,	i.kit_ce_consignment_id
		from 	dcsdba.inventory_transaction i
		inner
		join	dcsdba.order_header o
		on	i.reference_id	= o.order_id
		and	i.client_id	= o.client_id
		and	i.site_id	= o.from_site_id
		and	o.status 	= 'In Progress'
		where 	i.from_status 	= 'Shipped' 
		and 	i.to_status 	= 'In Progress' 
		and	i.code		= 'Order Status'
		and	i.reference_id	= i.order_id	
		;


		-- update order status
		update	dcsdba.order_header odh
		set 	odh.status = l_status
		where	odh.order_id = i.order_id
		and	odh.from_site_id = p_site_id_i
		and	odh.client_id 	= p_client_id_i
		;

	end loop;	
	commit;
		--
	for i in 1..t_keys.count 
	loop
		dcsdba.libabtrans.createitlabdtrans(t_keys(i).itl_key);
	end loop;
	commit;
exception
	when others
	then
		null;
end set_order_back_to_shipped_p;

------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels
-- Purpose : Create interface receipt records for pre-advice lines.
-- 	     This is build to replace receive all functionlity.
--	     Receive all does not work in combination with serial numbers at receipt.
--	     Designed specifically for WLGore.
------------------------------------------------------------------------------------------------
procedure replace_receive_all_p( p_client_id_i		varchar2
			       , p_site_id_i		varchar2
			       , p_receipt_loc_i	varchar2
			       )
is
	cursor	c_pah
	is
		select	p.pre_advice_id
		,	p.supplier_id
		,	c.client_group
		,   nvl((	select 	distinct 
					'Y'
				from 	dcsdba.interface_receipt ir
				where	ir.client_id 		= p.client_id
				and	ir.RECEIPT_ID		= p.pre_advice_id),'N')already_in_interface
		from	dcsdba.pre_advice_header p
		inner
		join	dcsdba.client c
		on	p.client_id 		= c.client_id
		where	p.client_id 		= p_client_id_i
		and	p.site_id		= p_site_id_i
		and	p.status 		= 'Released'
		and	p.status_reason_code 	= 'RAREQUIRED'
	;
	--
	cursor	c_pal(b_pre_advice_id varchar2)
	is
		select 	l.*
		,	s.serial_at_receipt
		,	s.qc_status
		,	s.expiry_reqd
		,	nvl(s.new_product,'N') new_product
		,	e.serial_number
		,	l.qty_due - nvl(l.qty_received,0) actual_qty
		from	dcsdba.pre_advice_line l
		inner 
		join	dcsdba.sku s
		on 	l.sku_id 		= s.sku_id
		and	l.client_id		= s.client_id
		left
		outer
		join	dcsdba.serial_number e
		on 	e.sku_id		= l.sku_id
		and	s.serial_at_receipt	= 'Y'
		and	e.tag_id		= l.tag_id
		and	l.qty_due 		= 1
		and	e.client_id 		= l.client_id
		and	e.receipt_id 		= l.pre_advice_id
		where	l.pre_advice_id 	= b_pre_advice_id
		and	l.client_id 		= p_client_id_i
		and	l.qty_due - nvl(l.qty_received,0) > 0
	;
	--
	cursor	c_pcf( b_sku_id	varchar2)
	is
		select	count(*)
		from	dcsdba.sku_sku_config c
		where	c.sku_id 		= b_sku_id
		and	c.client_id		= p_client_id_i
		and	c.config_id 		!= 'NOPACKCONFIG'
		and	c.config_id 		is not null
		and	nvl(c.disabled,'N')	= 'N'
	;
	--
	l_missing_value		varchar2(1) := 'N';
	l_line_notes		varchar2(80);
	l_rec_key		integer;
	l_exception		varchar2(1) := 'N';
	l_config_cnt		integer;
	l_qty_on_hand		dcsdba.inventory.qty_on_hand%type;
begin
	<<Header_loop>>
	for	r_pah in c_pah
	loop
		if	r_pah.already_in_interface	= 'Y'
		then
			update	dcsdba.pre_advice_header p
			set	p.notes 		= 'Records are already in Interface Record. Receive all not possible.'
			,	p.status_reason_code 	= 'RAERROR'
			where	p.client_id 		= p_client_id_i
			and	p.site_id 		= p_site_id_i
			and	p.pre_advice_id		= r_pah.pre_advice_id
			;
			commit;
		else
			-- set status to pending.
			update	dcsdba.pre_advice_header p
			set	p.status_reason_code = 'RAPENDING'
			where	p.client_id 	= p_client_id_i
			and	p.site_id 	= p_site_id_i
			and	p.pre_advice_id	= r_pah.pre_advice_id
			;
			commit;
			-- Start checking lines
			<<line_error_loop>>
			for 	r_pal in c_pal(r_pah.pre_advice_id)
			loop
				-- Chk pack config
				l_config_cnt := 0;
				open	c_pcf(r_pal.sku_id);
				fetch	c_pcf
				into	l_config_cnt;
				close	c_pcf;

				if	l_config_cnt = 0
				then
					l_missing_value := 'Y';
					l_line_notes	:= 'Sku has no pack configuration or only NOPACKCONFIG as configuration linked.';
				elsif	r_pal.new_product = 'Y'
				then
					l_missing_value := 'Y';
					l_line_notes	:= 'SKU is a new product and can''t be received';
				elsif	r_pal.tag_id is null
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Tag id not defined. receive all not possible';
				elsif	r_pal.serial_number is null
				and	nvl(r_pal.serial_at_receipt,'N') = 'Y'
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Serial number is missing. Receive all not possible';
				elsif	r_pal.owner_id 	is null
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Owner id is missing. Receive all not possible due to possible interface errors';				
				elsif	r_pal.config_id is null
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Pack configuration is not defined. receive all not possible';
				elsif	r_pal.pallet_config is null
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Pallet type is not defined. receive all not possible';
				elsif	r_pal.qc_status is not null
				and	r_pal.batch_id is null
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Batch id is not defined. receive all not possible';
				elsif	nvl(r_pal.expiry_reqd,'N') = 'Y'
				and	r_pal.expiry_dstamp is null
				then
					l_missing_value := 'Y';
					l_line_notes 	:= 'Expiry date is not defined. receive all not possible';
				end if;

				--
				update	dcsdba.pre_advice_line l
				set	notes 		= l_line_notes
				where	l.client_id 	= p_client_id_i
				and	l.pre_advice_id = r_pal.pre_advice_id
				and	l.line_id	= r_pal.line_id
				;
				commit;

				l_line_notes	:= null;
			end loop;
			--
			if	l_missing_value = 'Y'
			then	-- one of the lines is missing data
				update	dcsdba.pre_advice_header p
				set	p.notes 		= 'A value is missing. Check line notes for more details.'
				,	p.status_reason_code 	= 'RAERROR'
				where	p.client_id 		= p_client_id_i
				and	p.site_id 		= p_site_id_i
				and	p.pre_advice_id		= r_pah.pre_advice_id
				;
				commit;
				l_missing_value	:= 'N';
				continue header_loop;
			else
				<<line_loop>>
				for	r_pal in c_pal(r_pah.pre_advice_id)
				loop
					l_rec_key	:= dcsdba.if_rec_pk_seq.nextval;
					begin
						insert into dcsdba.interface_receipt i
						values( l_rec_key		--key
						      , p_site_id_i		--site_id
						      , p_receipt_loc_i		--location_id
						      , r_pal.owner_id		--owner_id
						      , p_client_id_i		--client_id
						      , r_pal.sku_id		--sku_id
						      , r_pal.config_id		--config_id
						      , r_pal.tag_id		--tag_id
						      , r_pal.actual_qty	--qty_on_hand
						      , r_pal.batch_id		--batch_id
						      , r_pal.expiry_dstamp	--expiry_dstamp
						      , r_pal.manuf_dstamp	--manuf_dstamp
						      , r_pal.pre_advice_id	--receipt_id
						      , r_pal.line_id		--line_id
						      , r_pah.supplier_id	--supplier_id
						      , r_pal.origin_id		--origin_id
						      , r_pal.condition_id	--condition_id
						      , r_pal.lock_code		--lock_code
						      , null			--tag_copies
						      , sysdate			--receipt_dstamp
						      , null			--pallet_id
						      ,	r_pal.pallet_config	--pallet_config
						      , null			--pallet_volume
						      , null			--pallet_height
						      , null			--pallet_depth
						      , null			--pallet_width
						      , null			--pallet_weight
						      , null			--pallet_grouped
						      , r_pal.notes		--notes
						      , r_pal.sampling_type	--sampling_type
						      , r_pal.user_def_type_1	--user_def_type_1
						      , r_pal.user_def_type_2	--user_def_type_2
						      , r_pal.user_def_type_3	--user_def_type_3
						      , r_pal.user_def_type_4	--user_def_type_4					      
						      , r_pal.user_def_type_5	--user_def_type_5
						      , r_pal.user_def_type_6	--user_def_type_6
						      , r_pal.user_def_type_7	--user_def_type_7					      
						      , r_pal.user_def_type_8	--user_def_type_8
						      , r_pal.user_def_chk_1	--user_def_chk_1
						      , r_pal.user_def_chk_2	--user_def_chk_2
						      , r_pal.user_def_chk_3	--user_def_chk_3
						      , r_pal.user_def_chk_4	--user_def_chk_4
						      , r_pal.user_def_date_1	--user_def_date_1
						      , r_pal.user_def_date_2	--user_def_date_2
						      , r_pal.user_def_date_3	--user_def_date_3
						      , r_pal.user_def_date_4	--user_def_date_4
						      , r_pal.user_def_num_1	--user_def_num_1
						      , r_pal.user_def_num_2	--user_def_num_2
						      , r_pal.user_def_num_3	--user_def_num_3
						      , r_pal.user_def_num_4	--user_def_num_4
						      , r_pal.user_def_note_1	--user_def_note_1
						      , r_pal.user_def_note_2	--user_def_note_2
						      , r_pal.tracking_level	--tracking_level
						      , null			--ce_consignment_id
						      , null			--ce_receipt_type
						      , null			--ce_originator
						      , null			--ce_originator_ref
						      , null			--ce_coo
						      , null			--ce_cwc
						      , null			--ce_ucr
						      , null			--ce_under_bond
						      , null			--ce_document_dstamp
						      , null			--ce_duty_stamp
						      , null			--spec_code
						      , null			--session_time_zone_name
						      , null			--time_zone_name
						      , null			--nls_calendar
						      , null			--client_group
						      , 'A'			--merge_action
						      , 'Pending'		--merge_status
						      , null			--merge_error
						      , sysdate			--merge_dstamp
						      );
					exception
						when others
						then
							rollback;
							l_exception	:= 'Y';
							--
							update	dcsdba.pre_advice_header p
							set	p.notes 		= 'An error occured creating interface records'
							,	p.status_reason_code 	= 'RAERROR'
							where	p.client_id 		= p_client_id_i
							and	p.site_id 		= p_site_id_i
							and	p.pre_advice_id		= r_pah.pre_advice_id
							;
							commit;
							exit line_loop;
					end;
					commit;
				end loop;
				--
				if 	l_exception = 'N'
				then
					update	dcsdba.pre_advice_header p
					set	p.status_reason_code 	= 'RASUCCESS'
					,	p.notes 		= ''
					where	p.client_id 		= p_client_id_i
					and	p.site_id 		= p_site_id_i
					and	p.pre_advice_id		= r_pah.pre_advice_id
					;
					commit;
				end if;
			end if;

			l_missing_value	:= 'N';
			l_exception	:= 'N';
		end if;
	end loop;
end replace_receive_all_p;

------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels
-- Purpose : Get sequence for CRL SSCC numbers (Moved from RIS_SYS to CNL_SYS and from 2009 to 2016)
------------------------------------------------------------------------------------------------
function get_crl_sscc_box_nr( p_ordered_qty_i  number
                            , p_full_box_qty_i number
                            ) 
   return varchar2
is
   cursor c_box 
   is 
      select cnl_crl_sscc_seq1.nextval box_nr
      from   dual
      ;

   l_retval       varchar2(11);
   --l_counter      number := 1;
   l_total_boxes  number;
   l_temp_box_nr  number;
   l_first_box_nr number;
   l_last_box_nr  number;
begin
   l_total_boxes := ceil(nvl( p_ordered_qty_i, 0) / nvl( p_full_box_qty_i, 0));

   if l_total_boxes > 0
   then

      for  l_counter in 1 .. l_total_boxes
      loop

         --l_counter := l_counter + 1;

         open  c_box;
         fetch c_box
         into  l_temp_box_nr;
         close c_box;

         if l_counter = 1
         then
            l_first_box_nr := l_temp_box_nr;
         end if;

         if l_counter = l_total_boxes
         then
            l_last_box_nr := l_temp_box_nr;
         end if;

      end loop;

      if l_last_box_nr - l_total_boxes = l_first_box_nr - 1
      then
         l_retval := lpad(l_first_box_nr,5,0)
                  || '^'
                  || lpad(l_last_box_nr,5,0)
                  ;
      else
         l_retval := 'ERROR^ERROR';
      end if;

   end if;

   return l_retval;              

end get_crl_sscc_box_nr;

------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels
-- Purpose : Create emergancy inventory backup on local PC at A58
-- File will be placed on the streamserve share and streamserve will move the file to The local PC
------------------------------------------------------------------------------------------------
procedure create_inv_backup_p( p_client_id_i	varchar2)
is
	cursor	c_inventory
	is
		select	i.client_id
		,	i.tag_id
		,	i.sku_id
		,	i.batch_id
		,	i.location_id
		,	to_char(i.qty_on_hand,'fm999999990') qty_on_hand
		,	to_char(i.qty_allocated,'fm999999990') qty_allocated
		,	i.description
		,	i.lock_code
		,	to_char(i.expiry_dstamp,'dd/mm/yyyy') expiry_dstamp
		,	i.condition_id
		,	i.pallet_id
		,	i.container_id
		,	i.origin_id
		,	i.user_def_type_5
		from	dcsdba.inventory i
		where	i.client_id = p_client_id_i
	;
	l_file_name		varchar2(50);
	l_file_type		utl_file.file_type;
	l_record		varchar2(4000);
	l_app_server_tmp_dir   	constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'APP_SERVER_MOUNT_TMP_DIR');
	l_app_server_arc_dir   	constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'APP_SERVER_MOUNT_ARC_DIR');
	l_app_server_out_dir   	constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'APP_SERVER_MOUNT_OUT_DIR');
	l_block_size		binary_integer;
	l_file_length		number;
	l_tmp_fexists		boolean;
	l_arc_fexists		boolean;
	l_db_alias		varchar2(20);

begin
	select	db_alias
	into 	l_db_alias
	from	dcsdba.system_options
	;
	if	l_db_alias	= 'PRDCNLJW'
	then

		-- define file name
		l_file_name 	:= 'INV_BACKUP_'||p_client_id_i||'.CSV';

		-- open new file
		l_file_type	:= utl_file.fopen( location     => l_app_server_tmp_dir
						 , filename     => l_file_name
						 , open_mode    => 'w'
						 , max_linesize => 32767
						 );

		-- add file header
		l_record := 'CLIENT_ID,TAG_ID,SKU_ID,BATCH_ID,LOCATION_ID,QTY_ON_HAND,QTY_ALLOCATED,DESCRIPTION,LOCK_CODE,EXPIRY_DSTAMP,CONDITION,PALLET_ID,CONTAINER_ID,ORIGIN_ID,USER_DEF_TYPE_5';

		utl_file.put_line( file   	=> l_file_type
				 , buffer	=> l_record
				 );

		-- Create records and add record to file
		<<inv_loop>>
		for	i in c_inventory
		loop
			l_record 	:= 	i.client_id 			|| ',' ||
						replace(i.tag_id,',','.') 	|| ',' ||
						replace(i.sku_id,',','.')	|| ',' ||
						replace(i.batch_id,',','.')	|| ',' ||
						i.location_id			|| ',' ||
						replace(i.qty_on_hand,',','.')	|| ',' ||
						replace(i.qty_allocated,',','.')|| ',' ||
						replace(i.description,',','.')	|| ',' ||
						i.lock_code			|| ',' ||
						i.expiry_dstamp			|| ',' ||
						i.condition_id			|| ',' ||
						replace(i.pallet_id,',','.')	|| ',' ||
						replace(i.container_id,',','.')	|| ',' ||
						i.origin_id			|| ',' ||
						replace(i.user_def_type_5,',','.');

			utl_file.put_line( file   	=> l_file_type
					 , buffer	=> l_record
					 );
		end loop inv_loop;

		-- copy file from tmp to archive
		utl_file.fgetattr( location    => l_app_server_tmp_dir 
				 , filename    => l_file_name
				 , fexists     => l_tmp_fexists
				 , file_length => l_file_length
				 , block_size  => l_block_size
				 );
		if	l_tmp_fexists
		then
			utl_file.fcopy( src_location  => l_app_server_tmp_dir
				      , src_filename  => l_file_name
				      , dest_location => l_app_server_arc_dir
				      , dest_filename => l_file_name
				      );
		end if;                                   

		-- move file from tmp to out
		utl_file.fgetattr( location    => l_app_server_arc_dir 
				 , filename    => l_file_name
				 , fexists     => l_arc_fexists
				 , file_length => l_file_length
				 , block_size  => l_block_size 
				 );
		if 	l_arc_fexists
		then
			utl_file.frename( src_location  => l_app_server_tmp_dir
					, src_filename  => l_file_name
					, dest_location => l_app_server_out_dir
					, dest_filename => l_file_name
					, overwrite     => true
					);
		end if;

		-- close file
		utl_file.fclose (file => l_file_type);

		-- add filename to table to be able to cleanup directory
		insert 	into 
			cnl_files_archive
		( 	application
		, 	location
		, 	filename
		)
		values
		( 	'INV_BACKUP'
		, 	l_app_server_arc_dir
		, 	l_file_name
		)
		;
		commit;
	end if;
exception
	when	utl_file.invalid_path
	then
		raise_application_error( -20100, 'Invalid path');
		utl_file.fclose (file => l_file_type);
	when 	utl_file.invalid_mode
	then
		raise_application_error( -20100, 'Invalid Mode');
		utl_file.fclose (file => l_file_type);
	when 	utl_file.invalid_filehandle
	then
		raise_application_error( -20100, 'Invalid File Handle');
		utl_file.fclose (file => l_file_type);
	when 	utl_file.invalid_operation
	then
		raise_application_error( -20100, 'Invalid Operation');
		utl_file.fclose (file => l_file_type);
	when 	utl_file.read_error
	then
		raise_application_error( -20100, 'Read Error');
		utl_file.fclose (file => l_file_type);
	when 	utl_file.write_error
	then
		raise_application_error( -20100, 'Write Error');
		utl_file.fclose (file => l_file_type);
	when 	utl_file.internal_error
	then
		raise_application_error( -20100, 'Internal Error');
		utl_file.fclose (file => l_file_type);
	when 	no_data_found
	then
		raise_application_error( -20100, 'No Data Found');
		utl_file.fclose (file => l_file_type);
	when 	value_error
	then
		raise_application_error( -20100, 'Value Error');
		utl_file.fclose (file => l_file_type);
	when 	others
	then
		raise_application_error( -20100, sqlerrm);
		utl_file.fclose (file => l_file_type);
end create_inv_backup_p;
------------------------------------------------------------------------------------------------
-- Author  : M.Swinkels
-- Purpose : Correct order status after it was set back to in-progress while it was shipped
------------------------------------------------------------------------------------------------
	procedure search_non_pf_zero_demand_p(	p_from_zone_i		dcsdba.location.zone_1		%type
					     , 	p_from_subzone_1_i	dcsdba.location.subzone_1	%type 	default null
					     ,	p_to_zone_i		dcsdba.location.zone_1		%type	default null
					     ,	p_pallet_type_i		dcsdba.pallet_config.config_id	%type 	default null
					     ,	p_location_level_i	dcsdba.location.levels		%type 	default null
					     ,	p_zero_alloc_i		dcsdba.address.user_def_chk_2	%type
					     ,	p_zero_demand_i		dcsdba.address.user_def_chk_1	%type
					     ,	p_exclude_rule_id_i	dcsdba.allocation_rule.rule_id	%type 	default null
					     ,	p_max_nr_relocates_i	dcsdba.address.user_def_num_3	%type 	default null
					     ,	p_client_id_i		dcsdba.client.client_id		%type
					     ,  p_loc_height_i		dcsdba.address.user_def_num_4	%type 	default null
					     )
	is
		-- select all inventory that have no tasks against it and number of relocates is not limited.
		cursor	c_zero_alloc_no_limit
		is
			select	i.tag_id
			,	i.sku_id
			,	i.location_id
			,	i.site_id
			,	i.qty_on_hand
			from	dcsdba.inventory	i
			,	dcsdba.location		l
			where	i.client_id		= p_client_id_i
			and	l.site_id		= i.site_id
			and	l.location_id		= i.location_id
			and	l.zone_1		= p_from_zone_i
			and	l.loc_type		in ('Tag-FIFO','Tag-Operator','Bulk','Bin')
			and	(	nvl(l.subzone_1,'N')	= p_from_subzone_1_i
				or	p_from_subzone_1_i	is null
				)
			and	(	nvl(i.pallet_config,'N')= p_pallet_type_i
				or	p_pallet_type_i		is null
				)
			and	(	nvl(l.levels,'N')	= p_location_level_i
				or	p_location_level_i	is null
				)
			and	(	nvl(l.height,0)		>= p_loc_height_i
				or 	p_loc_height_i		is null
				)
			and	nvl(i.pick_face,'N')	= 'N'		
			and	(	select	count(*)				
					from	dcsdba.move_task m
					where	m.site_id	= i.site_id
					and	m.client_id	= i.client_id
					and	m.from_loc_id	= i.location_id
					and	m.task_type	in ('M','O','P','R')
				)			= 0
		;

		-- select all locations that have no tasks against it when number of relocates must be limited.
		cursor	c_zero_alloc
		is
			select	i.location_id
			,	i.site_id
			,	max(to_char(i.move_dstamp,'YYYYMMDD'))
			from	dcsdba.inventory	i
			,	dcsdba.location		l
			where	i.client_id		= p_client_id_i
			and	l.site_id		= i.site_id
			and	l.location_id		= i.location_id
			and	l.zone_1		= p_from_zone_i
			and	l.loc_type		in ('Tag-FIFO','Tag-Operator','Bulk','Bin')
			and	(	nvl(l.subzone_1,'N')	= p_from_subzone_1_i
				or	p_from_subzone_1_i	is null
				)
			and	(	nvl(i.pallet_config,'N')= p_pallet_type_i
				or	p_pallet_type_i		is null
				)
			and	(	nvl(l.levels,'N')	= p_location_level_i
				or	p_location_level_i	is null
				)
			and	(	nvl(l.height,0)		>= p_loc_height_i
				or 	p_loc_height_i		is null
				)
			and	nvl(i.pick_face,'N')	= 'N'		
			and	(	select	count(*)				
					from	dcsdba.move_task m
					where	m.site_id	= i.site_id
					and	m.client_id	= i.client_id
					and	m.from_loc_id	= i.location_id
					and	m.task_type	in ('M','O','P','R')
				)			= 0
			group
			by	i.location_id
			,	i.site_id
			order
			by	3 asc
		;

		-- select all locations that have no tasks against it and include pending orders in the descision making
		cursor	c_zero_demand_no_limit
		is
			select	i.tag_id
			,	i.sku_id
			,	i.location_id
			,	i.site_id
			,	i.qty_on_hand
			from	dcsdba.inventory	i
			,	dcsdba.location		l
			where	i.client_id		= p_client_id_i
			and	l.site_id		= i.site_id
			and	l.location_id		= i.location_id
			and	l.zone_1		= p_from_zone_i
			and	l.loc_type		in ('Tag-FIFO','Tag-Operator','Bulk','Bin')
			and	(	nvl(l.subzone_1,'N')	= p_from_subzone_1_i
				or	p_from_subzone_1_i	is null
				)
			and	(	nvl(i.pallet_config,'N')= p_pallet_type_i
				or	p_pallet_type_i		is null
				)
			and	(	nvl(l.levels,'N')	= p_location_level_i
				or	p_location_level_i	is null
				)
			and	(	nvl(l.height,0)		>= p_loc_height_i
				or 	p_loc_height_i		is null
				)
			and	nvl(i.pick_face,'N')	= 'N'		
			and	(	select	count(*)				
					from	dcsdba.move_task m
					where	m.site_id	= i.site_id
					and	m.client_id	= i.client_id
					and	m.from_loc_id	= i.location_id
					and	m.task_type	in ('M','O','P','R')
				)			= 0
			and	i.sku_id	not in	(	select	odl.sku_id
								from	dcsdba.order_line odl
								,	dcsdba.order_header odh
								where	odl.sku_id		= i.sku_id
								and	odl.client_id		= i.client_id
								and	odh.order_id		= odl.order_id
								and	odh.client_id		= odl.client_id
								and	odh.from_site_id	= i.site_id
								and	odh.status		in ('Released','Hold')
								and	nvl(odl.qty_tasked,0)	= 0
								and	nvl(odl.qty_picked,0) 	= 0
								and 	(	p_exclude_rule_id_i	is null  -- Exclude all pending lines
									or	(	odl.rule_id 	is null  -- Exclude all lines with no rule id + the lines with a different rule id that specified
										or	odl.rule_id	!= p_exclude_rule_id_i
										)
									)

							)
		;

		-- select all locations that have no tasks against it and exclude certain sku's from search based on line urle id.
		cursor	c_zero_demand
		is
			select	i.location_id
			,	i.site_id
			,	max(to_char(i.move_dstamp,'YYYYMMDD'))
			from	dcsdba.inventory	i
			,	dcsdba.location		l
			where	i.client_id		= p_client_id_i
			and	l.site_id		= i.site_id
			and	l.location_id		= i.location_id
			and	l.zone_1		= p_from_zone_i
			and	l.loc_type		in ('Tag-FIFO','Tag-Operator','Bulk','Bin')
			and	(	nvl(l.subzone_1,'N')	= p_from_subzone_1_i
				or	p_from_subzone_1_i	is null
				)
			and	(	nvl(i.pallet_config,'N')= p_pallet_type_i
				or	p_pallet_type_i		is null
				)
			and	(	nvl(l.levels,'N')	= p_location_level_i
				or	p_location_level_i	is null
				)
			and	(	nvl(l.height,0)		>= p_loc_height_i
				or 	p_loc_height_i		is null
				)
			and	nvl(i.pick_face,'N')	= 'N'		
			and	(	select	count(*)				
					from	dcsdba.move_task m
					where	m.site_id	= i.site_id
					and	m.client_id	= i.client_id
					and	m.from_loc_id	= i.location_id
					and	m.task_type	in ('M','O','P','R')
				)			= 0
			and	i.sku_id	not in  	(	select	odl.sku_id
									from	dcsdba.order_line odl
									,	dcsdba.order_header odh
									where	odl.sku_id		= i.sku_id
									and	odl.client_id		= i.client_id
									and	odh.order_id		= odl.order_id
									and	odh.client_id		= odl.client_id
									and	odh.from_site_id	= i.site_id
									and	odh.status		in ('Released','Hold')
									and	nvl(odl.qty_tasked,0)	= 0
									and	nvl(odl.qty_picked,0) 	= 0
									and 	(	p_exclude_rule_id_i 	is null  -- Exclude all pending lines
										or	(	odl.rule_id 	is null  -- Exclude all lines with no rule id + the lines with a different rule id that specified
											or	odl.rule_id	!= p_exclude_rule_id_i
											)
										)
								)
			group
			by	i.location_id
			,	i.site_id
			order
			by	3 asc
		;

		-- Inventory details
		cursor	c_inv_details( b_site_id	dcsdba.site.site_id%type
				     , b_location_id	dcsdba.location.location_id%type
				     )
		is
			select	i.tag_id
			,	i.sku_id
			,	i.location_id
			,	i.site_id
			,	i.qty_on_hand
			from	dcsdba.inventory i
			where	i.client_id		= p_client_id_i
			and	i.site_id 		= b_site_id
			and	i.location_id		= b_location_id
		;
		--
		l_merge_error	varchar2(10);
		l_retval	integer;
		l_loop_counter	integer := 0;
		l_api_errors	integer := 0; --Max 10
		pragma		autonomous_transaction;
	begin
		-- zero allocate and no limit to the number of relocates to generate and 
		if	nvl(p_zero_demand_i,'N') = 'Y'
		and	(	p_max_nr_relocates_i = 0 
			or	p_max_nr_relocates_i is null
			)
		then	
			<<NO_LIMIT_ZERO_DEMAND>>
			for	i in c_zero_demand_no_limit
			loop
				l_retval	:= dcsdba.libmergeinvmove.directinventorymove	( p_mergeerror		=> l_merge_error
												, p_toupdatecols		=> null
												, p_mergeaction		=> 'A'
												, p_tolocid		=> null
												, p_tagid			=> i.tag_id
												, p_skuid			=> i.sku_id
												, p_palletid		=> null
												, p_fromlocid		=> i.location_id
												, p_clientid		=> p_client_id_i
												, p_siteid		=> i.site_id
												, p_quantity		=> i.qty_on_hand
												, p_moveifallocated	=> 'N'
												, p_movetaskstatus	=> 'Released'
												, p_findlocation		=> 'Y'
												, p_disallowtagswap	=> 'N'
												, p_timezonename		=> null
												);
			end loop NO_LIMIT_ZERO_DEMAND;
		end if;

		-- zero allocate with limit to the number of relocates to generate
		if	nvl(p_zero_demand_i,'N') = 'Y'
		and	p_max_nr_relocates_i > 0
		then
			<<LIMIT_ZERO_ALLOC>>
			for	r in c_zero_demand
			loop
				l_loop_counter	:= l_loop_counter + 1;
				<<INNER_LIMIT_ZERO_DEMAND>>
				for	i in c_inv_details( r.site_id
							  , r.location_id
							  )
				loop
					l_retval	:= dcsdba.libmergeinvmove.directinventorymove	( p_mergeerror		=> l_merge_error
													, p_toupdatecols	=> null
													, p_mergeaction		=> 'A'
													, p_tolocid		=> null
													, p_tagid		=> i.tag_id
													, p_skuid		=> i.sku_id
													, p_palletid		=> null
													, p_fromlocid		=> i.location_id
													, p_clientid		=> p_client_id_i
													, p_siteid		=> i.site_id
													, p_quantity		=> i.qty_on_hand
													, p_moveifallocated	=> 'N'
													, p_movetaskstatus	=> 'Released'
													, p_findlocation	=> 'Y'
													, p_disallowtagswap	=> 'N'
													, p_timezonename	=> null
													);
				end loop INNER_LIMIT_ZERO_ALLOC;	
				-- check if number of locations is reached
				if	l_loop_counter >= p_max_nr_relocates_i
				then
					exit;
				end if;
				-- Force an extra attempt when one API failed. Max 10 failed before giving up.
				if	nvl(l_retval,1) = 0
				and	l_api_errors < 10 -- Max 10 errors
				then
					l_loop_counter 	:= l_loop_counter-1;
					l_api_errors	:= l_api_errors + 1;
				end if;
			end loop;			
		end if;

		-- zero allocate and no limit to the number of relocates to generate and 
		if	nvl(p_zero_alloc_i,'N') = 'Y'
		and	nvl(p_zero_demand_i,'N') = 'N'
		and	(	p_max_nr_relocates_i = 0 
			or	p_max_nr_relocates_i is null
			)
		then	
			<<NO_LIMIT_ZERO_ALLOC>>
			for	i in c_zero_alloc_no_limit
			loop
				l_retval	:= dcsdba.libmergeinvmove.directinventorymove	( p_mergeerror		=> l_merge_error
												, p_toupdatecols	=> null
												, p_mergeaction		=> 'A'
												, p_tolocid		=> null
												, p_tagid		=> i.tag_id
												, p_skuid		=> i.sku_id
												, p_palletid		=> null
												, p_fromlocid		=> i.location_id
												, p_clientid		=> p_client_id_i
												, p_siteid		=> i.site_id
												, p_quantity		=> i.qty_on_hand
												, p_moveifallocated	=> 'N'
												, p_movetaskstatus	=> 'Released'
												, p_findlocation	=> 'Y'
												, p_disallowtagswap	=> 'N'
												, p_timezonename	=> null
												);
			end loop NO_LIMIT_ZERO_ALLOC;
		end if;

		-- zero allocate with limit to the number of relocates to generate
		if	nvl(p_zero_alloc_i,'N') = 'Y'
		and	nvl(p_zero_demand_i,'N') = 'N'
		and	p_max_nr_relocates_i > 0
		then
			<<LIMIT_ZERO_ALLOC>>
			for	r in c_zero_alloc
			loop
				l_loop_counter	:= l_loop_counter +1;
				<<INNER_LIMIT_ZERO_ALLOC>>
				for	i in c_inv_details( r.site_id
							  , r.location_id
							  )
				loop
					l_retval	:= dcsdba.libmergeinvmove.directinventorymove	( p_mergeerror		=> l_merge_error
													, p_toupdatecols	=> null
													, p_mergeaction		=> 'A'
													, p_tolocid		=> null
													, p_tagid		=> i.tag_id
													, p_skuid		=> i.sku_id
													, p_palletid		=> null
													, p_fromlocid		=> i.location_id
													, p_clientid		=> p_client_id_i
													, p_siteid		=> i.site_id
													, p_quantity		=> i.qty_on_hand
													, p_moveifallocated	=> 'N'
													, p_movetaskstatus	=> 'Released'
													, p_findlocation	=> 'Y'
													, p_disallowtagswap	=> 'N'
													, p_timezonename	=> null
													);
				end loop INNER_LIMIT_ZERO_ALLOC;	
				-- check if number of locations is reached
				if	l_loop_counter >= p_max_nr_relocates_i
				then
					exit;
				end if;
				-- Force an extra attempt when one API failed. Max 10 failed before giving up.
				if	nvl(l_retval,1) = 0
				and	l_api_errors < 10 -- Max 10 errors
				then
					l_loop_counter 	:= l_loop_counter-1;
					l_api_errors	:= l_api_errors + 1;
				end if;
			end loop;			
		end if;
		commit;
	exception
		when	others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> 'cnl_wms_mergerule_pck'		-- Package name the error occured
							  , p_routine_name_i		=> 'search_non_pf_zero_demand_p'	-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> 'from_zone = '		||p_from_zone_i		||' '
										        || 'from_subzone_1 = '		||p_from_subzone_1_i	||' '
											|| 'to_zone = '			||p_to_zone_i		||' '
											|| 'only_pallet_type = '	||p_pallet_type_i	||' '
											|| 'only from level = '		||p_location_level_i	||' '
											|| 'zero allocation only = '	||p_zero_alloc_i	||' '
											|| 'zero demand = '		||p_zero_demand_i	||' '
											|| 'Exclude rule id = '		||p_exclude_rule_id_i	||' '
											|| 'Max number of locations = '	||p_max_nr_relocates_i	||' '
											|| 'client_id = '		||p_client_id_i	
							  , p_comments_i		=> 'Something went wrong during the creation of relocates for zero demand non pick faces'					-- Additional comments describing the issue
							  );

	end search_non_pf_zero_demand_p;
--
begin
  -- Initialization
  null;

end cnl_client_specifics_pck;