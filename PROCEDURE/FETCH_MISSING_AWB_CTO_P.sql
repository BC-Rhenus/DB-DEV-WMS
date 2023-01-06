CREATE OR REPLACE PROCEDURE "CNL_SYS"."FETCH_MISSING_AWB_CTO_P" 
is
	-- Fetch AWB per ship unit
	cursor	c_ship_unit_awb
	is
		select	max(p."idPRC") 	pcl_id
		,	p."SequenceNo"	p_tracking_number
		,	p."CodeSEN"	client_id
		,	p."OrderNo"	order_id
		,	p."idSHP"	shipment_id
		,	p."ParcelID"	id
		,	w.id_type	
		from    Parcels@centiro.rhenus.de p
		inner
		join	(
			select 	distinct 
				decode(labelled,'Y',container_id,pallet_id)	id
			,	decode(labelled,'Y','C','P')			id_type
			,	client_id||'@'||site_id				client_id
			,	order_id
			from	dcsdba.shipping_manifest m
			where 	carrier_consignment_id 	is null
			and	(	
				labelled = 'Y'
				or
				pallet_labelled = 'Y'
				)
			and	m.client_id not in (select client_id from dcsdba.client_group_clients where client_group = 'CTOSAAS')
			and	m.picked_dstamp > sysdate -7
			) w
		on	p."ParcelID" = w.id
		and	p."CodeSEN"  = w.client_id	
		and	p."OrderNo"  = w.order_id
		group 
		by	p."SequenceNo"
		,	p."CodeSEN"
		,	p."OrderNo"
		,	p."ParcelID"
		,	p."idSHP"
		,	w.id_type
		order
		by	p."OrderNo"
		,	p."ParcelID"
	;
	--
	cursor	c_shipment_awb(	b_shipment_id	number)
	is
		select	s."SequenceNo"	s_tracking_number
		from    Shipments@centiro.rhenus.de s
		where	s."idSHP"	= b_shipment_id
	;
	--
	l_tracking_number	varchar2(100);
	l_busy			dcsdba.system_profile.text_data%type;
	l_shipment_id		number;
	l_order_id		varchar2(50);
	l_client_id		varchar2(50);
	l_id			varchar2(50);
begin
	select	text_data
	into	l_busy
	from	dcsdba.system_profile
	where	profile_id = '-ROOT-_USER_CENTIRO_FETCHAWB_FETCHAWBBUSY';

	if	l_busy = 'SLEEPING'
	then
		update	dcsdba.system_profile
		set 	text_data = 'BUSY'
		where	profile_id = '-ROOT-_USER_CENTIRO_FETCHAWB_FETCHAWBBUSY';
		commit;

		<<miss_loop>>
		for	i in c_ship_unit_awb
		loop
			l_shipment_id := null;
			l_order_id := null;
			l_client_id := null;
			l_id	:= null;

			l_shipment_id := i.shipment_id;
			l_order_id := i.order_id;
			l_client_id := i.client_id;
			l_id		:= i.id;

			-- Set ship unit track number of shipment track number
			if	i.p_tracking_number is null
			then

				open	c_shipment_awb( i.shipment_id);
				fetch 	c_shipment_awb
				into	l_tracking_number;
				if	c_shipment_awb%notfound
				then
					l_tracking_number	:= 'NA';
				end if;
				close 	c_shipment_awb;
			else
				l_tracking_number	:= i.p_tracking_number;
			end if;
			-- Update shipping manifest
			if	i.id_type = 'C'
			then	-- container id
				update	dcsdba.shipping_manifest m
				set	m.carrier_consignment_id	= l_tracking_number
				where	m.client_id||'@'||m.site_id	= i.client_id
				and	m.order_id 			= i.order_id
				and	m.container_id			= i.id
				and	m.carrier_consignment_id 	is null
				;
			elsif	i.id_type = 'P'
			then
				update	dcsdba.shipping_manifest m
				set	m.carrier_consignment_id	= l_tracking_number
				where	m.client_id||'@'||m.site_id	= i.client_id
				and	m.order_id 			= i.order_id
				and	m.pallet_id			= i.id
				and	m.carrier_consignment_id 	is null
				;
			end if;
			commit;
		end loop;
		update	dcsdba.system_profile
		set 	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_CENTIRO_FETCHAWB_FETCHAWBBUSY';
		commit;
	end if;	
exception
	when others 
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> 'no package'		-- Package name the error occured
						  , p_routine_name_i		=> 'fetch_missing_awb_cto_p'	-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> 'Tracking number: '||l_tracking_number||', order: '||l_order_id||', client: '||l_client_id||', pallet or container: '||l_id
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );	
		update	dcsdba.system_profile
		set 	text_data = 'SLEEPING'
		where	profile_id = '-ROOT-_USER_CENTIRO_FETCHAWB_FETCHAWBBUSY';
		--
		commit;

end fetch_missing_awb_cto_p;