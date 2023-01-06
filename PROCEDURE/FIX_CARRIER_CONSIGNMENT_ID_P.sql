CREATE OR REPLACE PROCEDURE "CNL_SYS"."FIX_CARRIER_CONSIGNMENT_ID_P" 
is
/**********************************************************************************************
* This procedure we create to fix an issue with tracking_number in shipping manifest table.
* During the shipping of products the tracking number field becomes emtpy
* 
* This procedure is scheduled from WMS.
**********************************************************************************************/
	cursor c_empty
	is 
		select  sm.carrier_consignment_id, 
		        sm.order_id, 
		        sm.client_id, 
			sm.container_id ,
			sm.pallet_id,
		        max(replace(csl.tracking_number,',','.')) tracking_number
	        from    dcsdba.shipping_manifest    sm
	        ,       cnl_sys.cnl_cto_ship_labels csl
	        where   1=1
		and     trunc(sm.shipped_dstamp) >= trunc(sysdate)-14
	        and     sm.carrier_consignment_id is null
	        and     csl.order_id             = sm.order_id
		--and     csl.order_id = 'WELCH220816002'
		and     (sm.container_id          = csl.parcel_id
		   or sm.pallet_id          = csl.parcel_id)
		group by sm.carrier_consignment_id, 
	                 sm.order_id, 
		         sm.client_id,
			 sm.container_id,
			 sm.pallet_id
	;

begin
	for	r in c_empty
	loop
	    if r.tracking_number is not null then
		update dcsdba.shipping_manifest sm
		set    sm.carrier_consignment_id = r.tracking_number
		where  sm.carrier_consignment_id is null
		and    sm.order_id  = r.order_id
		--and    sm.client_id = r.client_id
		and    (sm.container_id = r.container_id
		   or sm.pallet_id = r.pallet_id)
		;
		dbms_output.put_line('order_id: '||r.order_id);
		commit;
		end if;

	end loop;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode
						  , p_sql_error_message_i	=> sqlerrm
						  , p_line_number_i		=> dbms_utility.format_error_backtrace
						  , p_package_name_i		=> 'No Package'
						  , p_routine_name_i		=> 'fix_carrier_consignment_id_p'
						  , p_routine_parameters_i	=> null
						  , p_comments_i		=> 'Something went wrong when trying to update the shipping manifest table with the correct tracking_number'					-- Additional comments describing the issue
						  );
end fix_carrier_consignment_id_p;