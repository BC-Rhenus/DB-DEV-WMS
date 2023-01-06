CREATE OR REPLACE PROCEDURE "CNL_SYS"."FIX_SERIAL_PALLET_ID_P" 
is
/**********************************************************************************************
* This procedure we create to fix an issue with pallet id's in the serial number table.
* During the pallet build process e.g. container closing => add to pallet the pallet id 
* in the erial number table is not always updated correctly. When an attempt is made to remove a container
* from a pallet WMS will alwats demand to scan the serials inside the container because of the mismatch 
* between serial number pallet id and actual pallet id.
* 
* This procedure is scheduled from WMS.
**********************************************************************************************/
	cursor	c_mismatch
	is
		select	distinct 
			c.pallet_id
		,	c.container_id
		,	c.order_id
		,	c.client_id
		,	s.pallet_id serial_pallet_id
		from	dcsdba.serial_number s
		inner   join
			dcsdba.order_container c
		on	c.order_id 	= s.order_id
		and	c.container_id 	= s.container_id
		and	c.client_id 	= s.client_id
		inner	join
			dcsdba.order_header o
		on	o.order_id = s.order_id 
		and 	o.status not in ('Shipped','Delivered','Complete')
		and	o.client_id 	= s.client_id
		where	s.pallet_id 	!= c.pallet_id
	;
begin
	for	r in c_mismatch
	loop
		update	dcsdba.serial_number 	s 
		set	s.pallet_id 	= r.pallet_id
		where 	s.container_id	= r.container_id
		and	s.order_id	= r.order_id
		and    	s.client_id	= r.client_id
		and    	s.pallet_id	!= r.pallet_id
		and	r.pallet_id 	!= r.serial_pallet_id
		and	s.status 	= 'I'
		and	s.container_id 	is not null
		and	s.order_id 	is not null
		;
		commit;
	end loop;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode
						  , p_sql_error_message_i	=> sqlerrm
						  , p_line_number_i		=> dbms_utility.format_error_backtrace
						  , p_package_name_i		=> 'No Package'
						  , p_routine_name_i		=> 'fix_serial_pallet_id_p'
						  , p_routine_parameters_i	=> null
						  , p_comments_i		=> 'Something went wrong when trying to update the serial number table with the correct pallet id'					-- Additional comments describing the issue
						  );
end fix_serial_pallet_id_p;