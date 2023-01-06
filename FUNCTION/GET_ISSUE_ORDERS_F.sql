CREATE OR REPLACE FUNCTION "CNL_SYS"."GET_ISSUE_ORDERS_F" (p_pallet_id_i varchar2)
	return varchar2  
is
	-- Fetch all orders on the pallet
	cursor c_ord (b_pallet varchar2)
	is 
		select	distinct order_id  
		,	client_id
		from 	dcsdba.order_container o 
		where 	pallet_id = b_pallet
	;
	-- Fetch number of serials required or order
	cursor c_lin ( 	b_order_id 	varchar2
		     ,	b_client_id 	varchar2
		     ) 
	is 
		select 	sum(l.qty_tasked+l.qty_picked)
		from	dcsdba.order_line l
		where	l.order_id 	= b_order_id
		and	l.client_id 	= b_client_id
		and	l.sku_id in (	select	s.sku_id 
					from 	dcsdba.sku s 
					where 	s.sku_id = l.sku_id 
					and	s.client_id = l.client_id 
					and    (s.serial_at_pick = 'Y' or 
						s.serial_at_pack = 'Y'))
	;
	-- Fetch number of serials available in order
	cursor c_ser (	b_order_id 	varchar2
		     ,	b_client_id	varchar2
		     )
	is
		select	count(*) 
		from 	dcsdba.serial_number 
		where 	order_id 	= b_order_id
		and	client_id 	= b_client_id
	;
	-- fetch all pallets for order
	cursor 	c_pal(	b_order_id	varchar2
		     , 	b_client_id	varchar2
		     )
	is
		select	distinct ocr.pallet_id
		from 	dcsdba.order_container ocr
		where	ocr.order_id 	= b_order_id
		and	ocr.client_id 	= b_client_id
	;
	-- fetch all containers records for order on pallet
	cursor 	c_ocr(	b_order_id	varchar2
		     , 	b_client_id	varchar2
		     ,	b_pallet_id 	varchar2
		     )
	is
		select	distinct ocr.container_id
		from 	dcsdba.order_container ocr
		where	ocr.order_id 	= b_order_id
		and	ocr.client_id 	= b_client_id
		and	ocr.pallet_id 	= b_pallet_id
	;
	--
	r_ser_req	number;
	r_ser_ava	number;
	l_order		varchar2(50);
	l_client	varchar2(50);
	l_pallet	varchar2(50);
	l_containers	varchar2(1000);
	l_retval 	varchar2(4000);
begin
	for 	r_ord in c_ord(p_pallet_id_i)
	loop
		l_order 	:= r_ord.order_id;
		l_client	:= r_ord.client_id;
		l_pallet	:= null;
		l_containers	:= null;
		r_ser_req := 0;
		r_ser_ava := 0;
		open	c_lin(r_ord.order_id, r_ord.client_id);
		fetch 	c_lin 
		into 	r_ser_req;
		if	c_lin%notfound
		then
			close	c_lin;
			continue;
		else
			close 	c_lin;
			open 	c_ser(r_ord.order_id, r_ord.client_id);
			fetch 	c_ser 
			into 	r_ser_ava;
			if	c_ser%notfound
			then
				r_ser_ava := 0;
			end if;
			close 	c_ser;
			if	r_ser_req != r_ser_ava
			then	-- A difference has been found.
				for	r_pal in c_pal(l_order, l_client)
				loop
					l_pallet := r_pal.pallet_id;
					for	r_ocr in c_ocr(l_order, l_client, l_pallet)
					loop
						if	l_containers is null
						then
							l_containers := '"'||r_ocr.container_id||'"';
						else
							l_containers := l_containers||',"'||r_ocr.container_id||'"';
						end if;
					end loop;
					if	l_retval is null
					then
						l_retval := 	'client_id = '||l_client||
								', order_id = '||l_order||
								', pallet_id = '||l_pallet||
								', containers = '||l_containers;
					else
						l_retval := 	l_retval||
								', client_id = '||l_client||
								', order_id = '||l_order||
								', pallet_id = '||l_pallet||
								', containers = '||l_containers;
					end if;
				end loop;
			else -- no difference found
				continue;
			end if;	
		end if;
	end loop;
	if 	l_retval is null
	then
		l_retval := 'NO SERIAL ISSUES FOUND';
	end if;
	return l_retval;
end;