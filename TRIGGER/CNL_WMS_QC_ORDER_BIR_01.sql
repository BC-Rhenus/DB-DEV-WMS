CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_WMS_QC_ORDER_BIR_01" 
	before insert on  CNL_SYS.cnl_wms_qc_order
	for each row
	--when (new.qc_key is null)
declare
	cursor c_key( b_key integer)
	is
		select 	count(*)
		from	cnl_sys.cnl_wms_qc_order
		where	qc_key = b_key
	;
	--
	r_key	integer := 1;
	l_key 	integer;
begin
	if 	:new.qc_key is null
	then
		while r_key > 0
		loop
			l_key := cnl_wms_qc_order_seq1.nextval;
			open	c_key(l_key);
			fetch 	c_key into r_key;
			close 	c_key;
		end loop;
		:new.qc_key := l_key;
	end if;
	--
	if	:new.qc_req_yn is null
	then	
		:new.qc_req_yn := 'N';
	end if;
	--
	if	:new.qc_batch_yn is null 
	then	
		:new.qc_batch_yn := 'N';
	end if;
	--
	if	:new.qc_qty_def_yn is null 
	then	
		:new.qc_qty_def_yn := 'N';
	end if;
	--
	if	:new.qc_sku_select_yn is null 
	then	
		:new.qc_sku_select_yn := 'N';
	end if;
	--
	if	:new.qc_qty_upd_yn is null 
	then	
		:new.qc_qty_upd_yn := 'N';
	end if;
	--
	if	:new.qc_serial_yn is null 
	then	
		:new.qc_serial_yn := 'N';
	end if;

end cnl_wms_qc_order_bir_01;