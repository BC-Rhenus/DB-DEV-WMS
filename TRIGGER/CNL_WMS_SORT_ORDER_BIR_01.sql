CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_WMS_SORT_ORDER_BIR_01" 
	before insert on  CNL_SYS.cnl_wms_sort_order
	for each row
         WHEN (new.key is null) declare
	cursor c_key(b_key integer)
	is
		select    s.key 
		from      cnl_wms_sort_order s
		where     s.key = b_key
	;
	--
	cursor  c_f_key
	is
		select    max(s.key) 
		from      cnl_wms_sort_order s
	;
	--
	l_key    integer;
	r_key    integer;
	l_timer  integer := 0;
	l_ok     integer := 1;
begin
	l_key 	:= cnl_wms_sort_order_seq1.nextval;
	while	l_ok = 1
	loop
		open      c_key(l_key);
		fetch     c_key into r_key;
		if        c_key%notfound
		then 
			close	c_key;
			:new.key := l_key;
			l_ok     := 0;
		elsif	l_timer  = 1000
		then    
			close   c_key;
			open    c_f_key;
			fetch   c_f_key 
			into 	r_key;
			if      c_f_key%notfound
			then  
				close c_f_key;
				:new.key  := 1;
				l_ok      := 0;
			else
				close c_f_key;
				:new.key  :=  r_key+1;
				l_ok      :=  0;
			end if;
		else      
			close c_key;
			l_key := cnl_wms_sort_order_seq1.nextval;
            end if;
            l_timer := l_timer + 1;
	end loop;
end	cnl_as_vas_sortation_bir_01;