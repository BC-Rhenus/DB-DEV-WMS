CREATE OR REPLACE PROCEDURE "CNL_SYS"."DEL_NON_EXIST_CONT_P" (	p_container_id_i varchar2
						,	p_pallet_id_i	 varchar2
						)
is
	cursor c_tasks
	is
		select	1
		from	dcsdba.move_task m
		where	m.pallet_id 	= p_pallet_id_i
		and	m.container_id 	= p_container_id_i
	;
	r_tasks integer;
begin
	open	c_tasks;
	fetch 	c_tasks into r_tasks;
	if	c_tasks%notfound
	then
		close 	c_tasks;
		delete 	dcsdba.order_container ocr
		where	ocr.pallet_id 	 = p_pallet_id_i
		and	ocr.container_id = p_container_id_i;
		commit;
	else
		close	c_tasks;
	end if;
exception
	when others
	then
		null;	
end del_non_exist_cont_p;