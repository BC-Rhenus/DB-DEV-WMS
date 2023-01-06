CREATE OR REPLACE PROCEDURE "CNL_SYS"."ACTIVE_CLIENTS_P" 
is
	cursor	c_new
	is
		select 	distinct 
			i.client_id
			from	dcsdba.inventory i
			where	i.client_id in 	(	
						select	t.client_id
						from	dcsdba.inventory_transaction t
						where	t.client_id is not null
						)
			and	 not exists	( 	
						select	1
						from	cnl_sys.cnl_active_clients a
						where	a.client_id = i.client_id
						)
	;
begin
	for 	i in c_new
	loop
		insert	into 
			cnl_active_clients
		(	db_link
		,	site_id
		,	client_id
		)
		values
		(	'JDA2016'
		,	'NLTLG01'
		,	i.client_id
		);
		commit;
	end loop;
exception
	when others
	then
		commit;
end active_clients_p;