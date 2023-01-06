CREATE OR REPLACE PROCEDURE "CNL_SYS"."APPLY_AVG_REGULATIONS_P" 
is
	l_db_alias	varchar2(50);
begin
	select 	db_alias 
	into   	l_db_alias
	from 	dcsdba.system_options;
	--
	if	l_db_alias != 'PRDCNLJW'
	then
		update	dcsdba.order_header o
		set	o.name			= 'Consignee Name'
		,	o.contact		= 'Consignee Contact'
		,	o.contact_phone		= 'Consignee Phone'
		,	o.contact_mobile	= 'Consignee Mobile'
		,	o.contact_fax 		= 'Consignee Fax'
		,	o.contact_email		= 'Consignee E-mail'
		,	o.inv_name           	= 'Invoice Name'
		,	o.inv_contact        	= 'Invoice Contact'
		,	o.inv_contact_phone  	= 'Invoice Phone'
		,	o.inv_contact_mobile 	= 'Invoice Mobile'
		,	o.inv_contact_fax    	= 'Invoice Fax'
		,	o.inv_contact_email  	= 'Invoice E-mail'
		,	o.hub_name           	= 'Hub Name'
		,	o.hub_contact        	= 'Hub Contact'
		,	o.hub_contact_phone  	= 'Hub Phone'
		,	o.hub_contact_mobile 	= 'Hub Mobile'
		,	o.hub_contact_fax    	= 'Hub Fax'
		,	o.hub_contact_email  	= 'Hub E-mail'
		where	o.creation_date 	< sysdate -30
		;
	end if;
	commit;
end apply_avg_regulations_p;