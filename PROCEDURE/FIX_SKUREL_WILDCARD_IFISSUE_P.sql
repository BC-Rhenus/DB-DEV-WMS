CREATE OR REPLACE PROCEDURE "CNL_SYS"."FIX_SKUREL_WILDCARD_IFISSUE_P" 
is
	cursor c_err
	is
		select	distinct r.* 
		from 	dcsdba.interface_sku_relocation r
		inner
		join	dcsdba.location_zone l
		on	l.site_id	= r.site_id
		and	nvl(l.zone_1,'*') 	like (replace(replace(nvl(r.from_zone		,'*'),'*','%'),'?','_'))
		and	nvl(l.subzone_1,'*')	like (replace(replace(nvl(r.from_subzone_1	,'*'),'*','%'),'?','_'))
		and	nvl(l.subzone_2,'*')	like (replace(replace(nvl(r.from_subzone_2	,'*'),'*','%'),'?','_'))
		where 	(	nvl(r.from_zone,'X') 		like '%*%'
			or	nvl(r.from_subzone_1,'X') 	like '%*%'
			or	nvl(r.from_subzone_2,'X')	like '%*%'
			)
		and	r.merge_error 	in ('IF0655','IF4592')
		and	r.merge_status 	= 'Error'
		and	r.merge_action 	= 'A'
	;
	l_key		integer;
	l_insert_issue	varchar2(1) := 'N';
begin
	for 	i in c_err
	loop
		l_insert_issue	:= 'N';
		l_key := dcsdba.sku_rel_seq.nextval;
		begin
			insert 
			into 	dcsdba.sku_relocation
			(	owner_id
			,	no_tags
			,	to_zone
			,	to_subzone_1
			,	to_subzone_2
			,	from_zone
			,	from_subzone_1
			,	from_subzone_2
			,	algorithm
			,	task_id
			,	qty_required
			,	trigger_qty
			,	disallow_tag_swap
			,	priority
			,	allowed_inv_status
			,	lock_code
			,	key
			,	site_id
			,	client_id
			,	sku_id
			,	condition_id
			,	origin_id
			)
			values
			(	i.owner_id
			,	i.no_tags
			,	i.to_zone
			,	i.to_subzone_1
			,	i.to_subzone_2
			,	i.from_zone
			,	i.from_subzone_1
			,	i.from_subzone_2
			,	i.algorithm
			,	i.task_id
			,	i.qty_required
			,	i.trigger_qty
			,	i.disallow_tag_swap
			,	i.priority
			,	i.allowed_inv_status
			,	i.lock_code
			,	l_key
			,	i.site_id
			,	i.client_id
			,	i.sku_id
			,	i.condition_id
			,	i.origin_id
			);
		exception
			when others
			then
				l_insert_issue := 'Y';
		end;
		if	l_insert_issue 	= 'N'
		then
			delete	dcsdba.interface_sku_relocation
			where	key = i.key;
		end if;
	end loop;
	commit;
end fix_skurel_wildcard_ifissue_p;