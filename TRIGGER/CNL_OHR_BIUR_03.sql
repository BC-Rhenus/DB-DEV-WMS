CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OHR_BIUR_03" 
  before insert or update of status_reason_code on dcsdba.order_header
  for each row
    WHEN (
       (nvl(old.status_reason_code,'X') != 'CSERROR' and new.status_reason_code = 'CSREQUIRED')
       or
       (old.status_reason_code = 'CSPENDING' and new.status_reason_code = 'CSSUCCESS')
       ) declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to to save and replace the original INSTRUCTIONS (Only Centiro on premise)
**********************************************************************************
* $Log: $
**********************************************************************************/
  cursor c_oin (b_site_id   in varchar2
               ,b_client_id in varchar2
               ,b_order_id  in varchar2
               )
  is
    select id
    ,      instructions
    from   cnl_sys.cnl_ohr_instructions
    where  site_id   = b_site_id
    and    client_id = b_client_id
    and    order_id  = b_order_id
    ;
	cursor c_saas(b_client_id varchar2)
	is
		select	count(*)
		from	dcsdba.client_group_clients
		where	client_group = 'CTOSAAS'
		and 	client_id = b_client_id
	;
  r_oin          c_oin%rowtype;
  l_id           number(10);
  l_instructions varchar2(180);
  l_saas		integer;
begin
	-- New Saas solution does not require this trigger. Therefor if client exists in client group CTOSAAS this trigger can be skipped.
	open 	c_saas(:new.client_id);
	fetch	c_saas
	into	l_saas;
	close	c_saas;
	if 	l_saas = 0
	then
			open  c_oin (b_site_id   => :new.from_site_id
					  ,b_client_id => :new.client_id
					  ,b_order_id  => :new.order_id
					  );
		  fetch c_oin
		  into  r_oin;
		  close c_oin;

		  l_id           := r_oin.id;
		  l_instructions := r_oin.instructions;

		  if inserting
		  then
			-- Register the instructions when order inserted with CSREQUIRED
			if  :new.status_reason_code = 'CSREQUIRED'                     
			then
			  if  l_id is null
			  and :new.instructions is not null
			  then
				insert into cnl_sys.cnl_ohr_instructions (site_id
														 ,client_id
														 ,order_id
														 ,instructions
														 )
				values                                   (:new.from_site_id
														 ,:new.client_id
														 ,:new.order_id
														 ,:new.instructions
														 );
			  end if;
			end if;

		  elsif updating
		  then
			-- Register the instructions when order updated to CSREQUIRED but not from status CSERROR 
			if  nvl(:old.status_reason_code,'X') != 'CSERROR'
			and :new.status_reason_code = 'CSREQUIRED'                     
			then
			  if  l_id is null
			  and :new.instructions is not null
			  then
				insert into cnl_sys.cnl_ohr_instructions (site_id
														 ,client_id
														 ,order_id
														 ,instructions
														 )
				values                                   (:new.from_site_id
														 ,:new.client_id
														 ,:new.order_id
														 ,:new.instructions
														 );
			  end if;
			end if;

			-- Restore the original instructions when orders updated to status CSSUCCESS after carrier selection, so from status CSPENDING
			if  :old.status_reason_code = 'CSPENDING'
			and :new.status_reason_code = 'CSSUCCESS'                     
			then
			  if  l_id is not null
			  then
				:new.instructions := l_instructions;
			  end if;
			end if;
		  end if;
	end if;
exception
   when others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_ohr_bur_03;