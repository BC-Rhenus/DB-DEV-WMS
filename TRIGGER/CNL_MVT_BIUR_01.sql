CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_MVT_BIUR_01" 
before insert or update on dcsdba.move_task for each row
 WHEN ( new.from_loc_id like '%WHH') declare
/**********************************************************************************
 * $Archive: $
 * $Revision: $
 * $Author: $
 * $Date: $
 **********************************************************************************
 * Description: Trigger to prevent warehouse handling tasks being updated manually
 * instead by the web portal
 **********************************************************************************
 * $Log: $
 *********************************************************************************/
	cursor	c_status
	is
		select	status
		from	dcsdba.move_task
		where	task_id = 'PALLET'
		and	pallet_id = :new.pallet_id
	;

	v_whhloc 	dcsdba.location.user_def_chk_4%type;
	v_group		dcsdba.application_user.group_id%type 	:= dcsdba.libsession.sessiongroupid;
	v_user		dcsdba.application_user.user_id%type 	:= dcsdba.libsession.sessionuserid;
	v_status	dcsdba.move_task.status%type;
begin
	if 	updating
	and 	(	:new.task_id 	= 'PALLET'
		or	:new.status 	= 'Consol'
		)
	then
		-- Marshal task from a different location to WHH location was executed.
		if	:old.from_loc_id != :new.from_loc_id
		then	
			select	nvl(l.user_def_chk_4,'N')
			into 	v_whhloc
			from	dcsdba.location l
			where	l.location_id 	= :new.from_loc_id
			and	l.site_id 	= :new.site_id;
			--
			if	v_whhloc = 'Y'
			then
				if 	:new.task_id 	= 'PALLET'
				then
					:new.status 	:= 'WHHandling';
				elsif	:new.status 	= 'Consol'
				then
					:new.status 	:= 'WHHConsol';
				end if;
			end if;
		-- Status update done by a person not WHH application
		elsif	(	:old.status 	= 'WHHandling'
			or	:old.status 	= 'WHHConsol'
			)
		and		:new.status 	!= 'WHHandling'
		and		:new.status 	!= 'WHHConsol'
		and	(	:new.last_held_reason_code != 'WHHANDLING' 
			or 	:new.last_held_reason_code is null
			)
		then	
			if	:new.task_id 	= 'PALLET'
			then
				:new.status 	:= 'WHHandling';
			else 	
				:new.status 	:= 'WHHConsol';
			end if;
		end if;
	end if;
	--
	if 	inserting
	and	v_user = 'Mvtcdae'
	then
		select	nvl(l.user_def_chk_4,'N')
		into 	v_whhloc
		from	dcsdba.location l
		where	l.location_id 	= :new.from_loc_id
		and	l.site_id 	= :new.site_id;
		--
		if	v_whhloc = 'Y'
		then
			if	:new.task_id = 'PALLET'
			then
				:new.status := 'WHHandling';
			else
				open 	c_status;
				fetch	c_status 
				into	v_status;
				close	c_status;
				if	v_status = 'WHHandling'
				then
					:new.status := 'WHHConsol';
				end if;
			end if;
		end if;
	end if;
end 	cnl_mvt_buir_01;