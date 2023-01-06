CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_MVT_BUR_01" 
before update on dcsdba.move_task for each row
 WHEN (old.status = 'Cubing' and new.status != 'Cubing') declare
/**********************************************************************************
 * $Archive: $
 * $Revision: $
 * $Author: $
 * $Date: $
 **********************************************************************************
 * Description: Trigger to prevent users from updating cubing or autostore tasks
 * instead by the web portal
 **********************************************************************************
 * $Log: $
 *********************************************************************************/
	v_group	DCSDBA.application_user.group_id%type 	:= dcsdba.libsession.sessiongroupid;
	v_user	DCSDBA.application_user.user_id%type 	:= dcsdba.libsession.sessionuserid;
begin
	if	v_user != 'AUTOSTORE'
	and	v_group not in ('BAM','CLADMIN','SUPERUSER')
	then 
		:new.status := :old.status;
	end if;
end 	cnl_mvt_buir_01;