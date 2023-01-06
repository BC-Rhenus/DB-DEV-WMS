CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_ITL_AIR_01" 
  after insert on dcsdba.inventory_transaction
  for each row
   WHEN ((   new.code = 'Marshal' and new.to_loc_id = '30APACK' and new.site_id = 'NLTLG01' and new.station_id != 'AUTOSTORE') or
        (   new.code = 'Deallocate' and new.site_id = 'NLTLG01' and new.reference_id != 'REPLENISH')
       ) declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to identify container ready for autostore
**********************************************************************************
* $Log: $
**********************************************************************************/
    l_work_group 	varchar2(50);
    l_consignment 	varchar2(50);
    l_line_id 		number;
    l_from_loc_id 	varchar2(50);
    l_to_loc_id 	varchar2(50);
    l_final_loc_id 	varchar2(50);
    l_tag_id		varchar2(50);
    l_database		varchar2(20);
    pragma autonomous_transaction;
begin
	select 	name 
	into 	l_database
	from 	v$database;
	if 	l_database in (/*'DEVCNLJW',*/'TSTCNLJW',/*'ACCCNLJW',*/'PRDCNLJW')
	then
		if :new.code = 'Marshal'
		then
			cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_as_outbound_pck.manual_pick_finished('
					                                   || :new.key
							                   || ','''
									   || :new.pallet_id
		                                                           || ''','''
				                                           || :new.container_id
						                           || ''','''
								           || :new.station_id
		                                                           || ''','''
				                                           || :new.site_id
						                           || ''','''
								           || :new.client_id
		                                                           || ''','''
				                                           || :new.to_loc_id
						                           || ''','''
								           || :new.consol_link
		                                                           || '''); end;',
				                             p_code_i      => 'P_MOS_' || cnl_sys.cnl_as_trigger_seq1.nextval,
						             p_delay_i     => 1
		                                           );
		end if;
		if :new.code = 'Deallocate'
		then
			if 	:new.work_group is null
			then
				l_work_group := 'NOWORKGROUP';
			else
				l_work_group := :new.work_group;
			end if;
			--
			if 	:new.consignment is null
			then
				l_consignment := 'NOCONSIGNMENT';
			else
				l_consignment := :new.consignment;
			end if;
			--
			if 	:new.line_id is null
			then
				l_line_id := 1;
			else
				l_line_id := :new.line_id;
			end if;
			--
			if 	:new.from_loc_id is null
			then
				l_from_loc_id := 'NOFROMLOCID';
			else
				l_from_loc_id := :new.from_loc_id;
			end if;
			--
			if 	:new.to_loc_id is null
			then
				l_to_loc_id := 'NOTOLOCID';
			else
				l_to_loc_id := :new.to_loc_id;
			end if;
			--
			if 	:new.final_loc_id is null
			then
				l_final_loc_id := 'NOFINALLOCID';
			else
				l_final_loc_id := :new.final_loc_id;
			end if;
			--
			if 	:new.tag_id is null
			then
				l_tag_id := 'NOTAGID';
			else
				l_tag_id := :new.tag_id;
			end if;
			--
			cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_as_outbound_pck.task_deallocation('''
									|| :new.site_id
		                                                           || ''','''
				                                           || :new.owner_id
						                           || ''','''
								           || :new.client_id
		                                                           || ''','''
				                                           || l_tag_id
						                           || ''','''
								           || :new.sku_id
		                                                           || ''','''
				                                           || l_from_loc_id
						                           || ''','''
								           || l_to_loc_id
		                                                           || ''','''
				                                           || l_final_loc_id
						                           || ''','
								           || :new.update_qty
		                                                           || ','''
				                                           || :new.reference_id
						                           || ''','
								           || l_line_id
		                                                           || ','''
				                                           || l_work_group
						                           || ''','''
		                                                           || l_consignment
				                                           ||'''); end;',
						             p_code_i      => 'P_DEA_' || cnl_sys.cnl_as_trigger_seq1.nextval,
		                                             p_delay_i     => 1
				                           );
		end if;
	end if;
exception
  when others
  then
    null;  -- In after Row trigger no raise is allowed
end cnl_as_itl_air_01;