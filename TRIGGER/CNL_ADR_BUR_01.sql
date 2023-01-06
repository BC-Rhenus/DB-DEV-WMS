CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_ADR_BUR_01" 
  BEFORE UPDATE ON "DCSDBA"."ADDRESS"
  REFERENCING FOR EACH ROW
  WHEN (	new.address_id 		= 'ZERODEMAND'
		and	(	new.user_def_chk_1	= 'Y'
			or	new.user_def_chk_2	= 'Y'
			)
		and	new.user_def_type_3 		is not null
		) declare
	l_zero_demand	dcsdba.address.user_def_chk_1%type;
	l_zero_alloc	dcsdba.address.user_def_chk_2%type;
	l_num_locations	dcsdba.address.user_def_num_3%type;
	l_loc_height	dcsdba.address.user_def_num_4%type; --BDS-5978
	pragma autonomous_transaction;
begin
	l_zero_demand		:= :new.user_def_chk_1;
	l_zero_alloc		:= :new.user_def_chk_2;
	l_num_locations		:= :new.user_def_num_3;
	l_loc_height		:= nvl(:new.user_def_num_4,0);
	:new.user_def_chk_1 	:= 'N';
	:new.user_def_chk_2 	:= 'N';
	:new.user_def_num_3	:= null;
	:new.user_def_num_4	:= null;
	cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i 	=> 'begin cnl_sys.cnl_client_specifics_pck.search_non_pf_zero_demand_p('''
							 	|| :new.user_def_type_3	||''',''' -- From zone
								|| :new.user_def_type_7	||''',''' -- From sub zone
								|| :new.user_def_type_4	||''',''' -- to zone (Obsolete)
								|| :new.user_def_type_5	||''',' -- pallet type
								|| :new.user_def_num_2	||',''' -- location levels
								|| l_zero_alloc		||''',''' -- zero alloc
								|| l_zero_demand	||''',''' -- zero demand
								|| :new.user_def_type_8	||''',' -- exclude rule id
								|| l_num_locations	||',''' -- max locations to empty
								|| :new.client_id	||''','
								|| l_loc_height	||'); end;'
					   , p_code_i      => 'P_ZERODEMAND'
					   , p_delay_i     => 1
					   );
end cnl_adr_aur_01;