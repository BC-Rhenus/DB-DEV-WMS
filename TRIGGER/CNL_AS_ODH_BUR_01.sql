CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_ODH_BUR_01" 
  before update on dcsdba.order_header
  for each row
    WHEN (new.status = 'In Progress' and old.status = 'Allocated' and new.from_site_id = 'NLTLG01' and new.tax_rate_5 is not null) declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to identify orders that have started for autostore
**********************************************************************************
* $Log: $
**********************************************************************************/
	l_database varchar2(20);
    pragma autonomous_transaction;
begin
	select 	name 
	into 	l_database
	from 	v$database;
	if 	l_database in (/*'DEVCNLJW',*/'TSTCNLJW',/*'ACCCNLJW',*/'PRDCNLJW')
	then
		cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_as_outbound_pck.wms_get_orders_started('''
								   || :new.order_id
								   || ''',null,'''
								   || :new.client_id
								   || ''','''
								   || :new.from_site_id
								   || '''); end;',
						     p_code_i      => 'P_MOS_' || cnl_sys.cnl_as_trigger_seq1.nextval,
						     p_delay_i     => 1
						   );
	end if;
exception
  when others
  then
    null;  -- In after Row trigger no raise is allowed
end cnl_as_odh_bur_01;