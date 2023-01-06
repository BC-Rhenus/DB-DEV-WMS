CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_SMT_AIR_01" 
  after insert on dcsdba.shipping_manifest
  for each row
      WHEN (new.carrier_consignment_id is null) declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to fetch track and trace from Centiro
**********************************************************************************
* $Log: $
**********************************************************************************/

  pragma autonomous_transaction;
begin
  cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_wms_pck.upd_tracking_number ('
                                                     || :new.key
                                                     || ','''
                                                     || :new.client_id
                                                     || ''','''
                                                     || :new.site_id
                                                     || ''','''
                                                     || :new.order_id
                                                     || ''','''
                                                     || :new.container_id
                                                     || ''','''
                                                     || :new.pallet_id
                                                     || ''','''
                                                     || :new.labelled
                                                     || ''','''
                                                     || :new.pallet_labelled
                                                     || '''); end;'
                                     , p_code_i      => 'P_SMT_' || :new.key
                                     , p_delay_i     => 1
                                     );
exception
   when   others
   then
      null;  -- In before Row trigger no raise is allowed
end cnl_smt_air_01;