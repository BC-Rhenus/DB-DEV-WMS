CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_ITN_AIR_01" 
  after insert on dcsdba.inventory_transaction
  for each row
    WHEN (
       (new.code = 'PreAdv Status' and new.to_status = 'Complete')
       or
       (new.code = 'Order Status' and new.to_status = 'Ready to Load')
       or
       (new.code = 'Adjustment' and new.update_qty < 0 and nvl(new.reason_id, '|^|') != 'CORRPOSADJ')
       ) declare
/**********************************************************************************
* $Archive: $
* $Revision: $
* $Author: $
* $Date: $
**********************************************************************************
* Description: Trigger to process the ITN from WMS for SSW
**********************************************************************************
* $Log: $
**********************************************************************************/
  cursor c_cltent ( b_client_id in varchar2)
  is
    select 1
    from   dcsdba.client_group_clients
    where  client_id    = b_client_id
    and    client_group = 'CSLENT'    -- Client Visibility Group with Bonded Warehouse Clients
    ;
  cursor c_cltexp ( b_client_id in varchar2)
  is
    select 1
    from   dcsdba.client_group_clients
    where  client_id    = b_client_id
    and    client_group = 'CSLEXP'    -- Client Visibility Group with Clients who require Export Declarations
    ;

  l_integer   integer;

  pragma autonomous_transaction;
begin
  -- check if the transaction is for a Bonded Warehouse Client
  open  c_cltent ( b_client_id => :new.client_id);
  fetch c_cltent
  into  l_integer;
  --
  if c_cltent%found
  then
    cnl_sys.cnl_db_job_pck.submit_once( p_procedure_i => 'begin cnl_sys.cnl_wms_pck.process_itn_csl (' || :new.key         || ','''
                                                                                                       || :new.client_id   || ''','''
                                                                                                       || :new.from_status || ''','''
                                                                                                       || :new.to_status   || '''); end;'
                                      , p_code_i      => 'CSL_' || :new.key
                                      , p_delay_i     => 1
                                      );
    close c_cltent;
  else
    close c_cltent;
    -- Check if the transaction (Order Status Change only !) is for a Client which requires an Export declaration
    open  c_cltexp ( b_client_id => :new.client_id);
    fetch c_cltexp
    into  l_integer;
    --
    if c_cltexp%found
    then
      if :new.code = 'Order Status'
      then
        cnl_sys.cnl_db_job_pck.submit_once( p_procedure_i => 'begin cnl_sys.cnl_wms_pck.process_itn_csl (' || :new.key         || ','''
                                                                                                           || :new.client_id   || ''','''
                                                                                                           || :new.from_status || ''','''
                                                                                                           || :new.to_status   || '''); end;'
                                          , p_code_i      => 'CSL_' || :new.key
                                          , p_delay_i     => 1
                                          );
      end if;
      close c_cltexp;
    else
      close c_cltexp;
    end if;  
  end if;
end cnl_itn_air_01;