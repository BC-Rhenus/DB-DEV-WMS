CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_SUP_AIDR_01" 
    after   insert or
            delete 
    on      dcsdba.supplier_sku
    for each row
    declare
        l_tbl       varchar2(3);
        l_action    varchar2(1);
        l_sku       varchar2(50);
        l_client    varchar2(20);
        l_sup       varchar2(50);
	l_database  varchar2(20);
        pragma autonomous_transaction;
    begin
    	select 	name 
	into 	l_database
	from 	v$database;
	if 	l_database in (/*'DEVCNLJW',*/'TSTCNLJW',/*'ACCCNLJW',*/'PRDCNLJW')
	then
		if inserting
		then
		    l_action    := 'I';
		    l_tbl       := 'SUP';
		    l_sku       := :new.sku_id;
		    l_sup       := :new.supplier_sku_id;
		    l_client    := :new.client_id;
		else
		    l_action    := 'D';
		    l_tbl       := 'SUP';
		    l_sku       := :old.sku_id;
		    l_sup       := :old.supplier_sku_id;
		    l_client    := :old.client_id;
		end if;
		cnl_sys.cnl_db_job_pck.submit_once( p_procedure_i => 'begin cnl_sys.cnl_as_masterdata_pck.save_wms_iud_record('''
								   || l_tbl
								   || ''','''
								   || l_action
								   || ''','''
								   || l_client
								   || ''','''
								   || l_sku
								   || ''',null,null,''' 
								   || l_sup
								   || '''); end;',
						    p_code_i       => 'P_SUP_' || cnl_sys.cnl_as_trigger_seq1.nextval,
						    p_delay_i      => 1);
	end if;
end cnl_as_sup_aidr_01;