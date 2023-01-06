CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_AS_SKU_AIUR_01" 
    after   insert or
            update
    on      dcsdba.sku
    for each row
    declare
        l_tbl       varchar2(3);
        l_action    varchar2(1);
        l_sku       varchar2(50);
        l_client    varchar2(30);
        l_process   varchar2(1) := 'N';
	l_database  varchar2(20);
        pragma autonomous_transaction;
    begin
    	select 	name 
	into 	l_database
	from 	v$database;
	if 	l_database in (/*'DEVCNLJW',*/'TSTCNLJW',/*'ACCCNLJW',*/'PRDCNLJW')
	then
		if      updating 
		and    (nvl(:old.description,'N')       != nvl(:new.description,'N')    or
			nvl(:old.fragile,'N')           != nvl(:new.fragile,'N')        or 
			nvl(:old.ugly,'N')              != nvl(:new.ugly,'N')           or
			nvl(:old.each_weight,'0')       != nvl(:new.each_weight,'0')    or
			nvl(:old.each_height,'0')       != nvl(:new.each_height,'0')    or
			nvl(:old.each_width,'0')        != nvl(:new.each_width,'0')     or
			nvl(:old.each_depth,'0')        != nvl(:new.each_depth,'0')     or
			nvl(:old.each_volume,'0')       != nvl(:new.each_volume,'0')    or
			nvl(:old.ean,'N')               != nvl(:new.ean,'N')            or
			nvl(:old.upc,'N')               != nvl(:new.upc,'N')            or
			nvl(:old.serial_at_pick,'N')    != nvl(:new.serial_at_pick,'N') or
			nvl(:old.user_def_chk_1,'N')    != nvl(:new.user_def_chk_1,'N') or
			nvl(:old.qc_status,'N')		!= nvl(:new.qc_status,'N')
		       ) 
		then
		       l_action     := 'U';
		       l_tbl        := 'SKU';
		       l_sku        := :new.sku_id;
		       l_client     := :new.client_id;
		       l_process    := 'Y';
		elsif  inserting
		then
		       l_action     := 'I';
		       l_tbl        := 'SKU';
		       l_sku        := :new.sku_id;
		       l_client     := :new.client_id;
		       l_process    := 'Y';
		end if;
		if  l_process = 'Y'
		then
			cnl_sys.cnl_db_job_pck.submit_once ( p_procedure_i => 'begin cnl_sys.cnl_as_masterdata_pck.save_wms_iud_record('''
									   || l_tbl
									   || ''','''
									   || l_action
									   || ''','''
									   || l_client
									   || ''','''
									   || l_sku
									   || ''',null,null,null); end;',
							      p_code_i      => 'P_SKU_' || cnl_sys.cnl_as_trigger_seq1.nextval,
							      p_delay_i     => 1);
		end if;
	end if;
end cnl_as_sku_aiur_01;