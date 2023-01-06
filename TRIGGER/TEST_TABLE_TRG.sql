CREATE OR REPLACE TRIGGER "CNL_SYS"."TEST_TABLE_TRG" 
before update
on  CNL_SYS.test_table
for each row
declare
	l cnl_sys.test_table%rowtype;
begin
	l.sku_id 	:= :new.sku_id;
	l.client_id	:= :new.client_id;
	l.weight	:= :new.weight;
	l.width		:= :new.WIDTH;
	l.depth		:= :new.DEPTH;
	l.height	:= :new.HEIGHT;
	l.volume	:= :new.VOLUME;
	l.comments_een	:= :new.COMMENTS_EEN;
	l.comments_twee	:= :new.COMMENTS_TWEE;
	:new.comments_twee := l.sku_id;
exception
	when others
	then
		null;
end test_table_trg;