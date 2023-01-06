CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_OLE_EXTEND_BUR_01" 
	before	update 
	on 	cnl_sys.cnl_wms_order_line_extend
	for 	each row
declare
	l_hazmat	varchar2(1);
	l_ugly		varchar2(1);
	l_kit_sku	varchar2(1);
	l_awkward	varchar2(1);
	l_two_man_lift	varchar2(1);
	l_conveyable	varchar2(1);
begin
	select	nvl(hazmat,'N')
	,	nvl(ugly,'N') 
	,	nvl(kit_sku,'N')
	,	nvl(awkward,'N')
	,	nvl(two_man_lift,'N')
	,	nvl(conveyable,'N')
	into	l_hazmat
	,	l_ugly
	,	l_kit_sku
	,	l_awkward
	,	l_two_man_lift
	,	l_conveyable
	from	dcsdba.sku
	where	sku_id 		= :new.sku_id
	and	client_id	= :new.client_id
	;
	--
	:new.contains_hazmat 		:= l_hazmat;
	:new.contains_ugly_sku		:= l_ugly;
	:new.contains_awkward_sku	:= l_awkward;
	:new.contains_two_man_lift	:= l_two_man_lift;
	:new.contains_conveyable_sku	:= l_conveyable;
	:new.contains_kit		:= l_kit_sku;
	--
exception 
	when others
	then
		null;
end cnl_ole_extend_bur_01;