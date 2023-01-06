CREATE OR REPLACE FORCE VIEW "CNL_SYS"."WMS_VS_STREAMLINER_V" ("CLIENT_ID", "ORDER_ID", "STATUS", "LINE_ID", "SKU_ID", "TAG_ID", "PRE_ADVICE_ID", "QTY_ORDERED", "QTY_SHIPPED", "VALUE", "CURRENCY", "ORIGIN_ID", "CONDITION_ID", "SHIPPED_DATE", "NETTO_WEIGHT", "AANTAL_DOSSIERS", "LINK", "DOSSIER_ID", "QTY_IN_STREAM", "TAG_STREAM", "STREAM_VALUE", "INVOICE_NUMBER") AS 
  with md as (
select	smt.client_id
,	smt.order_id
,	(select o.status from dcsdba.order_header o where o.order_id = smt.order_id and o.client_id = smt.client_id) status
,	smt.line_id
,	smt.sku_id
,      	smt.tag_id
,	(select distinct i.reference_id from dcsdba.inventory_transaction i where i.code = 'Receipt' and i.tag_id = smt.tag_id and i.client_id = smt.client_id and rownum =1) pre_advice_id
,	(select l.qty_ordered from dcsdba.order_line l where l.order_id = smt.order_id and l.line_id = smt.line_id) qty_ordered
,      	smt.qty_shipped
,	smt.qty_shipped * (select l.line_value/l.qty_ordered from dcsdba.order_line l where l.order_id = smt.order_id and l.line_id = smt.line_id) value
,	(select o.inv_currency from dcsdba.order_header o where o.order_id = smt.order_id and o.client_id = smt.client_id) currency
,	smt.origin_id
,	smt.condition_id
,	to_char(smt.shipped_dstamp,'DD-MON-YYYY HH:MI:SS') Shipped_date
,	(select (s.each_weight/100)*90 from dcsdba.sku s where s.sku_id = smt.sku_id and s.client_id = smt.client_id) netto_weight
,      	stream.link
,      	stream.dossierid
,      	stream.pieces 		qty_in_stream
,      	stream.serialnumber 	tag_stream
,	stream.price		stream_value
,	stream.invoicenumber	
from dcsdba.shipping_manifest smt
,    (select 	replace(dsr.linkiderp,' ','') order_id
      ,      	replace(dsr.linkiderp,' ','')||substr(dln.linkiderp,2,5)||dln.serialnumber link
      ,      	dsr.dossierid dossierid
      ,      	dln.pieces
      ,      	dln.serialnumber
      ,		dln.price
      ,		dsr.invoicenumber
      from customs_basic.dossier@CSL_RCL      dsr
      ,    customs_basic.dossierline@CSL_RCL  dln
      where dln.dossierid = dsr.dossierid 
      ) stream
where smt.order_id||lpad(smt.line_id,5,'0')||smt.tag_id = stream.link (+)      
and   smt.client_id in ('FLIRS','FLIRB')
and   nvl(smt.shipped,'N') = 'Y'
order by smt.order_id desc)
select 	md.client_id
,	md.order_id
,	md.status
,	md.line_id
,	md.sku_id
,	md.tag_id
,	md.pre_advice_id
,	md.qty_ordered
,	md.qty_shipped
,	md.value
,	md.currency
,	md.origin_id
,	md.condition_id
,	md.shipped_date
,	md.netto_weight
,	(	select 	count(distinct dossierid)
		from	customs_basic.dossier@CSL_RCL  
		where	invoicenumber = substr(md.order_id,3,35)) aantal_dossiers
,	md.link
,	md.dossierid
,	md.qty_in_stream
,	md.tag_stream
,	md.stream_value
,	md.invoicenumber
from	md