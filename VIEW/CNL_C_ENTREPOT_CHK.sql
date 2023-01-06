CREATE OR REPLACE FORCE VIEW "CNL_SYS"."CNL_C_ENTREPOT_CHK" ("INB_DOSSIER_ID", "MUTATION_YN", "MUT_DOSSIER_ID", "INB_DOSSIER_STATUS", "MUT_DOSSIER_STATUS", "LINE_AVAILABLE_AMOUNT", "NUM_LINES", "MUT_LINE_AVAILABLE_AMOUNT", "MUT_NUM_LINES", "CSL_COMPANY", "CSL_BUSINESS_UNIT", "CSL_WAREHOUSE", "INB_CREATION_DATE", "MUT_CREATION_DATE", "CSL_ARRIVED_DATE", "CSL_CUSTOMS_NOTIFIED", "WMS_INBOUND_FILE_ID", "WMS_CLIENT_ID", "WMS_PRE_ADVICE_ID", "WMS_PAH_RECEIVE_DATE", "WMS_INV_TAG_RECEIVE_DATE", "WMS_INV_TAG_ID", "WMS_INV_LOCATION_ID", "WMS_ENTREPOT_TYPE_LOCATION") AS 
  select  dsr.dossierid                                                           inb_dossier_id
,       decode( dsr.mutation, 1, 'Y', 'N')                                      mutation_yn
,       mut.dossierid                                                           mut_dossier_id
,       dsr.status                                                              inb_dossier_status
,       mut.status                                                              mut_dossier_status
,       nvl(dle.available_stockamount, 0)                                       line_available_amount
,       nvl(dle.num_lines, 0)                                                   num_lines
,       nvl(mle.available_stockamount, 0)                                       mut_line_available_amount
,       nvl(mle.num_lines, 0)                                                   mut_num_lines
,       dsr.activecompany                                                       csl_company
,       dsr.bussinesunit                                                        csl_business_unit
,       dsr.warehouse                                                           csl_warehouse
,       dsr.ds_timestamp                                                        inb_creation_date
,       mut.ds_timestamp                                                        mut_creation_date
,       dsr.arriveddate || dsr.arrivedtime                                      csl_arrived_date
,       dsr.notifydate  || dsr.notifytime                                       csl_customs_notified
,       dsr.ordernumber                                                         wms_inbound_file_id
,       par.client_id                                                           wms_client_id
,       par.pre_advice_id                                                       wms_pre_advice_id
,       to_char(par.actual_dstamp, 'yyyymmddhh24miss')                          wms_pah_receive_date
,       to_char(inv.receipt_dstamp,'yyyymmddhh24miss')                          wms_inv_tag_receive_date
,       inv.tag_id                                                              wms_inv_tag_id
,       inv.location_id                                                         wms_inv_location_id
,       decode(loc.user_def_type_8,'C-ENTREPOT','C','E')                        wms_entrepot_type_location
from    customs_basic.dossier@csl_rcl                                           dsr
,       customs_basic.dossier@csl_rcl                                           mut
,       dcsdba.pre_advice_header                                                par
,       dcsdba.inventory                                                        inv
,       dcsdba.location                                                         loc
,       (
        select dossierid
        ,      count(*)                num_lines
        ,      nvl(sum(available_stockamount),0) available_stockamount
        from   customs_basic.dossierline@csl_rcl
        group  by dossierid
        ) dle
,       (
        select dossierid
        ,      count(*)                num_lines
        ,      nvl(sum(available_stockamount),0) available_stockamount
        from   customs_basic.dossierline@csl_rcl
        group  by dossierid
        ) mle
where   dsr.dossierid                                                           = dle.dossierid (+)
and     dsr.activecompany                                                       = 'NLTLG01'
and     dsr.dossiertype                                                         = 'I'
and     dsr.warehouse                                                           = decode( par.site_id, 'RCLTLB', 'NLTLG01'
                                                                                                     , 'RCLEHV', 'NLSBR01'
                                                                                                               , par.site_id
                                                                                        )
and     mut.dossiertype (+)                                                     = 'I'
and     mut.mutation (+)                                                        = 1
and     mut.original_dossierid (+)                                              = dsr.dossierid
and     mut.bussinesunit (+)                                                    = dsr.bussinesunit
and     mut.dossierid                                                           = mle.dossierid (+)
and     dsr.ordernumber                                                         = decode( par.client_id, 'NLTLGINB', par.pre_advice_id
                                                                                                                   , par.yard_container_id
                                                                                        )
and     inv.site_id                                                             = par.site_id
and     inv.client_id                                                           = par.client_id
and     inv.receipt_id                                                          = par.pre_advice_id
and     loc.site_id                                                             = inv.site_id
and     loc.location_id                                                         = inv.location_id