CREATE OR REPLACE FORCE VIEW "CNL_SYS"."CNL_C_ENTREPOT_LOCATION_CHK" ("WMS_TAG_ID", "WMS_CLIENT_ID", "WMS_LOCATION_ID", "WMS_PRE_ADVICE", "DOSSIER", "STATUS", "HOURS_OLD", "ENTREPOT") AS 
  select  inv.tag_id                      wms_tag_id
,       inv.client_id                   wms_client_id
,       inv.location_id                 wms_location_id
,       pah.pre_advice_id               wms_pre_advice
,       dsr.dossierid                   dossier
,       dsr.status                      status
,       (case when floor(24*(sysdate-to_date(dsr.ds_timestamp,'yyyymmddhh24miss'))) >= 48
              then '> 48 hrs'
              else to_char(floor(24*(sysdate-to_date(dsr.ds_timestamp,'yyyymmddhh24miss')))) || ' hrs'
          end)                          hours_old
,       decode( inv.client_id, 'NLTLGINB', decode( to_char( floor( sysdate - to_date( dsr.ds_timestamp, 'yyyymmddhh24miss'))), 0, decode( clt.notes, 'CSLUSE', decode( inv.condition_id, 'D', 'E  !!!'
                                                                                                                                                                                              , 'V'
                                                                                                                                                                       )
                                                                                                                                                               , decode( dsr.status, 'OK', 'V'
                                                                                                                                                                                         , 'C'
                                                                                                                                                                       )
                                                                                                                                          )
                                                                                                                               , 1, decode( clt.notes, 'CSLUSE', decode( inv.condition_id, 'D', 'E  !!!'
                                                                                                                                                                                              , 'V'
                                                                                                                                                                       )
                                                                                                                                                               , decode( dsr.status, 'OK', 'V'
                                                                                                                                                                                         , 'C'
                                                                                                                                                                       )
                                                                                                                                          )
                                                                                                                                  , decode( dsr.status, 'OK', 'V'
                                                                                                                                                            , 'C'
                                                                                                                                          )
                                                   )
                                           , decode( clt.notes, 'CSLUSE', decode( inv.condition_id, 'D', 'E  !!!'
                                                                                                       , 'V'
                                                                                )
                                                                        , decode( dsr.status, 'OK', 'V'
                                                                                                  , 'C'
                                                                                )
                                                   )
              )                         entrepot
from    dcsdba.client                   clt
,       dcsdba.inventory                inv
,       dcsdba.pre_advice_header        pah
,       dcsdba.location                 lcn
,       customs_basic.dossier@csl_rcl   dsr
where   clt.client_id                   = inv.client_id
and     pah.site_id                     = inv.site_id
and     pah.client_id                   = inv.client_id
and     pah.pre_advice_id               = inv.receipt_id
and     inv.site_id                     = lcn.site_id
and     inv.location_id                 = lcn.location_id
and     lcn.user_def_type_8             = 'C-ENTREPOT'
and     dsr.ordernumber                 = decode( inv.client_id, 'NLTLGINB', pah.pre_advice_id
                                                                           , pah.yard_container_id
                                                )