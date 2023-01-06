CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_ABC_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: WMS ABC Analasys functionality within CNL_SYS schema
*
* Sku's are ranked per client group and not per induvidual client.
* Assumed is that all clients within the same cient group use the same ABC_RANKING.
* One client/owner/site combination is used for all clients in the client group.
* The client group must have the pre_fix "ABC"
**********************************************************************************
* $Log: $
**********************************************************************************/

  procedure abc_ranking ( p_client_group  varchar2  			      -- client group defined in WMS starting with ABC_.... that holds all clients that require a single abc rank over all sku's
                        , p_num_months    integer   			      -- The number of months to analyse.
                        , p_site_id       varchar2  			      -- The site in which to search for transactions.
                        );
                                    
end cnl_wms_abc_pck;