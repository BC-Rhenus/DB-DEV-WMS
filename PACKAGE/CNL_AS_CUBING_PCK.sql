CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_CUBING_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 04-05-2018
**********************************************************************************
*
* Description: 
* Package to initiate and start process cubing tasks.
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
    --
    --
    procedure get_cubing_tasks ( p_site_id_i        varchar2
                               , p_client_group_i   varchar2
                               );
    --
    --
    procedure synq_order_master( p_site_id_i        varchar2
                               , p_client_group_i   varchar2
                               );
    --
    --
    procedure cubing_result( p_hme_tbl_key_i    in number
                           , p_hme_key_i        in number
                           );
    --
    --
    Procedure wms_update_pick_tasks;
    --
    --
end cnl_as_cubing_pck;