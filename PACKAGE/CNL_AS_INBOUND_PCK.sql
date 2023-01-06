CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_INBOUND_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 07-05-2018
**********************************************************************************
*
* Description: 
* Share and process putaway and relocate tasks WMS and Synq
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
    procedure wms_get_put_rel_tsk(p_final_loc_i in varchar2,
                                  p_site_i      in varchar2);
    --
    --
    procedure asn_receiving_notification;
    --
    --
    procedure asn_check_in_confirmation( p_hme_tbl_key_i    in  number
                                       , p_client_i         in varchar2
                                       , p_ok_yn_o          out varchar2
                                       );
    --
    --
end cnl_as_inbound_pck;