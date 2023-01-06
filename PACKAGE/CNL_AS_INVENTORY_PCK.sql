CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_INVENTORY_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 04-05-2018
**********************************************************************************
*
* Description: 
* Package to share master data with SynQ WCS from Swisslog.
* SynQ WCS is the WMS software from Swisslog that controlls the Autostore storage system
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
    procedure autostore_adjustment( p_key_i     in  number
                                  , p_site_id_i in  varchar2
                                  , p_ok_yn_o   out varchar2
                                  );
    --
    procedure process_as_adjustment;
    --
    procedure inventory_reconciliation(p_site_id_i  in varchar2);
    --
end cnl_as_inventory_pck;