CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_AS_MASTERDATA_PCK" is
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
    procedure synq_product_master;
    --
    --
    procedure save_wms_iud_record (p_data_type_i            in varchar2,
                                   p_action_i               in varchar2,
                                   p_client_id_i            in varchar2 default null,
                                   p_sku_id_i               in varchar2 default null,
                                   p_config_id_i            in varchar2 default null,
                                   p_tuc_i                  in varchar2 default null,
                                   p_supplier_sku_i         in varchar2 default null
                                  );
    --
    --
end cnl_as_masterdata_pck;