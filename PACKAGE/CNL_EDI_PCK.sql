CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_EDI_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 07-03-2019
**********************************************************************************
*
* Description: 
* Package used by integrators like XIB, Seeburger BIS
* 
**********************************************************************************
* $Log: $
**********************************************************************************/
    --
    --
    function get_tracking_nbr_f( p_client_id_i          varchar2
                               , p_site_id_i            varchar2
                               , p_order_id_i           varchar2
                               , p_container_id_i       varchar2 default null
                               , p_pallet_id_i          varchar2 default null
                               , p_con_labelled_i       varchar2 default null
                               , p_pal_labelled_i       varchar2 default null
                               )
    return varchar2;
    --
    --
    procedure get_tracking_nbr_p( p_client_id_i     in  varchar2
                                , p_site_id_i       in  varchar2
                                , p_order_id_i      in  varchar2
                                , p_container_id_i  in  varchar2 default null
                                , p_pallet_id_i     in  varchar2 default null
                                , p_con_labelled_i  in  varchar2 default null
                                , p_pal_labelled_i  in  varchar2 default null
                                , p_bol_o           out varchar2
                                );
    --
    procedure check_csl_dossier( p_vat_sales_order_nr 	in  varchar2
			       , p_ok_yn		out integer
			       );

end cnl_edi_pck;