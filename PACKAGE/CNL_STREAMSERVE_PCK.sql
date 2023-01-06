CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_STREAMSERVE_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with StreamServe (Output Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/

  procedure create_packlist ( p_site_id_i       in  varchar2
                            , p_client_id_i     in  varchar2
                            , p_owner_id_i      in  varchar2
                            , p_order_id_i      in  varchar2
                            , p_carrier_id_i    in  varchar2  := null
                            , p_pallet_id_i     in  varchar2  := null
                            , p_container_id_i  in  varchar2  := null
                            , p_reprint_yn_i    in  varchar2
                            , p_user_i          in  varchar2
                            , p_workstation_i   in  varchar2
                            , p_locality_i      in  varchar2  := null
                            , p_report_name_i   in  varchar2
                            , p_rtk_key         in  integer
                            , p_pdf_link_i      in  varchar2  := null
                            , p_pdf_autostore_i in  varchar2  := null
			    , p_run_task_i	in  dcsdba.run_task%rowtype
                            );
  --
    procedure create_trolley_list( p_site_id_i        	in  varchar2 
                                 , p_list_id_i        	in  varchar2
                                 , p_report_name_i    	in  varchar2 --UREPSSVTRL     
                                 , p_user_i           	in  varchar2
                                 , p_workstation_i    	in  varchar2
                                 , p_rtk_key          	in  integer
                                 , p_pdf_link_i       	in  varchar2 := null
                                 , p_pdf_autostore_i  	in  varchar2 := null
				 , p_run_task_i		in  dcsdba.run_task%rowtype
                                 );
  --
end cnl_streamserve_pck;