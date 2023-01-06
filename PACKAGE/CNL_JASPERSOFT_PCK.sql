CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_JASPERSOFT_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Jaspersoft document trigger 
**********************************************************************************
* $Log: $
**********************************************************************************/
	procedure print_doc_p( p_site_id_i       	in  dcsdba.site.site_id%type
			     , p_client_id_i     	in  dcsdba.client.client_id%type
			     , p_order_id_i      	in  dcsdba.order_header.order_id%type
			     , p_pallet_id_i     	in  dcsdba.order_container.pallet_id%type
			     , p_container_id_i  	in  dcsdba.order_container.container_id%type
			     , p_reprint_yn_i    	in  varchar2	
			     , p_user_i          	in  varchar2	
			     , p_workstation_i   	in  varchar2	
			     , p_report_name_i   	in  varchar2	
			     , p_rtk_key         	in  integer
			     , p_pdf_link_i      	in  varchar2  	default null -- filename for pdf link in e-mail (internal e-mail)
			     , p_pdf_autostore_i 	in  varchar2  	default null -- filename for pdf created for autostore/maas
			     , p_run_task_i		in  dcsdba.run_task%rowtype
			     );
	--
	function fetch_printers_f( p_key_i		in dcsdba.java_report_map.key%type
				 , p_template_name_i	in dcsdba.java_report_export.template_name%type
				 , p_header_template_i	in varchar2					-- Y is header template, N = specific export template
				 , p_export_targets_o	out dcsdba.run_task.command%type
				 , p_export_types_o	out dcsdba.run_task.command%type
				 , p_export_copies_o	out dcsdba.run_task.command%type
				 )
	return integer; -- 1 is success, 0 is no export printers
	--
	procedure insert_rtsk_p( p_site_id_i 		in	  varchar2
			       , p_station_id_i 	in        varchar2
			       , p_user_id_i        	in        varchar2
			       , p_command_i        	in        varchar2
			       , p_report_name_i     	in        varchar2
			       , p_priority_i        	in        number
			       , p_client_id_i        	in        varchar2
			       , p_email_recipients_i   in        varchar2
			       , p_email_attachment_i   in        varchar2
			       , p_email_subject_i      in        varchar2
			       , p_email_message_i      in        varchar2
			       );	
end cnl_jaspersoft_pck;