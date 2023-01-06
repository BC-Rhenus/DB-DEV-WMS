CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: WMS functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
	function is_ohr_restriction_valid( p_client_id_i   in varchar2
					 , p_order_id_i    in varchar2
					 , p_where_i       in varchar2
					 )
		return integer;
	--
	procedure process_saveorder;
	--
	procedure process_packparcelresponse( p_errorcode_i     in  varchar2
					    , p_errormessage_i  in  varchar2
					    , p_clientid_i      in  varchar2
					    , p_carrier_i       in  varchar2
					    , p_service_i       in  varchar2
					    , p_orderno_i       in  varchar2
					    , p_shipmentid_i    in  varchar2
					    , p_sequenceno_i    in  varchar2
					    , p_parcelid_i      in  varchar2
					    , p_trackingno_i    in  varchar2
					    , p_trackingurl_i   in  varchar2 := null
					    , p_error_o         out varchar2
					    , p_errortext_o     out varchar2
					    );
	--
	procedure process_trackingeventupdate( p_clientid_i       in  varchar2
                                             , p_orderno_i        in  varchar2
					     , p_eventtime_i      in  varchar2
					     , p_eventsignature_i in  varchar2 := null
					     , p_error_o          out integer
					     , p_errortext_o      out varchar2
					     );
	--
	procedure process_runtask( p_key_i			in dcsdba.run_task.key%type
				 , p_report_i			in varchar2
				 , p_user_id_i  		in dcsdba.run_task.user_id%type
				 , p_station_id_i 		in dcsdba.run_task.station_id%type
				 , p_site_id_i			in dcsdba.run_task.site_id%type
				 , p_status_i			in dcsdba.run_task.status%type
				 , p_command_i			in dcsdba.run_task.command%type
				 , p_pid_i			in dcsdba.run_task.pid%type
				 , p_old_dstamp_i		in dcsdba.run_task.old_dstamp%type
				 , p_dstamp_i			in dcsdba.run_task.dstamp%type
				 , p_language_i			in dcsdba.run_task.language%type
				 , p_name_i			in dcsdba.run_task.name%type
				 , p_time_zone_name_i		in dcsdba.run_task.time_zone_name%type
				 , p_nls_calendar_i		in dcsdba.run_task.nls_calendar%type
				 , p_print_label_i		in dcsdba.run_task.print_label%type
				 , p_java_report_i		in dcsdba.run_task.java_report%type
				 , p_run_light_i		in dcsdba.run_task.run_light%type
				 , p_server_instance_i		in dcsdba.run_task.server_instance%type
				 , p_priority_i			in dcsdba.run_task.priority%type
				 , p_archive_i			in dcsdba.run_task.archive%type
				 , p_archive_ignore_screen_i	in dcsdba.run_task.archive_ignore_screen%type
				 , p_archive_restrict_user_i	in dcsdba.run_task.archive_restrict_user%type
				 , p_client_id_i		in dcsdba.run_task.client_id%type
				 , p_email_recipients_i		in dcsdba.run_task.email_recipients%type
				 , p_email_attachment_i		in dcsdba.run_task.email_attachment%type
				 , p_email_subject_i		in dcsdba.run_task.email_subject%type
				 , p_email_message_i		in dcsdba.run_task.email_message%type
				 , p_master_key_i		in dcsdba.run_task.master_key%type
				 , p_use_db_time_zone_i		in dcsdba.run_task.use_db_time_zone%type
				 );
	--
	procedure process_itn_csl( p_key_i         in integer
                                 , p_client_id_i   in varchar2
				 , p_from_status_i in varchar2 := null
				 , p_to_status_i   in varchar2 := null
				 );
	--
	procedure process_wlgore_vat( p_site_id_i      in varchar2
                                    , p_client_id_i    in varchar2
				    , p_shipped_date_i in date
				    );
	--
	procedure process_csl_cbs( p_site_id_i      in varchar2
                                 , p_client_id_i    in varchar2
				 , p_shipped_date_i in date
				 , p_csl_bu_i       in varchar2
				 , p_trans_type_i   in integer  := null
				 );
	--
	function get_jr_email_recipients( p_jrp_key_i    in number
                                        , p_parameters_i in varchar2
					)
		return varchar2;
	--
	function get_tracking_url( p_wms_carrier_id  in  varchar2
				 , p_wms_tracking_nr in  varchar2
				 )
		return varchar2;
	--
	procedure get_order_sequence( p_client_id_i in  varchar2
                                    , p_udp_1_i     in  varchar2 := null
				    , p_udp_2_i     in  varchar2 := null
				    , p_ose_type_i  in  varchar2
				    , p_date_i      in  date     := null
				    , p_sequence_o  out varchar2
				    );
	--
	function ins_order_accessorial( p_client_id_i    in  varchar2
                                      , p_order_id_i     in  varchar2
				      , p_accessorial_i  in  varchar2
				      , p_timezonename_i in  varchar2 := 'Europe/Amsterdam'
				      , p_errortext_o    out varchar2
				      )
		return integer;
	--
	procedure sync_sku_special_links;
	--
	procedure cnl_inventory_adjustment( p_client_id_i       in varchar2
					  , p_location_id_i     in varchar2
					  , p_owner_id_i        in varchar2 
					  , p_days_i            in number   
					  , p_user_id_i         in varchar2
					  , p_station_id_i      in varchar2
					  , p_reason_id_i       in varchar2
					  , p_site_id_i         in varchar2
					  );
	--
	procedure upd_tracking_number( p_key_i			integer
				     , p_client_id_i		varchar2
				     , p_site_id_i		varchar2
				     , p_order_id_i		varchar2
				     , p_container_id_i		varchar2
				     , p_pallet_id_i		varchar2
				     , p_labelled_i		varchar2
				     , p_pallet_labelled_i	varchar2
				     ) ;
	--
	procedure update_tmp_run_task_p;
	--
	procedure save_tmp_run_task_p(p_run_task_i dcsdba.run_task%rowtype);
	--
end cnl_wms_pck;