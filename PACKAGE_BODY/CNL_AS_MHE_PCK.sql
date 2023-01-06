CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_AS_MHE_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Common functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--
  g_yes                      constant varchar2(1)              := 'Y';
  g_no                       constant varchar2(1)              := 'N';
  g_true                     constant varchar2(5)              := 'TRUE';
  g_false                    constant varchar2(5)              := 'FALSE';
  --g_wms_db                   constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'DB_NAME');
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : Martijn swinkels, 04-Okt-2021
-- Purpose : Check if client is a Centiro Saas client
------------------------------------------------------------------------------------------------
	function ctosaas_yn_f( p_client_id_i	dcsdba.client.client_id%type)
		return boolean
	is
		l_client	integer;
	begin
		select	count(*)
		into	l_client
		from	dcsdba.client_group_clients
		where	client_group 	= 'CTOSAAS'
		and	client_id 	= p_client_id_i
		;
		if	l_client > 0
		then
			return true;
		else
			return false;
		end if;
	exception
		when others
		then 
			return false;
	end ctosaas_yn_f;
------------------------------------------------------------------------------------------------
-- Author  : Martijn swinkels, 23-05-2018
-- Purpose : Gets sortation id / chute id from WMS location
------------------------------------------------------------------------------------------------
	function get_box_close_instruction( p_client_id_i 	in varchar2
					  , p_site_id_i		in varchar2
					  )
		return varchar2
	is
		cursor c_spec
		is
			select	s.text
			from	dcsdba.special_ins_code s
			where	s.code = 'ASBOXCLOSE'
			and	s.client_id 	= p_client_id_i
			and	s.site_id 	= p_site_id_i
		;
		--
		r_spec		c_spec%rowtype;
		l_retval	varchar2(4000);
		--
	begin
		open	c_spec;
		fetch	c_spec
		into	l_retval;
		close 	c_spec;
		return 	l_retval;
	exception 
		when others 
		then
			case
			when c_spec%isopen
			then
				close c_spec;
			else
				null;
			end case;
		l_retval := null;
		return 	l_retval;

	end get_box_close_instruction;
------------------------------------------------------------------------------------------------
-- Author  : Martijn swinkels, 23-05-2018
-- Purpose : Gets sortation id / chute id from WMS location
------------------------------------------------------------------------------------------------
    Function get_chute_id( p_unit_id_i     varchar2
                         , p_site_id_i     varchar2
                         , p_client_id_i   varchar2
                         )
            return varchar2
    is
            cursor c_pal (  b_unit_id varchar2)
            is
                    select  distinct ocr.pallet_id
                    from    dcsdba.order_container ocr
                    where   ocr.client_id   = p_client_id_i
                    and     (   ocr.pallet_id       = b_unit_id or 
                                ocr.container_id    = b_unit_id
                            )
            ;
            --
            cursor c_getloc (   b_pallet_id varchar2)
            is
                    select  distinct mt.to_loc_id
                    from    dcsdba.move_task mt
                    where   mt.pallet_id    = b_pallet_id
                    and     mt.task_id      = 'PALLET'
                    and     mt.site_id      = p_site_id_i
                    and     task_type       = 'T' -- Marshal Header
            ;
            --
            cursor c_lcn( b_site_id_i     in varchar2
                        , b_location_id_i in varchar2
                        )
            is
                select to_char(floor(lcn.put_sequence)) put_seq
                from   dcsdba.location lcn
                where  lcn.site_id     = b_site_id_i
                and    lcn.location_id = b_location_id_i
            ;
            --
            l_pal    varchar2(20);
            l_loc    varchar2(20);
            l_lcn    varchar2(3);
            l_retval varchar2(3);
    begin
            open    c_pal(p_unit_id_i);
            fetch   c_pal
            into    l_pal;
            if      c_pal%notfound or 
                    l_pal is null 
            then
                    close c_pal;
                    l_retval := '101';
                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_chute_id','Search for pallet id. Could not find a pallet id in order container for '|| p_unit_id_i);
            else
                    close c_pal;
                    open    c_getloc(l_pal);
                    fetch   c_getloc
                    into    l_loc;
                    if      c_getloc%notfound or 
                            l_loc is null 
                    then
                            close c_getloc;
                            l_retval := '101';
                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_chute_id','Check workstation. Could not work out the site using workstation id for '|| p_unit_id_i);
                    else
                            close c_getloc;
                            open    c_lcn(p_site_id_i, l_loc);
                            fetch   c_lcn
                            into    l_lcn;
                            if      c_lcn%notfound or 
                                    l_lcn is null
                            then
                                    close c_lcn;
                                    l_retval := '101';
                                    cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_chute_id','Getting put_sequence from location. Location ' || l_loc || ' not found or put sequence not filled. ' || p_unit_id_i);               
                            else
                                    close c_lcn;
                                    l_retval := l_lcn;
                            end if;
                    end if;
            end if;
    return l_retval;
    exception
        when others
        then
                l_retval := '101';
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_chute_id',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
                commit;
                return l_retval;
    end get_chute_id;
------------------------------------------------------------------------------------------------
-- Author       : Martijn Swinkels, 24-05-2018
-- Purpose      : If containers must be merged together on a single pallet
------------------------------------------------------------------------------------------------
    Function merge_pallet_yn( p_unit_id_i   varchar2
                            , p_site_id_i   varchar2
                            , p_client_id_i varchar2)
            return varchar2
    is
            cursor c_pal( b_unit_id varchar2)
            is
                    select  distinct ocr.pallet_id
                    from    dcsdba.order_container ocr
                    where   ocr.client_id   = p_client_id_i
                    and     (   ocr.pallet_id       = b_unit_id or 
                                ocr.container_id    = b_unit_id
                            )
            ;
            --
            cursor c_getloc( b_pallet_id varchar2)
            is
                    select  distinct mt.to_loc_id
                    from    dcsdba.move_task mt
                    where   mt.pallet_id    = b_pallet_id
                    and     mt.task_id      = 'PALLET'
                    and     mt.site_id      = p_site_id_i
                    and     task_type       = 'T' -- Marshal Header
            ;
            --
            cursor c_lcn( b_site_id_i     varchar2
                        , b_location_id_i varchar2
                        )
            is
                select nvl(lcn.user_def_chk_1,'N')
                from   dcsdba.location lcn
                where  lcn.site_id     = b_site_id_i
                and    lcn.location_id = b_location_id_i
            ;
            --
            l_pal    varchar2(20);
            l_loc    varchar2(20);
            l_lcn    varchar2(3);
            l_retval varchar2(3);
    begin
            open    c_pal(p_unit_id_i);
            fetch   c_pal into l_pal;
            if      c_pal%notfound or 
                    l_pal is null 
            then
                    close   c_pal;
                    l_retval := 'N';
            else
                    close   c_pal;
                    open    c_getloc(l_pal);
                    fetch   c_getloc into l_loc;
                    if      c_getloc%notfound or 
                            l_loc is null 
                    then
                            close   c_getloc;                    
                            l_retval := 'N';
                    else
                            close   c_getloc;
                            open    c_lcn(p_site_id_i, l_loc);
                            fetch   c_lcn into l_lcn;
                            if      c_lcn%notfound or 
                                    l_lcn is null
                            then
                                    close   c_lcn;
                                    l_retval := 'N';
                            else
                                    close   c_lcn;
                                    l_retval := l_lcn;
                            end if;
                    end if;
            end if;
            return l_retval;
    exception
        when others
        then    
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.merge_pallet_yn',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            l_retval := 'N';
            return l_retval;
            commit;
    end merge_pallet_yn;
------------------------------------------------------------------------------------------------
-- Author       : Martijn Swinkels, 24-05-2018
-- Purpose      : register carton and generate documents.
------------------------------------------------------------------------------------------------
    procedure create_maas_logging( p_mhe_position_number_i   in  varchar2 default null
                                 , p_container_id_i          in  varchar2 default null
                                 , p_mhe_station_id_i        in  varchar2 default null
                                 , p_package_type_i          in  varchar2 default null
                                 , p_weight_i                in  number default null
                                 , p_height_i                in  number default null
                                 , p_width_i                 in  number default null
                                 , p_depth_i                 in  number default null
                                 , p_pallet_id_i             in  varchar2 default null
                                 , p_pallet_type_i           in  varchar2 default null
                                 , p_print_documents_i       in  varchar2 default null
                                 , p_close_box_i             in  varchar2 default null
                                 , p_bypass_i                in  varchar2 default null
                                 , p_print_label_i           in  varchar2 default null
                                 , p_sortation_loc_i         in  varchar2 default null
                                 , p_tracking_number_i       in  varchar2 default null
                                 , p_instruction_i           in  varchar2 default null
                                 , p_ok_i                    in  varchar2 default null
                                 , p_error_message_i         in  varchar2 default null
                                 , p_skip_validation_i       in  number default null
                                 , p_match_or_contains_i     in  number default null
                                 )
    is
    begin
            insert into cnl_as_maas_logging( mhe_position_number
                                           , dstamp
                                           , container_id
                                           , mhe_station_id
                                           , dws_package_type
                                           , dws_box_weight
                                           , dws_box_height
                                           , dws_box_width
                                           , dws_box_depth
                                           , sort_pallet_id
                                           , sort_pallet_type
                                           , print_documents
                                           , close_box
                                           , bypass
                                           , print_label
                                           , sortation_loc
                                           , tracking_number
                                           , instruction
                                           , ok
                                           , error_message
                                           , skip_validation
                                           , match_or_contains
                                           )
                                     values( p_mhe_position_number_i
                                           , sysdate 
                                           , p_container_id_i
                                           , p_mhe_station_id_i
                                           , p_package_type_i
                                           , p_weight_i
                                           , p_height_i
                                           , p_width_i
                                           , p_depth_i
                                           , p_pallet_id_i
                                           , p_pallet_type_i
                                           , p_print_documents_i
                                           , p_close_box_i
                                           , p_bypass_i
                                           , p_print_label_i
                                           , p_sortation_loc_i
                                           , p_tracking_number_i
                                           , p_instruction_i
                                           , p_ok_i
                                           , p_error_message_i
                                           , decode(p_skip_validation_i,1,'Y',0,'N',null)
                                           , decode(p_match_or_contains_i,1,'BARCODE CONTAINS AWB',0,'BARCODE MUST MATCH AWB',null) -- 1 is contains, 0 is match
                                           );
            commit;
    exception
        when others
        then 
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.create_maas_logging',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
    end create_maas_logging;
------------------------------------------------------------------------------------------------
-- Author       : Martijn Swinkels, 24-05-2018
-- Purpose      : register carton and generate documents.
------------------------------------------------------------------------------------------------
    procedure print_doc ( p_wms_unit_id_i     in  varchar2
                        , p_mhe_position_i    in  varchar2 default null
                        , p_mht_unit_id_i     in  varchar2 default null
                        , p_mht_station_id_i  in  varchar2
                        , p_print_doc_o       out varchar2 -- If documents will be printed for box.
                        , p_close_box_o       out varchar2 -- If box must be closed Y/N can be shown on display.
                        , p_pass_trough_o     out varchar2 -- skip packing and printing
                        , p_instruction_o     out varchar2 -- Instructions for operator.
                        , p_ok_yn_o           out varchar2 -- When no an error occured. 
                        , p_error_message_o   out varchar2 -- Shown on display at packing area when an error occured
                        )
    is
        -- Get site id where workstation is originated
        cursor c_wsn (b_station_id in varchar2)
        is
            select  wsn.site_id
            from    dcsdba.workstation wsn
            where   wsn.station_id = b_station_id
            ;

        -- Get container details from WMS
        cursor c_ocr ( b_site_id   in varchar2
                     , b_parcel_id in varchar2
                     )
        is
            select ocr.container_id
            ,      ocr.container_type
            ,      ocr.pallet_id
            ,      ocr.config_id        pallet_type
            ,      ocr.container_n_of_n
            ,      ohr.from_site_id     site_id
            ,      ocr.client_id
            ,      ohr.owner_id
            ,      ocr.order_id
            ,      ocr.labelled         container_labelled
            ,      ocr.pallet_labelled
            ,      ohr.customer_id
            ,      ohr.carrier_id
            ,      ohr.service_level
            ,      nvl( ocr.container_weight, ocr.pallet_weight) weight
            ,      nvl( ocr.container_height, ocr.pallet_height) height
            ,      nvl( ocr.container_width,  ocr.pallet_width)  width
            ,      nvl( ocr.container_depth,  ocr.pallet_depth)  depth
            from   dcsdba.order_container ocr
            ,      dcsdba.order_header    ohr
            where  ohr.client_id    = ocr.client_id
            and    ohr.order_id     = ocr.order_id
            and    ohr.from_site_id = b_site_id
            and    ocr.container_id = b_parcel_id
            ; 

        -- Count number of tasks still pending before printing should be triggered
        cursor  c_tsk( b_order_id_i     varchar2
                     , b_client_id_i    varchar2
                     , b_parcel_id_i    varchar2
                     , b_drop_loc_i     varchar2
                     )
        is
            select  count(mt.task_id)
            from    dcsdba.move_task mt
            where   mt.task_id     = b_order_id_i
            and     mt.client_id   = b_client_id_i
            and     (
                    (mt.container_id is null and mt.pallet_id is null)or  
                    (mt.from_loc_id  = 'CONTAINER')or  
                    (mt.from_loc_id in (b_drop_loc_i, '30AVASWHH') and mt.container_id != b_parcel_id_i)or
                    (mt.container_id is not null and mt.from_loc_id != 'CONTAINER' and mt.from_loc_id not in (b_drop_loc_i,'30AVASWHH') and (mt.to_loc_id in (b_drop_loc_i,'30AVASWHH') or mt.final_loc_id in (b_drop_loc_i,'30AVASWHH')))
                    )
            ;

        -- Get all document types that must be printed.
        cursor c_doc ( b_station_id_i   varchar2
                     , b_site_id_i      varchar2
                     , b_client_id_i    varchar2
		     , b_order_id_i	varchar2
                     )
        is
            select  jrm.template_name
            ,       extra_parameters
            from    dcsdba.java_report_map jrm
            where   jrm.station_id  = b_station_id_i
            and     jrm.site_id     = b_site_id_i
            and     jrm.client_id   = b_client_id_i
            and     jrm.template_name is not null
            and     jrm.key in (    select  jre.key
                                    from    dcsdba.java_report_export jre
                                    where   export_target = cnl_util_pck.get_constant( p_name_i => p_mht_station_id_i)
                                )
	    and     cnl_sys.cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i	=> b_client_id_i
								, p_order_id_i	=> b_order_id_i
								, p_where_i	=> nvl(jrm.extra_parameters,'1=1') -- 1=1 to ensure validation will never fail if no parameters.
								) != 0
	;

	-- Count relevant APM records
        cursor c_apm( b_station_id_i   varchar2
                     , b_site_id_i      varchar2
                     , b_client_id_i    varchar2
                     )
        is
            select  count (*)
            from    dcsdba.java_report_map jrm
            where   jrm.station_id  = b_station_id_i
            and     jrm.site_id     = b_site_id_i
            and     jrm.client_id   = b_client_id_i
            and     jrm.template_name is not null
            ;		

        -- Get the inventory inside the container
        cursor  c_inv ( b_site_id   varchar2
                      , b_client_id varchar2
                      , b_parcel_id varchar2
                      )
        is
            select      ( select count(i.key)
                          from   dcsdba.inventory i
                          where  i.site_id   = b_site_id
                          and    i.client_id = b_client_id
                          and    i.container_id = b_parcel_id
                        )nbr_rec
            ,           i.sku_id
            ,           i.tag_id
            ,           i.batch_id
            ,           i.qty_on_hand
            ,           i.condition_id
            ,           i.origin_id
            ,           i.config_id
            from        dcsdba.inventory i
            where       i.site_id   = b_site_id
            and         i.client_id = b_client_id
            and         i.container_id = b_parcel_id
            group by    i.sku_id
            ,           i.tag_id
            ,           i.batch_id
            ,           i.qty_on_hand
            ,           i.condition_id
            ,           i.origin_id
            ,           i.config_id
            ;

        -- Check if parcel is shipping unit
        cursor  c_shp ( b_client_id varchar2
                      , b_config_id varchar2
                      )
        is
            select  sc.ratio_1_to_2
            ,       sc.ratio_2_to_3
            ,       sc.ratio_3_to_4
            ,       sc.ratio_4_to_5
            ,       sc.ratio_5_to_6
            ,       sc.ratio_6_to_7
            ,       sc.ratio_7_to_8
            ,       sc.shipping_unit_lev_1
            ,       sc.shipping_unit_lev_2
            ,       sc.shipping_unit_lev_3
            ,       sc.shipping_unit_lev_4
            ,       sc.shipping_unit_lev_5
            ,       sc.shipping_unit_lev_6
            ,       sc.shipping_unit_lev_7
            ,       sc.shipping_unit_lev_8
            from    dcsdba.sku_config sc
            where   sc.config_id = b_config_id
            and     sc.client_id = b_client_id
            ;

        -- Get marshal task from WMS
        cursor c_mar( b_container_id    varchar2
                    , b_site_id         varchar2
                    )
        is
            select  from_loc_id
            ,       to_loc_id
	    , 	    status
            from    dcsdba.move_task
            where   pallet_id       = b_container_id
            and     site_id         = b_site_id
            and     task_id         = 'PALLET'
        ;

        -- Get drop location outstage
        cursor  c_out( b_location_id    varchar2
                     , b_site_id        varchar2
                     )
        is
            select  out_stage
            from    dcsdba.location
            where   site_id     = b_site_id
            and     location_id = b_location_id
        ;

        --  variables
        r_ocr           c_ocr%rowtype;
        r_tsk           number;
        r_shp           c_shp%rowtype;
        r_mar           c_mar%rowtype;
        r_out           c_out%rowtype;
	r_no_apm	c_doc%rowtype;
	r_apm		number;

        l_ok_yn         varchar2(1);           
        l_doc_req       varchar2(1) := g_yes;  -- Are documents required Yes No
        l_only_mail     varchar2(1) := g_no;   -- Only email required Yes No		
        l_cls_req       varchar2(1) := g_yes;  -- Is box closing required Yes No
        l_ins_ops       varchar2(4000);        -- Instructions for screen Operator 
        l_mhe_site_id   varchar2(10);
        l_err           varchar2(1) := g_no;
        l_err_txt       varchar2(4000);  
        l_wms_unit_id   varchar2(30);
        l_drp_loc       varchar2(20);
	l_vas_loc	varchar2(20);
        l_result        integer;
	l_special_ins	varchar2(4000); 

    begin
        l_wms_unit_id := p_wms_unit_id_i; -- #C1.... or #C2....

-- A request was made without a station id.
	if 	p_mht_station_id_i is null
        then
		l_err     := g_yes;
		l_err_txt := 'MHT Station ID empty: p_mht_station_id_i [' 
			  || nvl(p_mht_station_id_i,'NO VALUE')
			  || '] can not be empty, check BOX or Scanner.'
			  ;
		l_doc_req := g_no;
		l_cls_req := g_no;
	end if;

-- get site id.
	if 	nvl(l_err, g_yes) = g_no
        then
		open    c_wsn (p_mht_station_id_i);
		fetch   c_wsn
		into    l_mhe_site_id;
		if      c_wsn%notfound
		then    -- No workstation found so site can't be worked out and return an error
			close   c_wsn;
			l_err       := g_yes;
			l_err_txt   := 'Workstation ['
				    || p_mht_station_id_i
				    || '] not identified. Must be identified to continue.'
				    ;
			l_doc_req   := g_no;
			l_cls_req   := g_no;
		else
			close   c_wsn;
			-- Instage location from convayor
			l_drp_loc := cnl_sys.cnl_as_pck.get_system_profile ( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_'
											    || l_mhe_site_id
											    || '_DROP-LOCATION_LOCATION');
			-- VAS location from conveyor
			l_vas_loc := cnl_sys.cnl_as_pck.get_system_profile ( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_'
											    || l_mhe_site_id
											    ||'_VAS-LOCATION_LOCATION');
			open    c_out( l_drp_loc, l_mhe_site_id);
			fetch   c_out
			into    r_out;
			close   c_out;
		end if;
        end if;

-- Parcel id empty it can't be worked out what actions to do
        if 	nvl(l_err, g_yes) = g_no
        then
		if l_wms_unit_id is null
		then
			l_err     := g_yes;
			l_err_txt := 'WMS Unit ID empty: p_wms_unit_id_i [' 
				  || nvl(l_wms_unit_id,'NO VALUE')
				  || '] can not be empty, check DWS.'
				  ;
			l_doc_req   := g_no;
			l_cls_req   := g_no;
		end if;
        end if;

-- Check marshal task it should be from any of the conveyor location 
        if	nvl(l_err, g_yes) = g_no
        then
		open    c_mar( l_wms_unit_id, l_mhe_site_id);
		fetch   c_mar
		into    r_mar;
		if      c_mar%notfound
		then
			close   c_mar;
			l_err       := g_yes;
			l_err_txt   := 'Could not find marshal task in WMS contact your supervisor or trouble shooter.';
			l_doc_req   := g_no;
			l_cls_req   := g_no;
		else
			close   c_mar;
			if	r_mar.status = 'Hold'
			then
				l_err       := g_yes;
				l_err_txt   := 'Marshal task is at status Hold and could not be executed. Contact Supervisor or trouble shooter';
				l_doc_req   := g_no;
				l_cls_req   := g_no;
			else
				if	r_mar.status = 'WHHandling'
				then
					l_err       := g_yes;
					l_err_txt   := 'This parcel should still be at the VAS area for Warehouse handling activities. Please move back to VAS area.';
					l_doc_req   := g_no;
					l_cls_req   := g_no;
				else	
					if  	r_mar.from_loc_id   not in ( l_drp_loc, r_out.out_stage, l_vas_loc)
					then
						l_err       := g_yes;
						l_err_txt   := 'Marshal task to ' ||r_out.out_stage||' could not be completed. Contact supervisor or trouble shooter';
						l_doc_req   := g_no;
						l_cls_req   := g_no;
					end if;
				end if;
			end if;
		end if;
        end if;

-- Get order container data
        if 	nvl(l_err, g_yes) = g_no
        then
		open    c_ocr ( l_mhe_site_id
                              , l_wms_unit_id
                              );
		fetch   c_ocr
		into    r_ocr;
		if      c_ocr%notfound
		then    
			l_err       := g_yes;
			l_err_txt   := 'WMS Unit ID unknown: l_wms_unit_id_i [' 
				    || l_wms_unit_id
				    || '] does not exist in WMS systems, check unit.';
			l_doc_req   := g_no;
			l_cls_req   := g_no;
		end if;
		close   c_ocr;
        end if;

-- check if documents are required.
        if  	nvl(l_err, g_yes)       = g_no
        and 	nvl(l_doc_req, g_yes)   = g_yes
        then
		-- check move taaks to identify last box y/n documents only triggered for last box
		open    c_tsk ( r_ocr.order_id
                              , r_ocr.client_id
                              , l_wms_unit_id
                              , l_drp_loc
                              );
		fetch   c_tsk
		into    r_tsk;
		if      r_tsk > 0
		then    
			l_doc_req := g_no;
		end if;
		close   c_tsk;
	end if;

-- Set instructions based on required documents if documents are required instructions can only be 400 characters long.
        if  	nvl(l_err, g_yes)     = g_no
        and 	nvl(l_doc_req, g_yes) = g_yes
        then    
		-- Check if documents are required but only require email
		open 	c_doc 	 ( p_mht_station_id_i
				 , l_mhe_site_id
				 , r_ocr.client_id
				 , r_ocr.order_id
				 );
		fetch 	c_doc
		into	r_no_apm;
		if 	c_doc%notfound
		then
			close	c_doc;
			l_only_mail 	:=  	g_yes;
			l_ins_ops 	:=  	'Order number = "'||r_ocr.order_id||'" ' || 'No documents will be printed. Box just requires closing or can be placed back onto main conveyor line.';
		else -- Printing is required
			close 	c_doc;
			l_ins_ops 	:= null;
			for 	r_doc in 	c_doc ( p_mht_station_id_i
						      , l_mhe_site_id
						      , r_ocr.client_id
						      , r_ocr.order_id
						      ) 
			loop
				if 	r_doc.extra_parameters is not null 
				then
					-- When parameters are valid
					if cnl_sys.cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i => r_ocr.client_id
										       , p_order_id_i  => r_ocr.order_id
										       , p_where_i     => r_doc.extra_parameters 
										       ) != 0
					then
						if      l_ins_ops is null
						then    
							l_ins_ops := 'Order number = "' || r_ocr.order_id ||'". ' ||'The documents for this order are: "'|| r_doc.template_name ||'" '; 
						else    
							l_ins_ops := l_ins_ops || ', "' || r_doc.template_name||'" '; 
						end if;
					end if;
				else
					if      l_ins_ops is null
					then    
						l_ins_ops := 'Order number = "' || r_ocr.order_id || '". ' ||'The documents for this order are: "' || r_doc.template_name || '" '; 
					else    
						l_ins_ops := l_ins_ops || ', "' || r_doc.template_name ||'" '; 
					end if;
				end if;
			end loop;
		end if;	    
        end if;

-- is box already closed?
        if  nvl(l_err, g_yes)     = g_no
        and nvl(l_cls_req, g_yes) = g_yes
        then
            if    r_ocr.container_type = 'NOOUTERCARTON'
            then
                  l_cls_req := g_no;
            end if;
        end if;    

-- get additional instructions from WMS special instructions with code ASBOXCLOSE
        if   nvl(l_err, g_yes)     = g_no
        and (nvl(l_doc_req, g_yes) = g_yes or nvl(l_cls_req, g_yes) = g_yes)
        then
		l_special_ins := get_box_close_instruction(r_ocr.client_id, l_mhe_site_id);
        end if;

-- Trigger streamserve documents
	open 	c_apm ( p_mht_station_id_i
		      , l_mhe_site_id
		      , r_ocr.client_id
		      );
	fetch 	c_apm
	into	r_apm;
	close	c_apm;
	--
	if r_apm > 0
	then
		if  nvl(l_err, g_yes)     = g_no
		and nvl(l_doc_req, g_yes) = g_yes        
		then
			    l_result := dcsdba.libruntask.createruntask ( stationid             => p_mht_station_id_i
									, userid                => p_mht_station_id_i
									, commandtext           => '"SSV_PLT_ALL" "lp" "P" "1" '
												|| '"site_id" "'        || l_mhe_site_id    || '"'
												|| '"client_id" "'      || r_ocr.client_id  || '"'
												|| '"owner_id" "'       || r_ocr.owner_id   || '"'
												|| '"order_id" "'       || r_ocr.order_id   || '"'
												|| '"pdf_autostore" "'  || p_wms_unit_id_i  || '"' -- file name of PDF.
												|| '"locality" "" "rdtlocality" ""'  
									, nametext              => 'UREPSSVPLT'
									, siteid                => l_mhe_site_id
									, tmplanguage           => 'EN_GB'
									, p_javareport          => g_yes
									, p_archive             => g_no
									, p_runlight            => null
									, p_serverinstance      => null
									, p_priority            => null
									, p_timezonename        => 'Europe/Amsterdam'
									, p_archiveignorescreen => null
									, p_archiverestrictuser => null
									, p_clientid            => r_ocr.client_id
									, p_emailrecipients     => null
									, p_masterkey           => null
									, p_usedbtimezone       => g_no
									, p_nlscalendar         => 'Gregorian'
									, p_emailattachment     => null
									, p_emailsubject        => null
									, p_emailmessage        => null
									);
									commit;
		end if;
	end if;

-- Move unit from instage location to outstage location !!! only when it is not yet at the outstage
        if      nvl(l_err, g_no) = g_no
        then    
            update  dcsdba.move_task mt
            set     mt.status       = 'Complete'
            ,       mt.user_id      = p_mht_station_id_i
            ,       mt.station_id   = p_mht_station_id_i
	    ,	    mt.logging_level= 3
            where   mt.pallet_id    = l_wms_unit_id  -- Must be pallet id. 
            and     mt.task_type    = 'T'            -- Marshal header
            and     mt.site_id      = l_mhe_site_id  -- can exist in multiple sites.
            and     mt.to_loc_id    = ( select  l.out_stage
                                        from    dcsdba.location l
                                        where   l.location_id   = mt.from_loc_id
                                        and     l.site_id       =  l_mhe_site_id 
                                      ); -- when already processed an update is not needed anymore.
            commit; -- When commit is done later and an error occurs with out parameters marshal task will not be executed and temp lock occurs.
        end if;

--set out parameters
	-- If there is an error then P_OK_YN_o must return yes.
        if      nvl(l_err, g_no) = g_no
        then    
                p_ok_yn_o := g_yes;
        else    
                p_ok_yn_o := g_no;
        end if;

	-- If no documents are required and no error found find out if box reuired closing YN
        if      ( nvl(l_doc_req, g_yes) = g_no or nvl(l_only_mail,g_no) =	g_yes)
        and     nvl(l_err, g_no) = g_no
        then
                if  nvl(l_cls_req, g_yes) = g_no
                then
                    l_ins_ops := 'Order number = "' || r_ocr.order_id || '". ' || 'Box Should have taken the by-pass. it should already be closed and no documents are required. No action furtehr required';
                else
                    l_ins_ops := 'Order number = "' || r_ocr.order_id || '". ' || 'No documents will be printed. Box just requires closing or can be placed back onto main conveyor line.';
                end if;
        end if;

	-- When no instruction where found
        if  l_ins_ops is null and
            l_err_txt is null and
	    l_special_ins is null
        then
            l_ins_ops   :=  'Order number = "' || r_ocr.order_id||'". ' || 'No instructions found.';
        end if;

	-- When only emails are triggerd for documents print doc must be set to no.
        if 	l_only_mail = g_yes
	then
		p_print_doc_o	:= g_no;
	else
		p_print_doc_o   := nvl(l_doc_req, g_no);
	end if;

	-- If box must be closed YN
        p_close_box_o               := nvl(l_cls_req, g_yes);

        -- Set instuctions Max length allowed in raspberry is 400 characters
	p_instruction_o             := substr((l_ins_ops||' '||l_special_ins),1,400);

	-- Set error message
	p_error_message_o           := l_err_txt;

	-- If box can take the by pass
        if      nvl(l_err, g_yes)       = g_no -- No error
        and     nvl(l_cls_req, g_yes)   = g_no -- No closing required
        and     ( nvl(l_doc_req, g_no)    = g_no or nvl(l_only_mail,g_no) = g_yes)-- No documents needed
--	and	l_special_ins 		is null -- no special instrcutions
        then    
		p_pass_trough_o         := g_yes; -- Ok to pass packaging
        else    
		p_pass_trough_o         := g_no;
        end if;

        -- Create logging
        create_maas_logging( p_mhe_position_number_i   => p_mhe_position_i
                           , p_container_id_i          => p_wms_unit_id_i
                           , p_mhe_station_id_i        => p_mht_station_id_i
                           , p_print_documents_i       => p_print_doc_o
                           , p_close_box_i             => p_close_box_o 
                           , p_bypass_i                => p_pass_trough_o
                           , p_instruction_i           => p_instruction_o
                           , p_ok_i                    => p_ok_yn_o
                           , p_error_message_i         => p_error_message_o
                           );
    exception
        when others
        then    
             cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.print_doc',substr('Exception handling: '||p_wms_unit_id_i || ' SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
             p_ok_yn_o               := g_no;
             p_error_message_o       := 'An unknown error occured';
             p_pass_trough_o         := g_no;
            commit;
    end print_doc;
--
------------------------------------------------------------------------------------------------
-- Author       : Martijn Swinkels, 28-05-2018
-- Purpose      : Create Centiro shipment, update WMS.
------------------------------------------------------------------------------------------------
    procedure create_parcel ( p_wms_unit_id_i     in  varchar2
                            , p_mhe_position_i    in  varchar2 default null
                            , p_mht_unit_id_i     in  varchar2 := null
                            , p_mht_station_id_i  in  varchar2
                            , p_lft_status_i      in  varchar2
                            , p_lft_description_i in  varchar2 := null
                            , p_package_type_i    in  varchar2 := null
                            , p_weight_i          in  number
                            , p_height_i          in  number
                            , p_width_i           in  number
                            , p_depth_i           in  number
                            , p_ok_yn_o           out varchar2
                            , p_error_message_o   out varchar2
                            , p_print_label_yn_o  out varchar2
                            , p_sort_pos_o        out varchar2
			    , p_ctosaas_yn_o	  out varchar2
                            )
    is
    -- Get order container, order header data
    cursor c_ocr ( b_site_id   in varchar2
                 , b_parcel_id in varchar2
                 )
    is
        select ocr.container_id
        ,      ocr.container_type
        ,      ocr.pallet_id
        ,      ocr.config_id        pallet_type
        ,      ocr.container_n_of_n
        ,      ocr.labelled         container_labelled
        ,      ohr.shipment_group 
        ,      ohr.from_site_id     site_id
        ,      ocr.client_id
        ,      ohr.owner_id
        ,      ocr.order_id
        ,      ohr.customer_id
        ,      ohr.carrier_id
        ,      ohr.service_level
        ,      nvl( ocr.container_weight, ocr.pallet_weight) weight
        ,      nvl( ocr.container_height, ocr.pallet_height) height
        ,      nvl( ocr.container_width,  ocr.pallet_width)  width
        ,      nvl( ocr.container_depth,  ocr.pallet_depth)  depth
        ,      'JDA2016' wms_database
        from   dcsdba.order_container ocr
        ,      dcsdba.order_header    ohr
        where  ohr.client_id    = ocr.client_id
        and    ohr.order_id     = ocr.order_id
        and    ohr.from_site_id = b_site_id
        and    ocr.container_id = b_parcel_id -- in WMS2016 ParcelID = Container_ID when from DWS
        ;

    --  Get order container data from cnl_sys
    cursor c_cda ( b_site_id      in varchar2
                 , b_client_id    in varchar2
                 , b_order_id     in varchar2
                 , b_parcel_id    in varchar2
                 , b_wms_database in varchar2
                 )
    is
        select cda.*
        from   cnl_container_data cda
        where  cda.site_id        = b_site_id
        and    cda.client_id      = b_client_id
        and    cda.order_id       = b_order_id
        and    cda.wms_database   = b_wms_database
        and    cda.container_id   = b_parcel_id
        ;

    -- Get carrier data
    cursor c_crr ( b_site_id       in varchar2
                 , b_client_id     in varchar2
                 , b_carrier_id    in varchar2
                 , b_service_level in varchar2
                 )
    is
        select distinct
               carrier_id       cto_carrier
        ,      service_level    cto_service
        ,      g_yes            cto_enabled_yn
        ,      decode( nvl(user_def_type_7, g_no) , g_true, g_yes, g_no) use_dws_yn
        from   dcsdba.carriers
        where  (
               site_id       = b_site_id
               or
               site_id       is null
               )
        and    client_id     = b_client_id
        and    carrier_id    = b_carrier_id
        and    service_level = b_service_level
        ;

    -- Get ws site id.
    cursor c_wsn (b_station_id in varchar2)
    is
        select wsn.site_id
        ,      decode( nvl(wsn.disabled, g_no), g_no, g_yes, g_no) dws_enabled_yn -- disabled N = enabled Y, disabled Y = enabled N
        from   dcsdba.workstation wsn
        where  wsn.station_id = b_station_id
        ;

    -- Get marshal task from WMS
    cursor c_mar( b_container_id    varchar2
                , b_site_id         varchar2
                )
    is
        select  from_loc_id
        from    dcsdba.move_task
        where   pallet_id       = b_container_id
        and     site_id         = b_site_id
        and     task_id         = 'PALLET'
    ;

    -- Get drop location outstage
    cursor  c_out( b_location_id    varchar2
                 , b_site_id        varchar2
                 )
    is
        select  out_stage
        from    dcsdba.location
        where   site_id     = b_site_id
        and     location_id = b_location_id
    ;

    -- Variable
    r_ocr            c_ocr%rowtype;
    r_cda            c_cda%rowtype;
    r_crr            c_crr%rowtype;
    --r_ads            c_ads%rowtype;
    r_wsn            c_wsn%rowtype;
    r_mar            c_mar%rowtype;
    r_out            c_out%rowtype;

    l_err            varchar2(1) := g_no;
    l_err_txt        varchar2(400);
    l_wms_unit_id_i  varchar2(30);
    l_wms_database   varchar2(20);
    l_new_site_id    varchar2(20);
    l_new_client_id  varchar2(20);
    l_cto_enabled_yn varchar2(1) := g_no;
    l_dws_enabled_yn varchar2(1) := g_no;
    l_use_dws_yn     varchar2(1) := g_no;
    l_result         integer;
    l_cto_carrier    varchar2(30);
    l_cto_service    varchar2(30);
    l_mht_site_id    varchar2(20);
    l_drp_loc        varchar2(30);
    begin
        --
        l_wms_unit_id_i := p_wms_unit_id_i;

        -- CHeck DWS output
        if nvl( p_weight_i, 0) = 0
        or nvl( p_height_i, 0) = 0
        or nvl( p_width_i, 0)  = 0
        or nvl( p_depth_i, 0)  = 0
        then
            l_err     := g_yes;
            l_err_txt := 'Weight and/or dimensions are zero: p_weight_i['
                      || nvl( p_weight_i, 0) 
                      || '], p_height_i['
                      || nvl( p_height_i, 0) 
                      || '], p_width_i['
                      || nvl( p_width_i, 0) 
                      || '], p_depth_i[' 
                      || nvl( p_depth_i, 0)
                      || '] can not be zero, check DWS.'
                      ;
        end if;

        -- Check legal for trade
        if p_lft_status_i = g_no
        then
            l_err     := g_yes;
            l_err_txt := 'Parcel not Legal For Trade: p_lft_status_i [' 
                      || nvl(p_lft_status_i,'NO VALUE')
                      || '] , Non-LFT Error: ['
                      || substr( p_lft_description_i, 1, 300)
                      || ']'
                      ;
        end if;

        -- Check station id
        if p_mht_station_id_i is null
        then
            l_err     := g_yes;
            l_err_txt := 'MHT Station ID empty: p_mht_station_id_i [' 
                      || nvl(p_mht_station_id_i,'NO VALUE')
                      || '] can not be empty, check DWS.'
                      ;
        end if;

        -- Check unit id and fetch site
        if l_wms_unit_id_i is null
        then
            l_err     := g_yes;
            l_err_txt := 'WMS Unit ID empty: p_wms_unit_id_i [' 
                       || nvl(l_wms_unit_id_i,'NO VALUE')
                       || '] can not be empty, check DWS.'
                      ;
        else
             -- get the Site from Workstation for DWS system
            open  c_wsn ( b_station_id => p_mht_station_id_i);
            fetch c_wsn
            into  l_mht_site_id
            ,     l_dws_enabled_yn;
            close c_wsn;
            l_drp_loc := cnl_sys.cnl_as_pck.get_system_profile ( p_profile_id_i => '-ROOT-_USER_AUTOSTORE_SITE_'
                                                                               || l_mht_site_id
                                                                               || '_DROP-LOCATION_LOCATION');
            open    c_out( l_drp_loc, l_mht_site_id);
            fetch   c_out
            into    r_out;
            close   c_out;

            -- check if parcel exists in WMS
            open  c_ocr ( b_site_id   => l_mht_site_id
                        , b_parcel_id => l_wms_unit_id_i
                        );
            fetch c_ocr
            into  r_ocr;
            --
            if c_ocr%notfound
            then
                l_err     := g_yes;
                l_err_txt := 'WMS Unit ID unknown: l_wms_unit_id_i [' 
                          || l_wms_unit_id_i
                          || '] does not exist in WMS systems, check unit.'
                          ;
            else
                l_wms_database := r_ocr.wms_database;
            end if;
            close c_ocr;
        end if;

        -- Check marshal task
        if  l_err = g_no
        then
            open    c_mar( l_wms_unit_id_i, l_mht_site_id);
            fetch   c_mar
            into    r_mar;
            if      c_mar%notfound
            then
                close   c_mar;
                l_err       := g_yes;
                l_err_txt   := 'There is something not correct with this box its marshal task. Please check marshal task.';
            else
                if  r_mar.from_loc_id   != r_out.out_stage
                then
                    l_err       := g_yes;
                    l_err_txt   := 'There is something not correct with this box its marshal task. Please check marshal task.';

                end if;
            end if;
        end if;

        --
        if  l_err = g_no
        and l_wms_database is not null
        then
            -- Check if Centiro and or DWS is enabled for this Client/Carrier/Service
            open  c_crr ( b_site_id       => r_ocr.site_id
                        , b_client_id     => r_ocr.client_id
                        , b_carrier_id    => r_ocr.carrier_id
                        , b_service_level => r_ocr.service_level
                        );
            fetch c_crr
            into  r_crr;
            close c_crr;
            --
            l_cto_carrier    := r_crr.cto_carrier;
            l_cto_service    := r_crr.cto_service;
            l_cto_enabled_yn := r_crr.cto_enabled_yn;
            l_use_dws_yn     := r_crr.use_dws_yn;
            --
            -- get the correct site/client
            l_new_site_id    := r_ocr.site_id;
            l_new_client_id  := r_ocr.client_id;

            -- Update the Order Container record
            if      r_ocr.container_labelled = 'Y'
            then
                    update dcsdba.order_container ocr
                    set    ocr.container_weight = p_weight_i
                    ,      ocr.container_height = round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                    ,      ocr.container_width  = round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                    ,      ocr.container_depth  = round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                    ,      ocr.container_type   = nvl( p_package_type_i, r_ocr.container_type)
                    ,      ocr.status           = 'Repacked'
                    where  ocr.client_id        = r_ocr.client_id
                    and    ocr.order_id         = r_ocr.order_id
                    and    ocr.container_id     = r_ocr.container_id
                    ;
            elsif   r_ocr.shipment_group = 'PALLET'
            then
                    null;
            else
                    update dcsdba.order_container ocr
                    set    ocr.container_weight = p_weight_i
                    ,      ocr.container_height = round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                    ,      ocr.container_width  = round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                    ,      ocr.container_depth  = round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                    ,      ocr.container_type   = nvl( p_package_type_i, r_ocr.container_type)
                    ,      ocr.labelled         = 'Y'   
                    ,      ocr.status           = 'Repacked'
                    where  ocr.client_id        = r_ocr.client_id
                    and    ocr.order_id         = r_ocr.order_id
                    and    ocr.container_id     = r_ocr.container_id
                    ;
            end if;
            commit
            ; 
            -- Check if Container Data exists and create/update accordingly for WMS2016 parcels          
            open  c_cda ( b_site_id      => l_mht_site_id
                        , b_client_id    => l_new_client_id
                        , b_order_id     => r_ocr.order_id
                        , b_parcel_id    => l_wms_unit_id_i
                        , b_wms_database => l_wms_database
                        );
            fetch c_cda
            into  r_cda;
            --
            if c_cda%notfound
            then
                -- insert cda record
                insert into 
                cnl_container_data ( container_id
                                   , container_type
                                   , pallet_id
                                   , pallet_type
                                   , container_n_of_n
                                   , site_id
                                   , client_id
                                   , owner_id
                                   , order_id
                                   , customer_id
                                   , carrier_id
                                   , service_level
                                   , wms_weight
                                   , wms_height
                                   , wms_width
                                   , wms_depth
                                   , wms_database
                                   , dws_unit_id
                                   , dws_station_id
                                   , dws_lft_status
                                   , dws_lft_description
                                   , dws_package_type
                                   , dws_weight
                                   , dws_height
                                   , dws_width
                                   , dws_depth
                                   , dws_dstamp
                                   , cto_enabled_yn
                                   )
                values             ( p_wms_unit_id_i
                                   , r_ocr.container_type
                                   , p_wms_unit_id_i
                                   , r_ocr.pallet_type
                                   , r_ocr.container_n_of_n
                                   , l_new_site_id
                                   , l_new_client_id
                                   , r_ocr.owner_id
                                   , r_ocr.order_id
                                   , r_ocr.customer_id
                                   , l_cto_carrier
                                   , l_cto_service
                                   , r_ocr.weight
                                   , r_ocr.height
                                   , r_ocr.width
                                   , r_ocr.depth
                                   , l_wms_database
                                   , p_mht_unit_id_i
                                   , p_mht_station_id_i
                                   , p_lft_status_i
                                   , p_lft_description_i
                                   , p_package_type_i
                                   , p_weight_i
                                   , round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                   , round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                   , round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                   , current_timestamp
                                   , l_cto_enabled_yn
                                   );
            else
                -- update cda record with dws data
                update cnl_container_data      cda
                set    cda.dws_unit_id         = p_mht_unit_id_i
                ,      cda.dws_station_id      = p_mht_station_id_i
                ,      cda.dws_lft_status      = p_lft_status_i
                ,      cda.dws_lft_description = p_lft_description_i
                ,      cda.dws_package_type    = p_package_type_i
                ,      cda.dws_weight          = p_weight_i
                ,      cda.dws_height          = round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                ,      cda.dws_width           = round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                ,      cda.dws_depth           = round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                ,      cda.dws_dstamp          = current_timestamp
                ,      cda.cto_enabled_yn      = l_cto_enabled_yn
                where  cda.container_id        = l_wms_unit_id_i
                and    cda.site_id             = l_mht_site_id
                and    cda.client_id           = l_new_client_id
                and    cda.wms_database        = l_wms_database
                ;
            end if;

            -- Create CTO_PACKPARCEL Run task to trigger the Centiro PackParcel interface
            --
            -- Create run task WMS2016
            if  r_ocr.shipment_group != 'PALLET'
            then
                    if  l_dws_enabled_yn = g_yes
                    and l_cto_enabled_yn = g_yes
                    and l_use_dws_yn     = g_yes
                    then
                        l_result := dcsdba.libruntask.createruntask ( stationid             => p_mht_station_id_i
                                                                    , userid                => p_mht_station_id_i
                                                                    , commandtext           => '"CTO_PACKPARCEL" "lp" "P" "1" '
                                                                                            || '"from_site_id" "'   || r_ocr.site_id
                                                                                            || '" "client_id" "'    || r_ocr.client_id
                                                                                            || '" "owner_id" "'     || r_ocr.owner_id
                                                                                            || '" "order_id" "'     || r_ocr.order_id
                                                                                            || '" "container_id" "' || l_wms_unit_id_i
                                                                                            || '"'
                                                                    , nametext              => 'UREPCTOPACKPARCEL'
                                                                    , siteid                => r_ocr.site_id
                                                                    , tmplanguage           => 'EN_GB'
                                                                    , p_javareport          => g_yes
                                                                    , p_archive             => g_no
                                                                    , p_runlight            => null
                                                                    , p_serverinstance      => null
                                                                    , p_priority            => null
                                                                    , p_timezonename        => 'Europe/Amsterdam'
                                                                    , p_archiveignorescreen => null
                                                                    , p_archiverestrictuser => null
                                                                    , p_clientid            => r_ocr.client_id
                                                                    , p_emailrecipients     => null
                                                                    , p_masterkey           => null
                                                                    , p_usedbtimezone       => g_no
                                                                    , p_nlscalendar         => 'Gregorian'
                                                                    , p_emailattachment     => null
                                                                    , p_emailsubject        => null
                                                                    , p_emailmessage        => null
                                                                    );
                                                                    commit;
                    end if;
                    -- close cursor c_cda
                    close c_cda;
                    commit;
            end if;
        end if;
        --

        if l_err = g_yes
        then
            p_ok_yn_o := g_no;
            p_sort_pos_o := '999'; -- Error chute
        else
            p_ok_yn_o    := g_yes;
            p_sort_pos_o := get_chute_id( p_unit_id_i    => l_wms_unit_id_i
                                        , p_site_id_i    => l_mht_site_id
                                        , p_client_id_i  => r_ocr.client_id
                                        );
        end if;

        -- log DWS call into CNL_DWS_LOG table
        insert into cnl_dws_log ( wms_parcel_id
                                , dws_unit_id
                                , dws_station_id
                                , dws_lft_status
                                , dws_lft_description
                                , dws_package_type
                                , dws_weight
                                , dws_height
                                , dws_width
                                , dws_depth
                                , dws_dstamp
                                , wms_print_label_yn
                                , error_yn
                                , error_text
                                )
        values                  ( nvl( l_wms_unit_id_i, '0000000000')
                                , p_mht_unit_id_i
                                , p_mht_station_id_i
                                , p_lft_status_i
                                , p_lft_description_i
                                , p_package_type_i
                                , p_weight_i
                                , round((p_height_i / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                , round((p_width_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                , round((p_depth_i  / 100), 6) -- in WMS2016 UOM for dimension is M but DWS returns CM
                                , current_timestamp
                                , l_cto_enabled_yn
                                , l_err
                                , l_err_txt
                                );

        -- return out parameter values
        p_error_message_o  := l_err_txt;

        if  l_dws_enabled_yn = g_yes
        and l_cto_enabled_yn = g_yes
        and l_use_dws_yn     = g_yes
        and r_ocr.shipment_group != 'PALLET'
        then
            p_print_label_yn_o := g_yes;
        else
            p_print_label_yn_o := g_no;      
        end if;

	if	ctosaas_yn_f(r_ocr.client_id)
	then 
		p_ctosaas_yn_o := 'Y';
	else
		p_ctosaas_yn_o := 'N';
	end if;
        --
        create_maas_logging( p_mhe_position_number_i   => p_mhe_position_i
                           , p_container_id_i          => p_wms_unit_id_i
                           , p_mhe_station_id_i        => p_mht_station_id_i
                           , p_package_type_i          => p_package_type_i
                           , p_weight_i                => p_weight_i
                           , p_height_i                => p_height_i
                           , p_width_i                 => p_width_i
                           , p_depth_i                 => p_depth_i
                           , p_print_label_i           => p_print_label_yn_o
                           , p_sortation_loc_i         => p_sort_pos_o
                           , p_ok_i                    => p_ok_yn_o
                           , p_error_message_i         => p_error_message_o
                           );
    exception
        when others
        then
            l_err     := g_yes;
            l_err_txt := substr(sqlerrm, 1, 350);     

            p_ok_yn_o          := g_no;
            p_error_message_o  := l_err_txt;

            if  l_dws_enabled_yn = g_yes
            and l_cto_enabled_yn = g_yes
            and l_use_dws_yn     = g_yes
            then
                p_print_label_yn_o := g_yes;
            else
                p_print_label_yn_o := g_no;      
            end if;

            case
            when c_ocr%isopen
            then
                close c_ocr;
            when c_cda%isopen
            then
                close c_cda;
            when c_crr%isopen
            then
                close c_crr;
            when c_wsn%isopen
            then
                close c_wsn;
            else
                null;
            end case;
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.create_parcel',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE at line ' ||dbms_utility.format_error_backtrace||' = '|| sqlcode,1,4000));
            commit;
  end create_parcel;
--
------------------------------------------------------------------------------------------------
-- Author       : Martijn Swinkels, 30-05-2018
-- Purpose      : Capture sortation destination
------------------------------------------------------------------------------------------------
    procedure get_sort_pos( p_wms_unit_id_i    in  varchar2
                          , p_mhe_position_i   in  varchar2 default null
                          , p_mht_station_id_i in  varchar2
                          , p_sort_pos_o       out varchar2
                          )
    is
        cursor c_site (b_station_id in varchar2)
        is
            select  wsn.site_id
            from    dcsdba.workstation wsn
            where   wsn.station_id = b_station_id;
        --
        cursor c_client( b_container_id varchar2
                       , b_site_id      varchar2
                       )
        is
            select  distinct inv.client_id
            from    dcsdba.inventory inv
            where   inv.site_id         = b_site_id
            and     inv.container_id    = b_container_id
            and     rownum = 1 -- To be sure it does not generate an error and always returns one row.
            ;

        r_site   varchar2(20);
        r_client varchar2(20);
    begin
        -- At the sorting area position 101 is for any sorting errors.
        open    c_site( p_mht_station_id_i);
        fetch   c_site
        into    r_site;
        if      c_site%notfound
        then    
            p_sort_pos_o := '101';
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_sort_pos','Check workstation. Could not work out the site using workstation id ' || p_wms_unit_id_i);
        else    
            open    c_client( p_wms_unit_id_i
                            , r_site
                            );
            fetch   c_client
            into    r_client;
            if      c_client%notfound
            then
                p_sort_pos_o := '101';
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_sort_pos','Check client. Could not find a client owning this container ' || p_wms_unit_id_i);
            else
                p_sort_pos_o := get_chute_id( p_unit_id_i    => p_wms_unit_id_i
                                            , p_site_id_i    => r_site
                                            , p_client_id_i  => r_client
                                            );
            end if;
        end if;
        --
        create_maas_logging( p_mhe_position_number_i   => p_mhe_position_i
                           , p_container_id_i          => p_wms_unit_id_i
                           , p_mhe_station_id_i        => p_mht_station_id_i
                           , p_sortation_loc_i         => p_sort_pos_o
                           );
    exception
        when others
        then
            p_sort_pos_o := '101';
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.get_sort_pos',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
            commit;
    end get_sort_pos;
------------------------------------------------------------------------------------------------
-- Author       : Martijn Swinkels, 30-05-2018
-- Purpose      : Complete sortation
------------------------------------------------------------------------------------------------   
    procedure comp_sort( p_wms_unit_id_i    in  varchar2
                       , p_mhe_position_i   in  varchar2 default null
                       , p_mht_pal_id_i     in  varchar2
                       , p_mht_pal_type_i   in  varchar2 default null
                       , p_mht_station_id_i in  varchar2
                       , p_ok_yn_o          out varchar2
                       , p_err_message_o    out varchar2          
                       )
    is
        cursor c_toloc ( b_pallet   varchar2
                       , b_container varchar2
                       , b_site     varchar2
                       )
        is
            select  (   select  distinct p.to_loc_id
                        from    dcsdba.move_task p
                        where   p.pallet_id     = b_pallet
                        and     p.site_id       = b_site
                        and     p.task_id       = 'PALLET'
                        and     rownum          = 1) to_location_pallet
            ,       (   select  distinct c.to_loc_id
                        from    dcsdba.move_task c
                        where   c.pallet_id     = b_container
                        and     c.site_id       = b_site
                        and     c.task_id       = 'PALLET'
                        and     rownum          = 1) to_location_container
            ,       (   select  l.put_sequence
                        from    dcsdba.location l
                        where   l.site_id       = b_site
                        and     l.location_id   = ( select  distinct p.to_loc_id
                                                    from    dcsdba.move_task p
                                                    where   p.pallet_id     = b_pallet
                                                    and     p.site_id       = b_site
                                                    and     p.task_id       = 'PALLET'
                                                    and     rownum          = 1)) sequence_pallet
            ,       (   select  l.put_sequence
                        from    dcsdba.location l
                        where   l.site_id       = b_site
                        and     l.location_id   = ( select  distinct c.to_loc_id
                                                    from    dcsdba.move_task c
                                                    where   c.pallet_id     = b_container
                                                    and     c.site_id       = b_site
                                                    and     c.task_id       = 'PALLET'
                                                    and     rownum          = 1 )) sequence_container

            from dual
        ;        --
        cursor c_site (b_station_id in varchar2)
        is
            select  wsn.site_id
            from    dcsdba.workstation wsn
            where   wsn.station_id = b_station_id;
        --
        cursor c_client( b_container_id varchar2
                       , b_site_id      varchar2
                       )
        is
            select  distinct inv.client_id
            from    dcsdba.inventory inv
            where   inv.site_id         = b_site_id
            and     inv.container_id    = b_container_id
            and     rownum = 1 -- To be sure it does not generate an error and always returns one row.
        ;
        --
        cursor  c_wht( b_pallet_id      varchar2
                     , b_container_id   varchar2
                     )
        is
                select nvl((select  sum(nvl(o.container_weight,0)) 
                            from    dcsdba.order_container o
                            where   o.pallet_id = b_pallet_id),0)
                            + 
                       nvl((select  nvl(c.container_weight,0)
                            from    dcsdba.order_container c
                            where   c.container_id  = b_container_id
                            and     c.pallet_id    != b_pallet_id),0) 
                from   dual;
        --
        r_site          varchar2(20);
        r_client        varchar2(20);
        r_wht           number;
        r_toloc         c_toloc%rowtype;
        --
        l_result        integer;
        l_pal_type      varchar2(30)    := 'OUTBLOK';
        l_pal_width     number          := 1;
        l_pal_depth     number          := 1.2;     
        l_pal_wht       number          := 18;
        l_err_message   varchar2(400);
        l_ok            varchar2(1) := 'X';
        --
	l_timezone integer;
    begin
	-- This is required because WMS will set logging level to 5 by default if no logging level is specified.
	-- Loads of logging records will be generated otherwise.
	dcsdba.libsession.setsessionuserid (userid => 'AUTOSTORE');
	dcsdba.libsession.setsessionworkstation (stationid => 'AUTOSTORE');
	l_timezone := dcsdba.libsession.settimezone ( p_timezonename => 'Europe/Amsterdam');

	dcsdba.libmqsdebug.setsessionid(userenv('SESSIONID'),'sql','AUTOSTORE');
	dcsdba.libmqsdebug.setdebuglevel(3);
        --      Check if site can be found
        if      l_ok = 'X'
        then
                open    c_site( p_mht_station_id_i);
                fetch   c_site into r_site;
                if      c_site%notfound
                then    
                        l_ok            := 'N';
                        l_err_message   := 'Could not work out site id. Invalid station id ' 
                                        || p_mht_station_id_i;
                        close   c_site;
                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.comp_sort','Station id used to call procedure is invalid');
                else    
                        l_ok            := 'Y';
                        close   c_site;
                end if; -- c_site;
        end if;
        --
        -- check if client can be found.
        if      l_ok = 'Y'
        then
                open    c_client( p_wms_unit_id_i, r_site);
                fetch   c_client into r_client;
                if      c_client%notfound
                then    
                        l_ok            := 'N';
                        l_err_message   := 'Could not find any client owning this container id ' 
                                        || p_wms_unit_id_i;
                        close   c_client;
                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.comp_sort','Can''t find a client id owning this container id '||p_wms_unit_id_i);
                else
                        l_ok            := 'Y';
                        close   c_client;
                end if; --client%notfound
        end if; -- client
        --
        -- Check if pallet has the same destination as container
        if      l_ok = 'Y'
        then
                open    c_toloc(p_mht_pal_id_i, p_wms_unit_id_i, r_site);
                fetch   c_toloc into r_toloc;
                close   c_toloc;
                if      r_toloc.to_location_pallet is not null
                and     r_toloc.to_location_pallet != nvl(r_toloc.to_location_container,'NOCONTAINER')
                then
                        l_ok    := 'N';
                        l_err_message   := 'pallet ' 
                                        || p_mht_pal_id_i 
                                        || ' has a different destination (' 
                                        || r_toloc.to_location_pallet 
                                        || ') then container ' 
                                        || p_wms_unit_id_i 
                                        || ' (' 
                                        || r_toloc.to_location_container 
                                        || ').';
                        cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.comp_sort','pallet ' || p_mht_pal_id_i || ' has a different destination (' || r_toloc.to_location_pallet || ') then container ' || p_wms_unit_id_i || ' (' || r_toloc.to_location_container || ').');
                else
                        l_ok    := 'Y';
                end if; -- c_to_loc
        end if;
        --
        -- Process sortation
        if      l_ok = 'Y'
        then
                -- set session settings
                dcsdba.libsession.InitialiseSession( UserID       => 'AUTOSTORE'
                                                   , GroupID      => null
                                                   , StationID    => 'AUTOSTORE'
                                                   , WksGroupID   => null
                                                   );
		l_timezone := dcsdba.libsession.settimezone ( p_timezonename => 'Europe/Amsterdam');

                -- Close container
                dcsdba.liborderrepack.closepallet( result        => l_result
                                                 , topalletid    => p_wms_unit_id_i
                                                 , tocontainerid => p_wms_unit_id_i -- First close container without new pallet id.
                                                 );
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.comp_sort','Closing container ' || p_wms_unit_id_i);
                commit;
                -- When units must be merged onto the pallet.
                if      merge_pallet_yn ( p_unit_id_i    => p_wms_unit_id_i
                                        , p_site_id_i    => r_site
                                        , p_client_id_i  => r_client
                                        ) = 'Y'
                then
                            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.comp_sort','Container ' || p_wms_unit_id_i || ' must be merged onto pallet ' || p_mht_pal_id_i);
                            -- Set pallet type details
                            dcsdba.liborderrepack.addcontainertopallet( p_result        => l_result
                                                                      , p_clientid      => r_client
                                                                      , p_palletid      => p_wms_unit_id_i
                                                                      , p_containerid   => p_wms_unit_id_i
                                                                      , p_newpalletid   => p_mht_pal_id_i
                                                                      , p_pallettype    => l_pal_type
                                                                      , p_siteid        => r_site
                                                                      );
                            commit;
                            -- fetch total weight
                            open    c_wht( p_mht_pal_id_i, p_wms_unit_id_i);
                            fetch   c_wht
                            into    r_wht;
                            close   c_wht;
                            l_pal_wht := l_pal_wht + nvl(r_wht,0);
                            -- Update order container, move_task and inventory with new pallet type
                            update  dcsdba.order_container o
                            set     o.config_id     = l_pal_type
                            ,       o.pallet_width  = l_pal_width
                            ,       o.pallet_depth  = l_pal_depth
                            ,       o.pallet_weight = l_pal_wht
                            where   o.pallet_id     = p_mht_pal_id_i
                            and     o.status = 'CClosed';
                            -- update move_task
                            update  dcsdba.move_task m
                            set     m.container_id  = null
                            ,       m.description   = 'Multi container pallet'
                            ,       m.pallet_config = l_pal_type
                            ,       m.client_id     = null
                            where   m.task_id       = 'PALLET'
                            and     m.task_type     = 'T'
                            and     m.pallet_id     = p_mht_pal_id_i;
                            -- update inventory
                            update  dcsdba.inventory i
                            set     i.pallet_config  = l_pal_type
                            where   i.pallet_id      = p_mht_pal_id_i
                            and     i.pallet_config != l_pal_type
                            and     i.container_id   = p_wms_unit_id_i;
                            -- Update serial numbers with new pallet
			    update  dcsdba.serial_number s
			    set     s.pallet_id      = p_mht_pal_id_i
			    where   s.container_id   = p_wms_unit_id_i
			    and     s.client_id      = r_client
			    and	    s.site_id        = r_site;
			    --
                            commit;
                            --
                            dcsdba.LibOrderRepack.ListOrderContainerForPallet(p_mht_pal_id_i);
                            --
                            commit;
                end if; -- merge_pallet_yn
        end if; --process sortation
        --
        if      l_ok = 'Y'
        then
                update  cnl_as_tu
                set     cnl_if_status = 'Sorted'
                where   wms_container_id = p_wms_unit_id_i;
                l_err_message := null;
                commit;
        end if;
        --
        p_ok_yn_o       := l_ok;
        p_err_message_o := l_err_message;
        --
        create_maas_logging( p_mhe_position_number_i   => p_mhe_position_i
                           , p_container_id_i          => p_wms_unit_id_i
                           , p_mhe_station_id_i        => p_mht_station_id_i
                           , p_pallet_id_i             => p_mht_pal_id_i
                           , p_pallet_type_i           => p_mht_pal_type_i
                           , p_ok_i                    => p_ok_yn_o
                           , p_error_message_i         => p_err_message_o
                           );
    exception
        when others
        then
            p_ok_yn_o         := 'N';
            p_err_message_o   := 'Unexpected error';
            cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.comp_sort',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode || ' ' || p_wms_unit_id_i,1,4000));
            commit;
    end comp_sort;
------------------------------------------------------------------------------------------------
-- Author       : Martijn, Swinkels 2018
-- Purpose      : API for Material Handling Equipment to validate Parcel label against trackingnr.
------------------------------------------------------------------------------------------------
    procedure validate_parcel( p_wms_unit_id_i      in  varchar2
                             , p_mhe_position_i     in  varchar2 default null
                             , p_mht_station_id_i   in  varchar2
                             , p_tracking_nr_o      out varchar2
                             , p_operator_o         out integer
                             , p_skip_val_o         out integer
                             )
    is
        --
        cursor c_lab( b_container_id varchar2)
        is
            select  print_label
            from    cnl_sys.cnl_as_maas_logging
            where   container_id = b_container_id
            and     key = ( select  max(key)
                            from    cnl_sys.cnl_as_maas_logging
                            where   container_id = b_container_id
                            and     print_label is not null)
        ;
        --
        cursor c_sit( b_station_id      varchar2)
        is
            select  w.site_id
            from    dcsdba.workstation w
            where   w.station_id = b_station_id
        ;
        --
        cursor c_clt( b_container_id    varchar2)
        is
            select  i.client_id
            ,       i.order_id
            ,       o.shipment_group
            from    dcsdba.order_container i
            ,       dcsdba.order_header o
            where   i.container_id  = b_container_id
            and     o.order_id      = i.order_id
            and     o.client_id     = i.client_id
        ;
        -- Below cursors makes a connection with a SQL server database. Columns are Case sensitive.
	cursor c_pcl( b_parcel_id varchar2
		    , b_site_id   varchar2
		    , b_client_id varchar2
                    , b_order_id  varchar2
                    )
	is
		select	max("idPRC") pcl_id
		from    Parcels@centiro.rhenus.de 
		where   "ParcelID" = b_parcel_id
		and     "CodeSEN"  = b_client_id || '@' || b_site_id
		and     "OrderNo"  = b_order_id
	;
	--
	cursor c_cto (b_pcl integer)
	is 
		select	p."SequenceNo"		tracking_number
		,       p."SequenceNo2"         additional_tracking_number
		,       p."SequenceNoSSCC"      sscc
		,       p."idPRC"               cto_parcel_id
		,       p."ParcelID"            wms_container_id
		,       p."idSHP"               cto_shipment_id
		,       p."OrderNo"             wms_order_id
		,       s.cto_shipment_status
		,       s.cto_routing_error
		,       s.carrier_id
		,       s.service_level
		from    Parcels@centiro.rhenus.de p
		,       ( 
			select  s."idSHP"             cto_shipment_id
			,       s."Status"            cto_shipment_status
			,       s."RoutingError"      cto_routing_error
			,       s."CodeCAR"           carrier_id
			,       s."CodeCSE"           service_level
			from    Shipments@centiro.rhenus.de s 
			)       s
		where   p."idPRC"   = b_pcl
		and     p."idSHP"   = s.cto_shipment_id
	;
	--
	cursor c_gateway_up
	is
		select 	c.value
		from	cnl_sys.cnl_constants c
		where	name = 'ORACLE_GATEWAY_UP_YN';
	--
	cursor c_ctosaas_label
	is
		select 	c.tracking_number
		,	c.cto_sscc
		,	o.client_id
		,	o.carrier_id
		,	o.service_level
		from	cnl_sys.cnl_cto_ship_labels c
		left 
		join	dcsdba.order_container o
		on	o.container_id	= p_wms_unit_id_i
		where	c.parcel_id	= p_wms_unit_id_i
		and	c.site_id	= 'NLTLG01'
		and	c.status 	in ('Created','Reprint')
		order
		by	c.creation_dstamp 	desc -- newest first
		,	c.status 		desc -- Reprint over Created if same time
	;
	r_pcl	    	c_pcl%rowtype;
        r_lab       	c_lab%rowtype;
        r_sit       	c_sit%rowtype;
        r_clt       	c_clt%rowtype;
        r_cto       	c_cto%rowtype;
	r_gateway_up 	c_gateway_up%rowtype;
	r_ctosaas_label c_ctosaas_label%rowtype;
        --
        l_awb       varchar2(40);
        l_car       varchar2(40);
        l_ser       varchar2(40);
        l_continue  varchar2(1) := 'Y';
        l_operator  integer := 0;
        l_skip      integer := 0;
	l_saas	    varchar2(1) := 'N';
        --
    begin
	open	c_gateway_up;
	fetch	c_gateway_up into r_gateway_up;
	close 	c_gateway_up;
        if      r_gateway_up.value = 'N' or 
		r_gateway_up.value is null
	then
		-- Gateway to fetch data from SQL server (Centiro) is not up and running and that causes issues. This is the escape
		p_tracking_nr_o := p_wms_unit_id_i;
		p_operator_o    := 0;
		p_skip_val_o    := 1;
	else
		if      l_continue = 'Y'
		then
			open    c_sit( p_mht_station_id_i);
			fetch   c_sit
			into    r_sit;
			if      c_sit%found
			then
				close   c_sit;
				open    c_clt( p_wms_unit_id_i);
				fetch   c_clt
				into    r_clt;
				if      c_clt%found
				then
					close   c_clt;
					if      r_clt.shipment_group = 'PALLET'
					then
						l_skip      := 1;
						l_continue  := 'N';
						l_awb       := 'PALLET SHIPMENT';
					else
						if	ctosaas_yn_f(r_clt.client_id)
						then
							l_saas	:= 'Y';
							open 	c_ctosaas_label;
							fetch 	c_ctosaas_label
							into	r_ctosaas_label;
							if	c_ctosaas_label%found
							then
								close 	c_ctosaas_label;
							else
								close	c_ctosaas_label;
								-- Check if label is printed
								open    c_lab(p_wms_unit_id_i);
								fetch   c_lab 
								into    r_lab;
								if      c_lab%notfound
								then
									close   c_lab;
									l_awb       := 'NO DWS DETAILS FOUND';
									l_continue  := 'N';
									cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Search for print label flag in Rhenus DWS response. No label should be printed');
								else
									close   c_lab;
									l_awb   := 'PARCEL NOT FOUND';
									cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Parcel not found in Centiro ' || p_wms_unit_id_i);
									commit;
									l_continue := 'N';
								end if;
							end if;
						else
							open 	c_pcl( p_wms_unit_id_i
								     , r_sit.site_id
								     , r_clt.client_id
								     , r_clt.order_id
								     );
							fetch 	c_pcl
							into	r_pcl;
							if	c_pcl%found
							then
								close	c_pcl;
								open    c_cto(r_pcl.pcl_id);
								fetch   c_cto
								into    r_cto;
								if	c_cto%found
								then
									close c_cto;
								else
									close c_cto;
									-- Check if label is printed
									open    c_lab(p_wms_unit_id_i);
									fetch   c_lab 
									into    r_lab;
									if      c_lab%notfound
									then
										close   c_lab;
										l_awb       := 'NO DWS DETAILS FOUND';
										l_continue  := 'N';
										cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Search for print label flag in Rhenus DWS response. No label should be printed');
									else
										close   c_lab;
										l_awb   := 'PARCEL NOT FOUND';
										cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Parcel not found in Centiro ' || p_wms_unit_id_i);
										commit;
										l_continue := 'N';
									end if;
								end if;
							else
								close   c_pcl;
								-- Check if label is printed
								open    c_lab(p_wms_unit_id_i);
								fetch   c_lab 
								into    r_lab;
								if      c_lab%notfound
								then
									close   c_lab;
									l_awb       := 'NO DWS DETAILS FOUND';
									l_continue  := 'N';
									cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Search for print label flag in Rhenus DWS response. No label should be printed');
								else
									close   c_lab;
									l_awb   := 'PARCEL NOT FOUND';
									cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Parcel not found in Centiro ' || p_wms_unit_id_i);
									commit;
									l_continue := 'N';
								end if;
							end if;
						end if;
					end if;
				else
					close   c_clt;
					l_awb       := 'UNKNOWN CONTAINER';
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','search for container id. No container id found ' || p_wms_unit_id_i);
					commit;
					l_continue := 'N';
				end if;
			else
				close c_sit;
				l_awb       := 'UNKNOWN SITE';
				cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Search site id from workstation. Workstation ' ||  p_mht_station_id_i || ' could not be found.');
				l_continue := 'N';
				commit;
			end if;
		end if;

		-- Check if Centiro Error else set return values
		if      l_continue = 'Y'
		then
			if	l_saas = 'Y'
			then
				l_car 	:= upper(r_ctosaas_label.carrier_id);
				l_ser	:= upper(r_ctosaas_label.service_level);
			else
				l_car	:= upper(r_cto.carrier_id);
				l_ser	:= upper(r_cto.service_level);
			end if;
			--
			if	l_saas = 'N'
			then
				if      r_cto.cto_shipment_status = '99'
				then    -- Shipment has an error
					l_continue  := 'N';
					l_awb       := 'CENTIRO ERROR';
					cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel','Shipment in Centiro has an error ' || p_wms_unit_id_i);
				else
					if      upper(l_car) = 'STD.BRINGPRC.SE'               -- Only SSCC on label
					then        
						l_awb := upper(r_cto.sscc);
					elsif   upper(l_car) = 'STD.TNT.COM'                -- Only Additional sequence avaialable.
					then 
						l_awb := upper(r_cto.additional_tracking_number);
					elsif   (   upper(l_car) = 'STD.POST.NL' and upper(l_ser) = 'MAIL' ) or
						(   upper(l_car) = 'STD.DPBRIEF.DE' and upper(l_ser) = 'BGROSS') or -- No track and trace available on label.
						(   upper(l_car) = 'TOF.DE') -- Bad readable barcode on label
					then 
						l_awb   := p_wms_unit_id_i;
						l_skip  := 1;
					else
						l_awb := upper(r_cto.tracking_number);      -- Standard sequence number on label
					end if;
				end if;
			else
				if      upper(l_car) = 'STD.BRINGPRC.SE'               -- Only SSCC on label
				then        
					l_awb := upper(r_ctosaas_label.cto_sscc);
				elsif   upper(l_car) = 'STD.TNT.COM'                -- Only Additional sequence avaialable.
				then 
					l_awb   := p_wms_unit_id_i;
					l_skip  := 1;
				elsif   (   upper(l_car) = 'STD.POST.NL' and upper(l_ser) = 'MAIL' ) or
					(   upper(l_car) = 'STD.DPBRIEF.DE' and upper(l_ser) = 'BGROSS') or -- No track and trace available on label.
					(   upper(l_car) = 'TOF.DE') -- Bad readable barcode on label
				then 
					l_awb   := p_wms_unit_id_i;
					l_skip  := 1;
				else
					l_awb := upper(r_ctosaas_label.tracking_number);      -- Standard sequence number on label
				end if;
			end if;
		end if;
		-- Set relational operator for Maas
		if      l_continue = 'Y' 
		then
			if      upper(l_car) in ('TOF.DE','DPD.COM','STD.POST.NL')
			then
				l_operator := 1; -- Contains
			end if;
		end if;

		-- What to do when tracking number null is returned
		if      l_continue = 'Y'
		then
			if      l_awb is null
			then
				l_awb       := 'NO AWB FOUND';
				l_continue  := 'Y';
			end if;
		end if;

		-- Create logging
		create_maas_logging( p_mhe_position_number_i   => p_mhe_position_i
				   , p_container_id_i          => p_wms_unit_id_i
				   , p_mhe_station_id_i        => p_mht_station_id_i
				   , p_tracking_number_i       => l_awb
				   , p_skip_validation_i       => l_skip
				   , p_match_or_contains_i     => l_operator
				   );

		-- Set out parameter
		p_tracking_nr_o := l_awb;
		p_operator_o    := l_operator;
		p_skip_val_o    := l_skip;
	end if;
    exception
            when others
            then
                p_tracking_nr_o := 'SQL ERROR';
                p_operator_o    := 0;
                p_skip_val_o    := 0;
                cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.validate_parcel',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
                commit;
    end validate_parcel;
------------------------------------------------------------------------------------------------
-- Author       : Martijn, Swinkels 2018
-- Purpose      : Update marshal task when container is pushed into VAS
------------------------------------------------------------------------------------------------
	procedure tu_pushed_in_vas_p( p_tu_id_i		varchar2
				    , p_site_id_i	varchar2
				    )
	is
		l_vas_loc_id varchar2(50) := cnl_sys.cnl_as_pck.get_system_profile('-ROOT-_USER_AUTOSTORE_SITE_'||upper(p_site_id_i)||'_VAS-LOCATION_LOCATION');
	begin
		-- Update all tasks marshal and consol tasks with new to location id.
		update	dcsdba.move_task mt
		set	to_loc_id = l_vas_loc_id
		,	status = 'Complete'
		where	mt.pallet_id = p_tu_id_i
		and	mt.site_id = p_site_id_i
		;
		commit;
	exception
		when others
		then
			cnl_sys.cnl_as_pck.create_log_record('cnl_sys.cnl_as_mhe_pck.tu_pushed_in_vas_p',substr('Exception handling: SQLERRM = ' || sqlerrm || ' and SQLCODE = ' || sqlcode,1,4000));
			commit;	
	end tu_pushed_in_vas_p;
------------------------------------------------------------------------------------------------
-- Author       : Martijn, Swinkels 04-Okt-2021
-- Purpose      : Add and print parcel shipping label via Centiro Saas
------------------------------------------------------------------------------------------------
	function get_ship_label_f( p_box_id_i	in varchar2)
		return clob
	is
		cursor	c_label
		is
			select 	shp_label_base64
			from	cnl_sys.cnl_cto_ship_labels
			where	parcel_id	= p_box_id_i
			and	site_id		= 'NLTLG01'
			and	status 		in ('Created','Reprint')
			order
			by	creation_dstamp desc -- newest first
			,	status 		desc -- Reprint over Created if same time
		;
		l_label	clob;
	begin
		open	c_label;
		fetch	c_label
		into	l_label;
		close	c_label;
		return	l_label;
	exception
		when others
		then 
			return l_label;
	end get_ship_label_f;

begin
  -- Initialization
  null;
end cnl_as_mhe_pck;