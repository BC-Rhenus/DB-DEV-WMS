CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_JASPERSOFT_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Jaspersoft PCK
**********************************************************************************
* $Log: $
**********************************************************************************/
	g_pck		varchar2(30)	:=	'cnl_jaspersoft_pck';
	g_print_id	integer;
	g_log		varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_JASPER-LOG_ENABLE');
/***************************************************************************************
* procedure insert run task
***************************************************************************************/

--this is for training purpose

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
			       )
	is
		l_result	integer ;
		l_rtn		varchar2(30) := 'insert_rtsk_p';
		pragma autonomous_transaction;
	begin
		l_result := dcsdba.libruntask.createruntask( stationid             => p_station_id_i
							   , userid                => p_user_id_i
							   , commandtext           => p_command_i
							   , nametext              => p_report_name_i
							   , siteid                => p_site_id_i
							   , tmplanguage           => 'EN_GB'
							   , p_javareport          => 'Y'
							   , p_archive             => 'Y'
							   , p_runlight            => null
							   , p_serverinstance      => null
							   , p_priority            => p_priority_i
							   , p_timezonename        => 'Europe/Amsterdam'
							   , p_archiveignorescreen => null
							   , p_archiverestrictuser => null
							   , p_clientid            => p_client_id_i
							   , p_emailrecipients     => p_email_recipients_i
							   , p_masterkey           => null
							   , p_usedbtimezone       => 'N'
							   , p_nlscalendar         => 'Gregorian'
							   , p_emailattachment     => p_email_attachment_i
							   , p_emailsubject        => p_email_subject_i
							   , p_emailmessage        => p_email_message_i
							   );
		commit;

	exception
		when others
		then	
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null					-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end insert_rtsk_p;

/***************************************************************************************
* Set run task command
***************************************************************************************/	

	function run_task_command_f( p_report_command_i		in dcsdba.report.command%type 			-- Jasper report name
				   , p_export_targets_i		in dcsdba.run_task.command%type 		-- A = mail, printer name, lp-d for as prefix for label printer
				   , p_export_types_i		in dcsdba.run_task.command%type 		-- p = printer, z = zebra, m = mail
				   , p_export_copies_i		in dcsdba.run_task.command%type
				   , p_client_id_i		in dcsdba.client.client_id%type			-- client id
				   , p_owner_id_i		in dcsdba.owner.owner_id%type			-- owner_id
				   , p_container_id_i		in dcsdba.order_container.container_id%type	-- container id
				   , p_pallet_id_i		in dcsdba.order_container.pallet_id%type	-- pallet id
				   , p_order_id_i		in dcsdba.order_header.order_id%type		-- order id
				   , p_order_header_i		in dcsdba.order_header%rowtype
				   )
		return dcsdba.run_task.command%type
	is
		l_retval	dcsdba.run_task.command%type;
		l_rtn		varchar2(30) := 'run_task_command';
	begin
		l_retval := '"'
			 || p_report_command_i
			 || '" "'
			 || p_export_targets_i
			 || '" "'
			 || p_export_types_i
			 || '" "'
			 || p_export_copies_i
			 ||'"';
		--
		if	p_client_id_i is not null
		then
			l_retval := l_retval
				 || ' "client_id" "'
				 || p_client_id_i
				 || '"';
		end if;
		--
		if	p_owner_id_i is not null
		then
			l_retval := l_retval
				 || ' "owner_id" "'
				 || p_owner_id_i
				 || '"';
		end if;
		--
		if	p_container_id_i is not null
		then
			l_retval := l_retval
				 || ' "container_id" "'
				 || p_container_id_i
				 || '"';
		end if;
		--
		if	p_pallet_id_i is not null
		then
			l_retval := l_retval
				 || ' "pallet_id" "'
				 || p_pallet_id_i
				 || '"';
		end if;
		--
		if	p_order_id_i is not null
		then
			l_retval := l_retval
				 || ' "order_id" "'
				 || p_order_id_i
				 || '"';
		end if;
		--
		return l_retval;
	exception
		when others
		then	
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> 'p_report_command_i = '	|| p_report_command_i
											|| ' p_export_targets_i = '	|| p_export_targets_i
											|| ' p_export_types_i = '	|| p_export_types_i
											|| ' p_client_id_i = '		|| p_client_id_i
											|| ' p_owner_id_i = '		|| p_owner_id_i
											|| ' p_container_id_i = '	|| p_container_id_i
											|| ' p_pallet_id_i = '		|| p_pallet_id_i
											|| ' p_order_id_i = '		||p_order_id_i					
																-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end run_task_command_f;

/***************************************************************************************
* function fetch printers
***************************************************************************************/	

	function fetch_printers_f( p_key_i		in dcsdba.java_report_map.key%type
				 , p_template_name_i	in dcsdba.java_report_export.template_name%type
				 , p_header_template_i	in varchar2					-- Y is header template, N = specific export template
				 , p_export_targets_o	out dcsdba.run_task.command%type
				 , p_export_types_o	out dcsdba.run_task.command%type
				 , p_export_copies_o	out dcsdba.run_task.command%type
				 )
	return integer -- 1 is success, 0 is no export printers
	is
		--
		l_retval		integer;
		l_rtn			varchar2(30) := 'fetch_printers_f';
		l_export_targets	dcsdba.run_task.command%type;
		l_export_types		dcsdba.run_task.command%type;
		l_export_copies		dcsdba.run_task.command%type;
	begin
		select 	listagg(decode(jre.export_type,'Z','lp -d '||jre.export_target,jre.export_target), ';')
		within 
		group	(order by rowid) export_targets
		into	l_export_targets
		from	dcsdba.java_report_export jre
		where	jre.key 	= p_key_i
			and	(	jre.template_name = p_template_name_i
				or	(	jre.template_name is null
					and	nvl(p_header_template_i,'Y') = 'Y'
					)
				)
			and	jre.export_target is not null
		;

		select 	listagg(jre.export_type, ';')
		within 
		group	(order by rowid) export_targets
		into	l_export_types
		from	dcsdba.java_report_export jre
		where	jre.key 	= p_key_i
			and	(	jre.template_name = p_template_name_i
				or	(	jre.template_name is null
					and	nvl(p_header_template_i,'Y') = 'Y'
					)
				)
			and	jre.export_target is not null
		;

		select 	listagg(nvl(jre.copies,1), ';')
		within 
		group	(order by rowid) export_targets
		into	l_export_copies
		from	dcsdba.java_report_export jre
		where	jre.key 	= p_key_i
			and	(	jre.template_name = p_template_name_i
				or	(	jre.template_name is null
					and	nvl(p_header_template_i,'Y') = 'Y'
					)
				)
			and	jre.export_target is not null
		;
		--
		p_export_targets_o 	:= l_export_targets;
		p_export_types_o	:= l_export_types;
		p_export_copies_o	:= l_export_copies;
		--
		return 1;
	exception
		when 	NO_DATA_FOUND
		then	-- no printers found
			p_export_targets_o 	:= null;
			p_export_types_o	:= null;
			p_export_copies_o	:= null;
			return 0;
		--
		when	others
		then	
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> 'p_key_i = '	|| p_key_i
											|| ' p_template_name_i = '	|| p_template_name_i
											|| ' p_header_template_i = '	|| p_header_template_i
																-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			p_export_targets_o 	:= null;
			p_export_types_o	:= null;
			p_export_copies_o	:= null;
			return 0;
	end fetch_printers_f;

/***************************************************************************************
* function fetch email
***************************************************************************************/	

	function fetch_email_f( p_key_i			in dcsdba.java_report_map.key%type
			      , p_client_id_i		in dcsdba.client.client_id%type
			      , p_order_id_i		in dcsdba.order_header.order_id%type
			      , p_customer_id_i		in dcsdba.order_header.customer_id%type
			      , p_inv_address_id_i	in dcsdba.order_header.inv_address_id%type
			      , p_address_id_i		in dcsdba.address.address_id%type
			      , p_site_id_i		in dcsdba.site.site_id%type
			      , p_recipients_int_o	out dcsdba.run_task.email_recipients%type
			      , p_recipients_ext_o	out dcsdba.run_task.email_recipients%type
			      , p_recipients_cus_o	out dcsdba.run_task.email_recipients%type
			      )
	return integer -- 1 is mails found, 0 is no mails found
	is
		cursor	c_group_int
		is
			select 	listagg(jre.email_address, ';')
			within 
			group	(order by rowid) email_addresses
			from	dcsdba.java_report_email jre
			where	jre.key 	= p_key_i
			and	jre.email_address is not null
			and	lower(jre.email_address) like '%nl.rhenus%'
		;
		--
		cursor	c_group_ext
		is
			select 	listagg(jre.email_address, ';')
			within 
			group	(order by rowid) email_addresses
			from	dcsdba.java_report_email jre
			where	jre.key 	= p_key_i
			and	jre.email_address is not null
			and	lower(jre.email_address) not like '%nl.rhenus%'
		;
		--
		cursor	c_sql
		is
			select	replace(replace(replace(replace(replace(replace(lower(jre.email_select),
				'<client_id>',''''	||upper(p_client_id_i)	||''''),
				'<order_id>',''''	||upper(p_order_id_i)	||''''),
				'<customer_id>',''''	||upper(p_customer_id_i)||''''),
				'<inv_address_id>',''''	||upper(p_inv_address_id_i)||''''),
				'<address_id>',''''	||upper(p_address_id_i)	||''''),
				'<site_id>',''''	||upper(p_site_id_i)	||'''') my_sql
			from	dcsdba.java_report_email jre
			where	jre.key 	= p_key_i
			and	jre.email_select is not null
		;
		--
		r_group_int		c_group_int%rowtype;
		l_recipients_int 	dcsdba.run_task.email_recipients%type;
		r_group_ext		c_group_ext%rowtype;
		l_recipients_ext 	dcsdba.run_task.email_recipients%type;
		l_sql_addresses		dcsdba.run_task.email_recipients%type;
		l_recipients_cus 	dcsdba.run_task.email_recipients%type;
		l_rtn			varchar2(30) := 'fetch_email_f';
	begin
		-- Fetch all internal email addresses from advanced print mapping.
		open 	c_group_int;
		fetch	c_group_int
		into 	r_group_int;
		close	c_group_int;
		l_recipients_int := r_group_int.email_addresses;

		-- Fetch all external email addresses from advanced print mapping.
		open 	c_group_ext;
		fetch	c_group_ext
		into 	r_group_ext;
		close	c_group_ext;
		l_recipients_ext := r_group_ext.email_addresses;

		-- loop true any SQL statements to find other email addresses
		for	r_sql in c_sql
		loop
			-- When no data found an exception is raised. This routine is to get arround that.
			begin
				l_sql_addresses := null;
				execute immediate r_sql.my_sql
				into	l_sql_addresses;
			exception
				when others 
				then
					null;
			end;
			--
			if	l_sql_addresses is not null
			then
				if	l_recipients_cus is not null
				then
					l_recipients_cus := l_recipients_cus||';'||replace(l_sql_addresses,',',';');
				else
					l_recipients_cus := replace(l_sql_addresses,',',';');
				end if;
			end if;
		end loop;
		--
		p_recipients_int_o := l_recipients_int;
		p_recipients_ext_o := l_recipients_ext;
		p_recipients_cus_o := l_recipients_cus;
		--
		if		l_recipients_int is null
		and		l_recipients_ext is null
		and		l_recipients_cus is null
		then
			return 0;
		else
			return 1;
		end if;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> 'p_key_i = '			|| p_key_i
											|| ' l_recipients_int = '	|| l_recipients_int
											|| ' l_recipients_ext = '	|| l_recipients_ext
											|| ' l_recipients_cus = '	|| l_recipients_cus
																-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			-- send alert email about sql error
			return 0;
	end fetch_email_f;

/***************************************************************************************
* function template jasper report command
***************************************************************************************/	
	function user_report_command_f(p_template_name_i	in dcsdba.java_report_map.template_name%type)
	return varchar2
	is
		l_retval	dcsdba.report.command%type;
		l_rtn		varchar2(30) := 'user_report_command_f';
	begin
		select 	r.command
		into	l_retval
		from	dcsdba.report r
		where	r.name = p_template_name_i
		;
		return l_retval;
	exception
		when NO_DATA_FOUND
		then
			return null;
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> 'user_report = ' 
											|| p_template_name_i			-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
			return null;
	end user_report_command_f;

/***************************************************************************************
* procedure triggering Jaspersoft printing
***************************************************************************************/
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
			     , p_run_task_i	 	in  dcsdba.run_task%rowtype
			     )
	is 
		-- fetch order details
		cursor	c_order_header
		is
			select	o.*
			from	dcsdba.order_header o
			where	o.from_site_id 	= p_site_id_i
			and	o.client_id 	= p_client_id_i
			and	o.order_id 	= p_order_id_i
		;

		-- fetch all advanced print mapping records setup for this trigger
		cursor	c_jrp( b_report_name in varchar2
			     , b_site_id     in varchar2
			     , b_client_id   in varchar2
			     , b_owner_id    in varchar2
			     , b_order_id    in varchar2
			     , b_carrier_id  in varchar2
			     , b_user_id     in varchar2
			     , b_station_id  in varchar2
			     )
		is
			select	jrp.key
			,       jrp.print_mode
			,       jrp.report_name
			,       jrp.template_name
			,       jrp.site_id
			,       jrp.client_id
			,       jrp.owner_id
			,       jrp.carrier_id
			,       jrp.user_id
			,       jrp.station_id
			,       jrp.customer_id
			,       jrp.extra_parameters
			,       jrp.email_enabled
			,       jrp.email_export_type
			,       jrp.email_attachment
			,       jrp.email_subject
			,       jrp.email_message
			,       jrp.copies
			,       jrp.locality
			from    dcsdba.java_report_map jrp
			where   jrp.report_name        = b_report_name
			and     (	jrp.site_id            = nvl(b_site_id, jrp.site_id)
				or	jrp.site_id            is null
				)
			and     (	jrp.client_id          = nvl(b_client_id, jrp.client_id)
				or	jrp.client_id          is null
				)
			and     (	jrp.owner_id           = nvl(b_owner_id, jrp.owner_id)
				or	jrp.owner_id           	is null
				)
			and     (	jrp.carrier_id         = nvl(b_carrier_id, jrp.carrier_id)
				or	jrp.carrier_id         is null
				)
			and     (	jrp.user_id            = nvl(b_user_id, jrp.user_id)
				or	jrp.user_id            is null
				)
			and     (	jrp.station_id         = nvl(b_station_id, jrp.station_id)
				or	jrp.station_id         is null
				)
			and     1       = cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i => b_client_id
									      , p_order_id_i  => b_order_id
									      , p_where_i     => nvl( jrp.extra_parameters, '1=1')
									      )
			and    instr(lower(jrp.extra_parameters),'jaspersoft') > 0 -- document must  be printed by Jaspersoft									   
			order  by jrp.template_name
			,      jrp.station_id       nulls last
			,      jrp.locality         nulls last
			,      jrp.site_id          nulls last
			,      jrp.client_id        nulls last
			,      jrp.owner_id         nulls last
			,      jrp.carrier_id       nulls last
			,      jrp.user_id          nulls last
			,      jrp.extra_parameters nulls last
		;

		-- fetch all templates to create
		cursor	c_templates( b_key		in dcsdba.java_report_export.key%type
				   , b_header_template	in dcsdba.java_report_map.template_name%type
				   )
		is
			select	distinct
				jre.template_name
			from	dcsdba.java_report_export jre
			where	jre.key 	= b_key
			and	jre.template_name is not null
			and	jre.template_name != b_header_template
			union
			select	b_header_template
			from	dual
		;

		--
		r_order_header		dcsdba.order_header%rowtype;
		v_result		integer; -- used for resuts from functions
		v_recipients_int	dcsdba.run_task.email_recipients%type; 	-- Internal email addersses
		v_recipients_ext	dcsdba.run_task.email_recipients%type;	-- External client email addresses
		v_recipients_cus	dcsdba.run_task.email_recipients%type;  -- Customer email addresses
		v_mail_yn		varchar2(1) := 'N';
		v_mail_sub		dcsdba.run_task.email_subject%type;
		v_mail_att		dcsdba.run_task.email_attachment%type;
		v_mail_msg		dcsdba.run_task.email_message%type;
		v_mail_targets		varchar2(1) := 'A';
		v_mail_types		varchar2(1) := 'M';
		v_mail_copies		varchar2(1) := '1';
		v_main_template		varchar2(1) := 'Y';
		v_report_command	dcsdba.report.command%type;
		v_print_yn		varchar2(1) := 'Y';
		v_printer_targets	dcsdba.run_task.command%type;
		v_printer_types		dcsdba.run_task.command%type;
		v_printer_copies	dcsdba.run_task.command%type;
		v_mail_cnt		integer :=0;
		v_run_task_cmd		dcsdba.run_task.command%type;
		l_export_targets	dcsdba.run_task.command%type;
		l_export_types		dcsdba.run_task.command%type;
		l_export_copies		dcsdba.run_task.command%type;
		l_recipients		dcsdba.run_task.email_recipients%type;
		l_rtn			varchar2(30) := 'print_doc_p';

	begin	
		-- Set global print id for logging
		g_print_id := p_rtk_key;

		-- fetch order header details
		open	c_order_header;
		fetch	c_order_header
		into	r_order_header;
		close	c_order_header;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start print_doc_p procedure'
								   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" "carrier_id" "'||r_order_header.carrier_id||'" "reprint_yn" "'||p_reprint_yn_i||'" "user_id" "'||p_user_i||'" "workstation_id" "'||p_workstation_i||'" "report_name" "'|| p_report_name_i||'" "pdf_link" "'|| p_pdf_link_i||'" "pdf_autostore" "'|| p_pdf_autostore_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- start fetching all advanced print mapping record setup for this trigger.
		<<advanced_reports>>
		for 	r_jrp in c_jrp( p_report_name_i
				      , p_site_id_i
				      , p_client_id_i
				      , r_order_header.owner_id
				      , p_order_id_i 
				      , r_order_header.carrier_id
				      , p_user_i
				      , p_workstation_i
				      )

		loop
			-- If email is enabled fetch all recipients
			if	r_jrp.email_enabled ='Y'
			then
				-- Create log record
				if 	g_log = 'ON'
				then
					cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
										   , p_file_name_i		=> null
										   , p_source_package_i		=> g_pck
										   , p_source_routine_i		=> l_rtn
										   , p_routine_step_i		=> 'Create mail segment'
										   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" "carrier_id" "'||r_order_header.carrier_id||'" "reprint_yn" "'||p_reprint_yn_i||'" "user_id" "'||p_user_i||'" "workstation_id" "'||p_workstation_i||'" "report_name" "'|| p_report_name_i||'" "pdf_link" "'|| p_pdf_link_i||'" "pdf_autostore" "'|| p_pdf_autostore_i
										   , p_order_id_i		=> p_order_id_i
										   , p_client_id_i		=> p_client_id_i
										   , p_pallet_id_i		=> p_pallet_id_i
										   , p_container_id_i		=> p_container_id_i
										   , p_site_id_i		=> p_site_id_i
										   );
				end if;
				v_result := fetch_email_f( p_key_i		=> r_jrp.key
							 , p_client_id_i	=> p_client_id_i
							 , p_order_id_i		=> p_order_id_i
							 , p_customer_id_i	=> r_order_header.customer_id 
							 , p_inv_address_id_i	=> r_order_header.inv_address_id
							 , p_address_id_i	=> r_order_header.customer_id
							 , p_site_id_i		=> p_site_id_i
							 , p_recipients_int_o	=> v_recipients_int
							 , p_recipients_ext_o	=> v_recipients_ext
							 , p_recipients_cus_o	=> v_recipients_cus
							 );
				if	v_result = 1
				then	-- set email variables
					v_mail_yn := 'Y';
					v_mail_sub := r_jrp.email_subject;
					v_mail_att := r_jrp.email_attachment;
					v_mail_msg := r_jrp.email_message;
				else	-- no mail can be send
					v_mail_yn := 'N';
					v_mail_sub := null;
					v_mail_att := null;
					v_mail_msg := null;
				end if;
				-- No main template means no email
				if	r_jrp.template_name is null
				then
					v_mail_yn := 'N';
					v_mail_sub := null;
					v_mail_att := null;
					v_mail_msg := null;
				end if;				
			end if;

			-- looping templates
			<<templates>>
			for 	r_templates in c_templates( b_key		=> r_jrp.key
							  , b_header_template	=> r_jrp.template_name
							  )
			loop
				-- mails and prints combined and possibly multiple run tasks required.
				if	r_templates.template_name = r_jrp.template_name
				and	v_mail_yn = 'Y'
				then	-- This template requires to be emailed.
					-- Fetch jasper report command
					v_report_command	:= user_report_command_f(r_templates.template_name);

					-- Fetch printers and related information
					if	v_report_command is not null
					then	
						v_result := fetch_printers_f( p_key_i			=> r_jrp.key
									    , p_template_name_i		=> r_templates.template_name
									    , p_header_template_i	=> 'Y'
									    , p_export_targets_o	=> v_printer_targets
									    , p_export_types_o		=> v_printer_types
									    , p_export_copies_o		=> v_printer_copies
									    );
						if	v_result = 0
						then	-- nothing to print
							v_print_yn		:= 'N';
							v_printer_targets 	:= null;
							v_printer_types		:= null;
							v_printer_copies	:= null;
						end if;
					end if;

					-- Count how many times a run task must be created. !! Only the first will have printers added to it.
					if	v_recipients_int is not null
					then	
						v_mail_cnt	:= v_mail_cnt +1;
					end if;
					if	v_recipients_ext is not null
					then	
						v_mail_cnt	:= v_mail_cnt +1;
					end if;
					if	v_recipients_cus is not null
					then	
						v_mail_cnt	:= v_mail_cnt +1;
					end if;

					-- Start creating run task(s) for header template
					if	v_mail_cnt > 0
					then	
						for 	i in 1..v_mail_cnt
						loop
							-- Set export targets with or without printers.
							if	i = 1
							and	v_print_yn = 'Y'
							then	-- first start with run task including printers
								l_export_targets	:= v_mail_targets ||';'|| v_printer_targets;
								l_export_types		:= v_mail_types ||';'|| v_printer_types;
								l_export_copies		:= v_mail_copies ||';'|| v_printer_copies;
							else
								l_export_targets	:= v_mail_targets;
								l_export_types		:= v_mail_types;
								l_export_copies		:= v_mail_copies;
							end if;
							-- set recipients
							if	i = 1
							then
								if	v_recipients_int	= null
								then
									if	v_recipients_ext	= null
									then
										l_recipients		:= v_recipients_cus;
									else
										l_recipients		:= v_recipients_ext;
									end if;
								else
									l_recipients		:= v_recipients_int;
								end if;
							elsif	i = 2
							then
								if	v_recipients_ext	= null
								then
									l_recipients		:= v_recipients_cus;
								else
									l_recipients		:= v_recipients_ext;
								end if;
							else
								l_recipients		:= v_recipients_cus;
							end if;
							-- Create log record
							if 	g_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
													   , p_file_name_i		=> null
													   , p_source_package_i		=> g_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Create Run Task Command for subtemplate with Mail'
													   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																	|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																	|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																	|| '"user_id" "'||p_user_i||'" '
																	|| '"workstation_id" "'||p_workstation_i||'" '
																	|| '"report_name" "'||p_report_name_i||'" '
																	|| '"pdf_link" "'||p_pdf_link_i||'" '
																	|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																	|| '"template_name" "'||r_templates.template_name||'" '
													   , p_order_id_i		=> p_order_id_i
													   , p_client_id_i		=> p_client_id_i
													   , p_pallet_id_i		=> p_pallet_id_i
													   , p_container_id_i		=> p_container_id_i
													   , p_site_id_i		=> p_site_id_i
													   );
							end if;
							-- Create run task command
							v_run_task_cmd	:= run_task_command_f( p_report_command_i	=> v_report_command
											     , p_export_targets_i	=> l_export_targets
											     , p_export_types_i		=> l_export_types
											     , p_export_copies_i	=> l_export_copies
											     , p_client_id_i		=> p_client_id_i
											     , p_owner_id_i		=> r_order_header.owner_id
											     , p_container_id_i		=> p_container_id_i
											     , p_pallet_id_i		=> p_pallet_id_i
											     , p_order_id_i		=> p_order_id_i
											     , p_order_header_i		=> r_order_header
											     );
							-- Create log record
							if 	g_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
													   , p_file_name_i		=> null
													   , p_source_package_i		=> g_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Insert Run Task with Mail'
													   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																	|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																	|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																	|| '"user_id" "'||p_user_i||'" '
																	|| '"workstation_id" "'||p_workstation_i||'" '
																	|| '"report_name" "'||p_report_name_i||'" '
																	|| '"pdf_link" "'||p_pdf_link_i||'" '
																	|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																	|| '"template_name" "'||r_templates.template_name||'" '
													   , p_order_id_i		=> p_order_id_i
													   , p_client_id_i		=> p_client_id_i
													   , p_pallet_id_i		=> p_pallet_id_i
													   , p_container_id_i		=> p_container_id_i
													   , p_site_id_i		=> p_site_id_i
													   );
							end if;
							-- insert run task
							insert_rtsk_p( p_site_id_i 		=>	  p_site_id_i
							    	     , p_station_id_i		=>        p_workstation_i
								     , p_user_id_i       	=>        p_user_i
								     , p_command_i        	=>        v_run_task_cmd
								     , p_report_name_i     	=>        r_templates.template_name
								     , p_priority_i        	=>        null
								     , p_client_id_i        	=>        p_client_id_i
								     , p_email_recipients_i  	=>        l_recipients
								     , p_email_attachment_i  	=>        v_mail_att
								     , p_email_subject_i     	=>        v_mail_sub
								     , p_email_message_i     	=>        v_mail_msg
								     );
						end loop;
					else	-- No mail is needed or not main template	
						if	v_print_yn = 'Y'
						then
							l_export_targets	:= v_printer_targets;
							l_export_types		:= v_printer_types;
							l_export_copies		:= v_printer_copies;

							-- Create log record
							if 	g_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
													   , p_file_name_i		=> null
													   , p_source_package_i		=> g_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Create Run Task Command No mail is needed or not main template'
													   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																	|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																	|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																	|| '"user_id" "'||p_user_i||'" '
																	|| '"workstation_id" "'||p_workstation_i||'" '
																	|| '"report_name" "'||p_report_name_i||'" '
																	|| '"pdf_link" "'||p_pdf_link_i||'" '
																	|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																	|| '"template_name" "'||r_templates.template_name||'" '
													   , p_order_id_i		=> p_order_id_i
													   , p_client_id_i		=> p_client_id_i
													   , p_pallet_id_i		=> p_pallet_id_i
													   , p_container_id_i		=> p_container_id_i
													   , p_site_id_i		=> p_site_id_i
													   );
							end if;

							-- Create run task command
							v_run_task_cmd	:= run_task_command_f( p_report_command_i	=> v_report_command
											     , p_export_targets_i	=> l_export_targets
											     , p_export_types_i		=> l_export_types
											     , p_export_copies_i	=> l_export_copies
											     , p_client_id_i		=> p_client_id_i
											     , p_owner_id_i		=> r_order_header.owner_id
											     , p_container_id_i		=> p_container_id_i
											     , p_pallet_id_i		=> p_pallet_id_i
											     , p_order_id_i		=> p_order_id_i
											     , p_order_header_i		=> r_order_header
											     );

							-- Create log record
							if 	g_log = 'ON'
							then
								cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
													   , p_file_name_i		=> null
													   , p_source_package_i		=> g_pck
													   , p_source_routine_i		=> l_rtn
													   , p_routine_step_i		=> 'Insert Run Task No mail is needed or not main template'
													   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																	|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																	|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																	|| '"user_id" "'||p_user_i||'" '
																	|| '"workstation_id" "'||p_workstation_i||'" '
																	|| '"report_name" "'||p_report_name_i||'" '
																	|| '"pdf_link" "'||p_pdf_link_i||'" '
																	|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																	|| '"template_name" "'||r_templates.template_name||'" '
													   , p_order_id_i		=> p_order_id_i
													   , p_client_id_i		=> p_client_id_i
													   , p_pallet_id_i		=> p_pallet_id_i
													   , p_container_id_i		=> p_container_id_i
													   , p_site_id_i		=> p_site_id_i
													   );
							end if;

							-- insert run task
							insert_rtsk_p( p_site_id_i 		=>	  p_site_id_i
								     , p_station_id_i 		=>        p_workstation_i
								     , p_user_id_i        	=>        p_user_i
								     , p_command_i        	=>        v_run_task_cmd
								     , p_report_name_i     	=>        r_templates.template_name
								     , p_priority_i        	=>        null
								     , p_client_id_i        	=>        p_client_id_i
								     , p_email_recipients_i  	=>        null
								     , p_email_attachment_i  	=>        null
								     , p_email_subject_i     	=>        null
								     , p_email_message_i     	=>        null
								     );
						end if;
					end if;
					-- Finished processing main template.
				elsif	r_templates.template_name = r_jrp.template_name
				and	v_mail_yn = 'N'
				then
				 	-- main template without mail.
					-- Fetch jasper report command
					v_report_command	:= user_report_command_f(r_templates.template_name);

					-- Fetch printers and related information
					if	v_report_command is not null
					then	
						v_result := fetch_printers_f( p_key_i			=> r_jrp.key
									    , p_template_name_i		=> r_templates.template_name
									    , p_header_template_i	=> 'Y'
									    , p_export_targets_o	=> v_printer_targets
									    , p_export_types_o		=> v_printer_types
									    , p_export_copies_o		=> v_printer_copies
									    );
						if	v_result = 0
						then	-- nothing to print
							v_print_yn		:= 'N';
							v_printer_targets 	:= null;
							v_printer_types		:= null;
							v_printer_copies	:= null;
						end if;
					end if;
					--
					if	v_print_yn = 'Y'
					then
						l_export_targets	:= v_printer_targets;
						l_export_types		:= v_printer_types;
						l_export_copies		:= v_printer_copies;

						-- Create log record
						if 	g_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
												   , p_file_name_i		=> null
												   , p_source_package_i		=> g_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Create Run Task Command main template without mail'
												   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																|| '"user_id" "'||p_user_i||'" '
																|| '"workstation_id" "'||p_workstation_i||'" '
																|| '"report_name" "'||p_report_name_i||'" '
																|| '"pdf_link" "'||p_pdf_link_i||'" '
																|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																|| '"template_name" "'||r_templates.template_name||'" '
												   , p_order_id_i		=> p_order_id_i
												   , p_client_id_i		=> p_client_id_i
												   , p_pallet_id_i		=> p_pallet_id_i
												   , p_container_id_i		=> p_container_id_i
												   , p_site_id_i		=> p_site_id_i
												   );
						end if;

						-- Create run task command
						v_run_task_cmd	:= run_task_command_f( p_report_command_i	=> v_report_command
										     , p_export_targets_i	=> l_export_targets
										     , p_export_types_i		=> l_export_types
										     , p_export_copies_i	=> l_export_copies
										     , p_client_id_i		=> p_client_id_i
										     , p_owner_id_i		=> r_order_header.owner_id
										     , p_container_id_i		=> p_container_id_i
										     , p_pallet_id_i		=> p_pallet_id_i
										     , p_order_id_i		=> p_order_id_i
										     , p_order_header_i		=> r_order_header
										     );

						-- Create log record
						if 	g_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
												   , p_file_name_i		=> null
												   , p_source_package_i		=> g_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Insert Run Task main template without mail'
												   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																|| '"user_id" "'||p_user_i||'" '
																|| '"workstation_id" "'||p_workstation_i||'" '
																|| '"report_name" "'||p_report_name_i||'" '
																|| '"pdf_link" "'||p_pdf_link_i||'" '
																|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																|| '"template_name" "'||r_templates.template_name||'" '
												   , p_order_id_i		=> p_order_id_i
												   , p_client_id_i		=> p_client_id_i
												   , p_pallet_id_i		=> p_pallet_id_i
												   , p_container_id_i		=> p_container_id_i
												   , p_site_id_i		=> p_site_id_i
												   );
						end if;				     

						-- insert run task
						insert_rtsk_p( p_site_id_i 		=>	  p_site_id_i
						  	     , p_station_id_i 		=>        p_workstation_i
							     , p_user_id_i        	=>        p_user_i
							     , p_command_i        	=>        v_run_task_cmd
							     , p_report_name_i     	=>        r_templates.template_name
							     , p_priority_i        	=>        null
							     , p_client_id_i        	=>        p_client_id_i
							     , p_email_recipients_i  	=>        null
							     , p_email_attachment_i  	=>        null
							     , p_email_subject_i     	=>        null
							     , p_email_message_i     	=>        null
							     );
					end if;
				else
					-- Not main template so no mail.
					-- Fetch jasper report command
					v_report_command	:= user_report_command_f(r_templates.template_name);

					-- Fetch printers and related information
					if	v_report_command is not null
					then	
						v_result := fetch_printers_f( p_key_i			=> r_jrp.key
									    , p_template_name_i		=> r_templates.template_name
									    , p_header_template_i	=> 'Y'
									    , p_export_targets_o	=> v_printer_targets
									    , p_export_types_o		=> v_printer_types
									    , p_export_copies_o		=> v_printer_copies
									    );
						if	v_result = 0
						then	-- nothing to print
							v_print_yn		:= 'N';
							v_printer_targets 	:= null;
							v_printer_types		:= null;
							v_printer_copies	:= null;
						end if;
					end if;
					--
					if	v_print_yn = 'Y'
					then
						l_export_targets	:= v_printer_targets;
						l_export_types		:= v_printer_types;
						l_export_copies		:= v_printer_copies;

						-- Create log record
						if 	g_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
												   , p_file_name_i		=> null
												   , p_source_package_i		=> g_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Create Run Task Command Not main template so no mail'
												   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																|| '"user_id" "'||p_user_i||'" '
																|| '"workstation_id" "'||p_workstation_i||'" '
																|| '"report_name" "'||p_report_name_i||'" '
																|| '"pdf_link" "'||p_pdf_link_i||'" '
																|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																|| '"template_name" "'||r_templates.template_name||'" '
												   , p_order_id_i		=> p_order_id_i
												   , p_client_id_i		=> p_client_id_i
												   , p_pallet_id_i		=> p_pallet_id_i
												   , p_container_id_i		=> p_container_id_i
												   , p_site_id_i		=> p_site_id_i
												   );
						end if;

						-- Create run task command
						v_run_task_cmd	:= run_task_command_f( p_report_command_i	=> v_report_command
										     , p_export_targets_i	=> l_export_targets
										     , p_export_types_i		=> l_export_types
										     , p_export_copies_i	=> l_export_copies
										     , p_client_id_i		=> p_client_id_i
										     , p_owner_id_i		=> r_order_header.owner_id
										     , p_container_id_i		=> p_container_id_i
										     , p_pallet_id_i		=> p_pallet_id_i
										     , p_order_id_i		=> p_order_id_i
										     , p_order_header_i		=> r_order_header
										     );

						-- Create log record
						if 	g_log = 'ON'
						then
							cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
												   , p_file_name_i		=> null
												   , p_source_package_i		=> g_pck
												   , p_source_routine_i		=> l_rtn
												   , p_routine_step_i		=> 'Insert Run Task Not main template so no mail'
												   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
																|| '"carrier_id" "'||r_order_header.carrier_id||'" '
																|| '"reprint_yn" "'||p_reprint_yn_i||'" '
																|| '"user_id" "'||p_user_i||'" '
																|| '"workstation_id" "'||p_workstation_i||'" '
																|| '"report_name" "'||p_report_name_i||'" '
																|| '"pdf_link" "'||p_pdf_link_i||'" '
																|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
																|| '"template_name" "'||r_templates.template_name||'" '
												   , p_order_id_i		=> p_order_id_i
												   , p_client_id_i		=> p_client_id_i
												   , p_pallet_id_i		=> p_pallet_id_i
												   , p_container_id_i		=> p_container_id_i
												   , p_site_id_i		=> p_site_id_i
												   );
						end if;				     

						-- insert run task
						insert_rtsk_p( p_site_id_i 		=>	  p_site_id_i
						  	     , p_station_id_i 		=>        p_workstation_i
							     , p_user_id_i        	=>        p_user_i
							     , p_command_i        	=>        v_run_task_cmd
							     , p_report_name_i     	=>        r_templates.template_name
							     , p_priority_i        	=>        null
							     , p_client_id_i        	=>        p_client_id_i
							     , p_email_recipients_i  	=>        null
							     , p_email_attachment_i  	=>        null
							     , p_email_subject_i     	=>        null
							     , p_email_message_i     	=>        null
							     );
					end if;				
				end if;
			end loop; --<<templates>>
		end loop;--<<advanced_reports>>
		-- Create log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished print_doc_p procedure'
								   , p_code_parameters_i 	=> '"owner_id" "'||r_order_header.owner_id||'" '
												|| '"carrier_id" "'||r_order_header.carrier_id||'" '
												|| '"reprint_yn" "'||p_reprint_yn_i||'" '
												|| '"user_id" "'||p_user_i||'" '
												|| '"workstation_id" "'||p_workstation_i||'" '
												|| '"report_name" "'||p_report_name_i||'" '
												|| '"pdf_link" "'||p_pdf_link_i||'" '
												|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
	exception
		when others
		then
			cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
							  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
							  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
							  , p_package_name_i		=> g_pck				-- Package name the error occured
							  , p_routine_name_i		=> l_rtn				-- Procedure or function generarting the error
							  , p_routine_parameters_i	=> null
																-- list of all parameters involved
							  , p_comments_i		=> null					-- Additional comments describing the issue
							  );
	end print_doc_p;

end cnl_jaspersoft_pck;