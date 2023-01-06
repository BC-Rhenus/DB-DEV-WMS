CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_STREAMSERVE_PCK" is
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
--
-- Private type declarations
--
--
-- Private constant declarations
--
	g_yes			constant varchar2(1)              := 'Y';
	g_no                    constant varchar2(1)              := 'N';
	g_streamserve_tmp_dir   constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSERVE_TMP_DIR');
	g_streamserve_arc_dir   constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSERVE_ARCHIVE_DIR');
	g_streamserve_out_dir   constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSERVE_OUTPUT_DIR');
	g_streamserve_wms_db    constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'DB_NAME');
	g_wms_hazardous_ads_id  constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'WMS_HAZARDOUS_ADS_ID');
	g_jr_urepssvplt         constant varchar2(30)             := 'UREPSSVPLT';
	g_jr_urepssvpltcon      constant varchar2(30)             := 'UREPSSVPLTCON';
	g_ruler                 constant number                   := 35;
	g_plt                   constant varchar2(3)              := 'PLT';
	g_trl                   constant varchar2(3)              := 'TRL';
	g_wms                   constant varchar2(3)              := 'WMS';
	g_adl                   constant varchar2(3)              := 'ADL';
	g_bto                   constant varchar2(3)              := 'BTO';
	g_cid                   constant varchar2(3)              := 'CID';
	g_clt                   constant varchar2(3)              := 'CLT';
	g_crr                   constant varchar2(3)              := 'CRR';
	g_hub                   constant varchar2(3)              := 'HUB';
	g_sfm                   constant varchar2(3)              := 'SFM';
	g_shp                   constant varchar2(3)              := 'SHP';
	g_sid                   constant varchar2(3)              := 'SID';
	g_sto                   constant varchar2(3)              := 'STO';
	g_haz                   constant varchar2(3)              := 'HAZ';
	g_log			varchar2(10) := cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_SSV-LOG_ENABLE');
	g_pck			varchar2(30) := 'cnl_streamserve_pck';
--
-- Private variable declarations
--
	g_print_id		integer;
	g_file_name		varchar2(100);
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Update  : M. Swinkels, 12-Sep-2018 
-- Purpose : Create file for StreamServe
------------------------------------------------------------------------------------------------
	procedure open_ssv_file( p_site_id_i    in  varchar2
			       , p_client_id_i  in  varchar2
			       , p_owner_id_i   in  varchar2 := null
			       , p_file_ext_i   in  varchar2 := null
			       , p_file_type_o  out utl_file.file_type
			       , p_file_name_o  out varchar2
			       )
	is
		cursor c_cgp( b_client_id in varchar2)
		is
			select substr(cgt.client_group,-1)  prefix
			from   dcsdba.client_group_clients  cgt
			where  cgt.client_id                = b_client_id
			and    substr(cgt.client_group,1,7) = 'SSV-GRP'
			order  
			by 	1
		;
		--
		l_file_type     utl_file.file_type;
		l_prefix        varchar2(1);
		l_counter       integer := 0;
		l_ssv_file_id_i	varchar2(10);
		l_filename      varchar2(100);
		l_rtn		varchar2(30) := 'open_ssv_file';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start Open new file. Prefix M means multiple client groups in use for client. Prefix X means client does not exist in any client group starting with SSV-GRP.'
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		--
		if      p_file_ext_i != 'TRL' or 
			p_file_ext_i is null
		then
			-- define prefix for StreamServe
			open 	c_cgp ( b_client_id => p_client_id_i);
			for	i in 1..2
			loop
				fetch	c_cgp
				into  	l_prefix; -- Get the number from the "SSV-GRP" Client Group in which the Client is added
				--
				exit 	when c_cgp%notfound;
				--
				if 	i = 2
				then
					l_prefix := 'M'; -- M in case Client exists in Multiple "SSV-GRP" Client Groups, should be only one
				end if;
			end loop;
		else
			l_prefix := 'T'; -- trolley 
		end if;

		-- fetch id for file
		select	lpad( to_char( cnl_ssv_file_id_seq1.nextval), 10, 0)
		into  	l_ssv_file_id_i
		from  	dual;

		-- create filename
		if      p_file_ext_i != 'TRL' or 
			p_file_ext_i is null
		then
			l_filename := nvl(l_prefix,'X') || '_' -- X in case Client doesn't exist in any "SSV-GRP" Client Group
				   || p_site_id_i       || '_' 
				   || p_client_id_i     || '_' 
				   || p_owner_id_i      || '_'
				   || l_ssv_file_id_i
				   || '.PLT'
				   ;
		else
			l_filename := nvl(l_prefix,'X') || '_' -- X in case Client doesn't exist in any "SSV-GRP" Client Group
				   || p_site_id_i       || '_' 
				   || l_ssv_file_id_i
				   || '.TRL'
				   ;
		end if;

		-- open/create file in tmp
		l_file_type := utl_file.fopen( location     => g_streamserve_tmp_dir
					     , filename     => l_filename
					     , open_mode    => 'w'
					     , max_linesize => 32767
					     );
		--
		p_file_type_o := l_file_type;
		p_file_name_o := l_filename;

	exception
		when	utl_file.invalid_path
		then
			raise_application_error( -20100, 'Invalid path');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.invalid_mode
		then
			raise_application_error( -20100, 'Invalid Mode');
			utl_file.fclose(file => l_file_type);
		when 	utl_file.invalid_filehandle
		then
			raise_application_error( -20100, 'Invalid File Handle');
			utl_file.fclose(file => l_file_type);
		when 	utl_file.invalid_operation
		then
			raise_application_error( -20100, 'Invalid Operation');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.read_error
		then
			raise_application_error( -20100, 'Read Error');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.write_error
		then
			raise_application_error( -20100, 'Write Error');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.internal_error
		then
			raise_application_error( -20100, 'Internal Error');
			utl_file.fclose (file => l_file_type);
		when 	no_data_found
		then
			raise_application_error( -20100, 'No Data Found');
			utl_file.fclose (file => l_file_type);
		when 	value_error
		then
			raise_application_error( -20100, 'Value Error');
			utl_file.fclose (file => l_file_type);
		when 	others
		then
			raise_application_error( -20100, sqlerrm);
			utl_file.fclose (file => l_file_type);
			case 
			when	c_cgp%isopen
			then
				close	c_cgp;
			else
				null;
			end 	case;

  end open_ssv_file;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 20-06-2016
-- Purpose : Add line to file
------------------------------------------------------------------------------------------------
	procedure write_line( p_file_type_i    in  utl_file.file_type
			    , p_field_prefix_i in  varchar2
			    , p_field_name_i   in  varchar2
			    , p_content_i      in  varchar2
			    )
	is
		l_field_name	varchar2(50);
		l_content     	varchar2(4096);
	begin
		if	p_field_prefix_i is null
		then
			l_field_name := p_field_name_i;
		else
			l_field_name := p_field_prefix_i|| '_' || p_field_name_i;
		end if;
		--
		l_field_name := rpad( l_field_name, g_ruler);
		l_content    := l_field_name || p_content_i;
		-- write line to file
		utl_file.put_line( file   	=> p_file_type_i
				 , buffer	=> l_content
				 );
	exception
		when	utl_file.invalid_path
		then
			raise_application_error( -20100, 'Invalid path ');
		when 	utl_file.invalid_mode
		then
			raise_application_error( -20100, 'Invalid Mode ');
		when 	utl_file.invalid_filehandle
		then
			raise_application_error( -20100, 'Invalid File Handle ');
		when 	utl_file.invalid_operation
		then
			raise_application_error( -20100, 'Invalid Operation ');
		when 	utl_file.read_error
		then
			raise_application_error( -20100, 'Read Error ');
		when 	utl_file.write_error
		then
			raise_application_error( -20100, 'Write Error ');
		when 	utl_file.internal_error
		then
			raise_application_error( -20100, 'Internal Error ');
		when 	no_data_found
		then
			raise_application_error( -20100, 'No Data Found ');
		when 	value_error
		then
			raise_application_error( -20100, 'Value Error ');
		when others
		then
			raise_application_error( -20100, sqlerrm);
	end write_line;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Close, archive and move file to out directory for StreamServe
------------------------------------------------------------------------------------------------
	procedure close_ssv_file( p_file_type_i in utl_file.file_type
				, p_file_name_i in varchar2
				)
	is
		l_file_type	utl_file.file_type;
		l_filename	varchar2(100);
		l_tmp_fexists	boolean;
		l_arc_fexists	boolean;
		l_file_length	number;
		l_block_size 	binary_integer;
	begin                            
		l_file_type := p_file_type_i;
		l_filename  := p_file_name_i;
		-- close file
		if	utl_file.is_open (file => l_file_type)
		then
			utl_file.fclose (file => l_file_type);
		end if;
		-- copy file from tmp to archive
		utl_file.fgetattr( location    => g_streamserve_tmp_dir 
				 , filename    => l_filename
				 , fexists     => l_tmp_fexists
				 , file_length => l_file_length
				 , block_size  => l_block_size
				 );
		if	l_tmp_fexists
		then
			utl_file.fcopy( src_location  => g_streamserve_tmp_dir
				      , src_filename  => l_filename
				      , dest_location => g_streamserve_arc_dir
				      , dest_filename => l_filename
				      );
		end if;                                          
		-- move file from tmp to out
		utl_file.fgetattr( location    => g_streamserve_arc_dir 
				 , filename    => l_filename
				 , fexists     => l_arc_fexists
				 , file_length => l_file_length
				 , block_size  => l_block_size 
				 );
		if 	l_arc_fexists
		then
			utl_file.frename( src_location  => g_streamserve_tmp_dir
					, src_filename  => l_filename
					, dest_location => g_streamserve_out_dir
					, dest_filename => l_filename
					, overwrite     => true
					);
			-- add filename to table to be able to cleanup directory
			insert 	into 
				cnl_files_archive
				( application
				, location
				, filename
				)
			values	( 'STREAMSERVE'
				, g_streamserve_arc_dir
				, l_filename
				)
			;
			commit;
		end if;
		--
		utl_file.fclose (file => l_file_type);
		--
	exception
		when	utl_file.invalid_path
		then
			raise_application_error( -20100, 'Invalid path');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.invalid_mode
		then
			raise_application_error( -20100, 'Invalid Mode');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.invalid_filehandle
		then
			raise_application_error( -20100, 'Invalid File Handle');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.invalid_operation
		then
			raise_application_error( -20100, 'Invalid Operation');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.read_error
		then
			raise_application_error( -20100, 'Read Error');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.write_error
		then
			raise_application_error( -20100, 'Write Error');
			utl_file.fclose (file => l_file_type);
		when 	utl_file.internal_error
		then
			raise_application_error( -20100, 'Internal Error');
			utl_file.fclose (file => l_file_type);
		when 	no_data_found
		then
			raise_application_error( -20100, 'No Data Found');
			utl_file.fclose (file => l_file_type);
		when 	value_error
		then
			raise_application_error( -20100, 'Value Error');
			utl_file.fclose (file => l_file_type);
		when 	others
		then
			raise_application_error( -20100, sqlerrm);
			utl_file.fclose (file => l_file_type);
	end close_ssv_file;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 04-Nov-2016
-- Purpose : Get Print Server for StreamServe
------------------------------------------------------------------------------------------------
	function get_print_server
		return varchar2
	is
		l_print_server varchar2(30);
	begin
		select	user_def_type_1
		into   	l_print_server
		from   	dcsdba.system_options
		;
		return l_print_server;
	end get_print_server;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create StreamServe Move Task block for Pick Label
------------------------------------------------------------------------------------------------
	procedure add_mtk_pll( p_file_type_i    in  utl_file.file_type
			     , p_field_prefix_i in  varchar2
			     , p_client_id_i    in  varchar2
			     , p_prt_lbl_id_i   in  number
			     )
	is
		-- Fetch move task details
		cursor c_mtk( b_client_id  in varchar2
			    , b_prt_lbl_id in number
			    )
		is
			select	mtk.*
			from   	dcsdba.move_task mtk
			where  	mtk.client_id      = b_client_id
			and    	mtk.print_label_id = b_prt_lbl_id
		;
		--
		r_mtk   c_mtk%rowtype;
		l_rtn	varchar2(30) := 'add_mtk_pll';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_field_prefix_i||'_MTK'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"print_label_id" "'||p_prt_lbl_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		open	c_mtk( b_client_id  => p_client_id_i
			     , b_prt_lbl_id => p_prt_lbl_id_i
			     );
		fetch 	c_mtk
		into  	r_mtk;
		if 	c_mtk%found
		then            
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_KEY'
				  , p_content_i      => r_mtk.key
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FIRST_KEY'
				  , p_content_i      => r_mtk.first_key
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TASK_TYPE'
				  , p_content_i      => r_mtk.task_type
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TASK_ID'
				  , p_content_i      => r_mtk.task_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_LINE_ID'
				  , p_content_i      => r_mtk.line_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CLIENT_ID'
				  , p_content_i      => r_mtk.client_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SKU_ID'
				  , p_content_i      => r_mtk.sku_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CONFIG_ID'
				  , p_content_i      => r_mtk.config_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SKU_DESCRIPTION'
				  , p_content_i      => r_mtk.description
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TAG_ID'
				  , p_content_i      => r_mtk.tag_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OLD_TAG_ID'
				  , p_content_i      => r_mtk.old_tag_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CUSTOMER_ID'
				  , p_content_i      => r_mtk.customer_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_ORIGIN_ID'
				  , p_content_i      => r_mtk.origin_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CONDITION_ID'
				  , p_content_i      => r_mtk.condition_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_QTY_TO_MOVE'
				  , p_content_i      => r_mtk.qty_to_move
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OLD_QTY_TO_MOVE'
				  , p_content_i      => r_mtk.old_qty_to_move
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SITE_ID'
				  , p_content_i      => r_mtk.site_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FROM_LOC_ID'
				  , p_content_i      => r_mtk.from_loc_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OLD_FROM_LOC_ID'
				  , p_content_i      => r_mtk.old_from_loc_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_LOC_ID'
				  , p_content_i      => r_mtk.to_loc_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OLD_TO_LOC_ID'
				  , p_content_i      => r_mtk.old_to_loc_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FINAL_LOC_ID'
				  , p_content_i      => r_mtk.final_loc_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OWNER_ID'
				  , p_content_i      => r_mtk.owner_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SEQUENCE'
				  , p_content_i      => r_mtk.sequence
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_STATUS'
				  , p_content_i      => r_mtk.status
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_LIST_ID'
				  , p_content_i      => r_mtk.list_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DSTAMP'
				  , p_content_i      => to_char( r_mtk.dstamp, 'DD-MM-YYYY')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DSTAMP_TIME'
				  , p_content_i      => to_char( r_mtk.dstamp, 'HH24:MI:SS')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_START_DSTAMP'
				  , p_content_i      => to_char( r_mtk.start_dstamp, 'DD-MM-YYYY')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_START_DSTAMP_TIME'
				  , p_content_i      => to_char( r_mtk.start_dstamp, 'HH24:MI:SS')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FINISH_DSTAMP'
				  , p_content_i      => to_char( r_mtk.finish_dstamp, 'DD-MM-YYYY')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FINISH_DSTAMP_TIME'
				  , p_content_i      => to_char( r_mtk.finish_dstamp, 'HH24:MI:SS')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_ORIGINAL_DSTAMP'
				  , p_content_i      => to_char( r_mtk.original_dstamp, 'DD-MM-YYYY')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_ORIGINAL_DSTAMP_TIME'
				  , p_content_i      => to_char( r_mtk.original_dstamp, 'HH24:MI:SS')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PRIORITY'
				  , p_content_i      => r_mtk.priority
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CONSOL_LINK'
				  , p_content_i      => r_mtk.consol_link
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FACE_TYPE'
				  , p_content_i      => r_mtk.face_type
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FACE_KEY'
				  , p_content_i      => r_mtk.face_key
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_WORK_ZONE'
				  , p_content_i      => r_mtk.work_zone
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_WORK_GROUP'
				  , p_content_i      => r_mtk.work_group
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CONSIGNMENT'
				  , p_content_i      => r_mtk.consignment
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_BOL_ID'
				  , p_content_i      => r_mtk.bol_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_REASON_CODE'
				  , p_content_i      => r_mtk.reason_code
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CONTAINER_ID'
				  , p_content_i      => r_mtk.container_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_CONTAINER_ID'
				  , p_content_i      => r_mtk.to_container_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_ID'
				  , p_content_i      => r_mtk.pallet_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_ID'
				  , p_content_i      => r_mtk.to_pallet_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_CONFIG'
				  , p_content_i      => r_mtk.to_pallet_config
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_VOLUME'
				  , p_content_i      => to_char( r_mtk.to_pallet_volume, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_HEIGHT'
				  , p_content_i      => to_char( r_mtk.to_pallet_height, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_DEPTH'
				  , p_content_i      => to_char( r_mtk.to_pallet_depth, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_WIDTH'
				  , p_content_i      => to_char( r_mtk.to_pallet_width, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TO_PALLET_WEIGHT'
				  , p_content_i      => to_char( r_mtk.to_pallet_weight, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_GROUPED'
				  , p_content_i      => r_mtk.pallet_grouped
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_CONFIG'
				  , p_content_i      => r_mtk.pallet_config
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_VOLUME'
				  , p_content_i      => to_char( r_mtk.pallet_volume, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_HEIGHT'
				  , p_content_i      => to_char( r_mtk.pallet_height, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_DEPTH'
				  , p_content_i      => to_char( r_mtk.pallet_depth, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_WIDTH'
				  , p_content_i      => to_char( r_mtk.pallet_width, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PALLET_WEIGHT'
				  , p_content_i      => to_char( r_mtk.pallet_weight, 'fm999990.90')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_USER_ID'
				  , p_content_i      => r_mtk.user_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_STATION_ID'
				  , p_content_i      => r_mtk.station_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SESSION_TYPE'
				  , p_content_i      => r_mtk.session_type
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SUMMARY_RECORD'
				  , p_content_i      => r_mtk.summary_record
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_REPACK'
				  , p_content_i      => r_mtk.repack
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_KIT_SKU_ID'
				  , p_content_i      => r_mtk.kit_sku_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_KIT_LINE_ID'
				  , p_content_i      => r_mtk.kit_line_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_KIT_RATIO'
				  , p_content_i      => r_mtk.kit_ratio
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_KIT_LINK'
				  , p_content_i      => r_mtk.kit_link
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DUE_TYPE'
				  , p_content_i      => r_mtk.due_type
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DUE_TASK_ID'
				  , p_content_i      => r_mtk.due_task_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DUE_LINE_ID'
				  , p_content_i      => r_mtk.due_line_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TRAILER_POSITION'
				  , p_content_i      => r_mtk.trailer_position
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CONSOLIDATED_TASK'
				  , p_content_i      => r_mtk.consolidated_task
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DISALLOW_TAG_SWAP'
				  , p_content_i      => r_mtk.disallow_tag_swap
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CE_UNDER_BOND'
				  , p_content_i      => r_mtk.ce_under_bond
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_INCREMENT_TIME'
				  , p_content_i      => r_mtk.increment_time
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_ESTIMATED_TIME'
				  , p_content_i      => r_mtk.estimated_time
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_UPLOADED_LABOR'
				  , p_content_i      => r_mtk.uploaded_labor
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PRINT_LABEL_ID'
				  , p_content_i      => r_mtk.print_label_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PRINT_LABEL'
				  , p_content_i      => r_mtk.print_label
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OLD_STATUS'
				  , p_content_i      => r_mtk.old_status
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_REPACK_QC_DONE'
				  , p_content_i      => r_mtk.repack_qc_done
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_OLD_TASK_ID'
				  , p_content_i      => r_mtk.old_task_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_CATCH_WEIGHT'
				  , p_content_i      => r_mtk.catch_weight
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_MOVED_LOCK_STATUS'
				  , p_content_i      => r_mtk.moved_lock_status
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PICK_REALLOC_FLAG'
				  , p_content_i      => r_mtk.pick_realloc_flag
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_STAGE_ROUTE_ID'
				  , p_content_i      => r_mtk.stage_route_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_STAGE_ROUTE_SEQUENCE'
				  , p_content_i      => r_mtk.stage_route_sequence
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_LABELLING'
				  , p_content_i      => r_mtk.labelling
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PF_CONSOL_LINK'
				  , p_content_i      => r_mtk.pf_consol_link
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_INV_KEY'
				  , p_content_i      => r_mtk.inv_key
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_FIRST_PICK'
				  , p_content_i      => r_mtk.first_pick
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_SERIAL_NUMBER'
				  , p_content_i      => r_mtk.serial_number
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_LABEL_EXCEPTIONED'
				  , p_content_i      => r_mtk.label_exceptioned
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_DECONSOLIDATE'
				  , p_content_i      => r_mtk.deconsolidate
				  );
		end if;

		close	c_mtk;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i||'_MTK'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"print_label_id" "'||p_prt_lbl_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

	exception
		when	others
		then
			case 
			when	c_mtk%isopen
			then
				close	 c_mtk;
			else
				null;
			end case;

	end add_mtk_pll;              
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Nov-2016
-- Purpose : Create StreamServe Shipment Items Lot block
------------------------------------------------------------------------------------------------
	procedure add_sim_lot( p_file_type_i    in  utl_file.file_type
			     , p_field_prefix_i in  varchar2
			     , p_segment_nr_i   in  number
			     , p_client_id_i    in  varchar2
			     , p_order_nr_i     in  varchar2
			     , p_pallet_id_i    in  varchar2 := null
			     , p_container_id_i in  varchar2 := null
			     , p_is_cont_yn_i   in  varchar2
			     )
	is
		-- Fetch total QTY from inventory, shipping manifest, move_task
		cursor c_qty_chk( b_client_id    in varchar2
				, b_order_id     in varchar2
				, b_pallet_id    in varchar2
				, b_container_id in varchar2
				)
		is
			select	(	select	sum(smt.qty_shipped)         	qty
					from   	dcsdba.shipping_manifest     	smt
					where  	smt.client_id                	= b_client_id
					and    	smt.order_id                 	= b_order_id
					and    	(	nvl( smt.pallet_id, '@#')    	= nvl( b_pallet_id,'@#') or
							b_pallet_id is null)
					and    	(	nvl( smt.container_id, '@#') 	= nvl( b_container_id, '@#') or
							b_container_id is null)
				) 	total_qty_smt
		      ,		( 	select 	sum(mtk.qty_to_move)		qty
					from   	dcsdba.move_task              	mtk
					where   mtk.client_id 			= b_client_id
					and    	mtk.task_id                   	= b_order_id
					and    	(	nvl( mtk.pallet_id, '@#')    	= nvl( b_pallet_id,'@#') or
							b_pallet_id is null)
					and    	(	nvl( mtk.container_id, '@#') 	= nvl( b_container_id, '@#') or 
							b_container_id is null)
					and	mtk.status 			= 'Consol'
					and    	not exists 	(	select 	1
									from   dcsdba.shipping_manifest smt
									where  smt.client_id            = b_client_id
									and    smt.order_id             = b_order_id
									and    smt.pallet_id            = mtk.pallet_id
									and    smt.container_id         = mtk.container_id
								)
				) 	total_qty_mvt
		      ,		(	select	sum(qty_on_hand)		qty
					from	dcsdba.inventory		ivy
					where	ivy.client_id 			= b_client_id
					and    	(	nvl( ivy.pallet_id, '@#')    	= nvl( b_pallet_id,'@#') or
							b_pallet_id is null)
					and    	(	nvl( ivy.container_id, '@#') 	= nvl( b_container_id, '@#') or
							b_container_id is null)
					and    	not exists 	(	select 	1
									from   dcsdba.shipping_manifest smt
									where  smt.client_id            = b_client_id
									and    smt.order_id             = b_order_id
									and    smt.pallet_id            = ivy.pallet_id
									and    smt.container_id         = ivy.container_id
								)
				)	total_qty_ivy
		from dual
		;	

		-- Fetch details from shipping_manifest or inventory and move task combo
		cursor c_ocr_dtl( b_client_id    in varchar2
				, b_order_id     in varchar2
				, b_pallet_id    in varchar2
				, b_container_id in varchar2
				)
		is
			select	smt.tag_id                   
			,      	smt.sku_id
			,      	smt.batch_id                 
			,      	smt.expiry_dstamp            
			,      	smt.origin_id                
			,      	sum(smt.qty_shipped)		qty
			,      	smt.container_id
			,      	smt.condition_id
			,      	smt.receipt_dstamp
			,      	smt.manuf_dstamp
			,      	smt.receipt_id
			,	nvl(smt.user_def_chk_1,'N') user_def_chk_1
			,	nvl(smt.user_def_chk_2,'N') user_def_chk_2
			,	nvl(smt.user_def_chk_3,'N') user_def_chk_3
			,	nvl(smt.user_def_chk_4,'N') user_def_chk_4
			from   	dcsdba.shipping_manifest     	smt
			where  	smt.client_id                	= b_client_id
			and    	smt.order_id                 	= b_order_id
			and    	nvl( smt.pallet_id, '@#')    	= nvl( b_pallet_id, nvl( smt.pallet_id, '@#'))
			and    	nvl( smt.container_id, '@#') 	= nvl( b_container_id, nvl( smt.container_id, '@#'))
			group  
			by 	smt.tag_id
			,      	smt.sku_id
			,      	smt.batch_id
			,      	smt.expiry_dstamp
			,      	smt.origin_id
			,      	smt.container_id
			,      	smt.condition_id
			,      	smt.receipt_dstamp
			,      	smt.manuf_dstamp
			,      	smt.receipt_id
			,	nvl(smt.user_def_chk_1,'N')
			,	nvl(smt.user_def_chk_2,'N')
			,	nvl(smt.user_def_chk_3,'N')
			,	nvl(smt.user_def_chk_4,'N')
			union  	-- For pallets which are not 'Marshalled'
			select 	tag_id
			,	sku_id
			,	batch_id
			,	expiry_dstamp
			,	origin_id
			,	sum(qty_to_move)          	qty
			,	decode(to_container_id,null,container_id,to_container_id)
			,	condition_id
			,	receipt_dstamp
			,	manuf_dstamp
			,	receipt_id
			,	user_def_chk_1
			,	user_def_chk_2
			,	user_def_chk_3
			,	user_def_chk_4
			from(	select 	mtk.tag_id
				,      	mtk.sku_id
				,      	(select i.batch_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) batch_id
				,      	(select i.expiry_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) expiry_dstamp
				,      	(select i.origin_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) origin_id
				,      	mtk.qty_to_move
				,      	mtk.container_id
				,       mtk.to_container_id
				,      	(select i.condition_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) condition_id
				,      	(select i.receipt_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_dstamp
				,      	(select i.manuf_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) manuf_dstamp
				,      	(select i.receipt_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_id
				,      	(select nvl(i.user_def_chk_1,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_1
				,      	(select nvl(i.user_def_chk_2,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_2
				,      	(select nvl(i.user_def_chk_3,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_3
				,      	(select nvl(i.user_def_chk_4,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_4
				from   	dcsdba.move_task		mtk
				where  	mtk.client_id                 	= b_client_id
				and    	mtk.task_id                   	= b_order_id
				and    	(	(	nvl( mtk.pallet_id, '@#')    	= nvl( b_pallet_id, nvl( mtk.pallet_id, '@#'))
						and    	nvl( mtk.container_id, '@#') 	= nvl( b_container_id, nvl( mtk.container_id, '@#'))
						)
						or
						(	nvl( mtk.to_pallet_id, '@#')    	= nvl( b_pallet_id, nvl( mtk.to_pallet_id, '@#'))
						and    	nvl( mtk.to_container_id, '@#') 	= nvl( b_container_id, nvl( mtk.to_container_id, '@#'))
						)
					)
				and    	not exists(	select	1
							from   	dcsdba.shipping_manifest smt
							where  	smt.client_id            = b_client_id
							and    	smt.order_id             = b_order_id
							and    	smt.pallet_id            = mtk.pallet_id
							and    	smt.container_id         = mtk.container_id
						  )
			    )
			group  
			by 	tag_id
			,      	sku_id
			,      	batch_id
			,      	expiry_dstamp
			,      	origin_id
			,	decode(to_container_id,null,container_id,to_container_id)
			,      	condition_id
			,      	receipt_dstamp
			,      	manuf_dstamp
			,      	receipt_id
			,	user_def_chk_1
			,	user_def_chk_2
			,	user_def_chk_3
			,	user_def_chk_4
		;


		-- Fetch serial numbers
		cursor c_snr( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_container_id in varchar2
			    , b_sku_id       in varchar2
			    , b_tag_id       in varchar2
			    )
		is
			select	snr.serial_number
			from   	dcsdba.serial_number snr
			where  	snr.client_id        = b_client_id
			and    	snr.order_id         = b_order_id
			and    	snr.container_id     = b_container_id
			and    	snr.sku_id           = b_sku_id
			and    	snr.tag_id           = b_tag_id
			order  
			by 	snr.serial_number
		;   

		-- 
		r_ocr_dtl	c_ocr_dtl%rowtype;
		r_snr         	c_snr%rowtype;

		l_pallet_id     varchar2(20);
		l_container_id  varchar(20);
		l_count_dtl     number(10) := 0;
		l_snr_prefix    varchar2(20);
		l_count_snr     number(10);
		l_snr_total     varchar2(3999);
		l_snr_line      varchar2(3999);
		l_no_val_chk    integer :=0;
		l_retry	    	number :=0;
		l_qty_chk	c_qty_chk%rowtype;
		l_rtn		varchar2(30) := 'add_sim_lot';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding SIM_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"is_cont_yn" "'||p_is_cont_yn_i 
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		-- set pallet/container acc. is_cont_yn parameter
		if	p_is_cont_yn_i = g_yes
		then
			l_pallet_id	:= null;
			l_container_id 	:= p_pallet_id_i;
		else
			l_pallet_id    	:= p_pallet_id_i;
			l_container_id 	:= p_container_id_i;
		end if;


		-- Add LOT lines
		<<lot_line_loop>>
		for	r_ocr_dtl in c_ocr_dtl( b_client_id    => p_client_id_i    
					      , b_order_id     => p_order_nr_i
					      , b_pallet_id    => l_pallet_id
					      , b_container_id => l_container_id
					      )
		loop
			l_no_val_chk	:= 1;
			l_count_dtl 	:= l_count_dtl + 1;
			--
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SIM_LOT_SEGMENT_NR'
				  , p_content_i      => 'Segment SIM / Lot: ' || to_char( p_segment_nr_i) || ' / ' || to_char( l_count_dtl)
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_TAG_ID'
                		  , p_content_i      => r_ocr_dtl.tag_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_SKU_ID'
                		  , p_content_i      => r_ocr_dtl.sku_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_BATCH_ID'
                		  , p_content_i      => r_ocr_dtl.batch_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_EXPIRY_DATE'
                		  , p_content_i      => to_char( r_ocr_dtl.expiry_dstamp, 'DD-MM-YYYY')
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_EXPIRY_TIME'
                		  , p_content_i      => to_char( r_ocr_dtl.expiry_dstamp, 'HH24:MI:SS')
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_RECEIPT_ID'
                		  , p_content_i      => r_ocr_dtl.receipt_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_RECEIPT_DATE'
                		  , p_content_i      => to_char( r_ocr_dtl.receipt_dstamp, 'DD-MM-YYYY')
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_RECEIPT_TIME'
                		  , p_content_i      => to_char( r_ocr_dtl.receipt_dstamp, 'HH24:MI:SS')
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_MANUF_DATE'
                		  , p_content_i      => to_char( r_ocr_dtl.manuf_dstamp, 'DD-MM-YYYY')
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_MANUF_TIME'
                		  , p_content_i      => to_char( r_ocr_dtl.manuf_dstamp, 'HH24:MI:SS')
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_ORIGIN_ID'
                		  , p_content_i      => r_ocr_dtl.origin_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_CONDITION_ID'
                		  , p_content_i      => r_ocr_dtl.condition_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_QTY'
                		  , p_content_i      => r_ocr_dtl.qty
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_CONTAINER_ID'
                		  , p_content_i      => r_ocr_dtl.container_id
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_USER_DEF_CHK_1'
                		  , p_content_i      => r_ocr_dtl.user_def_chk_1
                		  );
        		write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_USER_DEF_CHK_2'
                		  , p_content_i      => r_ocr_dtl.user_def_chk_2
                		  );
        		write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_USER_DEF_CHK_3'
                		  , p_content_i      => r_ocr_dtl.user_def_chk_3
                		  );
        		write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LOT_USER_DEF_CHK_4'
                		  , p_content_i      => r_ocr_dtl.user_def_chk_4
                		  );
			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> g_file_name
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Add SIM_LOT_SERIALS'
									   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
													|| '"segment_nr" "'||p_segment_nr_i||'" '
													|| '"is_cont_yn" "'||p_is_cont_yn_i 
									   , p_order_id_i		=> p_order_nr_i
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> l_pallet_id
									   , p_container_id_i		=> l_container_id
									   , p_site_id_i		=> null
									   );
			end if;
			-- Add Serial Numbers
			l_count_snr := 0;
			l_snr_total := null;
			l_snr_line  := null;
			for	r_snr in c_snr( b_client_id    => p_client_id_i
					      , b_order_id     => p_order_nr_i
					      , b_container_id => r_ocr_dtl.container_id
					      , b_sku_id       => r_ocr_dtl.sku_id
					      , b_tag_id       => r_ocr_dtl.tag_id
					      )
			loop
				l_count_snr := l_count_snr + 1;
				l_snr_total := l_snr_line || ', ' || r_snr.serial_number;
				case
				when	length( l_snr_total) > 110
				then
					l_count_snr := 1;
					--
					write_line( p_file_type_i    => p_file_type_i
						  , p_field_prefix_i => p_field_prefix_i
						  , p_field_name_i   => 'SIM_LOT_SERIAL_TOTAL_NRS'
						  , p_content_i      => l_snr_line
						  );
					l_snr_line  := r_snr.serial_number;
					l_snr_total := r_snr.serial_number;
				else
					case	l_count_snr
					when 	1
					then
						l_snr_line  := r_snr.serial_number;
						l_snr_total := r_snr.serial_number;
					else
						l_snr_line  := l_snr_line || ', ' || r_snr.serial_number;
					end case;
				end case;
			end loop; -- SIM LOT SNR loop

			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SIM_LOT_SERIAL_TOTAL_NRS'
				  , p_content_i      => l_snr_line
				  );
		end loop; -- SIM LOT loop

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding SIM_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"is_cont_yn" "'||p_is_cont_yn_i 
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

	end add_sim_lot;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Nov-2016
-- Purpose : Create StreamServe Shipment Items block
------------------------------------------------------------------------------------------------
	procedure add_sim( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 , p_pallet_id_i    in  varchar2 := null
			 , p_container_id_i in  varchar2 := null
			 )
	is
		cursor c_ocr( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    )
		is
			select	rownum
			,      	plt.*
			from   	(
				select	distinct
					smt.client_id
				,      	smt.order_id
				,      	nvl( smt.labelled, g_no)                                            			is_cont_yn
				,      	decode( smt.labelled, g_yes, smt.container_id, smt.pallet_id)				pallet_id
				,      	decode( smt.labelled, g_yes, smt.container_type, smt.pallet_config)			pallet_type
				,      	smt.carrier_consignment_id                                          			tracking_nr
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_weight, smt.pallet_weight), 1))	weight
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_depth, smt.pallet_depth), 1))	length
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_width, smt.pallet_width), 1))	width
				,      	max( nvl( decode( smt.labelled, g_yes, smt.container_height, smt.pallet_height), 1))	height
				,      	max( nvl( decode( smt.labelled, g_yes, round( ( 
					smt.container_depth * smt.container_width * smt.container_height) / 1000000, 6), 
					round( ( smt.pallet_depth * smt.pallet_width * smt.pallet_height) / 1000000, 6)), 1))	volume
				,      	1                                                                   			cnt
				,      	decode( smt.labelled, g_yes, 1, nvl( smt.transport_boxes, 1))				no_of_boxes
				from   	dcsdba.shipping_manifest            							smt
				where  	smt.client_id                       	= b_client_id
				and    	smt.order_id                        	= b_order_id
				and    	smt.pallet_id                       	= nvl( b_pallet_id, smt.pallet_id)
				and    	smt.container_id                    	= nvl( b_container_id, smt.container_id)
				group  
				by 	smt.client_id
				,      	smt.order_id
				,      	nvl( smt.labelled, g_no)
				,      	decode( smt.labelled, g_yes, smt.container_id, smt.pallet_id)
				,      	decode( smt.labelled, g_yes, smt.container_type, smt.pallet_config)
				,      	smt.carrier_consignment_id
				,      	decode( smt.labelled, g_yes, 1, nvl( smt.transport_boxes, 1))
				union  	-- for pallets which are not 'marshalled' yet
				select 	distinct
					ocr.client_id
				,      	ocr.order_id
				,      	nvl( ocr.labelled, g_no)                                            			is_cont_yn
				,      	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id)				pallet_id
				,      	decode( ocr.labelled, g_yes, nvl(to_container_config, container_config), nvl(to_pallet_config, pallet_config)) pallet_type --jira DBS-5398
				,      	ocr.carrier_consignment_id                                          			tracking_nr
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_weight, ocr.pallet_weight), 1))	weight
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_depth, ocr.pallet_depth), 1))	length
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_width, ocr.pallet_width), 1))	width
				,      	max( nvl( decode( ocr.labelled, g_yes, ocr.container_height, ocr.pallet_height), 1))	height
				,      	max( nvl( decode( ocr.labelled, g_yes, round( ( 
					ocr.container_depth * ocr.container_width * ocr.container_height) / 1000000, 6), 
					round( ( ocr.pallet_depth * ocr.pallet_width * ocr.pallet_height) / 1000000, 6)), 1))	volume
				,      	1                                                                   			cnt
				,      	decode( ocr.labelled, g_yes, 1, nvl( ocr.transport_boxes, 1))				no_of_boxes
				from   	dcsdba.order_container              ocr
				,       dcsdba.move_task mt 
				where  	ocr.client_id                       = b_client_id
				and    	ocr.order_id                        = b_order_id
				and    	ocr.pallet_id                       = nvl( b_pallet_id, ocr.pallet_id)
				and    	ocr.container_id                    = nvl( b_container_id, ocr.container_id)
				and     mt.client_id = ocr.client_id 
				AND     mt.task_id = ocr.order_id 
				AND     ocr.container_id = nvl( mt.to_container_id, mt.container_id)
			        AND     ocr.pallet_id    = nvl( mt.to_pallet_id, mt.pallet_id)
				and    	not exists (	select 1
							from   dcsdba.shipping_manifest smt
							where  smt.client_id            = ocr.client_id
							and    smt.order_id             = ocr.order_id
							and    smt.pallet_id            = ocr.pallet_id
							and    smt.container_id         = ocr.container_id
						   )
				group  
				by 	ocr.client_id
				,      	ocr.order_id
				,      	nvl( ocr.labelled, g_no)
				,      	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id)
				,       decode( ocr.labelled, g_yes, nvl(to_container_config, container_config), nvl(to_pallet_config, pallet_config))
				,      	ocr.carrier_consignment_id
				,      	decode( ocr.labelled, g_yes, 1, nvl( ocr.transport_boxes, 1))
				order  
				by 	client_id
				,      	order_id
				,      	pallet_id) 	plt
		;

		--
		cursor c_pcg( b_client_id in varchar2
			    , b_config_id in varchar2
			    )
		is
			select pcg.client_id              config_client_id
			,      pcg.config_id              config_type
			,      pcg.notes                  config_notes
			,      pcg.pallet_type_group      config_group
			,      ptp.notes                  config_group_notes
			,      nvl( pcg.weight, 0)        config_weight
			,      nvl( pcg.depth, 0)         config_length
			,      nvl( pcg.width, 0)         config_width
			,      nvl( pcg.height, 0)        config_height
			,      (nvl( pcg.depth, 0) * nvl( pcg.width, 0) * nvl( pcg.height, 0)) / 1000000                config_volume
			from   dcsdba.pallet_config       pcg
			,      dcsdba.pallet_type_grp     ptp
			where  pcg.pallet_type_group      = ptp.pallet_type_group (+)
			and    (pcg.client_id = ptp.client_id or ptp.client_id is null)
			and    (pcg.client_id = b_client_id or pcg.client_id is null)
			and    	pcg.config_id = b_config_id
			order  
			by 	pcg.client_id	nulls last
		;

		--
		cursor c_haz( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    )
		is
			select	distinct
				replace(hmt.user_def_type_1, ' ', null) un_code
			from   	dcsdba.shipping_manifest      smt
			,      	dcsdba.sku                    sku
		        ,      	dcsdba.hazmat                 hmt    
		        where  	smt.client_id                 = b_client_id
		        and    	smt.order_id                  = b_order_id
		        and    	(smt.container_id = b_pallet_id or smt.pallet_id = b_pallet_id)
		        and    	smt.client_id                 = sku.client_id
		        and    	smt.sku_id                    = sku.sku_id 
		        and    	sku.hazmat_id                 = hmt.hazmat_id
		        and    	hmt.hazmat_id                 like 'RHS%'
		        union	all
		        select 	distinct
				replace(hmt.user_def_type_1, ' ', null) un_code
		        from   	dcsdba.order_container        ocr
		        ,       dcsdba.move_task              mvt
		        ,      	dcsdba.sku                    sku
		        ,      	dcsdba.hazmat                 hmt    
		        where  	ocr.client_id                 = b_client_id
		        and    	ocr.order_id                  = b_order_id
		        and    	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id) = b_pallet_id
		        and    	ocr.client_id                 = mvt.client_id
		        and    	ocr.pallet_id                 = mvt.pallet_id
		        and    	ocr.container_id              = mvt.container_id
			and	mvt.task_id 		     != 'PALLET'
		        and    	mvt.client_id                 = sku.client_id
		        and    	mvt.sku_id                    = sku.sku_id 
		        and    	sku.hazmat_id                 = hmt.hazmat_id
		        and    	hmt.hazmat_id                 like 'RHS%'
		        and    	not exists (	select	1
						from   	dcsdba.shipping_manifest smt
						where  	smt.client_id            = ocr.client_id
						and    	smt.order_id             = ocr.order_id
						and    	smt.pallet_id            = ocr.pallet_id
						and    	smt.container_id         = ocr.container_id
					   )
			order  
			by 	1
		;

		--
		r_ocr         	c_ocr%rowtype;
		r_pcg         	c_pcg%rowtype;
		r_haz         	c_haz%rowtype;

		l_count       	number(10) := 0;
		l_un_cnt      	number(10) := 0;
		l_un_total    	varchar2(3999);
		l_un_line     	varchar2(3999);
		l_rtn		varchar2(30) := 'add_sim';	
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding SIM'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		-- Start looping all pallet id's
		for	r_ocr in c_ocr( b_client_id    => p_client_id_i
				      , b_order_id     => p_order_nr_i
				      , b_pallet_id    => p_pallet_id_i
				      , b_container_id => p_container_id_i
				      )
		loop
			l_count := l_count + 1;
			--
			open	c_pcg( b_client_id => r_ocr.client_id
				     , b_config_id => r_ocr.pallet_type
				     );
			fetch 	c_pcg
			into  	r_pcg;
			close 	c_pcg;
			--
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_SEGMENT_NR'
                		  , p_content_i      => 'Segment SIM: '|| to_char( l_count)
				  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_ID'
                		  , p_content_i      => to_char( l_count)
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_SIL_ID'
                		  , p_content_i      => null
                		  );           
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE'
                		  , p_content_i      => r_pcg.config_type
                		  );             
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE_DESC'
                		  , p_content_i      => r_pcg.config_notes
                		  );        
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_GROUP_TYPE'
                		  , p_content_i      => r_pcg.config_group
                		  );             
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_GROUP_TYPE_DESC'
                		  , p_content_i      => r_pcg.config_group_notes
                		  );
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE_WEIGHT'
                		  , p_content_i      => to_char( r_pcg.config_weight, 'fm999990.90')
                		  );        
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE_LENGTH'
                		  , p_content_i      => to_char( r_pcg.config_length, 'fm999990.90')
                		  );        
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE_WIDTH'
                		  , p_content_i      => to_char( r_pcg.config_width, 'fm999990.90')
                		  );        
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE_HEIGHT'
                		  , p_content_i      => to_char( r_pcg.config_height, 'fm999990.90')
                		  );        
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TYPE_VOLUME'
                		  , p_content_i      => to_char( r_pcg.config_volume, 'fm999990.90')
                		  );        
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_UNIT_IS_CONT_YN'
                		  , p_content_i      => r_ocr.is_cont_yn
                		  );          
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_UNIT_NR'
                		  , p_content_i      => r_ocr.pallet_id
                		  );          
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_UNIT_NR_MASTER'
                		  , p_content_i      => null
                		  );   
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_COLLO_NR'
                		  , p_content_i      => r_ocr.rownum
                		  );         
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_TRACKING_NR'
                		  , p_content_i      => r_ocr.tracking_nr
                		  );      
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_WEIGHT'
                		  , p_content_i      => to_char( r_ocr.weight, 'fm999990.90')
                		  );           
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_LENGTH'
                		  , p_content_i      => to_char( r_ocr.length, 'fm999990.90')
                		  );           
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_WIDTH'
                		  , p_content_i      => to_char( r_ocr.width, 'fm999990.90')
                		  );            
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_HEIGHT'
                		  , p_content_i      => to_char( r_ocr.height, 'fm999990.90')
                		  );           
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_VOLUME'
                		  , p_content_i      => to_char( r_ocr.volume, 'fm999990.90')
                		  );           
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_PIECES'
                		  , p_content_i      => r_ocr.cnt
                		  );           
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_PIECES_PER_ITEM'
                		  , p_content_i      => r_ocr.no_of_boxes
                		  );  
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_DRY_ICE_WEIGHT'
                		  , p_content_i      => null
                		  );   
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_UNDG_NUMBER'
                		  , p_content_i      => null
                		  );      
			write_line( p_file_type_i    => p_file_type_i
                		  , p_field_prefix_i => p_field_prefix_i
                		  , p_field_name_i   => 'SIM_MAIN_DANGER_CLASS'
                		  , p_content_i      => null
                		  );

			-- Write all UN Codes into 1 line
			l_un_cnt    := 0;
			for 	r_haz in c_haz( b_client_id    => r_ocr.client_id
					      , b_order_id     => r_ocr.order_id
					      , b_pallet_id    => r_ocr.pallet_id  -- can be either container or pallet ID
					      )
			loop
				l_un_cnt   := l_un_cnt + 1;
				l_un_total := l_un_line || ', '|| r_haz.un_code;
				case
				when	length( l_un_total) > 110
				then
					l_un_cnt := 1;
					write_line( p_file_type_i    => p_file_type_i
						  , p_field_prefix_i => p_field_prefix_i
						  , p_field_name_i   => 'SIM_TOTAL_UN_CODES'
						  , p_content_i      => l_un_line
						  );
					l_un_line  := r_haz.un_code;
					l_un_total := r_haz.un_code;
				else
					case	l_un_cnt
					when 	1
					then
						l_un_line  := r_haz.un_code;
						l_un_total := r_haz.un_code;
					else
						l_un_line  := l_un_line || ', ' || r_haz.un_code;
					end case;
				end case;
			end loop;
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SIM_TOTAL_UN_CODES'
				  , p_content_i      => l_un_line
				  );

			-- Add SIM LOT lines
		        add_sim_lot( p_file_type_i    => p_file_type_i
				   , p_field_prefix_i => p_field_prefix_i
				   , p_segment_nr_i   => l_count
				   , p_client_id_i    => r_ocr.client_id
				   , p_order_nr_i     => r_ocr.order_id
				   , p_pallet_id_i    => r_ocr.pallet_id
				   , p_container_id_i => p_container_id_i
				   , p_is_cont_yn_i   => r_ocr.is_cont_yn
				   );
		end loop;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding SIM'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;
	end add_sim;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create StreamServe Serial Number block
------------------------------------------------------------------------------------------------
	procedure add_snr( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_segment_nr_i   in  number
			 , p_client_id_i    in  varchar2
			 , p_order_id_i     in  varchar2
			 , p_line_id_i      in  number
			 )
	is
		cursor c_snr( b_client_id in varchar2
			    , b_order_id  in varchar2
			    , b_line_id   in number
			    )
		is
			select	snr.serial_number
			,      	snr.client_id
			,      	snr.sku_id
			,      	snr.order_id
			,      	snr.line_id
			,      	snr.tag_id
		        ,      	snr.original_tag_id
		        ,      	snr.pick_key
		        ,      	snr.old_pick_key
		        ,      	snr.manifest_key
		        ,      	snr.old_manifest_key
		        ,      	snr.status
		        ,      	snr.supplier_id
		        ,      	snr.site_id
		        ,      	snr.receipt_dstamp
		        ,      	snr.picked_dstamp
		        ,      	snr.shipped_dstamp
		        ,      	snr.uploaded
		        ,      	snr.uploaded_ws2pc_id
		        ,      	snr.uploaded_filename
		        ,      	snr.uploaded_dstamp
		        ,      	snr.repacked
		        ,      	snr.created
		        ,      	snr.screen_mode
		        ,     	snr.station_id
		        ,      	snr.receipt_id
		        ,      	snr.receipt_line_id
		        ,      	snr.kit_sku_id
		        ,      	snr.kit_serial_number
		        ,      	snr.alloc_key
		        ,      	snr.pallet_id
		        ,      	snr.container_id
		        ,       snr.old_pallet_id
		        ,      	snr.old_container_id
		        ,      	snr.tag_adjusted
		        ,      	snr.reused
		        from   	dcsdba.serial_number snr
		        where  	snr.client_id        = b_client_id
		        and    	snr.order_id         = b_order_id
		        and    	snr.line_id          = b_line_id
		        order  	
			by 	snr.serial_number
		;

		--
		r_snr1         	c_snr%rowtype;
		r_snr2         	c_snr%rowtype;

		l_total        	varchar2(3999);
		l_line         	varchar2(3999);
		l_user_field_1 	varchar2(1000);
		l_user_field_2 	varchar2(1000);
		l_user_field_3 	varchar2(1000);
		l_user_field_4 	varchar2(1000);
		l_user_field_5 	varchar2(1000);
		l_cnt          	number(10);
		l_rtn		varchar2(30) := 'add_srn';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE_SNR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		-- Write all Serials into lines
		l_cnt   := 0;
		for 	r_snr1 in c_snr( b_client_id => p_client_id_i
				       , b_order_id  => p_order_id_i
				       , b_line_id   => p_line_id_i
				       )
		loop
			l_cnt  	:= l_cnt + 1;
			l_total := l_line || ', ' || r_snr1.serial_number;
			case
			when	length( l_total) > 110
			then
				l_cnt := 1;
				write_line( p_file_type_i    => p_file_type_i
					  , p_field_prefix_i => p_field_prefix_i
					  , p_field_name_i   => 'SERIAL_TOTAL_NRS'
					  , p_content_i      => l_line
					  );
				l_line  := r_snr1.serial_number;
				l_total := r_snr1.serial_number;
			else
				case	l_cnt
				when 	1
				then
					l_line  := r_snr1.serial_number;
					l_total := r_snr1.serial_number;
				else
					l_line  := l_line || ', ' || r_snr1.serial_number;
				end case;
			end case;
		end loop;
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'SERIAL_TOTAL_NRS'
			  , p_content_i      => l_line
			  );

		-- Write separate segments per Serial
		l_cnt  	:= 0;
		for	r_snr2 in c_snr( b_client_id => p_client_id_i
				       , b_order_id  => p_order_id_i
				       , b_line_id   => p_line_id_i
				       )
		loop
			l_cnt := l_cnt + 1;
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SERIAL_SEGMENT_NR'
				  , p_content_i      => 'Segment OLE / Serial: ' || to_char( p_segment_nr_i) || ' / ' || to_char( l_cnt));
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SERIAL_NUMBER'
				  , p_content_i      => r_snr2.serial_number
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SERIAL_TAG_ID'
				  , p_content_i      => r_snr2.tag_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'SERIAL_SKU_ID'
				  , p_content_i      => r_snr2.sku_id
				  );
		end loop;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding OLE_SNR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

	exception
		when	others
		then
			case
			when	c_snr%isopen
			then
			close	c_snr;
			else
				null;
			end case;

	end add_snr;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Nov-2016
-- Purpose : Create StreamServe Lot block
------------------------------------------------------------------------------------------------
	procedure add_lot( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_segment_nr_i   in  number
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 , p_line_id_i      in  number
			 , p_pallet_id_i    in  varchar2 := null
			 , p_container_id_i in  varchar2 := null
			 )
	is
		cursor c_lot( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_line_id      in number
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    )
		is
			select	rowid
			,	smt.tag_id
			,      	smt.sku_id
			,      	smt.batch_id
			,      	smt.expiry_dstamp
			,      	smt.origin_id
			,      	smt.qty_shipped               qty
			,      	smt.container_id
			,      	smt.pallet_id
			,      	smt.condition_id
			,      	smt.receipt_dstamp
			,      	smt.manuf_dstamp
			,      	smt.receipt_id
			,	nvl(smt.user_def_chk_1,'N') user_def_chk_1
			,	nvl(smt.user_def_chk_2,'N') user_def_chk_2
			,	nvl(smt.user_def_chk_3,'N') user_def_chk_3
			,	nvl(smt.user_def_chk_4,'N') user_def_chk_4
			from   	dcsdba.shipping_manifest      smt
			where  	smt.client_id                 = b_client_id
			and    	smt.order_id                  = b_order_id
			and    	smt.line_id                   = b_line_id
			and    	(smt.pallet_id = b_pallet_id or b_pallet_id is null)
			and    	(smt.container_id = b_container_id or b_container_id is null)
			union  	-- For pallets which are not 'Marshalled' yet (deleted)
			select 	rowid
			,	mtk.tag_id
			,      	mtk.sku_id
			,      	(select i.batch_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) batch_id
			,      	(select i.expiry_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) expiry_dstamp
			,      	(select i.origin_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) origin_id
			,      	mtk.qty_to_move               qty
			,      	mtk.container_id
			,      	mtk.pallet_id
			,      	(select i.condition_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) condition_id
			,      	(select i.receipt_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_dstamp
			,      	(select i.manuf_dstamp from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) manuf_dstamp
			,      	(select i.receipt_id from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) receipt_id
			,      	(select nvl(i.user_def_chk_1,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_1
			,      	(select nvl(i.user_def_chk_2,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_2
			,      	(select nvl(i.user_def_chk_3,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_3
			,      	(select nvl(i.user_def_chk_4,'N') from dcsdba.inventory i where i.sku_id = mtk.sku_id and i.tag_id = mtk.tag_id and i.client_id = mtk.client_id and i.site_id = mtk.site_id and rownum = 1) user_def_chk_4
			from   	dcsdba.move_task              	mtk
			where  	mtk.client_id                 	= b_client_id
			and    	mtk.task_id                   	= b_order_id
			and    	mtk.line_id                   	= b_line_id
			and 	( 
					(	nvl( mtk.pallet_id, '@#') 	= nvl( null, nvl( mtk.pallet_id, '@#'))
					and 	nvl( mtk.container_id, '@#') 	= nvl( null, nvl( mtk.container_id, '@#'))
					)
				or
					( 	nvl( mtk.to_pallet_id, '@#') 	= nvl( null, nvl( mtk.to_pallet_id, '@#'))
					and 	nvl( mtk.to_container_id, '@#') = nvl( null, nvl( mtk.to_container_id, '@#'))
					)
				)
			and    	not exists (	select	1
						from   	dcsdba.shipping_manifest smt
						where  	smt.client_id            = b_client_id
						and    	smt.order_id             = b_order_id
						and    	smt.line_id              = b_line_id
						and    	smt.pallet_id            = mtk.pallet_id
					   )
			order  
			by 	container_id	
			,      	tag_id
		;

		--
		r_lot  	c_lot%rowtype;
		l_count number(10) := 0;
		l_rtn	varchar2(30) := 'add_lot';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		for	r_lot in c_lot( b_client_id    => p_client_id_i
				      , b_order_id     => p_order_nr_i
				      , b_line_id      => p_line_id_i
				      , b_pallet_id    => p_pallet_id_i
				      , b_container_id => p_container_id_i
				      )
		loop
			l_count := l_count +1;
			--
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_SEGMENT_NR'
				  , p_content_i      => 'Segment OLE / Lot: '|| to_char( p_segment_nr_i)|| ' / '|| to_char( l_count));
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_TAG_ID'
				  , p_content_i      => r_lot.tag_id
				  );      
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_SKU_ID'
				  , p_content_i      => r_lot.sku_id
				  );                
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_BATCH_ID'
				  , p_content_i      => r_lot.batch_id
				  );              
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_EXPIRY_DATE'
				  , p_content_i      => to_char( r_lot.expiry_dstamp, 'DD-MM-YYYY')
				  );           
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_EXPIRY_TIME'
				  , p_content_i      => to_char( r_lot.expiry_dstamp, 'HH24:MI:SS')
				  );           
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_RECEIPT_ID'
				  , p_content_i      => r_lot.receipt_id
				  );            
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_RECEIPT_DATE'
				  , p_content_i      => to_char( r_lot.receipt_dstamp, 'DD-MM-YYYY')
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_RECEIPT_TIME'
				  , p_content_i      => to_char( r_lot.receipt_dstamp, 'HH24:MI:SS')
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_MANUF_DATE'
				  , p_content_i      => to_char( r_lot.manuf_dstamp, 'DD-MM-YYYY')
				  );            
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_MANUF_TIME'
				  , p_content_i      => to_char( r_lot.manuf_dstamp, 'HH24:MI:SS')
				  );            
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_ORIGIN_ID'
				  , p_content_i      => r_lot.origin_id
				  );             
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_CONDITION_ID'
				  , p_content_i      => r_lot.condition_id
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_QTY_UPDATE'
				  , p_content_i      => r_lot.qty
				  );            
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_CONTAINER_ID'
				  , p_content_i      => r_lot.container_id
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_USER_DEF_CHK_1'
				  , p_content_i      => r_lot.user_def_chk_1
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_USER_DEF_CHK_2'
				  , p_content_i      => r_lot.user_def_chk_2
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_USER_DEF_CHK_3'
				  , p_content_i      => r_lot.user_def_chk_3
				  );          
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'LOT_USER_DEF_CHK_4'
				  , p_content_i      => r_lot.user_def_chk_4
				  );          
		end loop;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding OLE_LOT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;
	end add_lot;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 27-Sep-2016
-- Purpose : Create StreamServe Order Lines block
------------------------------------------------------------------------------------------------
	procedure add_ole( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_segment_nr_i   in  number
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 , p_line_id_i      in  number   := null
			 , p_pallet_id_i    in  varchar2 := null
			 , p_container_id_i in  varchar2 := null
			 )
	is
		-- Fetch order line details
		cursor c_ole( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_line_id      in number
			    )
		is
			select	ole.*
			from    dcsdba.order_line ole
			where  	ole.client_id      = b_client_id
			and    	ole.order_id       = b_order_id
			and    	ole.line_id        = nvl( b_line_id, ole.line_id)
			order  
			by 	ole.line_id 
		;

		-- Fetch SKU details
		cursor c_sku( b_client_id in varchar2
			    , b_sku_id    in varchar2
			    )
		is
			select 	sku.*
			from   	dcsdba.sku sku
			where  	sku.client_id = b_client_id
			and    	sku.sku_id    = b_sku_id
		;

		-- Fetch hazmat details
		cursor c_haz( b_hazmat_id in varchar2)
		is
			select	hmt.hazmat_id
			,      	hhs.hazmat_class
			,      	hmt.metapack
			,      	hmt.notes
			,      	replace(hmt.user_def_type_1, ' ', null) user_def_type_1
			,      	hmt.user_def_type_2
			,      	hmt.user_def_type_3
			,      	hmt.user_def_type_4
			,      	hmt.user_def_type_5
			,      	hmt.user_def_type_6
			,     	hmt.user_def_type_7
			,      	hmt.user_def_type_8
			,      	hmt.user_def_chk_1
			,      	hmt.user_def_chk_2
			,      	hmt.user_def_chk_3
			,      	hmt.user_def_chk_4
			,      	hmt.user_def_date_1
			,      	hmt.user_def_date_2
			,      	hmt.user_def_date_3
			,      	hmt.user_def_date_4
			,      	hmt.user_def_num_1
			,      	hmt.user_def_num_2
			,      	hmt.user_def_num_3
			,      	hmt.user_def_num_4
			,      	hmt.user_def_note_1
			,      	hmt.user_def_note_2
			,      	decode(upper(replace(hmt.user_def_type_4,',',null)), '965II', 1, '9651B', 2, '966II', 3, '967II', 4, '968II', 5, '9681B', 6, '969II', 7, '970II', 8, 0) lithium_check_box
			from   	dcsdba.hazmat              hmt
			,      	dcsdba.hazmat_hazmat_class hhs
			where  	hmt.hazmat_id              = b_hazmat_id
			and    	hmt.hazmat_id              = hhs.hazmat_id (+)
		;

		-- Fetch hazmat regulation details
		cursor c_sha( b_hazmat_id in varchar2
			    , b_client_id in varchar2
			    , b_sku_id    in varchar2
			    )
		is
			select 	sha.regulation_id
			,      	sha.hazmat_class
			,      	sha.hazmat_subclass
			,      	sha.classification_code
			,	sha.un_packing_group
			, 	sha.hazmat_labels
			,	sha.transport_category
			,	sha.marine_pollutant
			,	sha.mfag
			,	sha.ems
			,	sha.hazmat_net_weight
			,	sha.hazmat_net_volume
			,	sha.hazmat_net_volume_unit
			,	sha.hazmat_flashpoint
			,	sha.flashpoint_category
			,	sha.wgk_class
			,	sha.ghs_symbol
			,	sha.limited_qty
			,	sha.r_sentence_code
			,	sha.r_sentence_group
			,	sha.r_sentence
			,	sha.proper_shipping_name
			,	sha.additional_shipping_name
			,	sha.un_packaging_code
			,	sha.water_endangerment_class
			,	sha.language
			,	sha.tunnel_code
			from	dcsdba.sku_hazmat_reg sha
			where	sha.sku_id 	= b_sku_id
			and	sha.client_id 	= b_client_id
			and	( sha.hazmat_id  	= b_hazmat_id or sha.hazmat_id is null)
		;

		-- select hazmat regulations description
		cursor c_hrn( b_regulation_id in varchar2)
		is
			select	hrn.notes
			from	dcsdba.hazmat_regulation hrn
			where	hrn.regulation_id = b_regulation_id
		;

		--
		r_ole     	c_ole%rowtype;
		r_sku     	c_sku%rowtype;
		r_haz     	c_haz%rowtype;
		r_sha		c_sha%rowtype;	
		r_hrn		c_hrn%rowtype;	

		l_counter	number := 1;
		l_rtn		varchar2(30) := 'add_ole';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		open  	c_ole( b_client_id => p_client_id_i
			     , b_order_id  => p_order_nr_i
			     , b_line_id   => p_line_id_i
			     );
		fetch 	c_ole
		into  	r_ole;
		--
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SEGMENT_NR'
			  , p_content_i      => 'Segment OLE: ' || to_char( p_segment_nr_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_LINE_ID'
			  , p_content_i      => to_char( r_ole.line_id)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_HOST_LINE_ID'
			  , p_content_i      => r_ole.host_line_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_HOST_ORDER_ID'
			  , p_content_i      => r_ole.host_order_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_ID'
			  , p_content_i      => r_ole.sku_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CUSTOMER_SKU_ID'
			  , p_content_i      => r_ole.customer_sku_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CONFIG_ID'
			  , p_content_i      => r_ole.config_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_TRACKING_LEVEL'
			  , p_content_i      => r_ole.tracking_level
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_BATCH_ID'
			  , p_content_i      => r_ole.batch_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_BATCH_MIXING'
			  , p_content_i      => r_ole.batch_mixing
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_BATCH_ID_SET'
			  , p_content_i      => r_ole.batch_id_set
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SHELF_LIFE_DAYS'
			  , p_content_i      => to_char( r_ole.shelf_life_days)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SHELF_LIFE_PERCENT'
			  , p_content_i      => to_char( r_ole.shelf_life_percent)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_ORIGIN_ID'
			  , p_content_i      => r_ole.origin_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CONDITION_ID'
			  , p_content_i      => r_ole.condition_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_LOCK_CODE'
			  , p_content_i      => r_ole.lock_code
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SPEC_CODE'
			  , p_content_i      => r_ole.spec_code
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_QTY_ORDERED'
			  , p_content_i      => to_char( r_ole.qty_ordered)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_QTY_TASKED'
			  , p_content_i      => to_char( r_ole.qty_tasked)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_QTY_PICKED'
			  , p_content_i      => to_char( r_ole.qty_picked)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_QTY_SHIPPED'
			  , p_content_i      => to_char( r_ole.qty_shipped)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_QTY_DELIVERED'
			  , p_content_i      => to_char( r_ole.qty_delivered)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_ALLOCATE'
			  , p_content_i      => r_ole.allocate
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_BACK_ORDERED'
			  , p_content_i      => r_ole.back_ordered
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_KIT_SPLIT'
			  , p_content_i      => r_ole.kit_split
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_DEALLOCATE'
			  , p_content_i      => r_ole.deallocate
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_DISALLOW_MERGE_RULES'
			  , p_content_i      => r_ole.disallow_merge_rules
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_RULE_ID'
			  , p_content_i      => r_ole.rule_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_LINE_VALUE'
			  , p_content_i      => to_char( r_ole.line_value, 'fm999999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_LINE_VALUE_USER_DEF'
			  , p_content_i      => r_ole.line_value_user_def
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_NOTES'
			  , p_content_i      => r_ole.notes
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_PSFT_INT_LINE'
			  , p_content_i      => to_char( r_ole.psft_int_line)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_PSFT_SCHD_LINE'
			  , p_content_i      => to_char( r_ole.psft_schd_line)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_PSFT_DMND_LINE'
			  , p_content_i      => to_char( r_ole.psft_dmnd_line)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SAP_PICK_REQ'
			  , p_content_i      => r_ole.sap_pick_req
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_ALLOC_SESSION_ID'
			  , p_content_i      => to_char( r_ole.alloc_session_id)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_ALLOC_STATUS'
			  , p_content_i      => r_ole.alloc_status
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_ORIGINAL_LINE_ID'
			  , p_content_i      => to_char( r_ole.original_line_id)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CONVERSION_FACOR'
			  , p_content_i      => to_char( r_ole.conversion_factor)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SUBSTITUTE_FLAG'
			  , p_content_i      => r_ole.substitute_flag
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_TASK_PER_EACH'
			  , p_content_i      => r_ole.task_per_each
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CATCH_WEIGHT'
			  , p_content_i      => to_char( r_ole.catch_weight)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USE_PICK_TO_GRID'
			  , p_content_i      => r_ole.use_pick_to_grid
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_IGNORE_WEIGHT_CAPTURE'
			  , p_content_i      => r_ole.ignore_weight_capture
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_STAGE_ROUTE_ID'
			  , p_content_i      => r_ole.stage_route_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_QTY_SUBSTITUTED'
			  , p_content_i      => to_char( r_ole.qty_substituted)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_MIN_QTY_ORDERED'
			  , p_content_i      => to_char( r_ole.min_qty_ordered)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_MAX_QTY_ORDERED'
			  , p_content_i      => to_char( r_ole.max_qty_ordered)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_EXPECTED_VOLUME'
			  , p_content_i      => to_char( r_ole.expected_volume, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_EXPECTED_WEIGHT'
			  , p_content_i      => to_char( r_ole.expected_weight, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_EXPECTED_VALUE'
			  , p_content_i      => to_char( r_ole.expected_value, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CUSTOMER_SKU_DESC1'
			  , p_content_i      => r_ole.customer_sku_desc1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CUSTOMER_SKU_DESC2'
			  , p_content_i      => r_ole.customer_sku_desc2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_PURCHASE_ORDER'
			  , p_content_i      => r_ole.purchase_order
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_PRODUCT_PRICE'
			  , p_content_i      => to_char( r_ole.product_price, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_PRODUCT_CURRENCY'
			  , p_content_i      => r_ole.product_currency
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_DOCUMENTATION_UNIT'
			  , p_content_i      => r_ole.documentation_unit
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_EXTENDED_PRICE'
			  , p_content_i      => to_char( r_ole.extended_price, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_TAX_1'
			  , p_content_i      => to_char( r_ole.tax_1, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_TAX_2'
			  , p_content_i      => to_char( r_ole.tax_2, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_DOCUMENTATION_TEXT_1'
			  , p_content_i      => r_ole.documentation_text_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SERIAL_NUMBER'
			  , p_content_i      => r_ole.serial_number
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_OWNER_ID'
			  , p_content_i      => r_ole.owner_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CE_RECEIPT_TYPE'
			  , p_content_i      => r_ole.ce_receipt_type
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_CE_COO'
			  , p_content_i      => r_ole.ce_coo
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_V_SN_SCAN'
			  , p_content_i      => null --r_ole.v_sn_scan
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_CHK_1'
			  , p_content_i      => r_ole.user_def_chk_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_CHK_2'
			  , p_content_i      => r_ole.user_def_chk_2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_CHK_3'
			  , p_content_i      => r_ole.user_def_chk_3
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_CHK_4'
			  , p_content_i      => r_ole.user_def_chk_4
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_DATE_1'
			  , p_content_i      => to_char( r_ole.user_def_date_1, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TIME_1'
			  , p_content_i      => to_char( r_ole.user_def_date_1, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_DATE_2'
			  , p_content_i      => to_char( r_ole.user_def_date_2, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TIME_2'
			  , p_content_i      => to_char( r_ole.user_def_date_2, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_DATE_3'
			  , p_content_i      => to_char( r_ole.user_def_date_3, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TIME_3'
			  , p_content_i      => to_char( r_ole.user_def_date_3, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_DATE_4'
			  , p_content_i      => to_char( r_ole.user_def_date_4, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TIME_4'
			  , p_content_i      => to_char( r_ole.user_def_date_4, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_NUM_1'
			  , p_content_i      => to_char( r_ole.user_def_num_1, 'fm999999990.9999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_NUM_2'
			  , p_content_i      => to_char( r_ole.user_def_num_2, 'fm999999990.9999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_NUM_3'
			  , p_content_i      => to_char( r_ole.user_def_num_3, 'fm999999990.9999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_NUM_4'
			  , p_content_i      => to_char( r_ole.user_def_num_4, 'fm999999990.9999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_NOTE_1'
			  , p_content_i      => r_ole.user_def_note_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_NOTE_2'
			  , p_content_i      => r_ole.user_def_note_2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_1'
			  , p_content_i      => r_ole.user_def_type_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_2'
			  , p_content_i      => r_ole.user_def_type_2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_3'
			  , p_content_i      => r_ole.user_def_type_3
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_4'
			  , p_content_i      => r_ole.user_def_type_4
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_5'
			  , p_content_i      => r_ole.user_def_type_5
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_6'
			  , p_content_i      => r_ole.user_def_type_6
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_7'
			  , p_content_i      => r_ole.user_def_type_7
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_USER_DEF_TYPE_8'
			  , p_content_i      => r_sku.user_def_type_8
			  );
		-- new fields after 2009
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_KIT_PLAN_ID'
			  , p_content_i      => r_ole.kit_plan_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_MASTER_ORDER_ID'
			  , p_content_i      => r_ole.master_order_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_MASTER_ORDER_LINE_ID'
			  , p_content_i      => r_ole.master_order_line_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_TM_SHIP_LINE_ID'
			  , p_content_i      => r_ole.tm_ship_line_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SOFT_ALLOCATED'
			  , p_content_i      => r_ole.soft_allocated
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_LOCATION_ID'
			  , p_content_i      => r_ole.location_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_UNALLOCATABLE'
			  , p_content_i      => r_ole.unallocatable
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_MIN_FULL_PALLET_PERC'
			  , p_content_i      => r_ole.min_full_pallet_perc
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_MAX_FULL_PALLET_PERC'
			  , p_content_i      => r_ole.max_full_pallet_perc
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_FULL_TRACKING_LEVEL_ONLY'
			  , p_content_i      => r_ole.full_tracking_level_only
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SUBSTITUTE_GRADE'
			  , p_content_i      => r_ole.substitute_grade
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_DISALLOW_SUBSTITUTION'
			  , p_content_i      => r_ole.disallow_substitution
 			  );

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OLE_SKU'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
												|| '"sku_id" "'||r_ole.sku_id||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		-- Add SKU lines for the current Order Line
		open  	c_sku( b_client_id => r_ole.client_id
			     , b_sku_id    => r_ole.sku_id
			     );
		fetch	c_sku
		into	r_sku;
		--
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EAN'
			  , p_content_i      => r_sku.ean
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UPC'
			  , p_content_i      => r_sku.upc
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DESC'
			  , p_content_i      => r_sku.description
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PRODUCT_GROUP'
			  , p_content_i      => r_sku.product_group
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_HEIGHT'
			  , p_content_i      => to_char( r_sku.each_height, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_WEIGHT'
			  , p_content_i      => to_char( r_sku.each_weight, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_VOLUME'
			  , p_content_i      => to_char( r_sku.each_volume, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_VALUE'
			  , p_content_i      => to_char( r_sku.each_value, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_QTY'
			  , p_content_i      => to_char( r_sku.each_quantity, 'fm999990.90')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_QC_STATUS'
			  , p_content_i      => r_sku.qc_status
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SHELF_LIFE'
			  , p_content_i      => to_char( r_sku.shelf_life)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_QC_FREQUENCY'
			  , p_content_i      => to_char( r_sku.qc_frequency)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_QC_REC_COUNT'
			  , p_content_i      => to_char( r_sku.qc_rec_count)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SPLIT_LOWEST'
			  , p_content_i      => r_sku.split_lowest
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CONDITION_REQD'
			  , p_content_i      => r_sku.condition_reqd
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EXPIRY_REQD'
			  , p_content_i      => r_sku.expiry_reqd
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_ORIGIN_REQD'
			  , p_content_i      => r_sku.origin_reqd
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_PACK'
			  , p_content_i      => r_sku.serial_at_pack
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_PICK'
			  , p_content_i      => r_sku.serial_at_pick
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_RECEIPT'
			  , p_content_i      => r_sku.serial_at_receipt
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_RANGE'
			  , p_content_i      => r_sku.serial_range
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_FORMAT'
			  , p_content_i      => r_sku.serial_format
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_VALID_MERGE'
			  , p_content_i      => r_sku.serial_valid_merge
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_NO_REUSE'
			  , p_content_i      => r_sku.serial_no_reuse
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PICK_SEQUENCE'
			  , p_content_i      => to_char( r_sku.pick_sequence)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PICK_COUNT_QTY'
			  , p_content_i      => to_char( r_sku.pick_count_qty)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COUNT_FREQUENCY'
			  , p_content_i      => to_char( r_sku.count_frequency)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COUNT_DSTAMP'
			  , p_content_i      => to_char( r_sku.count_dstamp, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COUNT_DSTAMP_TIME'
			  , p_content_i      => to_char( r_sku.count_dstamp, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COUNT_LIST_ID'
			  , p_content_i      => r_sku.count_list_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_OAP_WIP_ENABLED'
			  , p_content_i      => r_sku.oap_wip_enabled
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_KIT_SKU'
			  , p_content_i      => r_sku.kit_sku
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_KIT_SPLIT'
			  , p_content_i      => r_sku.kit_split
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_KIT_TRIGGER_QTY'
			  , p_content_i      => to_char( r_sku.kit_trigger_qty)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_KIT_QTY_DUE'
			  , p_content_i      => to_char( r_sku.kit_qty_due)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_KITTING_LOC_ID'
			  , p_content_i      => r_sku.kitting_loc_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_ALLOCATION_GROUP'
			  , p_content_i      => r_sku.allocation_group
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PUTAWAY_GROUP'
			  , p_content_i      => r_sku.putaway_group
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_ABC_DISABLE'
			  , p_content_i      => r_sku.abc_disable
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_HANDLING_CLASS'
			  , p_content_i      => r_sku.handling_class
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_OBSOLETE_PRODUCT'
			  , p_content_i      => r_sku.obsolete_product
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_NEW_PRODUCT'
			  , p_content_i      => r_sku.new_product
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DISALLOW_UPLOAD'
			  , p_content_i      => r_sku.disallow_upload
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DISALLOW_CROSS_DOCK'
			  , p_content_i      => r_sku.disallow_cross_dock
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_MANUF_DSTAMP_REQD'
			  , p_content_i      => r_sku.manuf_dstamp_reqd
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_MANUF_DSTAMP_DFLT'
			  , p_content_i      => r_sku.manuf_dstamp_dflt
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_MIN_SHELF_LIFE'
			  , p_content_i      => to_char( r_sku.min_shelf_life)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COLOUR'
			  , p_content_i      => r_sku.colour
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SKU_SIZE'
			  , p_content_i      => r_sku.sku_size
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_HAZMAT'
			  , p_content_i      => r_sku.hazmat
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_HAZMAT_ID'
			  , p_content_i      => r_sku.hazmat_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SHIP_SHELF_LIFE'
			  , p_content_i      => to_char( r_sku.ship_shelf_life)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_NMFC_NUMBER'
			  , p_content_i      => to_char( r_sku.nmfc_number)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_INCUB_RULE'
			  , p_content_i      => r_sku.incub_rule
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_INCUB_HOURS'
			  , p_content_i      => to_char( r_sku.incub_hours)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_WIDTH'
			  , p_content_i      => to_char( r_sku.each_width, 'fm999990.900')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EACH_DEPTH'
			  , p_content_i      => to_char( r_sku.each_depth, 'fm999990.900')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_REORDER_TRIGGER_QTY'
			  , p_content_i      => to_char( r_sku.reorder_trigger_qty)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_LOW_TRIGGER_QTY'
			  , p_content_i      => to_char( r_sku.low_trigger_qty)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DISALLOW_MERGE_RULES'
			  , p_content_i      => r_sku.disallow_merge_rules
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PACK_DESPATCH_REPACK'
			  , p_content_i      => r_sku.pack_despatch_repack
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SPEC_ID'
			  , p_content_i      => r_sku.spec_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_BEAM_UNITS'
			  , p_content_i      => to_char( r_sku.beam_units)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_1'
			  , p_content_i      => r_sku.user_def_type_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_2'
			  , p_content_i      => r_sku.user_def_type_2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_3'
			  , p_content_i      => r_sku.user_def_type_3
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_4'
			  , p_content_i      => r_sku.user_def_type_4
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_5'
			  , p_content_i      => r_sku.user_def_type_5
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_6'
			  , p_content_i      => r_sku.user_def_type_6
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_7'
			  , p_content_i      => r_sku.user_def_type_7
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TYPE_8'
			  , p_content_i      => r_sku.user_def_type_8
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_CHK_1'
			  , p_content_i      => r_sku.user_def_chk_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_CHK_2'
			  , p_content_i      => r_sku.user_def_chk_2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_CHK_3'
			  , p_content_i      => r_sku.user_def_chk_3
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_CHK_4'
			  , p_content_i      => r_sku.user_def_chk_4
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_DATE_1'
			  , p_content_i      => to_char( r_sku.user_def_date_1, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TIME_1'
			  , p_content_i      => to_char( r_sku.user_def_date_1, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_DATE_2'
			  , p_content_i      => to_char( r_sku.user_def_date_2, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TIME_2'
			  , p_content_i      => to_char( r_sku.user_def_date_2, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_DATE_3'
			  , p_content_i      => to_char( r_sku.user_def_date_3, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TIME_3'
			  , p_content_i      => to_char( r_sku.user_def_date_3, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_DATE_4'
			  , p_content_i      => to_char( r_sku.user_def_date_4, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_TIME_4'
			  , p_content_i      => to_char( r_sku.user_def_date_4, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_NUM_1'
			  , p_content_i      => to_char( r_sku.user_def_num_1, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_NUM_2'
			  , p_content_i      => to_char( r_sku.user_def_num_2, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_NUM_3'
			  , p_content_i      => to_char( r_sku.user_def_num_3, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_NUM_4'
			  , p_content_i      => to_char( r_sku.user_def_num_4, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_NOTE_1'
			  , p_content_i      => r_sku.user_def_note_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_USER_DEF_NOTE_2'
			  , p_content_i      => r_sku.user_def_note_2
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_WAREHOUSE_TYPE'
			  , p_content_i      => r_sku.ce_warehouse_type
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_CUSTOMS_EXCISE'
			  , p_content_i      => r_sku.ce_customs_excise
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_STANDARD_COST'
			  , p_content_i      => to_char( r_sku.ce_standard_cost)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_STANDARD_CURRENCY'
			  , p_content_i      => r_sku.ce_standard_currency
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COUNT_LIST_ID_1'
			  , p_content_i      => r_sku.count_list_id_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DISALLOW_CLUSTERING'
			  , p_content_i      => r_sku.disallow_clustering
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_MAX_STACK'
			  , p_content_i      => to_char( r_sku.max_stack)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_STACK_DESCRIPTION'
			  , p_content_i      => r_sku.stack_description
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_STACK_LIMITATION'
			  , p_content_i      => to_char( r_sku.stack_limitation)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_DUTY_STAMP'
			  , p_content_i      => r_sku.ce_duty_stamp
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CAPTURE_WEIGHT'
			  , p_content_i      => r_sku.capture_weight
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_WEIGH_AT_RECEIPT'
			  , p_content_i      => r_sku.weigh_at_receipt
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UPPER_WEIGHT_TOLERANCE'
			  , p_content_i      => to_char( r_sku.upper_weight_tolerance)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_LOWER_WEIGHT_TOLERANCE'
			  , p_content_i      => to_char( r_sku.lower_weight_tolerance)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_LOADING'
			  , p_content_i      => r_sku.serial_at_loading
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_KITTING'
			  , p_content_i      => r_sku.serial_at_kitting
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_UNKITTING'
			  , p_content_i      => r_sku.serial_at_unkitting
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_ALLOCALG_LOCKING_STN'
			  , p_content_i      => r_sku.allocalg_locking_stn
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PUTALG_LOCKING_STN'
			  , p_content_i      => r_sku.putalg_locking_stn
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_COMMODITY_CODE'
			  , p_content_i      => r_sku.ce_commodity_code
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_COO'
			  , p_content_i      => r_sku.ce_coo
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_CWC'
			  , p_content_i      => r_sku.ce_cwc
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_VAT_CODE'
			  , p_content_i      => r_sku.ce_vat_code
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_PRODUCT_TYPE'
			  , p_content_i      => r_sku.ce_product_type
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COMMODITY_CODE'
			  , p_content_i      => r_sku.commodity_code
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_COMMODITY_DESC'
			  , p_content_i      => r_sku.commodity_desc
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_FAMILY_GROUP'
			  , p_content_i      => r_sku.family_group
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_BREAKPACK'
			  , p_content_i      => r_sku.breakpack
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CLEARABLE'
			  , p_content_i      => r_sku.clearable
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_STAGE_ROUTE_ID'
			  , p_content_i      => r_sku.stage_route_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_DYNAMIC_RANGE'
			  , p_content_i      => r_sku.serial_dynamic_range
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_MAX_RANGE'
			  , p_content_i      => to_char( r_sku.serial_max_range)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_MANUFACTURE_AT_REPACK'
			  , p_content_i      => r_sku.manufacture_at_repack
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_EXPIRY_AT_REPACK'
			  , p_content_i      => r_sku.expiry_at_repack
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UDF_AT_REPACK'
			  , p_content_i      => r_sku.udf_at_repack
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_REPACK_BY_PIECE'
			  , p_content_i      => r_sku.repack_by_piece
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PACKED_HEIGHT'
			  , p_content_i      => to_char( r_sku.packed_height, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PACKED_WIDTH'
			  , p_content_i      => to_char( r_sku.packed_width, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PACKED_DEPTH'
			  , p_content_i      => to_char( r_sku.packed_depth, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PACKED_VOLUME'
			  , p_content_i      => to_char( r_sku.packed_volume, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PACKED_WEIGHT'
			  , p_content_i      => to_char( r_sku.packed_weight, 'fm999990.900000')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_TWO_MAN_LIFT'
			  , p_content_i      => r_sku.two_man_lift
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_AWKWARD'
			  , p_content_i      => r_sku.awkward
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DECATALOGUED'
			  , p_content_i      => r_sku.decatalogued
			  );
		-- new fields after 2009
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_STOCK_CHECK_RULE_ID'
			  , p_content_i      => r_sku.stock_check_rule_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UNKITTING_INHERIT'
			  , p_content_i      => r_sku.unkitting_inherit
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_STOCK_CHECK'
			  , p_content_i      => r_sku.serial_at_stock_check
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_AT_STOCK_ADJUST'
			  , p_content_i      => r_sku.serial_at_stock_adjust
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_KIT_SHIP_COMPONENTS'
			  , p_content_i      => r_sku.kit_ship_components
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UNALLOCATABLE'
			  , p_content_i      => r_sku.unallocatable
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_BATCH_AT_KITTING'
			  , p_content_i      => r_sku.batch_at_kitting
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_BATCH_ID_GENERATION_ALG'
			  , p_content_i      => r_sku.batch_id_generation_alg
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_VMI_ALLOW_ALLOCATION'
			  , p_content_i      => r_sku.vmi_allow_allocation
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_VMI_ALLOW_REPLENISH'
			  , p_content_i      => r_sku.vmi_allow_replenish
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_VMI_ALLOW_MANUAL'
			  , p_content_i      => r_sku.vmi_allow_manual
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_VMI_ALLOW_INTERFACED'
			  , p_content_i      => r_sku.vmi_allow_interfaced
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_VMI_OVERSTOCK_QTY'
			  , p_content_i      => r_sku.vmi_overstock_qty
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_VMI_AGING_DAYS'
			  , p_content_i      => r_sku.vmi_aging_days
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			   , p_field_name_i   => 'OLE_SKU_SCRAP_ON_RETURN'
			   , p_content_i      => r_sku.scrap_on_return
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_HARMONISED_PRODUCT_CODE'
			  , p_content_i      => r_sku.harmonised_product_code
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_TAG_MERGE'
			  , p_content_i      => r_sku.tag_merge
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UPLOADED'
			  , p_content_i      => r_sku.uploaded
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UPLOADED_WS2PC_ID'
			  , p_content_i      => r_sku.uploaded_ws2pc_id
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UPLOADED_FILENAME'
			  , p_content_i      => r_sku.uploaded_filename
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_UPLOADED_DSTAMP'
			  , p_content_i      => r_sku.uploaded_dstamp
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CARRIER_PALLET_MIXING'
			  , p_content_i      => r_sku.carrier_pallet_mixing
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SPECIAL_CONTAINER_TYPE'
			  , p_content_i      => r_sku.special_container_type
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_DISALLOW_RDT_OVER_PICKING'
			  , p_content_i      => r_sku.disallow_rdt_over_picking
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_NO_ALLOC_BACK_ORDER'
			  , p_content_i      => r_sku.no_alloc_back_order
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_RETURN_MIN_SHELF_LIFE'
			  , p_content_i      => r_sku.return_min_shelf_life
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_WEIGH_AT_GRID_PICK'
			  , p_content_i      => r_sku.weigh_at_grid_pick
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_EXCISE_PRODUCT_CODE'
			  , p_content_i      => r_sku.ce_excise_product_code
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_DEGREE_PLATO'
			  , p_content_i      => r_sku.ce_degree_plato
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_DESIGNATION_ORIGIN'
			  , p_content_i      => r_sku.ce_designation_origin
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_DENSITY'
			  , p_content_i      => r_sku.ce_density
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_BRAND_NAME'
			  , p_content_i      => r_sku.ce_brand_name
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_ALCOHOLIC_STRENGTH'
			  , p_content_i      => r_sku.ce_alcoholic_strength
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_FISCAL_MARK'
			  , p_content_i      => r_sku.ce_fiscal_mark
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_SIZE_OF_PRODUCER'
			  , p_content_i      => r_sku.ce_size_of_producer
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CE_COMMERCIAL_DESC'
			  , p_content_i      => r_sku.ce_commercial_desc
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_NO_OUTBOUND'
			  , p_content_i      => r_sku.serial_no_outbound
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_FULL_PALLET_AT_RECEIPT'
			  , p_content_i      => r_sku.full_pallet_at_receipt
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_ALWAYS_FULL_PALLET'
			  , p_content_i      => r_sku.always_full_pallet
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SUB_WITHIN_PRODUCT_GRP'
			  , p_content_i      => r_sku.sub_within_product_grp
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_SERIAL_CHECK_STRING'
			  , p_content_i      => r_sku.serial_check_string
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_CARRIER_PRODUCT_TYPE'
			  , p_content_i      => r_sku.carrier_product_type
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_MAX_PACK_CONFIGS'
			  , p_content_i      => r_sku.max_pack_configs
 			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OLE_SKU_PARCEL_PACKING_BY_PIECE'
			  , p_content_i      => r_sku.parcel_packing_by_piece
 			  );

		-- Add Hazmat lines for the current SKU
		if	r_sku.hazmat_id is not null--like 'RHS%' 
		then
			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> g_file_name
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Start adding OLE_HAZ'
									   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
													|| '"segment_nr" "'||p_segment_nr_i||'" '
													|| '"line_id" "'||p_line_id_i||'" '
													|| '"sku_id" "'||r_ole.sku_id||'" '
													|| '"hazmat_id" "'||r_sku.hazmat_id||'" '
									   , p_order_id_i		=> p_order_nr_i
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> p_pallet_id_i
									   , p_container_id_i		=> p_container_id_i
									   , p_site_id_i		=> null
									   );
			end if;
			--
			open	c_haz( b_hazmat_id => r_sku.hazmat_id);
			fetch	c_haz
			into	r_haz;
			--
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_HAZMAT_ID'
				  , p_content_i      => r_haz.hazmat_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_HAZMAT_CLASS'
				  , p_content_i      => r_haz.hazmat_class
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_NOTES'
				  , p_content_i      => r_haz.notes
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_1'
				  , p_content_i      => r_haz.user_def_type_1
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_2'
				  , p_content_i      => r_haz.user_def_type_2
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_3'
				  , p_content_i      => r_haz.user_def_type_3
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_4'
				  , p_content_i      => r_haz.user_def_type_4
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_5'
				  , p_content_i      => r_haz.user_def_type_5
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_6'
				  , p_content_i      => r_haz.user_def_type_6
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_7'
				  , p_content_i      => r_haz.user_def_type_7
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TYPE_8'
				  , p_content_i      => r_haz.user_def_type_8
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_CHK_1'
				  , p_content_i      => r_haz.user_def_chk_1
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_CHK_2'
				  , p_content_i      => r_haz.user_def_chk_2
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_CHK_3'
				  , p_content_i      => r_haz.user_def_chk_3
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_CHK_4'
				  , p_content_i      => r_haz.user_def_chk_4
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_DATE_1'
				  , p_content_i      => to_char( r_haz.user_def_date_1, 'dd-mm-yyyy')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TIME_1'
				  , p_content_i      => to_char( r_haz.user_def_date_1, 'hh24:mi:ss')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_DATE_2'
				  , p_content_i      => to_char( r_haz.user_def_date_2, 'dd-mm-yyyy')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TIME_2'
				  , p_content_i      => to_char( r_haz.user_def_date_2, 'hh24:mi:ss')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_DATE_3'
				  , p_content_i      => to_char( r_haz.user_def_date_3, 'dd-mm-yyyy')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TIME_3'
				  , p_content_i      => to_char( r_haz.user_def_date_3, 'hh24:mi:ss')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_DATE_4'
				  , p_content_i      => to_char( r_haz.user_def_date_4, 'dd-mm-yyyy')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_TIME_4'
				  , p_content_i      => to_char( r_haz.user_def_date_4, 'hh24:mi:ss')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_NUM_1'
				  , p_content_i      => to_char( r_haz.user_def_num_1, 'fm999999990.099999')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_NUM_2'
				  , p_content_i      => to_char( r_haz.user_def_num_2, 'fm999999990.099999')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_NUM_3'
				  , p_content_i      => to_char( r_haz.user_def_num_3, 'fm999999990.099999')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_NUM_4'
				  , p_content_i      => to_char( r_haz.user_def_num_4, 'fm999999990.099999')
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_NOTE_1'
				  , p_content_i      => r_haz.user_def_note_1
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_USER_DEF_NOTE_2'
				  , p_content_i      => r_haz.user_def_note_2
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_METAPACK'
				  , p_content_i      => r_haz.metapack
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_UN_CLASS'
				  , p_content_i      => r_haz.hazmat_class
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_UN_CODE'
				  , p_content_i      => r_haz.user_def_type_1
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_DG_TYPE'
				  , p_content_i      => r_haz.user_def_type_2
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_PACKAGE_GROUP'
				  , p_content_i      => r_haz.user_def_type_3
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_PACKAGE_INSTR'
				  , p_content_i      => r_haz.user_def_type_4
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_DG_ACCESSIBILITY'
				  , p_content_i      => r_haz.user_def_type_5
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_DG_CARRIER_DESC'
				  , p_content_i      => r_haz.user_def_note_1
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'OLE_HAZ_LITHIUM_CHECK_BOX'
				  , p_content_i      => r_haz.lithium_check_box
				  );
			close c_haz;

			--
			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> g_file_name
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Start adding OLE_SHA'
									   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
													|| '"segment_nr" "'||p_segment_nr_i||'" '
													|| '"line_id" "'||p_line_id_i||'" '
													|| '"sku_id" "'||r_ole.sku_id||'" '
													|| '"hazmat_id" "'||r_sku.hazmat_id||'" '
									   , p_order_id_i		=> p_order_nr_i
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> p_pallet_id_i
									   , p_container_id_i		=> p_container_id_i
									   , p_site_id_i		=> null
									   );
			end if;

			for	r_sha in c_sha( b_hazmat_id	=> r_sku.hazmat_id
					      , b_client_id	=> r_ole.client_id
					      , b_sku_id	=> r_ole.sku_id
					      )
			loop
				write_line( p_file_type_i    => p_file_type_i
					  , p_field_prefix_i => p_field_prefix_i
					  , p_field_name_i   => 'OLE_SHA_SEGMENT_NR'
					  , p_content_i      => 'Segment SHA: '|| to_char( l_counter)
					  );
				l_counter := l_counter +1; -- For next iteration
				write_line( p_file_type_i    => p_file_type_i
					  , p_field_prefix_i => p_field_prefix_i
					  , p_field_name_i   => 'OLE_SHA_REGULATION_ID'
					  , p_content_i      => r_sha.regulation_id
					  );
				write_line( p_file_type_i    => p_file_type_i
					  , p_field_prefix_i => p_field_prefix_i
					  , p_field_name_i   => 'OLE_SHA_HAZMAT_CLASS'
					  , p_content_i      => r_sha.hazmat_class
					  );
				write_line( p_file_type_i    => p_file_type_i
					  , p_field_prefix_i => p_field_prefix_i
					  , p_field_name_i   => 'OLE_SHA_HAZMAT_SUBCLASS'
					  , p_content_i      => r_sha.hazmat_subclass
					  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_CLASSIFICATION_CODE'
			  		  , p_content_i      => r_sha.classification_code
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_UN_PACKING_GROUP'
			  		  , p_content_i      => r_sha.un_packing_group
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_HAZMAT_LABELS'
			  		  , p_content_i      => r_sha.hazmat_labels
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_TRANSPORT_CATEGORY'
			  		  , p_content_i      => r_sha.transport_category
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_MARINE_POLLUTANT'
			  		  , p_content_i      => r_sha.marine_pollutant
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_MFAG'
			  		  , p_content_i      => r_sha.mfag
			  		  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_EMS'
		  			  , p_content_i      => r_sha.ems
				  	  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_HAZMAT_NET_WEIGHT'
			  		  , p_content_i      => r_sha.hazmat_net_weight
			  		  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_HAZMAT_NET_VOLUME'
			  		  , p_content_i      => r_sha.hazmat_net_volume
			  		  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_HAZMAT_NET_VOLUME_UNIT'
			  		  , p_content_i      => r_sha.hazmat_net_volume_unit
			  		  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_HAZMAT_FLASHPOINT'
			  		  , p_content_i      => r_sha.hazmat_flashpoint
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_FLASHPOINT_CATEGORY'
			  		  , p_content_i      => r_sha.flashpoint_category
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_WGK_CLASS'
			  		  , p_content_i      => r_sha.wgk_class
			  		  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_GHS_SYMBOL'
		  			  , p_content_i      => r_sha.ghs_symbol
				  	  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
		  			  , p_field_name_i   => 'OLE_SHA_LIMITED_QTY'
		  			  , p_content_i      => r_sha.limited_qty
		  			  );
				write_line( p_file_type_i    => p_file_type_i
				  	  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_R_SENTENCE_CODE'
			  		  , p_content_i      => r_sha.r_sentence_code
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_R_SENTENCE_GROUP'
			  		  , p_content_i      => r_sha.r_sentence_group
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_R_SENTENCE'
			  		  , p_content_i      => r_sha.r_sentence
			  		  );
				write_line( p_file_type_i    => p_file_type_i
		  			  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_PROPER_SHIPPING_NAME'
			  		  , p_content_i      => r_sha.proper_shipping_name
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_ADDITIONAL_SHIP_NAME'
			  		  , p_content_i      => r_sha.additional_shipping_name
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_UN_PACKAGING_CODE'
			  		  , p_content_i      => r_sha.un_packaging_code
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_WATER_ENDANGER_CLASS'
			  		  , p_content_i      => r_sha.water_endangerment_class
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
				  	  , p_field_name_i   => 'OLE_SHA_LANGUAGE'
			  		  , p_content_i      => r_sha.language
			  		  );
				write_line( p_file_type_i    => p_file_type_i
			  		  , p_field_prefix_i => p_field_prefix_i
			  		  , p_field_name_i   => 'OLE_SHA_TUNNEL_CODE'
			  		  , p_content_i      => r_sha.tunnel_code
			  		  );
				if	r_sha.regulation_id is not null
				then
					open	c_hrn(r_sha.regulation_id);
					fetch 	c_hrn
					into	r_hrn;
					close	c_hrn;
					write_line( p_file_type_i    => p_file_type_i
						  , p_field_prefix_i => p_field_prefix_i
						  , p_field_name_i   => 'OLE_SHA_NOTES'
						  , p_content_i      => r_hrn.notes
						  );
				end if;
			end loop;
		end if;
		close c_sku;

		if	nvl(r_ole.unallocatable,'N') = 'N'			-- No unallocatbale line
		and	nvl(r_ole.qty_ordered,0) > 0				-- QTY ordered higher than 0
		and	nvl(r_ole.qty_tasked,0) + nvl(r_ole.qty_picked,0) > 0 	-- Not a zero picked order line
		then
			-- Add LOT lines for the current Order Line
			add_lot ( p_file_type_i    => p_file_type_i
				, p_field_prefix_i => p_field_prefix_i
				, p_segment_nr_i   => p_segment_nr_i
				, p_client_id_i    => r_ole.client_id
				, p_order_nr_i     => r_ole.order_id
				, p_line_id_i      => r_ole.line_id
				, p_pallet_id_i    => p_pallet_id_i
				, p_container_id_i => p_container_id_i
				);

			-- Add Serial Lines for the current Order Line
			add_snr ( p_file_type_i    => p_file_type_i
				, p_field_prefix_i => p_field_prefix_i
				, p_segment_nr_i   => p_segment_nr_i
				, p_client_id_i    => r_ole.client_id
				, p_order_id_i     => r_ole.order_id
				, p_line_id_i      => r_ole.line_id
				);
		end if;

		close c_ole;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding OLE'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"segment_nr" "'||p_segment_nr_i||'" '
												|| '"line_id" "'||p_line_id_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

	exception
		when	others
		then
			case 
			when	c_ole%isopen
			then
				close	c_ole;
			when 	c_sku%isopen
			then
			close 	c_sku;
			when 	c_haz%isopen
			then
				close	c_haz;
			else
				null;
			end 	case;
	end add_ole;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 05-Nov-2016
-- Purpose : Create StreamServe Address block
------------------------------------------------------------------------------------------------
	procedure add_ads( p_file_type_i        in  utl_file.file_type
			 , p_field_prefix_i     in  varchar2
			 , p_ads_type_i         in  varchar2
			 , p_name_1_i           in  varchar2  := null
			 , p_address_1_i        in  varchar2  := null
			 , p_city_i             in  varchar2  := null
			 , p_zip_code_i         in  varchar2  := null
			 , p_state_code_i       in  varchar2  := null
			 , p_cty_iso_i          in  varchar2  := null
			 , p_address_2_i        in  varchar2  := null
			 , p_address_3_i        in  varchar2  := null
			 , p_address_4_i        in  varchar2  := null
			 , p_phone_i            in  varchar2  := null
			 , p_mobile_i           in  varchar2  := null
			 , p_fax_i              in  varchar2  := null
			 , p_email_i            in  varchar2  := null
			 , p_contact_name_i     in  varchar2  := null
			 , p_web_i              in  varchar2  := null
			 , p_ads_udf_type_1_i   in  varchar2  := null
			 , p_ads_udf_type_2_i   in  varchar2  := null
			 , p_ads_udf_type_3_i   in  varchar2  := null
			 , p_ads_udf_type_4_i   in  varchar2  := null
			 , p_ads_udf_type_5_i   in  varchar2  := null
			 , p_ads_udf_type_6_i   in  varchar2  := null
			 , p_ads_udf_type_7_i   in  varchar2  := null
			 , p_ads_udf_type_8_i   in  varchar2  := null
			 , p_ads_udf_num_1_i    in  number    := null
			 , p_ads_udf_num_2_i    in  number    := null
			 , p_ads_udf_num_3_i    in  number    := null
			 , p_ads_udf_num_4_i    in  number    := null
			 , p_ads_udf_chk_1_i    in  varchar2  := null
			 , p_ads_udf_chk_2_i    in  varchar2  := null
			 , p_ads_udf_chk_3_i    in  varchar2  := null
			 , p_ads_udf_chk_4_i    in  varchar2  := null
			 , p_ads_udf_dstamp_1_i in  timestamp := null
			 , p_ads_udf_dstamp_2_i in  timestamp := null
			 , p_ads_udf_dstamp_3_i in  timestamp := null
			 , p_ads_udf_dstamp_4_i in  timestamp := null
			 , p_ads_udf_note_1_i   in  varchar2  := null
			 , p_ads_udf_note_2_i   in  varchar2  := null
			 , p_directions_i       in  varchar2  := null
			 , p_vat_number_i       in  varchar2  := null
			 , p_address_type_1     in  varchar2  := null
			 , p_address_id_i       in  varchar2  := null
			 )
	is
		-- Get ISO country code
		cursor	c_cty ( b_cty_iso in varchar2)
		is
			select	cty.iso2_id
			,      	cty.iso3_id
			,      	decode( cty.ce_eu_type, 'EU', g_yes, g_no)                 eu_type
			,      	upper(ltt.text)         cty_desc
			from   	dcsdba.country          cty
			,      	dcsdba.language_text    ltt
			where  	cty.iso3_id             = substr(label, 4) 
			and    	substr(ltt.label, 1, 3) = 'WLK' 
			and    	ltt.language            = 'EN_GB'
			and    	(cty.iso3_id = b_cty_iso or cty.iso2_id = b_cty_iso)
		;

		r_cty             	c_cty%rowtype;

		l_field_prefix_i  	varchar2(35); 
		l_rtn			varchar2(30) := 'add_ads';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_ads_type_i
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"address_id" "'||p_address_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		l_field_prefix_i := p_field_prefix_i || '_' || p_ads_type_i;
		-- get country details
		open	c_cty ( b_cty_iso => p_cty_iso_i);
		fetch 	c_cty
		into  	r_cty;
		close 	c_cty;

		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'TYPE'
			  , p_content_i      => p_ads_type_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'NAME_1'
			  , p_content_i      => p_name_1_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ADDRESS_1'
			  , p_content_i      => p_address_1_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'CITY'
			  , p_content_i      => p_city_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ZIP_CODE'
			  , p_content_i      => p_zip_code_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'STATE_CODE'
			  , p_content_i      => p_state_code_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'CTY_ISO2'
			  , p_content_i      => r_cty.iso2_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'CTY_ISO3'
			  , p_content_i      => r_cty.iso3_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'CTY_DESC'
			  , p_content_i      => r_cty.cty_desc
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'EU_IND'
			  , p_content_i      => r_cty.eu_type
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ADDRESS_2'
			  , p_content_i      => p_address_2_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ADDRESS_3'
			  , p_content_i      => p_address_3_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ADDRESS_4'
			  , p_content_i      => p_address_4_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'PHONE'
			  , p_content_i      => p_phone_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'MOBILE'
			  , p_content_i      => p_mobile_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'FAX'
			  , p_content_i      => p_fax_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'EMAIL'
			  , p_content_i      => p_email_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'CONTACT_NAME'
			  , p_content_i      => p_contact_name_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'WEB'
			  , p_content_i      => p_web_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_1'
			  , p_content_i      => p_ads_udf_type_1_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_2'
			  , p_content_i      => p_ads_udf_type_2_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_3'
			  , p_content_i      => p_ads_udf_type_3_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_4'
			  , p_content_i      => p_ads_udf_type_4_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_5'
			  , p_content_i      => p_ads_udf_type_5_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_6'
			  , p_content_i      => p_ads_udf_type_6_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_7'
			  , p_content_i      => p_ads_udf_type_7_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TYPE_8'
			  , p_content_i      => p_ads_udf_type_8_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_NUM_1'
			  , p_content_i      => to_char( p_ads_udf_num_1_i, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_NUM_2'
			  , p_content_i      => to_char( p_ads_udf_num_2_i, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_NUM_3'
			  , p_content_i      => to_char( p_ads_udf_num_3_i, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_NUM_4'
			  , p_content_i      => to_char( p_ads_udf_num_4_i, 'fm999999990.999990')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_CHK_1'
			  , p_content_i      => p_ads_udf_chk_1_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_CHK_2'
			  , p_content_i      => p_ads_udf_chk_2_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_CHK_3'
			  , p_content_i      => p_ads_udf_chk_3_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_CHK_4'
			  , p_content_i      => p_ads_udf_chk_4_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_DATE_1'
			  , p_content_i      => to_char( p_ads_udf_dstamp_1_i, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TIME_1'
			  , p_content_i      => to_char( p_ads_udf_dstamp_1_i, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_DATE_2'
			  , p_content_i      => to_char( p_ads_udf_dstamp_2_i, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TIME_2'
			  , p_content_i      => to_char( p_ads_udf_dstamp_2_i, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_DATE_3'
			  , p_content_i      => to_char( p_ads_udf_dstamp_3_i, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TIME_3'
			  , p_content_i      => to_char( p_ads_udf_dstamp_3_i, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_DATE_4'
			  , p_content_i      => to_char( p_ads_udf_dstamp_4_i, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_TIME_4'
			  , p_content_i      => to_char( p_ads_udf_dstamp_4_i, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_NOTE_1'
			  , p_content_i      => p_ads_udf_note_1_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'USER_DEF_NOTE_2'
			  , p_content_i      => p_ads_udf_note_2_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'DIRECTIONS'
			  , p_content_i      => p_directions_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'VAT_NUMBER'
			  , p_content_i      => p_vat_number_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ADDRESS_TYPE'
			  , p_content_i      => p_address_type_1
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => l_field_prefix_i
			  , p_field_name_i   => 'ADDRESS_ID'
			  , p_content_i      => p_address_id_i
			  );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_ads_type_i
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"address_id" "'||p_address_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
	exception 
		when	others
		then
			case 
			when	c_cty%isopen
			then
				close	c_cty;
			else
				null;
			end case;
	end add_ads;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 27-Sep-2016
-- Purpose : Create StreamServe Shipment and Order Header block
------------------------------------------------------------------------------------------------
	procedure add_smt( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 )
	is
		-- Fetch order header details
		cursor c_ohr( b_client_id  varchar2
			    , b_order_id   varchar2
			    )
		is
			select	ohr.*
			from   	dcsdba.order_header ohr
			where  	ohr.client_id = b_client_id
			and    	ohr.order_id  = b_order_id 
		;
		-- fetch unit details
		cursor c_ocr( b_client_id  varchar2
			    , b_order_id   varchar2
			    )
		is	
			select	smt.client_id
			,      	smt.order_id
			,      	nvl( smt.labelled, g_no) is_cont_yn
			,      	decode( smt.labelled, g_yes, smt.container_id, smt.pallet_id) pallet_id
			,      	decode( smt.labelled, g_yes, round( ( smt.container_depth * smt.container_width * smt.container_height) / 1000000, 6), round( ( smt.pallet_depth    * smt.pallet_width    * smt.pallet_height)    / 1000000, 6)) volume
			,      	decode( smt.labelled, g_yes, smt.container_weight, smt.pallet_weight ) weight
			,      	1 cnt
			,      	decode( smt.labelled, g_yes, 1, nvl( smt.transport_boxes, 1)) no_of_boxes
			from   	dcsdba.shipping_manifest smt
			where  	smt.client_id            = b_client_id
			and    	smt.order_id             = b_order_id
			union  	-- for pallets which are not 'marshalled' yet
			select 	ocr.client_id
			,      	ocr.order_id
			,      	nvl( ocr.labelled, g_no)                            is_cont_yn
			,      	decode( ocr.labelled, g_yes, ocr.container_id, ocr.pallet_id) pallet_id
			,      	decode( ocr.labelled, g_yes, round( ( ocr.container_depth * ocr.container_width * ocr.container_height) / 1000000, 6), round( ( ocr.pallet_depth    * ocr.pallet_width    * ocr.pallet_height)    / 1000000, 6)) volume
			,      	decode( ocr.labelled, g_yes, ocr.container_weight, ocr.pallet_weight ) weight
			,      	1 cnt
			,      	decode( ocr.labelled, g_yes, 1, nvl( ocr.transport_boxes, 1)) no_of_boxes
			from   	dcsdba.order_container   ocr
			where  	ocr.client_id            = b_client_id
			and    	ocr.order_id             = b_order_id
			and    	not exists( 	select 1
						from   dcsdba.shipping_manifest smt
						where  smt.client_id            = ocr.client_id
						and    smt.order_id             = ocr.order_id
						and    smt.pallet_id            = ocr.pallet_id
					  )
			order  
			by 	1,2,4
		;
		-- Fetch client details
		cursor c_clt( b_client_id in varchar2)
		is
			select clt.name
			,      clt.address1
			,      clt.address2
			,      clt.postcode
			,      clt.town
			,      clt.county         
			,      clt.country
			,      clt.contact_phone  
			,      clt.contact_mobile 
			,      clt.contact_fax    
			,      clt.contact_email  
			,      clt.contact        contact_name
			,      clt.notes
			,      clt.url
			,      clt.user_def_type_1
			,      clt.user_def_type_2
			,      clt.user_def_type_3
			,      clt.user_def_type_4
			,      clt.user_def_type_5
			,      clt.user_def_type_6
			,      clt.user_def_type_7
			,      clt.user_def_type_8
			,      clt.user_def_num_1
			,      clt.user_def_num_2
			,      clt.user_def_num_3
			,      clt.user_def_num_4
			,      clt.user_def_chk_1
			,      clt.user_def_chk_2
			,      clt.user_def_chk_3
			,      clt.user_def_chk_4
			,      clt.user_def_date_1
			,      clt.user_def_date_2
			,      clt.user_def_date_3
			,      clt.user_def_date_4
			,      clt.user_def_note_1
			,      clt.user_def_note_2
			,      clt.vat_number
			from   dcsdba.client clt
			where  clt.client_id = b_client_id
		;
		-- Fetch carrier details
		cursor c_crr( b_client_id     in varchar2
			    , b_carrier_id    in varchar2
			    , b_service_level in varchar2
			    )
		is
			select crr.name
			,      crr.address1
			,      crr.address2
			,      crr.postcode
			,      crr.town
			,      crr.county         
			,      crr.country
			,      crr.contact_phone  
			,      crr.contact_mobile 
			,      crr.contact_fax    
			,      crr.contact_email  
			,      crr.contact        contact_name
			,      crr.notes
			,      crr.url
			,      crr.user_def_type_1
			,      crr.user_def_type_2
			,      crr.user_def_type_3
			,      crr.user_def_type_4
			,      crr.user_def_type_5
			,      crr.user_def_type_6
			,      crr.user_def_type_7
			,      crr.user_def_type_8
			,      crr.user_def_num_1
			,      crr.user_def_num_2
			,      crr.user_def_num_3
			,      crr.user_def_num_4
			,      crr.user_def_chk_1
			,      crr.user_def_chk_2
			,      crr.user_def_chk_3
			,      crr.user_def_chk_4
			,      crr.user_def_date_1
			,      crr.user_def_date_2
			,      crr.user_def_date_3
			,      crr.user_def_date_4
			,      crr.user_def_note_1
			,      crr.user_def_note_2
			from   dcsdba.carriers crr
			where  crr.client_id     = b_client_id
			and    crr.carrier_id    = b_carrier_id
			and    crr.service_level = b_service_level
		;
		-- Fetch address details
		cursor c_ads( b_client_id    in varchar2
			    , b_address_id   in varchar2
			    )
		is
			select ads.address_id
			,      ads.address_type
			,      ads.name
			,      ads.address1
			,      ads.town
			,      ads.postcode
			,      ads.county
			,      ads.country
			,      ads.address2
			,      ads.contact_phone
			,      ads.contact_mobile
			,      ads.contact_fax
			,      ads.contact_email
			,      ads.contact        contact_name
			,      ads.url
			,      ads.user_def_type_1
			,      ads.user_def_type_2
			,      ads.user_def_type_3
			,      ads.user_def_type_4
			,      ads.user_def_type_5
			,      ads.user_def_type_6
			,      ads.user_def_type_7
			,      ads.user_def_type_8
			,      ads.user_def_num_1
			,      ads.user_def_num_2
			,      ads.user_def_num_3
			,      ads.user_def_num_4
			,      ads.user_def_chk_1
			,      ads.user_def_chk_2
			,      ads.user_def_chk_3
			,      ads.user_def_chk_4
			,      ads.user_def_date_1
			,      ads.user_def_date_2
			,      ads.user_def_date_3
			,      ads.user_def_date_4
			,      ads.user_def_note_1
			,      ads.user_def_note_2
			,      ads.directions
			,      ads.vat_number
			from   dcsdba.address ads
			where  ads.client_id  = b_client_id
			and    ads.address_id = b_address_id
		;                    
		-- Fetch track and trace URL
		cursor c_url( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select 	url
			from	(
				select	ccd.cto_tracking_url url
				from	cnl_sys.cnl_container_data ccd
				where	ccd.cto_tracking_url is not null
				and	ccd.client_id = b_client_id
				and	ccd.order_id = b_order_id
	--			and 	rownum = 1
				union -- with CTO saas data
				select	cto.tracking_url url
				from	cnl_sys.cnl_cto_ship_labels cto
				where	cto.tracking_url is not null
				and	cto.client_id = b_client_id
				and	cto.order_id = b_order_id
	--			and 	rownum = 1
				)
			where 	rownum = 1
		;
		--
		r_url	    c_url%rowtype;
		r_ohr           c_ohr%rowtype;
		r_ocr           c_ocr%rowtype;
		r_clt           c_clt%rowtype;
		r_crr           c_crr%rowtype;
		r_ads           c_ads%rowtype;

		l_volume        number(15,6);
		l_weight        number(15,6);
		l_pieces        number(10);
		l_no_of_boxes   number(10);
		l_rtn		varchar(30) := 'add_smt';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding SMT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		-- Fetch order header details
		open	c_ohr( b_client_id => p_client_id_i
			     , b_order_id  => p_order_nr_i
			     );
		fetch 	c_ohr
		into  	r_ohr;
		-- Fetch track and trace URL
		open 	c_url( b_client_id => p_client_id_i
			     , b_order_id => p_order_nr_i
			     );
		fetch  	c_url 
		into   	r_url;
		close  	c_url;
		-- Add order header details
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'SMT_ID'
			  , p_content_i      => null
			  );
		write_line ( p_file_type_i    => p_file_type_i
               	  	  , p_field_prefix_i => p_field_prefix_i
	                  , p_field_name_i   => 'SMT_ORDER_NR'
                          , p_content_i      => p_order_nr_i
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_WAYBILL_NR'
                          , p_content_i      => r_ohr.trax_id
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TRACKING_URL'
                          , p_content_i      => r_url.url
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_SHIP_DATE'
                          , p_content_i      => to_char( nvl( r_ohr.shipped_date, sysdate), 'DD-MM-YYYY')
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_ORDER_DATE'
                          , p_content_i      => to_char( r_ohr.order_date, 'DD-MM-YYYY')
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_REQ_DEL_DATE'
                          , p_content_i      => to_char( r_ohr.deliver_by_date, 'DD-MM-YYYY')
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_INV_AMOUNT'
                          , p_content_i      => ltrim( to_char( r_ohr.inv_total_1, 'fm99999990.00'))
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_CURRENCY_CODE'
                          , p_content_i      => r_ohr.inv_currency
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COD_YN'
                          , p_content_i      => nvl( r_ohr.cod, g_no)
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COD_AMOUNT'
                          , p_content_i      => ltrim( to_char( r_ohr.cod_value, 'fm9999990.00'))
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COD_CURRENCY_CODE'
                          , p_content_i      => r_ohr.cod_currency
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_GOOD_DESC'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_DAN_GOOD_YN'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_ORIGIN_PORT'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_DESTINATION_PORT'
                          , p_content_i      => r_ohr.delivery_point
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_DELIVERY_COND_CODE'
                          , p_content_i      => r_ohr.tod
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_CARRIER_ID'
                          , p_content_i      => r_ohr.carrier_id
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_SERVICE_LEVEL'
                          , p_content_i      => r_ohr.service_level
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_DEPARTURE_ISO2'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_DEPARTURE_ISO3'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_DEPARTURE_NUM3'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_DEPARTURE_DESC'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_ORIGIN_ISO2'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_ORIGIN_ISO3'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_ORIGIN_NUM3'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COUNTRY_ORIGIN_DESC'
                          , p_content_i      => null --@@@
                          );
		-- summarize unit details
		for 	r_ocr in c_ocr( b_client_id => p_client_id_i
				      , b_order_id  => p_order_nr_i
				      )
		loop
			l_weight      := nvl( l_weight, 0) + round( r_ocr.weight, 2);
			l_volume      := nvl( l_volume, 0) + round( r_ocr.volume, 6);
			l_pieces      := nvl( l_pieces, 0) + r_ocr.cnt;
			l_no_of_boxes := nvl( l_no_of_boxes, 0) + r_ocr.no_of_boxes;
		end loop;
		-- add unit totals
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_ITEMS'
                          , p_content_i      => to_char( l_no_of_boxes)
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_BOXES_IN_UNITS'
                          , p_content_i      => to_char( l_no_of_boxes)
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_WEIGHT'
                          , p_content_i      => ltrim( to_char(l_weight, 'fm999990.90'))
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_PIECES'
                          , p_content_i      => to_char(l_pieces)
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_SHIP_UNITS'
                          , p_content_i      => to_char(l_pieces)
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_VOLUME'
                          , p_content_i      => ltrim( to_char( l_volume, 'fm999990.90'))
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_TOTAL_DRY_ICE_WEIGHT'
                          , p_content_i      => null
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_FREIGHT_CHARGES'
                          , p_content_i      => r_ohr.freight_charges
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_COLLECT_ACCOUNT_NR'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_THIRD_ACCOUNT_NR'
                          , p_content_i      => null --@@@
                          );
		write_line ( p_file_type_i    => p_file_type_i
                          , p_field_prefix_i => p_field_prefix_i
                          , p_field_name_i   => 'SMT_REFERENCE_NR'
                          , p_content_i      => r_ohr.purchase_order
                          );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding OHR segment'
								   , p_code_parameters_i 	=> null
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		-- switch to OHR field prefix
		write_line ( p_file_type_i    => p_file_type_i
			   , p_field_prefix_i => p_field_prefix_i
	       		   , p_field_name_i   => 'OHR_WORK_ORDER_TYPE'
			   , p_content_i      => r_ohr.work_order_type
               		   );		   
		write_line ( p_file_type_i    => p_file_type_i
		           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_TYPE'
                           , p_content_i      => r_ohr.order_type
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_STATUS'
                           , p_content_i      => r_ohr.status
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_MOVE_TASK_STATUS'
                           , p_content_i      => r_ohr.move_task_status
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_PRIORITY'
                           , p_content_i      => to_char( r_ohr.priority)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_REPACK'
                           , p_content_i      => r_ohr.repack
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_REPACK_LOC_ID'
                           , p_content_i      => r_ohr.repack_loc_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SHIP_DOCK'
                           , p_content_i      => r_ohr.ship_dock
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_WORK_GROUP'
                           , p_content_i      => r_ohr.work_group
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CONSIGNMENT'
                           , p_content_i      => r_ohr.consignment
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DELIVERY_POINT'
                           , p_content_i      => r_ohr.delivery_point
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_LOAD_SEQUENCE'
                           , p_content_i      => to_char( r_ohr.load_sequence)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_TO_SITE_ID'
                           , p_content_i      => r_ohr.to_site_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_OWNER_ID'
                           , p_content_i      => r_ohr.owner_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CUSTOMER_ID'
                           , p_content_i      => r_ohr.customer_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SHIP_BY_DATE'
                           , p_content_i      => to_char( r_ohr.ship_by_date, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SHIP_BY_TIME'
                           , p_content_i      => to_char( r_ohr.ship_by_date, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DELIVER_BY_DATE'
                           , p_content_i      => to_char( r_ohr.deliver_by_date, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DELIVER_BY_TIME'
                           , p_content_i      => to_char( r_ohr.deliver_by_date, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DELIVERED_DSTAMP'
                           , p_content_i      => to_char( r_ohr.delivered_dstamp, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DELIVERED_TIME'
                           , p_content_i      => to_char( r_ohr.delivered_dstamp, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SIGNATORY'
                           , p_content_i      => r_ohr.signatory
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_PURCHASE_ORDER'
                           , p_content_i      => r_ohr.purchase_order
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DISPATCH_METHOD'
                           , p_content_i      => r_ohr.dispatch_method
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SERVICE_LEVEL'
                           , p_content_i      => r_ohr.service_level
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_FASTEST_CARRIER'
                           , p_content_i      => r_ohr.fastest_carrier
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CHEAPEST_CARRIER'
                           , p_content_i      => r_ohr.cheapest_carrier
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_INV_ADDRESS_ID'
                           , p_content_i      => r_ohr.inv_address_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_INSTRUCTIONS'
                           , p_content_i      => r_ohr.instructions
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_VOLUME'
                           , p_content_i      => ltrim( to_char( r_ohr.order_volume, 'fm999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_WEIGHT'
                           , p_content_i      => ltrim( to_char( r_ohr.order_weight, 'fm999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ROUTE_PLANNED'
                           , p_content_i      => r_ohr.route_planned
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED'
                           , p_content_i      => r_ohr.uploaded
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_WS2PC_ID'
                           , p_content_i      => to_char( r_ohr.uploaded_ws2pc_id)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_DSTAMP'
                           , p_content_i      => to_char( r_ohr.uploaded_dstamp, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_FILENAME'
                           , p_content_i      => r_ohr.uploaded_filename
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_VVIEW'
                           , p_content_i      => r_ohr.uploaded_vview
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_HEADER_KEY'
                           , p_content_i      => to_char( r_ohr.uploaded_header_key)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_PSFT_DMND_SRCE'
                           , p_content_i      => r_ohr.psft_dmnd_srce
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_PSFT_ORDER_ID'
                           , p_content_i      => r_ohr.psft_order_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SITE_REPLEN'
                           , p_content_i      => r_ohr.site_replen
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_ID_LINK'
                           , p_content_i      => r_ohr.order_id_link
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ALLOCATION_RUN'
                           , p_content_i      => to_char( r_ohr.allocation_run)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_NO_SHIPMENT_EMAIL'
                           , p_content_i      => r_ohr.no_shipment_email
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CID_NUMBER'
                           , p_content_i      => r_ohr.cid_number
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SID_NUMBER'
                           , p_content_i      => r_ohr.sid_number
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_LOCATION_NUMBER'
                           , p_content_i      => r_ohr.location_number
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_FREIGHT_CHARGES'
                           , p_content_i      => r_ohr.freight_charges
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DISALLOW_MERGE_RULES'
                           , p_content_i      => r_ohr.disallow_merge_rules
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_SOURCE'
                           , p_content_i      => r_ohr.order_source
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_EXPORT'
                           , p_content_i      => r_ohr.export
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_NUM_LINES'
                           , p_content_i      => to_char( r_ohr.num_lines)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_HIGHEST_LABEL'
                           , p_content_i      => to_char( r_ohr.highest_label)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_1'
                           , p_content_i      => r_ohr.user_def_type_1
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_2'
                           , p_content_i      => r_ohr.user_def_type_2
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_3'
                           , p_content_i      => r_ohr.user_def_type_3
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_4'
                           , p_content_i      => r_ohr.user_def_type_4
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_5'
                           , p_content_i      => r_ohr.user_def_type_5
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_6'
                           , p_content_i      => r_ohr.user_def_type_6
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_7'
                           , p_content_i      => r_ohr.user_def_type_7
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TYPE_8'
                           , p_content_i      => r_ohr.user_def_type_8
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_CHK_1'
                           , p_content_i      => r_ohr.user_def_chk_1
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_CHK_2'
                           , p_content_i      => r_ohr.user_def_chk_2
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_CHK_3'
                           , p_content_i      => r_ohr.user_def_chk_3
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_CHK_4'
                           , p_content_i      => r_ohr.user_def_chk_4
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_DATE_1'
                           , p_content_i      => to_char( r_ohr.user_def_date_1, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TIME_1'
                           , p_content_i      => to_char( r_ohr.user_def_date_1, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_DATE_2'
                           , p_content_i      => to_char( r_ohr.user_def_date_2, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TIME_2'
                           , p_content_i      => to_char( r_ohr.user_def_date_2, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_DATE_3'
                           , p_content_i      => to_char( r_ohr.user_def_date_3, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TIME_3'
                           , p_content_i      => to_char( r_ohr.user_def_date_3, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_DATE_4'
                           , p_content_i      => to_char( r_ohr.user_def_date_4, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_TIME_4'
                           , p_content_i      => to_char( r_ohr.user_def_date_4, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_NUM_1'
                           , p_content_i      => to_char( r_ohr.user_def_num_1, 'fm999999990.999990')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_NUM_2'
                           , p_content_i      => to_char( r_ohr.user_def_num_2, 'fm999999990.999990')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_NUM_3'
                           , p_content_i      => to_char( r_ohr.user_def_num_3, 'fm999999990.999990')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_NUM_4'
                           , p_content_i      => to_char( r_ohr.user_def_num_4, 'fm999999990.999990')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_NOTE_1'
                           , p_content_i      => r_ohr.user_def_note_1
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_USER_DEF_NOTE_2'
                           , p_content_i      => r_ohr.user_def_note_2
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ROUTE_ID'
                           , p_content_i      => r_ohr.route_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CROSS_DOCK_TO_SITE'
                           , p_content_i      => r_ohr.cross_dock_to_site
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_WEB_SERVICE_ALLOC_IMMED'
                           , p_content_i      => r_ohr.web_service_alloc_immed
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_WEB_SERVICE_ALLOC_CLEAN'
                           , p_content_i      => r_ohr.web_service_alloc_clean
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DISALLOW_SHORT_SHIP'
                           , p_content_i      => r_ohr.disallow_short_ship
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_CUSTOMS'
                           , p_content_i      => r_ohr.uploaded_customs
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_UPLOADED_LABOR'
                           , p_content_i      => r_ohr.uploaded_labor
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CANCEL_REASON_CODE'
                           , p_content_i      => null
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_STATUS_REASON_CODE'
                           , p_content_i      => r_ohr.status_reason_code
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_STAGE_ROUTE_ID'
                           , p_content_i      => r_ohr.stage_route_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SINGLE_ORDER_SORTATION'
                           , p_content_i      => r_ohr.single_order_sortation
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ARCHIVED'
                           , p_content_i      => r_ohr.archived
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CLOSURE_DATE'
                           , p_content_i      => to_char( r_ohr.closure_date, 'DD-MM-YYYY')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_CLOSURE_TIME'
                           , p_content_i      => to_char( r_ohr.closure_date, 'HH24:MI:SS')
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_CLOSED'
                           , p_content_i      => r_ohr.order_closed
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_TOTAL_REPACK_CONTAINERS'
                           , p_content_i      => to_char( r_ohr.total_repack_containers)
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_FORCE_SINGLE_CARRIER'
                           , p_content_i      => r_ohr.force_single_carrier
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_HUB_CARRIER_ID'
                           , p_content_i      => r_ohr.hub_carrier_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_HUB_SERVICE_LEVEL'
                           , p_content_i      => r_ohr.hub_service_level
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_GROUPING_ID'
                           , p_content_i      => r_ohr.order_grouping_id
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SHIP_BY_DATE_ERR'
                           , p_content_i      => r_ohr.ship_by_date_err
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DEL_BY_DATE_ERR'
                           , p_content_i      => r_ohr.del_by_date_err
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SHIP_BY_DATE_ERR_MSG'
                           , p_content_i      => r_ohr.ship_by_date_err_msg
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DEL_BY_DATE_ERR_MSG'
                           , p_content_i      => r_ohr.del_by_date_err_msg
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_ORDER_VALUE'
                           , p_content_i      => ltrim( to_char( r_ohr.order_value, 'fm99999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_EXPECTED_VOLUME'
                           , p_content_i      => ltrim(to_char( r_ohr.expected_volume, 'fm999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_EXPECTED_WEIGHT'
                           , p_content_i      => ltrim( to_char( r_ohr.expected_weight, 'fm9999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_EXPECTED_VALUE'
                           , p_content_i      => ltrim( to_char( r_ohr.expected_value, 'fm99999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_TOD'
                           , p_content_i      => r_ohr.tod
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_TOD_PLACE'
                           , p_content_i      => r_ohr.tod_place
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_LANGUAGE'
                           , p_content_i      => r_ohr.language
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SELLER_NAME'
                           , p_content_i      => r_ohr.seller_name
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_SELLER_PHONE'
                           , p_content_i      => r_ohr.seller_phone
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DOCUMENTATION_TEXT_1'
                           , p_content_i      => r_ohr.documentation_text_1
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DOCUMENTATION_TEXT_2'
                           , p_content_i      => r_ohr.documentation_text_2
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_DOCUMENTATION_TEXT_3'
                           , p_content_i      => r_ohr.documentation_text_3
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_COD'
                           , p_content_i      => r_ohr.cod
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_COD_VALUE'
                           , p_content_i      => ltrim( to_char( r_ohr.cod_value, 'fm99999990.90'))
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_COD_CURRENCY'
                           , p_content_i      => r_ohr.cod_currency
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_COD_TYPE'
                           , p_content_i      => r_ohr.cod_type
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_VAT_NUMBER'
                           , p_content_i      => r_ohr.vat_number
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_INV_VAT_NUMBER'
                           , p_content_i      => r_ohr.inv_vat_number
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_HUB_VAT_NUMBER'
                           , p_content_i      => r_ohr.hub_vat_number
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_PRINT_INVOICE'
                           , p_content_i      => r_ohr.print_invoice
                           );
		write_line ( p_file_type_i    => p_file_type_i
                           , p_field_prefix_i => p_field_prefix_i
                           , p_field_name_i   => 'OHR_INV_REFERENCE'
                           , p_content_i      => r_ohr.inv_reference
                           );
		write_line ( p_file_type_i    => p_file_type_i
			   , p_field_prefix_i => p_field_prefix_i
			   , p_field_name_i   => 'OHR_INV_DSTAMP'
			   , p_content_i      => to_char( r_ohr.inv_dstamp, 'DD-MM-YYYY')
			   );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INV_CURRENCY'
			  , p_content_i      => r_ohr.inv_currency
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_LETTER_OF_CREDIT'
			  , p_content_i      => r_ohr.letter_of_credit
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_PAYMENT_TERMS'
			  , p_content_i      => r_ohr.payment_terms
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SUBTOTAL_1'
			  , p_content_i      => ltrim( to_char( r_ohr.subtotal_1, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SUBTOTAL_2'
			  , p_content_i      => ltrim( to_char( r_ohr.subtotal_2, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SUBTOTAL_3'
			  , p_content_i      => ltrim( to_char( r_ohr.subtotal_3, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SUBTOTAL_4'
			  , p_content_i      => ltrim( to_char( r_ohr.subtotal_4, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_FREIGHT_COST'
			  , p_content_i      => ltrim( to_char( r_ohr.freight_cost, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_FREIGHT_TERMS'
			  , p_content_i      => r_ohr.freight_terms
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INSURANCE_COST'
			  , p_content_i      => ltrim( to_char( r_ohr.insurance_cost, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MISC_CHARGES'
			  , p_content_i      => ltrim( to_char( r_ohr.misc_charges, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_DISCOUNT'
			  , p_content_i      => ltrim( to_char( r_ohr.discount, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_OTHER_FEE'
			  , p_content_i      => ltrim( to_char( r_ohr.other_fee, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INV_TOTAL_1'
			  , p_content_i      => ltrim( to_char( r_ohr.inv_total_1, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INV_TOTAL_2'
			  , p_content_i      => ltrim( to_char( r_ohr.inv_total_2, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INV_TOTAL_3'
			  , p_content_i      => ltrim( to_char( r_ohr.inv_total_3, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INV_TOTAL_4'
			  , p_content_i      => ltrim( to_char( r_ohr.inv_total_4, 'fm99999990.90'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_RATE_1'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_rate_1, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_BASIS_1'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_basis_1, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_AMOUNT_1'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_amount_1, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_RATE_2'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_rate_2, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_BASIS_2'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_basis_2, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_AMOUNT_2'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_amount_2, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_RATE_3'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_rate_3, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_BASIS_3'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_basis_3, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_AMOUNT_3'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_amount_3, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_RATE_4'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_rate_4, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_BASIS_4'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_basis_4, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_AMOUNT_4'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_amount_4, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_RATE_5'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_rate_5, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_BASIS_5'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_basis_5, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TAX_AMOUNT_5'
			  , p_content_i      => ltrim( to_char( r_ohr.tax_amount_5, 'fm999999990.990'))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_ORDER_REFERENCE'
			  , p_content_i      => r_ohr.order_reference
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_PROFORMA_INVOICE_NUM'
			  , p_content_i      => r_ohr.proforma_invoice_num
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TRAX_ID'
			  , p_content_i      => r_ohr.trax_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_START_BY_DATE'
			  , p_content_i      => to_char( r_ohr.start_by_date, 'DD-MM-YYYY')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_START_BY_TIME'
			  , p_content_i      => to_char( r_ohr.start_by_date, 'HH24:MI:SS')
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_EXCLUDE_POSTCODE'
			  , p_content_i      => r_ohr.exclude_postcode
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_METAPACK_CARRIER_PRE'
			  , p_content_i      => r_ohr.metapack_carrier_pre
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_GROSS_WEIGHT'
			  , p_content_i      => to_char( r_ohr.gross_weight, 'fm99999990.90')
			  );
		-- new fields after 2009
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MASTER_ORDER'
			  , p_content_i      => r_ohr.master_order
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MASTER_ORDER_ID'
			  , p_content_i      => r_ohr.master_order_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TM_STOP_SEQ'
			  , p_content_i      => r_ohr.tm_stop_seq
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TM_STOP_NAM'
			  , p_content_i      => r_ohr.tm_stop_nam
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_FREIGHT_CURRENCY'
			  , p_content_i      => r_ohr.freight_currency
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SOFT_ALLOCATED'
			  , p_content_i      => r_ohr.soft_allocated
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MRN'
			  , p_content_i      => r_ohr.mrn
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_NCTS'
			  , p_content_i      => r_ohr.ncts
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MPACK_CONSIGNMENT'
			  , p_content_i      => r_ohr.mpack_consignment
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MPACK_PRE_CAR_ERR'
			  , p_content_i      => r_ohr.mpack_pre_car_err
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MPACK_PACK_ERR'
			  , p_content_i      => r_ohr.mpack_pack_err
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MPACK_NOMINATED_DSTAMP'
			  , p_content_i      => r_ohr.mpack_nominated_dstamp
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MPACK_PRE_CAR_DSTAMP'
			  , p_content_i      => r_ohr.mpack_pre_car_dstamp
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_MPACK_PACK_DSTAMP'
			  , p_content_i      => r_ohr.mpack_pack_dstamp
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_GLN'
			  , p_content_i      => r_ohr.gln
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_HUB_GLN'
			  , p_content_i      => r_ohr.hub_gln
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_INV_GLN'
			  , p_content_i      => r_ohr.inv_gln
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_ALLOW_PALLET_PICK'
			  , p_content_i      => r_ohr.allow_pallet_pick
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SPLIT_SHIPPING_UNITS'
			  , p_content_i      => r_ohr.split_shipping_units
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_VOL_PCK_SSCC_LABEL'
			  , p_content_i      => r_ohr.vol_pck_sscc_label
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_ALLOCATION_PRIORITY'
			  , p_content_i      => r_ohr.allocation_priority
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_TRAX_USE_HUB_ADDR'
			  , p_content_i      => r_ohr.trax_use_hub_addr
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_CONSIGNMENT_GROUPING_ID'
			  , p_content_i      => r_ohr.consignment_grouping_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_SHIPMENT_GROUPING_ID'
			  , p_content_i      => r_ohr.shipment_grouping_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_WORK_GROUPING_ID'
			  , p_content_i      => r_ohr.work_grouping_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_DIRECT_TO_STORE'
			  , p_content_i      => r_ohr.direct_to_store
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_VOL_CTR_LABEL_FORMAT'
			  , p_content_i      => r_ohr.vol_ctr_label_format
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_CE_ORDER_ID'
			  , p_content_i      => r_ohr.ce_order_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_RETAILER_ID'
			  , p_content_i      => r_ohr.retailer_id
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_FOREIGN_DOCUMENTATION'
			  , p_content_i      => r_ohr.foreign_documentation
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'OHR_CARRIER_BAGS'
			  , p_content_i      => r_ohr.carrier_bags
			  );

		-- Add Addresses
		-- SFM (If HUB Address is OLL or OLH (Carrier_ID), else SFM Address from ADDRESS table)
		if	r_ohr.hub_carrier_id in ('OLL','OLH')
		and 	r_ohr.hub_name       is not null
		then
			add_ads ( p_file_type_i        => p_file_type_i
				, p_field_prefix_i     => p_field_prefix_i
				, p_ads_type_i         => g_sfm
				, p_name_1_i           => r_ohr.hub_name
			        , p_address_1_i        => r_ohr.hub_address1
			        , p_city_i             => r_ohr.hub_town
			        , p_zip_code_i         => r_ohr.hub_postcode
			        , p_state_code_i       => r_ohr.hub_county
			        , p_cty_iso_i          => r_ohr.hub_country
			        , p_address_2_i        => r_ohr.hub_address2
			        , p_address_3_i        => null
			        , p_address_4_i        => null
			        , p_phone_i            => r_ohr.hub_contact_phone
			        , p_mobile_i           => r_ohr.hub_contact_mobile
			        , p_fax_i              => r_ohr.hub_contact_fax
			        , p_email_i            => r_ohr.hub_contact_email
			        , p_contact_name_i     => r_ohr.hub_contact
			        , p_web_i              => null
			        , p_ads_udf_type_1_i   => null
			        , p_ads_udf_type_2_i   => null
			        , p_ads_udf_type_3_i   => null
			        , p_ads_udf_type_4_i   => null
			        , p_ads_udf_type_5_i   => null
			        , p_ads_udf_type_6_i   => null
			        , p_ads_udf_type_7_i   => null
			        , p_ads_udf_type_8_i   => null
			        , p_ads_udf_num_1_i    => null
			        , p_ads_udf_num_2_i    => null
			        , p_ads_udf_num_3_i    => null
			        , p_ads_udf_num_4_i    => null
			        , p_ads_udf_chk_1_i    => null
			        , p_ads_udf_chk_2_i    => null
			        , p_ads_udf_chk_3_i    => null
			        , p_ads_udf_chk_4_i    => null
			        , p_ads_udf_dstamp_1_i => null
			        , p_ads_udf_dstamp_2_i => null
			        , p_ads_udf_dstamp_3_i => null
			        , p_ads_udf_dstamp_4_i => null
			        , p_ads_udf_note_1_i   => null
			        , p_ads_udf_note_2_i   => null
			        , p_directions_i       => null
			        , p_vat_number_i       => r_ohr.hub_vat_number
			        , p_address_type_1     => null
			        , p_address_id_i       => null
			        );
		else
			open	c_ads( b_client_id  => p_client_id_i
				     , b_address_id => g_sfm
				     );
			fetch 	c_ads
			into  	r_ads;
			if 	c_ads%found
			then
				add_ads ( p_file_type_i        => p_file_type_i
					, p_field_prefix_i     => p_field_prefix_i
					, p_ads_type_i         => g_sfm
					, p_name_1_i           => r_ads.name
					, p_address_1_i        => r_ads.address1
					, p_city_i             => r_ads.town
					, p_zip_code_i         => r_ads.postcode
					, p_state_code_i       => r_ads.county
					, p_cty_iso_i          => r_ads.country
					, p_address_2_i        => r_ads.address2
					, p_address_3_i        => null
					, p_address_4_i        => null
					, p_phone_i            => r_ads.contact_phone
					, p_mobile_i           => r_ads.contact_mobile
					, p_fax_i              => r_ads.contact_fax
					, p_email_i            => r_ads.contact_email
					, p_contact_name_i     => r_ads.contact_name
					, p_web_i              => r_ads.url
					, p_ads_udf_type_1_i   => r_ads.user_def_type_1
					, p_ads_udf_type_2_i   => r_ads.user_def_type_2
					, p_ads_udf_type_3_i   => r_ads.user_def_type_3
					, p_ads_udf_type_4_i   => r_ads.user_def_type_4
					, p_ads_udf_type_5_i   => r_ads.user_def_type_5
					, p_ads_udf_type_6_i   => r_ads.user_def_type_6
					, p_ads_udf_type_7_i   => r_ads.user_def_type_7
					, p_ads_udf_type_8_i   => r_ads.user_def_type_8
					, p_ads_udf_num_1_i    => r_ads.user_def_num_1
					, p_ads_udf_num_2_i    => r_ads.user_def_num_2
					, p_ads_udf_num_3_i    => r_ads.user_def_num_3
					, p_ads_udf_num_4_i    => r_ads.user_def_num_4
					, p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
					, p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
					, p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
					, p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
					, p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
					, p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
					, p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
					, p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
					, p_ads_udf_note_1_i   => r_ads.user_def_note_1
					, p_ads_udf_note_2_i   => r_ads.user_def_note_2
					, p_directions_i       => r_ads.directions
					, p_vat_number_i       => r_ads.vat_number
					, p_address_type_1     => r_ads.address_type
					, p_address_id_i       => r_ads.address_id
					);
			end if;
			close	c_ads;        
		end if;
		-- HUB - Hub Address
  	        add_ads( p_file_type_i        => p_file_type_i
		       , p_field_prefix_i     => p_field_prefix_i
		       , p_ads_type_i         => g_hub
		       , p_name_1_i           => r_ohr.hub_name
		       , p_address_1_i        => r_ohr.hub_address1
		       , p_city_i             => r_ohr.hub_town
		       , p_zip_code_i         => r_ohr.hub_postcode
		       , p_state_code_i       => r_ohr.hub_county
		       , p_cty_iso_i          => r_ohr.hub_country
		       , p_address_2_i        => r_ohr.hub_address2
		       , p_address_3_i        => null
		       , p_address_4_i        => null
		       , p_phone_i            => r_ohr.hub_contact_phone
		       , p_mobile_i           => r_ohr.hub_contact_mobile
		       , p_fax_i              => r_ohr.hub_contact_fax
		       , p_email_i            => r_ohr.hub_contact_email
		       , p_contact_name_i     => r_ohr.hub_contact
		       , p_web_i              => null
		       , p_ads_udf_type_1_i   => null
		       , p_ads_udf_type_2_i   => null
		       , p_ads_udf_type_3_i   => null
		       , p_ads_udf_type_4_i   => null
		       , p_ads_udf_type_5_i   => null
		       , p_ads_udf_type_6_i   => null
		       , p_ads_udf_type_7_i   => null
		       , p_ads_udf_type_8_i   => null
		       , p_ads_udf_num_1_i    => null
		       , p_ads_udf_num_2_i    => null
		       , p_ads_udf_num_3_i    => null
		       , p_ads_udf_num_4_i    => null
		       , p_ads_udf_chk_1_i    => null
		       , p_ads_udf_chk_2_i    => null
		       , p_ads_udf_chk_3_i    => null
		       , p_ads_udf_chk_4_i    => null
		       , p_ads_udf_dstamp_1_i => null
		       , p_ads_udf_dstamp_2_i => null
		       , p_ads_udf_dstamp_3_i => null
		       , p_ads_udf_dstamp_4_i => null
		       , p_ads_udf_note_1_i   => null
		       , p_ads_udf_note_2_i   => null
		       , p_directions_i       => null
		       , p_vat_number_i       => r_ohr.hub_vat_number
		       , p_address_type_1     => null
		       , p_address_id_i       => null
		       );
		-- STO - Delivery Address
		add_ads( p_file_type_i        => p_file_type_i
		       , p_field_prefix_i     => p_field_prefix_i
	               , p_ads_type_i         => g_sto
		       , p_name_1_i           => r_ohr.name
		       , p_address_1_i        => r_ohr.address1
		       , p_city_i             => r_ohr.town
		       , p_zip_code_i         => r_ohr.postcode
		       , p_state_code_i       => r_ohr.county
		       , p_cty_iso_i          => r_ohr.country
		       , p_address_2_i        => r_ohr.address2
		       , p_address_3_i        => null
		       , p_address_4_i        => null
		       , p_phone_i            => r_ohr.contact_phone
		       , p_mobile_i           => r_ohr.contact_mobile
		       , p_fax_i              => r_ohr.contact_fax
		       , p_email_i            => r_ohr.contact_email
		       , p_contact_name_i     => r_ohr.contact
		       , p_web_i              => null
		       , p_ads_udf_type_1_i   => null
		       , p_ads_udf_type_2_i   => null
		       , p_ads_udf_type_3_i   => null
		       , p_ads_udf_type_4_i   => null
		       , p_ads_udf_type_5_i   => null
		       , p_ads_udf_type_6_i   => null
		       , p_ads_udf_type_7_i   => null
		       , p_ads_udf_type_8_i   => null
		       , p_ads_udf_num_1_i    => null
		       , p_ads_udf_num_2_i    => null
		       , p_ads_udf_num_3_i    => null
		       , p_ads_udf_num_4_i    => null
		       , p_ads_udf_chk_1_i    => null
		       , p_ads_udf_chk_2_i    => null
		       , p_ads_udf_chk_3_i    => null
		       , p_ads_udf_chk_4_i    => null
		       , p_ads_udf_dstamp_1_i => null
		       , p_ads_udf_dstamp_2_i => null
		       , p_ads_udf_dstamp_3_i => null
		       , p_ads_udf_dstamp_4_i => null
		       , p_ads_udf_note_1_i   => null
		       , p_ads_udf_note_2_i   => null
		       , p_directions_i       => null
		       , p_vat_number_i       => r_ohr.vat_number
		       , p_address_type_1     => null
		       , p_address_id_i       => r_ohr.customer_id
		       );

		-- SHP - Shipper Address
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => g_shp
			     );
		fetch 	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
		        add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_shp
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       );
		end if;
		close	c_ads;        

		-- BTO - Invoice Address
		add_ads( p_file_type_i        => p_file_type_i
		       , p_field_prefix_i     => p_field_prefix_i
		       , p_ads_type_i         => g_bto
		       , p_name_1_i           => r_ohr.inv_name
		       , p_address_1_i        => r_ohr.inv_address1
		       , p_city_i             => r_ohr.inv_town
		       , p_zip_code_i         => r_ohr.inv_postcode
   		       , p_state_code_i       => r_ohr.inv_county
		       , p_cty_iso_i          => r_ohr.inv_country
		       , p_address_2_i        => r_ohr.inv_address2
		       , p_address_3_i        => null
		       , p_address_4_i        => null
		       , p_phone_i            => r_ohr.inv_contact_phone
		       , p_mobile_i           => r_ohr.inv_contact_mobile
		       , p_fax_i              => r_ohr.inv_contact_fax
		       , p_email_i            => r_ohr.inv_contact_email
		       , p_contact_name_i     => r_ohr.inv_contact
		       , p_web_i              => null
		       , p_ads_udf_type_1_i   => null
		       , p_ads_udf_type_2_i   => null
		       , p_ads_udf_type_3_i   => null
		       , p_ads_udf_type_4_i   => null
		       , p_ads_udf_type_5_i   => null
		       , p_ads_udf_type_6_i   => null
		       , p_ads_udf_type_7_i   => null
		       , p_ads_udf_type_8_i   => null
		       , p_ads_udf_num_1_i    => null
		       , p_ads_udf_num_2_i    => null
		       , p_ads_udf_num_3_i    => null
		       , p_ads_udf_num_4_i    => null
		       , p_ads_udf_chk_1_i    => null
		       , p_ads_udf_chk_2_i    => null
		       , p_ads_udf_chk_3_i    => null
		       , p_ads_udf_chk_4_i    => null
		       , p_ads_udf_dstamp_1_i => null
		       , p_ads_udf_dstamp_2_i => null
		       , p_ads_udf_dstamp_3_i => null
		       , p_ads_udf_dstamp_4_i => null
		       , p_ads_udf_note_1_i   => null
		       , p_ads_udf_note_2_i   => null
		       , p_directions_i       => null
		       , p_vat_number_i       => r_ohr.inv_vat_number
		       , p_address_type_1     => null
		       , p_address_id_i       => r_ohr.inv_address_id
		       );

		-- CRR - Carrier Address
		open  	c_crr( b_client_id     => p_client_id_i
			     , b_carrier_id    => r_ohr.carrier_id
			     , b_service_level => r_ohr.service_level
			     );
		fetch	c_crr
		into  	r_crr;
		if 	c_crr%found
		then
			add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_crr
			       , p_name_1_i           => r_crr.name
			       , p_address_1_i        => r_crr.address1
			       , p_city_i             => r_crr.town
			       , p_zip_code_i         => r_crr.postcode
			       , p_state_code_i       => r_crr.county
			       , p_cty_iso_i          => r_crr.country
			       , p_address_2_i        => r_crr.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_crr.contact_phone
			       , p_mobile_i           => r_crr.contact_mobile
			       , p_fax_i              => r_crr.contact_fax
			       , p_email_i            => r_crr.contact_email
			       , p_contact_name_i     => r_crr.contact_name
			       , p_web_i              => r_crr.url
			       , p_ads_udf_type_1_i   => r_crr.user_def_type_1
			       , p_ads_udf_type_2_i   => r_crr.user_def_type_2
			       , p_ads_udf_type_3_i   => r_crr.user_def_type_3
			       , p_ads_udf_type_4_i   => r_crr.user_def_type_4
			       , p_ads_udf_type_5_i   => r_crr.user_def_type_5
			       , p_ads_udf_type_6_i   => r_crr.user_def_type_6
			       , p_ads_udf_type_7_i   => r_crr.user_def_type_7
			       , p_ads_udf_type_8_i   => r_crr.user_def_type_8
			       , p_ads_udf_num_1_i    => r_crr.user_def_num_1
			       , p_ads_udf_num_2_i    => r_crr.user_def_num_2
			       , p_ads_udf_num_3_i    => r_crr.user_def_num_3
			       , p_ads_udf_num_4_i    => r_crr.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_crr.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_crr.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_crr.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_crr.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_crr.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_crr.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_crr.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_crr.user_def_date_4
			       , p_ads_udf_note_1_i   => r_crr.user_def_note_1
			       , p_ads_udf_note_2_i   => r_crr.user_def_note_2
			       , p_directions_i       => r_crr.notes
			       , p_vat_number_i       => null
			       , p_address_type_1     => null
			       , p_address_id_i       => null
			       );
		end if;
		close 	c_crr;        

		-- ADL - Additional Address (CID in Order)
		open  	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => r_ohr.cid_number
			     );
		fetch	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_adl
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       );
		end if;
		close 	c_ads;        

		-- CID - Consignee ID Address (CID in Order)
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => r_ohr.cid_number
			     );
		fetch	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_cid
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       );
		end if;
		close	c_ads;        

		-- SID - Shipment ID Address (SID in Order)
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => r_ohr.sid_number
			     );
		fetch 	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_sid
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       );
		end if;
		close 	c_ads;        

		-- CLT - Client Address
		open	c_clt( b_client_id => p_client_id_i);
		fetch 	c_clt
		into  	r_clt;
		if 	c_clt%found
		then
			add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_clt
			       , p_name_1_i           => r_clt.name
			       , p_address_1_i        => r_clt.address1
			       , p_city_i             => r_clt.town
			       , p_zip_code_i         => r_clt.postcode
			       , p_state_code_i       => r_clt.county
			       , p_cty_iso_i          => r_clt.country
			       , p_address_2_i        => r_clt.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_clt.contact_phone
			       , p_mobile_i           => r_clt.contact_mobile
			       , p_fax_i              => r_clt.contact_fax
			       , p_email_i            => r_clt.contact_email
			       , p_contact_name_i     => r_clt.contact_name
			       , p_web_i              => r_clt.url
			       , p_ads_udf_type_1_i   => r_clt.user_def_type_1
			       , p_ads_udf_type_2_i   => r_clt.user_def_type_2
			       , p_ads_udf_type_3_i   => r_clt.user_def_type_3
			       , p_ads_udf_type_4_i   => r_clt.user_def_type_4
			       , p_ads_udf_type_5_i   => r_clt.user_def_type_5
			       , p_ads_udf_type_6_i   => r_clt.user_def_type_6
			       , p_ads_udf_type_7_i   => r_clt.user_def_type_7
			       , p_ads_udf_type_8_i   => r_clt.user_def_type_8
			       , p_ads_udf_num_1_i    => r_clt.user_def_num_1
			       , p_ads_udf_num_2_i    => r_clt.user_def_num_2
			       , p_ads_udf_num_3_i    => r_clt.user_def_num_3
			       , p_ads_udf_num_4_i    => r_clt.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_clt.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_clt.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_clt.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_clt.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_clt.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_clt.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_clt.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_clt.user_def_date_4
			       , p_ads_udf_note_1_i   => r_clt.user_def_note_1
			       , p_ads_udf_note_2_i   => r_clt.user_def_note_2
			       , p_directions_i       => r_clt.notes
			       , p_vat_number_i       => r_clt.vat_number
			       , p_address_type_1     => null
			       , p_address_id_i       => null
			       );
		end if;	
		close c_clt;        

		-- HAZ - Hazardous Goods Shipper Address (AddressID defined in Constants table e.g. 'HAZARDOUS' )
		open	c_ads( b_client_id  => p_client_id_i
			     , b_address_id => g_wms_hazardous_ads_id
			     );
		fetch	c_ads
		into  	r_ads;
		if 	c_ads%found
		then
			add_ads( p_file_type_i        => p_file_type_i
			       , p_field_prefix_i     => p_field_prefix_i
			       , p_ads_type_i         => g_haz
			       , p_name_1_i           => r_ads.name
			       , p_address_1_i        => r_ads.address1
			       , p_city_i             => r_ads.town
			       , p_zip_code_i         => r_ads.postcode
			       , p_state_code_i       => r_ads.county
			       , p_cty_iso_i          => r_ads.country
			       , p_address_2_i        => r_ads.address2
			       , p_address_3_i        => null
			       , p_address_4_i        => null
			       , p_phone_i            => r_ads.contact_phone
			       , p_mobile_i           => r_ads.contact_mobile
			       , p_fax_i              => r_ads.contact_fax
			       , p_email_i            => r_ads.contact_email
			       , p_contact_name_i     => r_ads.contact_name
			       , p_web_i              => r_ads.url
			       , p_ads_udf_type_1_i   => r_ads.user_def_type_1
			       , p_ads_udf_type_2_i   => r_ads.user_def_type_2
			       , p_ads_udf_type_3_i   => r_ads.user_def_type_3
			       , p_ads_udf_type_4_i   => r_ads.user_def_type_4
			       , p_ads_udf_type_5_i   => r_ads.user_def_type_5
			       , p_ads_udf_type_6_i   => r_ads.user_def_type_6
			       , p_ads_udf_type_7_i   => r_ads.user_def_type_7
			       , p_ads_udf_type_8_i   => r_ads.user_def_type_8
			       , p_ads_udf_num_1_i    => r_ads.user_def_num_1
			       , p_ads_udf_num_2_i    => r_ads.user_def_num_2
			       , p_ads_udf_num_3_i    => r_ads.user_def_num_3
			       , p_ads_udf_num_4_i    => r_ads.user_def_num_4
			       , p_ads_udf_chk_1_i    => r_ads.user_def_chk_1
			       , p_ads_udf_chk_2_i    => r_ads.user_def_chk_2
			       , p_ads_udf_chk_3_i    => r_ads.user_def_chk_3
			       , p_ads_udf_chk_4_i    => r_ads.user_def_chk_4
			       , p_ads_udf_dstamp_1_i => r_ads.user_def_date_1
			       , p_ads_udf_dstamp_2_i => r_ads.user_def_date_2
			       , p_ads_udf_dstamp_3_i => r_ads.user_def_date_3
			       , p_ads_udf_dstamp_4_i => r_ads.user_def_date_4
			       , p_ads_udf_note_1_i   => r_ads.user_def_note_1
			       , p_ads_udf_note_2_i   => r_ads.user_def_note_2
			       , p_directions_i       => r_ads.directions
			       , p_vat_number_i       => r_ads.vat_number
			       , p_address_type_1     => r_ads.address_type
			       , p_address_id_i       => r_ads.address_id
			       );
		end if;
		close	c_ads;        
		close	c_ohr;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finihed adding SMT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

	exception
		when	others
		then
			case 
			when	c_ohr%isopen
			then
				close	c_ohr;
			when 	c_ocr%isopen
			then
				close 	c_ocr;
			when 	c_clt%isopen
			then
				close 	c_clt;
			when 	c_crr%isopen
			then
				close 	c_crr;
			when 	c_ads%isopen
			then
				close 	c_ads;
			else
				null;
			end case;
	end add_smt;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create StreamServe Move Task block for normal Packlist file
------------------------------------------------------------------------------------------------
	procedure add_mtk( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_order_nr_i     in  varchar2
			 )
	is
		cursor c_mtk( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select 	mtka.list_id                    list_id
			,      	mtkb.qty_orders                 qty_orders
			,      	lpad(mtka.seq_num,2,0)          order_seq_num
			,      	lpad(mtka.print_label_id,10,0)  print_label_id
			from   	(
				select  rownum               seq_num
				,       mtk1.client_id       client_id
				,       mtk2.list_id         list_id
				,       mtk1.task_id         task_id
				,       mtk1.print_label_id  print_label_id
				from    dcsdba.move_task     mtk1
				,       dcsdba.move_task     mtk2
				where   mtk1.client_id       = mtk2.client_id
				and     mtk1.list_id         = mtk2.list_id
				and     mtk1.sku_id          = mtk2.sku_id
				and     mtk1.sku_id          = 'DOCUMENT'
				and     mtk1.client_id       = b_client_id
				and     mtk2.task_id         = b_order_id
				order   by mtk1.print_label_id
				)       mtka
			,     	(
				select  mtk.client_id                   client_id
				,       mtk.list_id                     list_id
				,       count(distinct mtk.task_id)     qty_orders
				from    dcsdba.move_task mtk
				where   mtk.client_id   = b_client_id
				group   by mtk.client_id
				,       mtk.list_id
				)       mtkb
			where 	mtka.client_id  = mtkb.client_id
			and   	mtka.list_id    = mtkb.list_id
			and   	mtka.task_id    = b_order_id
		;
		--
		r_mtk	c_mtk%rowtype;
		l_rtn	varchar2(30) := 'add_mtk';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding MTK'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		open	c_mtk( b_client_id => p_client_id_i
			     , b_order_id => p_order_nr_i
			     );
		fetch 	c_mtk
		into  	r_mtk;
		--
		if 	c_mtk%found
		then            
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_MAIN_LIST'
				  , p_content_i      => r_mtk.list_id
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_TOTAL_ORDER_PER_LIST'
				  , p_content_i      => r_mtk.qty_orders
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_ORDER_SEQ_NUM'
				  , p_content_i      => r_mtk.order_seq_num
				  );
			write_line( p_file_type_i    => p_file_type_i
				  , p_field_prefix_i => p_field_prefix_i
				  , p_field_name_i   => 'MTK_PRINT_LABEL_ID'
				  , p_content_i     => r_mtk.print_label_id
				  );
		end if;
		close 	c_mtk;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding MTK'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
								   , p_order_id_i		=> p_order_nr_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
	exception
		when others
		then
			case	c_mtk%isopen
			when 	true
			then
				close 	c_mtk;
			else
				null;
			end 	case;
	end add_mtk;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create StreamServe Header block
------------------------------------------------------------------------------------------------
	procedure add_ptr( p_file_type_i         in  utl_file.file_type
			 , p_field_prefix_i      in  varchar2
			 , p_segment_nr_i        in  number
			 , p_jrp_key_i           in  number
			 , p_template_name_i     in  varchar2
			 , p_ptr_template_name_i in  varchar2
			 , p_ptr_name_i          in  varchar2
			 , p_ptr_unc_path_i      in  varchar2
			 , p_copies_i            in  number
			 , p_print_yn_i          in  varchar2
			 , p_eml_addresses_to_i  in  varchar2
			 , p_eml_addresses_bcc_i in  varchar2
			 , p_email_yn_i          in  varchar2
			 , p_email_attachment_i  in  varchar2
			 , p_email_subject_i     in  varchar2
			 , p_email_message_i     in  varchar2
			 , p_pdf_link_yn_i       in  varchar2
			 , p_pdf_autostore_i     in  varchar2
			 )
	is
		l_begin varchar2(100);
		l_rtn	varchar2(30) := 'add_ptr';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding PTR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"Segment_nr" "'||p_segment_nr_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_SEGMENT'
			  , p_content_i      => 'Segment Printer: '
			  || to_char( p_segment_nr_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_JRP_KEY'
			  , p_content_i      =>  p_jrp_key_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_DOC_TYPE'
			  , p_content_i      => p_template_name_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_MAIN_DOC_TYPE'
			  , p_content_i      => p_ptr_template_name_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_PRINTER'
			  , p_content_i      => p_ptr_name_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_PATH'
			  , p_content_i      => p_ptr_unc_path_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_COPIES'
			  , p_content_i      => to_char( nvl( p_copies_i, 1))
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_ACTIVE'
			  , p_content_i      => p_print_yn_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_EMAIL_TO'
			  , p_content_i      => p_eml_addresses_to_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_EMAIL_BCC'
			  , p_content_i      => p_eml_addresses_bcc_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_EMAIL_ACTIVE'
			  , p_content_i      => p_email_yn_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_EMAIL_ATTACHMENT_NAME'
			  , p_content_i      => p_email_attachment_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_EMAIL_SUBJECT'
			  , p_content_i      => p_email_subject_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_EMAIL_BODY'
			  , p_content_i      => p_email_message_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_PDF_LINK'
			  , p_content_i      => p_pdf_link_yn_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'PTR_PDF_AUTOSTORE'
			  , p_content_i      => p_pdf_autostore_i
			  );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding PTR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"Segment_nr" "'||p_segment_nr_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
	end add_ptr;  
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create StreamServe Header block
------------------------------------------------------------------------------------------------
	procedure add_hdr( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_site_id_i      in  varchar2
			 , p_client_id_i    in  varchar2
			 , p_owner_id_i     in  varchar2
			 , p_order_id_i     in  varchar2
			 , p_user_i         in  varchar2
			 , p_workstation_i  in  varchar2
			 , p_locality_i     in  varchar2
			 )
	is
		cursor c_ohr( b_client_id in varchar2
			    , b_order_id  in varchar2
			    )
		is
			select	ohr.carrier_id
			,      	ohr.service_level
			from   	dcsdba.order_header ohr
			where  	ohr.client_id = b_client_id
			and    	ohr.order_id  = b_order_id
		;
		--
		r_ohr	c_ohr%rowtype;
		--
		l_begin varchar2(100);
		l_rtn	varchar2(30) := 'add_hdr';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_field_prefix_i
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" "user_id" "'||p_user_i||'"workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		open	c_ohr( b_client_id => p_client_id_i
			     , b_order_id  => p_order_id_i
			     );
		fetch 	c_ohr
		into  	r_ohr;
		close 	c_ohr;
		--
		l_begin	:= upper( p_client_id_i)|| '_'|| upper( p_owner_id_i)|| '_'|| g_wms|| '_EVENT';
		--	
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => null
			  , p_field_name_i   => 'BEGIN'
			  , p_content_i      => l_begin
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_DATABASE'
			  , p_content_i      => g_streamserve_wms_db
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_APPLICATION_CODE'
			  , p_content_i      => g_wms
			  );
		 write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_RELATION_NAME_CTM'
			  , p_content_i      => upper( p_client_id_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_SITE_ID'
			  , p_content_i      => upper( p_site_id_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_CLIENT_ID'
			  , p_content_i      => upper( p_client_id_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_OWNER_ID'
			  , p_content_i      => upper( p_owner_id_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_CARRIER_CODE'
			  , p_content_i      => upper( r_ohr.carrier_id)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_CARRIER_SERVICE'
			  , p_content_i      => upper( r_ohr.service_level)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_USER'
			  , p_content_i      => upper( p_user_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_WORKSTATION'
			  , p_content_i      => upper( p_workstation_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_LOCALITY'
			  , p_content_i      => upper( p_locality_i)
			  );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" "user_id" "'||p_user_i||'"workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
	end add_hdr;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-Sep-2018
-- Purpose : Create StreamServe Trolley Header block
------------------------------------------------------------------------------------------------
	procedure add_trl( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_site_id_i      in  varchar2
			 , p_list_id_i      in  varchar2
			 , p_user_i         in  varchar2
			 , p_workstation_i  in  varchar2
			 )
	is
		-- COunt number of containers on list
		cursor c_cnt( b_list_id varchar2
			    , b_site_id varchar2
			    )
		is
			select  count(distinct to_container_id)
			from    dcsdba.move_task mkt
			where   mkt.list_id = b_list_id
			and     mkt.site_id = b_site_id
		;

		-- Count unique number of carton types
		cursor c_tpe( b_list_id varchar2
			    , b_site_id varchar2
			    )
		is
			select  count(distinct to_container_config)
			from    dcsdba.move_task mkt
			where   mkt.list_id = b_list_id
			and     mkt.site_id = b_site_id
		;

		--
		r_cnt   number;
		r_tpe   number;
		l_begin varchar2(100);
		l_rtn	varchar2(30) := 'add_trl';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_field_prefix_i||'_HDR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"list_id" "'||p_list_id_i||'" '
												|| '"user_id" "'||p_user_i||'" '
												|| '"workstation" "'||p_workstation_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		open    c_cnt( p_list_id_i, p_site_id_i);
		fetch   c_cnt into r_cnt;
		close   c_cnt;
		open    c_tpe( p_list_id_i, p_site_id_i);
		fetch   c_tpe into r_tpe;
		close   c_tpe;
		--
		l_begin := 'PICK_TROLLEY_EVENT';
		--
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => null
			  , p_field_name_i   => 'BEGIN'
			  , p_content_i      => l_begin
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_DATABASE'
			  , p_content_i      => g_streamserve_wms_db
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_APPLICATION_CODE'
			  , p_content_i      => g_wms
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_SITE_ID'
			  , p_content_i      => upper( p_site_id_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_USER'
			  , p_content_i      => upper( p_user_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_WORKSTATION'
			  , p_content_i      => upper( p_workstation_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_LIST_ID'
			  , p_content_i      => upper( p_list_id_i)
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_NBR_CONTAINERS'
			  , p_content_i      => r_cnt
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'HDR_NBR_CONTAINER_TYPES'
			  , p_content_i      => r_tpe
			  );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i||'_HDR'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
												|| '"list_id" "'||p_list_id_i||'" '
												|| '"user_id" "'||p_user_i||'" '
												|| '"workstation" "'||p_workstation_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
	end add_trl;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-Sep-2018
-- Purpose : Create StreamServe TRL_CNT block
------------------------------------------------------------------------------------------------
	procedure add_cnt( p_file_type_i    in  utl_file.file_type
			 , p_field_prefix_i in  varchar2
			 , p_cnt_type_i     in  varchar2
			 , p_cnt_qty_i      in  number  
			 , p_width_i        in  number
			 , p_depth_i        in  number
			 , p_height_i       in  number
			 , p_volume_i       in  number
			 , p_weight_i       in  number
			 , p_segment_id_i   in  number
			 )
	is	
		l_rtn	varchar2(30) := 'add_cnt';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_field_prefix_i||'_CNT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
											        || '"container_type" "'||p_cnt_type_i||'" '
												|| '"cnt_qty" "'||p_cnt_qty_i||'" '
												|| '"segment_id" "'||p_segment_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_SEGMENT'
			  , p_content_i      => 'Segment container type: ' || p_segment_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
		          , p_field_name_i   => 'CNT_TYPE'
		          , p_content_i      => p_cnt_type_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_QTY'
			  , p_content_i      => p_cnt_qty_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_WIDTH_M'
			  , p_content_i      => p_width_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_DEPTH_M'
			  , p_content_i      => p_width_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_HEIGHT_M'
			  , p_content_i      => p_height_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_VOLUME_M3'
			  , p_content_i      => p_volume_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CNT_WEIGHT_KG'
			  , p_content_i      => p_weight_i
			  );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i||'_CNT'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
											        || '"container_type" "'||p_cnt_type_i||'" '
												|| '"cnt_qty" "'||p_cnt_qty_i||'" '
												|| '"segment_id" "'||p_segment_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;
	end add_cnt;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-Sep-2018
-- Purpose : Create StreamServe TRL_CON, OHR, OLE block
------------------------------------------------------------------------------------------------
	procedure add_con( p_file_type_i      in  utl_file.file_type
			 , p_field_prefix_i   in  varchar2
			 , p_container_id_i   in  varchar2
			 , p_container_type_i in  varchar2
			 , p_slot_id_i        in  varchar2
			 , p_order_id_i       in  varchar2
			 , p_client_id_i      in  varchar2
			 , p_carrier_id_i     in varchar2
			 , p_service_level_i  in varchar2
			 , p_ship_dock_i      in varchar2
			 , p_stage_route_id_i in varchar2
			 , p_repack_loc_id_i  in varchar2
			 , p_work_group_i     in varchar2
			 , p_consignment_i    in varchar2
			 , p_shipment_group_i in varchar2
			 , p_segment_id_i     in  number
			 )
	is
		l_rtn	varchar2(30) := 'add_con';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding '||p_field_prefix_i||'_CON'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
											        || '"container_type" "'||p_container_type_i||'" '
												|| '"slot_id" "'||p_slot_id_i||'" '
												|| '"segment_id" "'||p_segment_id_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;

		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_SEGMENT'
			  , p_content_i      => 'Segment container: ' || p_segment_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_CONTAINER_ID'
			  , p_content_i      => p_container_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_CONTAINER_TYPE'
			  , p_content_i      => p_container_type_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_SLOT_ID'
			  , p_content_i      => p_slot_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_ORDER_ID'
			  , p_content_i      => p_order_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_CLIENT_ID'
			  , p_content_i      => p_client_id_i 
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_CARRIER_ID'
			  , p_content_i      => p_carrier_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_SERVICE_LEVEL'
			  , p_content_i      => p_service_level_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_SHIP_DOCK'
			  , p_content_i      => p_ship_dock_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_STAGE_ROUTE_ID'
			  , p_content_i      => p_stage_route_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_REPACK_LOC_ID'
			  , p_content_i      => p_repack_loc_id_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_WORK_GROUP'
			  , p_content_i      => p_work_group_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_CONSIGNMENT'
			  , p_content_i      => p_consignment_i
			  );
		write_line( p_file_type_i    => p_file_type_i
			  , p_field_prefix_i => p_field_prefix_i
			  , p_field_name_i   => 'CON_SHIPMENT_GROUP'
			  , p_content_i      => p_shipment_group_i
			  );
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding '||p_field_prefix_i||'_CON'
								   , p_code_parameters_i 	=> '"field_prefix" "'||p_field_prefix_i||'" '
											        || '"container_type" "'||p_container_type_i||'" '
												|| '"slot_id" "'||p_slot_id_i||'" '
												|| '"segment_id" "'||p_segment_id_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> null
								   );
		end if;
	end add_con;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Packlist file for StreamServe
------------------------------------------------------------------------------------------------
	procedure create_packlist( p_site_id_i		in  varchar2
				 , p_client_id_i     	in  varchar2
				 , p_owner_id_i      	in  varchar2
				 , p_order_id_i      	in  varchar2
				 , p_carrier_id_i    	in  varchar2  := null
				 , p_pallet_id_i     	in  varchar2  := null
				 , p_container_id_i  	in  varchar2  := null
				 , p_reprint_yn_i    	in  varchar2
				 , p_user_i          	in  varchar2
				 , p_workstation_i   	in  varchar2
				 , p_locality_i      	in  varchar2  := null
				 , p_report_name_i   	in  varchar2
				 , p_rtk_key         	in  integer
				 , p_pdf_link_i      	in  varchar2  := null -- filename for pdf link in e-mail (internal e-mail)
				 , p_pdf_autostore_i 	in  varchar2  := null -- filename for pdf created for autostore/maas
				 , p_run_task_i		in  dcsdba.run_task%rowtype
				 )
	is
		-- Get advance print mapping records 
		cursor	c_jrp( b_report_name	in varchar2
			     , b_site_id     	in varchar2
			     , b_client_id   	in varchar2
			     , b_owner_id    	in varchar2
			     , b_order_id    	in varchar2
			     , b_carrier_id  	in varchar2
			     , b_user_id     	in varchar2
			     , b_station_id  	in varchar2
			     , b_locality    	in varchar2
			     )
		is
			select	jrp.key
			,      	jrp.print_mode
			,      	jrp.report_name
			,      	jrp.template_name
			,      	jrp.site_id
			,      	jrp.client_id
			,      	jrp.owner_id
			,      	jrp.carrier_id
			,      	jrp.user_id
			,      	jrp.station_id
			,      	jrp.customer_id
			,      	jrp.extra_parameters
			,      	jrp.email_enabled
			,      	jrp.email_export_type
			,      	jrp.email_attachment
			,     	jrp.email_subject
			,      	jrp.email_message
			,      	jrp.copies
			,      	jrp.locality
			from   	dcsdba.java_report_map	jrp
			where  	jrp.report_name        	= b_report_name
			and    	(jrp.site_id	= nvl(b_site_id, jrp.site_id) 		or jrp.site_id		is null)
			and    	(jrp.client_id	= nvl(b_client_id, jrp.client_id)	or jrp.client_id        is null)
			and    	(jrp.owner_id	= nvl(b_owner_id, jrp.owner_id)		or jrp.owner_id		is null)
			and    	(jrp.carrier_id	= nvl(b_carrier_id, jrp.carrier_id)	or jrp.carrier_id	is null)
			and    	(jrp.user_id	= nvl(b_user_id, jrp.user_id)		or jrp.user_id		is null)
			and    	(jrp.station_id	= nvl(b_station_id, jrp.station_id)	or jrp.station_id	is null)
			and    	(jrp.locality	= b_locality				or jrp.locality		is null)
			and    	1		= cnl_wms_pck.is_ohr_restriction_valid( p_client_id_i	=> b_client_id
										      , p_order_id_i  	=> b_order_id
										      , p_where_i     	=> nvl( jrp.extra_parameters, '1=1')
										      )
			-- Below is to split documents that must be printed by Jaspersoft.
			and    	instr(lower(nvl(jrp.extra_parameters,'EMPTY')),'jaspersoft') = 0
			order  
			by 	jrp.template_name
			,      	jrp.station_id       	nulls last
			,      	jrp.locality         	nulls last
			,      	jrp.site_id          	nulls last
			,      	jrp.client_id        	nulls last
			,      	jrp.owner_id         	nulls last
			,      	jrp.carrier_id       	nulls last
			,      	jrp.user_id          	nulls last
			,      	jrp.extra_parameters	nulls last
		;

		-- Get printers or output method 
		cursor	c_jrt( b_key in number)
		is
			select	rownum
			,      	jrt.key
			,      	jrt.export_type
			,      	jrt.export_target
			,      	jrt.copies
			,      	jrt.template_name
			from   	dcsdba.java_report_export  jrt
			where  	jrt.key = b_key
			order  	
			by 	upper(jrt.export_target)
		;

		-- Get order lines
		cursor	c_ole( b_client_id	in varchar2
			     , b_order_id     	in varchar2
			     , b_pallet_id    	in varchar2
			     , b_container_id 	in varchar2
			     )
		is
			-- Get already picked lines without tasks
			select	distinct
				smt.client_id
			,      	smt.order_id
			,      	smt.line_id
			from   	dcsdba.shipping_manifest  smt
			where  	smt.client_id             = b_client_id
			and    	smt.order_id              = b_order_id
			and    	smt.pallet_id             = nvl( b_pallet_id, smt.pallet_id)
			and    	smt.container_id          = nvl( b_container_id, smt.container_id)
			union 
			-- Get tasked lines
			select	distinct
				mtk.client_id
			,     	mtk.task_id               order_id
			,      	mtk.line_id
			from   	dcsdba.move_task          mtk
			where  	mtk.client_id             = b_client_id
			and    	mtk.task_id               = b_order_id
			and    	mtk.pallet_id             = nvl( b_pallet_id, mtk.pallet_id)
			and    	mtk.container_id          = nvl( b_container_id, mtk.container_id)
			-- Get unallocatable and full short lines
			union
			select	distinct
				l.client_id
			,	l.order_id
			,	l.line_id
			from	dcsdba.order_line l
			where	l.order_id		= b_order_id
			and	l.client_id		= b_client_id
			and	(	nvl(l.unallocatable,'N') = 'Y' 
				or	nvl(qty_tasked,0) + nvl(qty_picked,0) = 0
				or	nvl(qty_ordered,0) = 0
				)	
			-- Only select when no specific pallet and/or container id is required
			and	b_pallet_id 		is null
			and	b_container_id 		is null
			order  
			by 	line_id
		;

		-- Cursor variables
		r_jrp			c_jrp%rowtype;
		r_jrt            	c_jrt%rowtype;
		r_ole            	c_ole%rowtype;

		-- Local variables
		l_file_type     	utl_file.file_type;
		l_file_name      	varchar2(100);
		l_jrp_key        	number;
		l_jrp_count      	number := 0;
		l_jrt_count      	number := 0;
		l_rtk_command    	varchar2(4000);        
		l_report_name    	varchar2(20);
		l_report         	varchar2(20);
		l_user_id        	varchar2(20);
		l_station_id     	varchar2(256);
		l_site_id        	varchar2(10);
		l_client_id      	varchar2(10);
		l_owner_id       	varchar2(10);
		l_carrier_id     	varchar2(25);
		l_order_id       	varchar2(20);
		l_pallet_id      	varchar2(20);
		l_container_id   	varchar2(20);
		l_locality       	varchar2(20);
		l_rdtlocality    	varchar2(20);
		l_linked_to_dws  	varchar2(1);
		l_print_yn       	varchar2(1) := g_no;
		l_printer        	varchar2(250);
		l_unc_path       	varchar2(250);
		l_copies         	number;
		l_template_name  	varchar2(250);
		l_export_target  	varchar2(250);
		l_eml_address    	varchar2(256);
		l_eml_select     	varchar2(4000);
		l_eml_ads_list   	varchar2(4000);
		l_email_yn       	varchar2(1) := g_no;
		l_eml_attachment 	varchar2(250);
		l_eml_subject    	varchar2(120);
		l_eml_message    	varchar2(4000);
		l_print_server   	varchar2(30);
		l_prev_template  	varchar2(250);
		l_ole_count      	number(10) := 0;
		l_pdf_link_name  	varchar2(256);
		l_rtn			varchar2(30) := 'create_packlist';
	begin
		-- Set global print id for logging
		g_print_id := p_rtk_key;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start create_packlist procedure'
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" "carrier_id" "'||p_carrier_id_i||'" "reprint_yn" "'||p_reprint_yn_i||'" "user_id" "'||p_user_i||'" "workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i||'" "report_name" "'|| p_report_name_i||'" "pdf_link" "'|| p_pdf_link_i||'" "pdf_autostore" "'|| p_pdf_autostore_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- Set time zone
		execute immediate 'alter session set time_zone = ''Europe/Berlin''';

		-- create a file first
		open_ssv_file ( p_site_id_i    => p_site_id_i
			      , p_client_id_i  => p_client_id_i
			      , p_owner_id_i   => p_owner_id_i
			      , p_file_type_o  => l_file_type
			      , p_file_name_o  => l_file_name
			      );
		g_file_name := l_file_name;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Opened new file'
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i|| '" "carrier_id" "'||p_carrier_id_i||'" "reprint_yn" "'||p_reprint_yn_i||'" "user_id" "'||p_user_i||'" "workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i||'" "report_name" "'|| p_report_name_i||'" "pdf_link" "'|| p_pdf_link_i||'" "pdf_autostore" "'|| p_pdf_autostore_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- add content
		if	utl_file.is_open ( file => l_file_type)
		then
			-- add header
			add_hdr( p_file_type_i    => l_file_type
			       , p_field_prefix_i => g_plt
			       , p_site_id_i      => p_site_id_i
			       , p_client_id_i    => p_client_id_i
			       , p_owner_id_i     => p_owner_id_i
			       , p_order_id_i     => p_order_id_i
			       , p_user_i         => p_user_i
			       , p_workstation_i  => p_workstation_i
			       , p_locality_i     => p_locality_i
			       );

			-- add printer_segment
			for	r_jrp in c_jrp( b_report_name => p_report_name_i -- always with UREPSSVPLT or UREPSSVPLTCON report name
					      , b_site_id     => p_site_id_i
					      , b_client_id   => p_client_id_i
					      , b_owner_id    => p_owner_id_i
					      , b_order_id    => p_order_id_i
					      , b_carrier_id  => p_carrier_id_i
					      , b_user_id     => p_user_i
					      , b_station_id  => p_workstation_i
					      , b_locality    => p_locality_i
					      )
			loop
				-- only create printer segments for every unique template_name (doc_type) once
				l_template_name := r_jrp.template_name;  -- set the current template 

				if 	nvl( l_prev_template, '|^|') != l_template_name -- check if the current template is the same as the previous one in the loop
				then                                               -- if not (so other doc_type) then create the segments for this template, else skip
					l_jrp_key   := r_jrp.key;

					-- check if email fields needs to be filled
					-- get the run task command
					l_rtk_command := p_run_task_i.command;

					-- get the email addresses			
					l_eml_ads_list	:= cnl_wms_pck.get_jr_email_recipients( p_jrp_key_i    => l_jrp_key
											      , p_parameters_i => l_rtk_command
											      ); 

					-- set email Y/N
					if 	l_eml_ads_list is not null
					then
						l_email_yn := g_yes;
					else
						l_email_yn := g_no;
					end if;

					-- check if it's e-mail only and create segment or run the loop for the printers
					-- Clean up variable
					r_jrt.rownum 		:= null;
					r_jrt.key 		:= null;
					r_jrt.export_type 	:= null;
					r_jrt.export_target 	:= null;
					r_jrt.copies 		:= null;
					r_jrt.template_name 	:= null;

					open	c_jrt ( b_key => l_jrp_key);
					fetch 	c_jrt
					into  	r_jrt;
					if 	c_jrt%notfound
					then
						close 	c_jrt;
						if 	l_email_yn = g_yes
						then
							-- create the unique filename for the pdf link file
							if 	p_pdf_link_i is not null
							then
								l_pdf_link_name := upper(nvl(r_jrt.template_name, l_template_name)
										|| '_'
										|| p_pdf_link_i
										|| '_'
										|| to_char(p_rtk_key)
										|| '_'
										|| to_char(nvl(l_jrp_key, 0)));
							else
								l_pdf_link_name	:= null;
							end if;

							--
							l_jrp_count 	:= l_jrp_count + 1;

							add_ptr( p_file_type_i         => l_file_type
							       , p_field_prefix_i      => g_plt
							       , p_segment_nr_i        => l_jrp_count
							       , p_jrp_key_i           => l_jrp_key
							       , p_template_name_i     => upper( l_template_name)
							       , p_ptr_template_name_i => upper( r_jrt.template_name)
							       , p_ptr_name_i          => null
							       , p_ptr_unc_path_i      => null
							       , p_copies_i            => null
							       , p_print_yn_i          => g_no
							       , p_eml_addresses_to_i  => l_eml_ads_list
							       , p_eml_addresses_bcc_i => null
							       , p_email_yn_i          => l_email_yn
							       , p_email_attachment_i  => r_jrp.email_attachment
							       , p_email_subject_i     => r_jrp.email_subject
							       , p_email_message_i     => r_jrp.email_message
							       , p_pdf_link_yn_i       => nvl(l_pdf_link_name, g_no)
							       , p_pdf_autostore_i     => nvl(p_pdf_autostore_i, g_no)
							       );
						end if;
					else
						close 	c_jrt;
						-- get the printer(s), copies and template
						for 	r_jrt2 in c_jrt ( b_key => l_jrp_key)
						loop
							-- set print Y/N and get print_server
							if	r_jrt2.export_type = 'P' -- Printer
							and 	r_jrt2.export_target is not null
							then
								l_print_yn      := g_yes;
								l_copies        := nvl( r_jrt2.copies, 1);
								l_export_target := r_jrt2.export_target;
								l_print_server  := get_print_server;
								if 	l_print_server is not null
								then 
									l_print_server 	:= '\\'
											|| upper( l_print_server)
											|| '\'
											;
								end if;
							else
								l_print_yn      := g_no;
								l_export_target := null;
								l_print_server  := null;
								l_copies        := null;
							end if;

							-- Only add email details if email_yn = Y in the first Java Report Export
							if	r_jrt2.rownum 	= 1
							and 	l_email_yn 	= g_yes
							then
								-- create the unique filename for the pdf link file
								if 	p_pdf_link_i is not null
								then
									l_pdf_link_name := upper(nvl(r_jrt2.template_name, l_template_name)
											|| '_'
											|| p_pdf_link_i
											|| '_'
											|| to_char(p_rtk_key)
											|| '_'
											|| to_char(nvl(l_jrp_key, 0))
											);
								else
									l_pdf_link_name := null;
								end if;
								--
								l_eml_attachment := r_jrp.email_attachment;
								l_eml_message    := r_jrp.email_message;
								l_eml_subject    := r_jrp.email_subject;
							else
								l_email_yn       := g_no;
								l_eml_ads_list   := null;
								l_eml_attachment := null;
								l_eml_message    := null;
								l_eml_subject    := null;
							end if;

							-- Only add add ptr_segment if Email or Print is needed
							if	l_email_yn = g_yes
							or 	l_print_yn = g_yes
							then
								-- increase ptr_segment counter
								l_jrp_count := l_jrp_count + 1;
								--
								add_ptr ( p_file_type_i         => l_file_type
									, p_field_prefix_i      => g_plt
									, p_segment_nr_i        => l_jrp_count
									, p_jrp_key_i           => l_jrp_key
									, p_template_name_i     => upper( l_template_name)
									, p_ptr_template_name_i => upper( r_jrt2.template_name)
									, p_ptr_name_i          => upper( l_export_target)
									, p_ptr_unc_path_i      => upper( l_print_server || l_export_target)
									, p_copies_i            => l_copies
									, p_print_yn_i          => l_print_yn
									, p_eml_addresses_to_i  => l_eml_ads_list
									, p_eml_addresses_bcc_i => null
									, p_email_yn_i          => l_email_yn
									, p_email_attachment_i  => l_eml_attachment
									, p_email_subject_i     => l_eml_subject
									, p_email_message_i     => l_eml_message
									, p_pdf_link_yn_i       => nvl(l_pdf_link_name, g_no)
									, p_pdf_autostore_i     => nvl(p_pdf_autostore_i, g_no)
									);
							end if;
						end loop;
					end if;
				end if;
			end loop;

			-- add Move Task segment
			add_mtk ( p_file_type_i    => l_file_type
				, p_field_prefix_i => g_plt
				, p_client_id_i    => p_client_id_i
				, p_order_nr_i     => p_order_id_i
				);

			-- add Shipment and Order Header segment
			add_smt ( p_file_type_i    => l_file_type
				, p_field_prefix_i => g_plt
				, p_client_id_i    => p_client_id_i
				, p_order_nr_i     => p_order_id_i
				);

			-- add Order Line segments incl. Lot and Serial lines
			for 	r_ole in c_ole ( b_client_id    => p_client_id_i
					       , b_order_id     => p_order_id_i
					       , b_pallet_id    => p_pallet_id_i
					       , b_container_id => p_container_id_i
					       )
			loop
				l_ole_count := l_ole_count + 1;
				--
				add_ole ( p_file_type_i    => l_file_type
					, p_field_prefix_i => g_plt
					, p_segment_nr_i   => l_ole_count
					, p_client_id_i    => r_ole.client_id
					, p_order_nr_i     => r_ole.order_id
					, p_line_id_i      => r_ole.line_id
					, p_pallet_id_i    => p_pallet_id_i
					, p_container_id_i => p_container_id_i
					);
			end loop;

			-- add Shipment Items segments incl. Lot and Serial lines
			add_sim ( p_file_type_i    => l_file_type
				, p_field_prefix_i => g_plt
				, p_client_id_i    => p_client_id_i
				, p_order_nr_i     => p_order_id_i
				, p_pallet_id_i    => p_pallet_id_i
				, p_container_id_i => p_container_id_i
				);

			-- close, archive and move file
			close_ssv_file ( p_file_type_i => l_file_type
				       , p_file_name_i => l_file_name
				       ); 
		else
			null;
		end if;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished create_packlist procedure'
								   , p_code_parameters_i 	=> '"owner_id" "'||p_owner_id_i||'" "carrier_id" "'||p_carrier_id_i||'" "reprint_yn" "'||p_reprint_yn_i||'" "user_id" "'||p_user_i||'" "workstation_id" "'||p_workstation_i||'" "locality" "'||p_locality_i||'" "report_name" "'|| p_report_name_i||'" "pdf_link" "'|| p_pdf_link_i||'" "pdf_autostore" "'|| p_pdf_autostore_i
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
	end create_packlist;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 12-Sep-2018
-- Purpose : Create Trolley file for StreamServe
------------------------------------------------------------------------------------------------
	procedure create_trolley_list( p_site_id_i        	in  varchar2 
				     , p_list_id_i        	in  varchar2
				     , p_report_name_i    	in  varchar2 --UREPSSVTRL     
				     , p_user_i           	in  varchar2
				     , p_workstation_i    	in  varchar2
				     , p_rtk_key          	in  integer
				     , p_pdf_link_i       	in  varchar2 := null
				     , p_pdf_autostore_i  	in  varchar2 := null
				     , p_run_task_i		in dcsdba.run_task%rowtype
				     )
	is
		-- Fetch advanced print mapping details
		cursor c_jrp( b_report_name in varchar2
			    , b_site_id     in varchar2
			    , b_station_id  in varchar2
			    )
		is
			select jrp.key
			,      jrp.print_mode
			,      jrp.report_name
			,      jrp.template_name
			,      jrp.site_id
			,      jrp.client_id
			,      jrp.owner_id
			,      jrp.carrier_id
			,      jrp.user_id
			,      jrp.station_id
			,      jrp.customer_id
			,      jrp.extra_parameters
			,      jrp.email_enabled
			,      jrp.email_export_type
			,      jrp.email_attachment
			,      jrp.email_subject
			,      jrp.email_message
			,      jrp.copies
			,      jrp.locality
			from   dcsdba.java_report_map jrp
			where  jrp.report_name        = b_report_name
			and    jrp.site_id            = b_site_id
			and    jrp.station_id         = b_station_id
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
		-- Fetch export targets
		cursor c_jrt( b_key in number)
		is
			select rownum
			,      jrt.key
	                ,      jrt.export_type
			,      jrt.export_target
			,      jrt.copies
			,      jrt.template_name
			from   dcsdba.java_report_export  jrt
			where  jrt.key = b_key
			order  by upper(jrt.export_target)
		;
		-- Fetch containers
		cursor c_cnt( b_list_id varchar2
			    , b_site_id varchar2
			    )
		is
		    select      count(distinct m.to_container_id) total_type
		    ,           m.to_container_config
		    from        dcsdba.move_task m
		    where       m.list_id   = b_list_id
		    and         m.site_id   = b_site_id
		    group by    m.to_container_config
		;
		-- Fetch container type details
		cursor c_typ( b_to_container_type varchar2)
		is
		    select      p.width
		    ,           p.depth
		    ,           p.height
		    ,           p.width*p.depth*p.height volume
		    ,           p.weight
		    from        dcsdba.pallet_config p
		    where       p.config_id = b_to_container_type
		    and         rownum = 1
		;
		-- Fetch container move task details
		cursor c_con ( b_list_id varchar2)
		is
		    select      distinct m.to_container_id
		    ,           m.to_container_config
		    ,           m.task_id
		    ,           m.trolley_slot_id
		    ,           m.client_id
		    from        dcsdba.move_task m
		    where       m.list_id = b_list_id
		    order by    m.trolley_slot_id asc            
		;
		-- Fetch order details
		cursor c_ord ( b_order_id   varchar2
			     , b_client_id  varchar2
			     )
		is
		    select  o.carrier_id
		    ,       o.service_level
		    ,       o.ship_dock
		    ,       o.stage_route_id
		    ,       o.repack_loc_id
		    ,       o.work_group
		    ,       o.consignment
		    ,       o.shipment_group
		    from    dcsdba.order_header o
		    where   o.order_id  = b_order_id
		    and     o.client_id = b_client_id
		;
		-- Fetch move task keys
		cursor c_lst( b_list_id varchar2
			    , b_site_id varchar2
			    )
		is
		    select  lst.key
		    from    dcsdba.move_task lst
		    where   lst.list_id = b_list_id
		    and     lst.site_id = b_site_id
		;
		--
		r_typ            c_typ%rowtype;
		r_ord            c_ord%rowtype;
		r_jrt            c_jrt%rowtype;
		--
		l_file_type      utl_file.file_type;
		l_file_name      varchar2(100);        
		l_cnt_width      number;
		l_cnt_depth      number;
		l_cnt_height     number;
		l_cnt_volume     number;
		l_cnt_weight     number;
		l_segment_id     number;
		l_template_name  varchar2(50);
		l_prev_template  varchar2(50);
		l_jrp_key        number;
		l_rtk_command    varchar2(4000);        
		l_eml_ads_list   varchar2(4000);
		l_email_yn       varchar2(1) := g_no;
		l_print_yn       varchar2(1) := g_no;
		l_copies         number;
		l_export_target  varchar2(250);
		l_print_server   varchar2(30);
		l_jrp_count      number := 0;
		l_pdf_link_name  varchar2(256);    
		l_eml_attachment varchar2(250);
		l_eml_subject    varchar2(120);
		l_eml_message    varchar2(4000);
		l_rtn		 varchar2(30) := 'create_trolley_list';

	begin
		-- Set global print id for logging
		g_print_id := p_rtk_key;

		-- Set time zone
		execute immediate 'alter session set time_zone = ''Europe/Berlin''';

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start create_trolley_list'
								   , p_code_parameters_i 	=> '"list_id" "'||p_list_id_i||'" '
												|| '"report_name" "'|| p_report_name_i||'" '
												|| '"user_id" "'|| p_user_i||'" '
												|| '"workstation_id" "'||p_workstation_i||'" '
												|| '"pdf_link" "'||p_pdf_link_i||'" '
												|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		-- create a file first
		open_ssv_file( p_site_id_i     => p_site_id_i
			     , p_client_id_i   => null
			     , p_owner_id_i    => null
			     , p_file_ext_i    => g_trl       -- To select what to in open_ssv_file procedure
			     , p_file_type_o   => l_file_type
			     , p_file_name_o   => l_file_name
			     );
		g_file_name := l_file_name;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Create new file'
								   , p_code_parameters_i 	=> '"list_id" "'||p_list_id_i||'" '
												|| '"report_name" "'|| p_report_name_i||'" '
												|| '"user_id" "'|| p_user_i||'" '
												|| '"workstation_id" "'||p_workstation_i||'" '
												|| '"pdf_link" "'||p_pdf_link_i||'" '
												|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;


		-- add content
		if 	utl_file.is_open ( file => l_file_type)
		then
			-- add trolley header
			add_trl( p_file_type_i    => l_file_type
			       , p_field_prefix_i => g_trl
			       , p_site_id_i      => p_site_id_i
			       , p_list_id_i      => p_list_id_i
			       , p_user_i         => p_user_i
			       , p_workstation_i  => p_workstation_i
			       );

			-- add printer_segment
			for 	r_jrp in c_jrp( b_report_name => p_report_name_i -- always UREPSSVTRL
					      , b_site_id     => p_site_id_i
					      , b_station_id  => p_workstation_i
					      )
			loop
				-- only create printer segments for every unique template_name (doc_type) once
				l_template_name := r_jrp.template_name;  -- set the current template 
				if      nvl( l_prev_template, '|^|') != l_template_name -- check if the current template is the same as the previous one in the loop
				then                                               -- if not (so other doc_type) then create the segments for this template, else skip
					l_jrp_key       := r_jrp.key;
					-- check if email fields needs to be filled
					-- get the run task command
					l_rtk_command 	:= p_run_task_i.command;

					-- get the email addresses
					l_eml_ads_list  := cnl_wms_pck.get_jr_email_recipients( p_jrp_key_i    => l_jrp_key
											      , p_parameters_i => l_rtk_command
											      ); 

					-- set email Y/N
					if      l_eml_ads_list is not null
					then
						l_email_yn := g_yes;
					else
						l_email_yn := g_no;
					end if;

					-- check if it's e-mail only and create segment or run the loop for the printers
					open    c_jrt ( b_key => l_jrp_key);
					fetch   c_jrt
					into    r_jrt;
					if      c_jrt%notfound
					then
						close   c_jrt;
						if      l_email_yn = g_yes
						then
							-- create the unique filename for the pdf link file
							if      p_pdf_link_i is not null
							then
								l_pdf_link_name := upper(nvl(r_jrt.template_name, l_template_name)|| '_'|| p_pdf_link_i|| '_'|| to_char(p_rtk_key)|| '_'|| to_char(nvl(l_jrp_key, 0)));
							else
								l_pdf_link_name := null;
							end if;
							--
							l_jrp_count := l_jrp_count + 1;
							add_ptr( p_file_type_i         => l_file_type
							       , p_field_prefix_i      => g_trl
							       , p_segment_nr_i        => l_jrp_count
							       , p_jrp_key_i           => l_jrp_key
							       , p_template_name_i     => upper( l_template_name)
							       , p_ptr_template_name_i => upper( r_jrt.template_name)
							       , p_ptr_name_i          => null
							       , p_ptr_unc_path_i      => null
							       , p_copies_i            => null
							       , p_print_yn_i          => g_no
							       , p_eml_addresses_to_i  => l_eml_ads_list
							       , p_eml_addresses_bcc_i => null
							       , p_email_yn_i          => l_email_yn
							       , p_email_attachment_i  => r_jrp.email_attachment
							       , p_email_subject_i     => r_jrp.email_subject
							       , p_email_message_i     => r_jrp.email_message
							       , p_pdf_link_yn_i       => nvl(l_pdf_link_name, g_no)
							       , p_pdf_autostore_i     => nvl(p_pdf_autostore_i, g_no)
							       );
						end if;
					else
						close c_jrt;
						-- get the printer(s), copies and template
						for     r_jrt in c_jrt ( b_key => l_jrp_key)
						loop
							-- set print Y/N and get print_server
							if      r_jrt.export_type = 'P' -- Printer
							and     r_jrt.export_target is not null
							then
								l_print_yn      := g_yes;
								l_copies        := nvl( r_jrt.copies, 1);
								l_export_target := r_jrt.export_target;
								l_print_server  := get_print_server;
								if      l_print_server is not null
								then 
									l_print_server := '\\'|| upper( l_print_server)|| '\';
								end if;
							else
								l_print_yn      := g_no;
								l_export_target := null;
								l_print_server  := null;
								l_copies        := null;
							end if;

							-- Only add email details if email_yn = Y in the first Java Report Export
							if      r_jrt.rownum    = 1
							and     l_email_yn      = g_yes
							then
								-- create the unique filename for the pdf link file
								if      p_pdf_link_i is not null
								then
									l_pdf_link_name := upper(nvl(r_jrt.template_name, l_template_name)|| '_'|| p_pdf_link_i|| '_'|| to_char(p_rtk_key)|| '_'|| to_char(nvl(l_jrp_key, 0)));
								else
									l_pdf_link_name := null;
								end if;
								--
								l_eml_attachment := r_jrp.email_attachment;
								l_eml_message    := r_jrp.email_message;
								l_eml_subject    := r_jrp.email_subject;
							else
								l_email_yn       := g_no;
								l_eml_ads_list   := null;
								l_eml_attachment := null;
								l_eml_message    := null;
								l_eml_subject    := null;
							end if;

							-- Only add add ptr_segment if Email or Print is needed
							if      l_email_yn = g_yes
							or      l_print_yn = g_yes
							then
								-- increase ptr_segment counter
								l_jrp_count := l_jrp_count + 1;
								--
								add_ptr( p_file_type_i         => l_file_type
								       , p_field_prefix_i      => g_trl
								       , p_segment_nr_i        => l_jrp_count
								       , p_jrp_key_i           => l_jrp_key
								       , p_template_name_i     => upper( l_template_name)
								       , p_ptr_template_name_i => upper( r_jrt.template_name)
								       , p_ptr_name_i          => upper( l_export_target)
								       , p_ptr_unc_path_i      => upper( l_print_server || l_export_target)
								       , p_copies_i            => l_copies
								       , p_print_yn_i          => l_print_yn
								       , p_eml_addresses_to_i  => l_eml_ads_list
								       , p_eml_addresses_bcc_i => null
								       , p_email_yn_i          => l_email_yn
								       , p_email_attachment_i  => l_eml_attachment
								       , p_email_subject_i     => l_eml_subject
								       , p_email_message_i     => l_eml_message
								       , p_pdf_link_yn_i       => nvl(l_pdf_link_name, g_no)
								       , p_pdf_autostore_i     => nvl(p_pdf_autostore_i, g_no)
								       );
							end if;
						end loop;
					end if;
				end if;
				l_prev_template := l_template_name; -- set the previous template as the current one for the next loop
			end loop;

			-- add container type details
			l_segment_id := 1;
			for     r_cnt in c_cnt(p_list_id_i, p_site_id_i)
			loop
				open    c_typ(r_cnt.to_container_config);
				fetch   c_typ into r_typ;
				if      c_typ%notfound
				then
					close   c_typ;
					l_cnt_width     := 0;
					l_cnt_depth     := 0;
					l_cnt_height    := 0;
					l_cnt_volume    := 0;
					l_cnt_weight    := 0;
				else
					close   c_typ;
					l_cnt_width     := r_typ.width;
					l_cnt_depth     := r_typ.depth;
					l_cnt_height    := r_typ.height;
					l_cnt_volume    := r_typ.volume;
					l_cnt_weight    := r_typ.weight;
				end if;                            

				add_cnt( p_file_type_i     => l_file_type
				       , p_field_prefix_i  => g_trl
				       , p_cnt_type_i      => r_cnt.to_container_config
				       , p_cnt_qty_i       => r_cnt.total_type
				       , p_width_i         => l_cnt_width 
				       , p_depth_i         => l_cnt_depth
				       , p_height_i        => l_cnt_height
				       , p_volume_i        => l_cnt_volume
				       , p_weight_i        => l_cnt_weight
				       , p_segment_id_i    => l_segment_id
				       );
				l_segment_id := l_segment_id +1;
			end loop c_cnt;

			-- add container id details
			l_segment_id := 1;
			for     r_con in c_con(p_list_id_i)
			loop
				open    c_ord(r_con.task_id, r_con.client_id);
				fetch   c_ord into r_ord;
				close   c_ord;

				add_con ( p_file_type_i         => l_file_type
					, p_field_prefix_i      => g_trl
					, p_container_id_i      => r_con.to_container_id
					, p_container_type_i    => r_con.to_container_config
					, p_slot_id_i           => r_con.trolley_slot_id
					, p_order_id_i          => r_con.task_id
					, p_client_id_i         => r_con.client_id
					, p_carrier_id_i        => r_ord.carrier_id
					, p_service_level_i     => r_ord.service_level
					, p_ship_dock_i         => r_ord.ship_dock
					, p_stage_route_id_i    => r_ord.stage_route_id
					, p_repack_loc_id_i     => r_ord.repack_loc_id
					, p_work_group_i        => r_ord.work_group
					, p_consignment_i       => r_ord.consignment
					, p_shipment_group_i    => r_ord.shipment_group
					, p_segment_id_i        => l_segment_id
					);
				l_segment_id := l_segment_id + 1;
			end loop;

			-- close, archive and move file
			close_ssv_file( p_file_type_i => l_file_type
				      , p_file_name_i => l_file_name
				      ); 
		else
			null;
		end if;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished create_trolley_list'
								   , p_code_parameters_i 	=> '"list_id" "'||p_list_id_i||'" '
												|| '"report_name" "'|| p_report_name_i||'" '
												|| '"user_id" "'|| p_user_i||'" '
												|| '"workstation_id" "'||p_workstation_i||'" '
												|| '"pdf_link" "'||p_pdf_link_i||'" '
												|| '"pdf_autostore" "'||p_pdf_autostore_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

	end create_trolley_list;
--
	begin
		-- Initialization
		null;
end cnl_streamserve_pck;