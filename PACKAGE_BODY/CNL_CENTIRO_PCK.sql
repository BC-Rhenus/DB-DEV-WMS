CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_CENTIRO_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with Centiro (Delivery Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--
	g_yes				constant varchar2(1)              	:= 'Y';
	g_no                       	constant varchar2(1)              	:= 'N';
	g_true                     	constant varchar2(30)             	:= 'TRUE';
	g_false                    	constant varchar2(30)             	:= 'FALSE';
	g_saveorder                	constant varchar2(30)             	:= 'SAVEORDER';
	g_packparcel               	constant varchar2(30)             	:= 'PACKPARCEL';
	g_cancelparcel             	constant varchar2(30)             	:= 'CANCELPARCEL';
	g_msg_version              	constant varchar2(10)             	:= '1.0';
	g_xml_encoding             	constant varchar2(1024)           	:= '<?xml version="1.0" encoding="utf-8"?>';
	g_xmlns                    	constant varchar2(50)             	:= 'http://centiro.com/Rhenus/1/0';
	g_centiro_wms_source       	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_WMS_SOURCE');  
	g_centiro_wms_dest         	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_WMS_DEST');
	g_centiro_tmp_dir          	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_TMP_DIR');
	g_centiro_arc_dir          	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_ARCHIVE_DIR');
	g_centiro_so_dir           	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_SO_OUTPUT_DIR');
	g_centiro_pp_dir           	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_PP_OUTPUT_DIR');
	g_centiro_cp_dir           	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_CP_OUTPUT_DIR');
	g_centiro_acss_logg_yn     	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_ACSS_LOGGING_YN');
	g_centiro_def_goods_desc   	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_DEFAULT_GOODS_DESC');
	g_centiro_max_square_size  	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_MAX_SQUARE_SIZE');
	g_centiro_dws_labels_path  	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_DWS_LABELS_PATH');
	g_centiro_billingcode_yn   	constant cnl_constants.value%type 	:= cnl_util_pck.get_constant( p_name_i => 'CENTIRO_BILLINGCODE_YN');
	g_receiver                 	constant varchar2(30)             	:= 'RECEIVER';
	g_buyer                    	constant varchar2(30)             	:= 'BUYER';
	g_collectionpoint          	constant varchar2(30)             	:= 'COLLECTIONPOINT';
	g_transportpayer           	constant varchar2(30)             	:= 'TRANSPORTPAYER';
	g_taxanddutiespayer        	constant varchar2(30)             	:= 'TAXANDDUTIESPAYER';
	g_sender                   	constant varchar2(30)             	:= 'SENDER';
	g_acss                     	constant varchar2(30)             	:= 'ACSS';
	g_acsslogg                 	constant varchar2(30)             	:= 'ACSSLOGG';
	g_consolidation            	constant varchar2(30)             	:= 'CONSOLIDATION';
	g_customer                 	constant varchar2(30)             	:= 'CUSTOMER';
	g_cod                      	constant varchar2(30)             	:= 'COD';
	g_dispatchmethod           	constant varchar2(30)             	:= 'DISPATCHMETHOD';
	g_misc                     	constant varchar2(30)             	:= 'MISC';
	g_addonservice             	constant varchar2(30)             	:= 'ADDONSERVICE';
	g_fedex                    	constant varchar2(30)             	:= 'STD.FEDEXWS.COM';
	g_dhl                      	constant varchar2(30)             	:= 'DHL.COM';
	g_bbx                      	constant varchar2(30)             	:= 'BBX';
	g_dhl_freight              	constant varchar2(30)             	:= 'STD.DHLFREIGHT.NL';
	g_nacex                    	constant varchar2(30)             	:= 'NACEX.COM';
	g_nacex_crw                	constant varchar2(30)             	:= 'NACEX_CRW';
	g_nacex_copies_spl         	constant varchar2(30)             	:= 'NACEX_CopiesSPL';
	g_dryice                   	constant varchar2(30)             	:= 'DRYICE';
	g_di                       	constant varchar2(2)              	:= 'DI';
	g_lb                       	constant varchar2(2)              	:= 'LB';
	g_lq                       	constant varchar2(2)              	:= 'LQ';
	g_eq                       	constant varchar2(2)              	:= 'EQ';
	g_hz                       	constant varchar2(2)              	:= 'HZ';
	g_dng                      	constant varchar2(30)             	:= 'DNG';
	g_dnglb                    	constant varchar2(30)             	:= 'DNGLB';
	g_dng_un                   	constant varchar2(30)             	:= 'DNG_UN';
	g_dng_description          	constant varchar2(30)             	:= 'DNG_DESCRIPTION';
	g_dng_class                	constant varchar2(30)             	:= 'DNG_CLASS';
	g_dng_packagegroup         	constant varchar2(30)             	:= 'DNG_PACKAGEGROUP';
	g_dng_packageinstructions  	constant varchar2(30)             	:= 'DNG_PACKAGEINSTRUCTIONS';
	g_dng_netweight            	constant varchar2(30)             	:= 'DNG_NETWEIGHT';
	g_dng_quantity             	constant varchar2(30)             	:= 'DNG_QUANTITY';
	g_dng_dryiceweight         	constant varchar2(30)             	:= 'DNG_DRYICEWEIGHT';
	g_adg                      	constant varchar2(30)             	:= 'ADG';
	g_idg                      	constant varchar2(30)             	:= 'IDG';
	g_dng_fdx_nv_name_access   	constant varchar2(30)             	:= 'Accessibility';
	g_dng_fdx_nv_name_lb       	constant varchar2(30)             	:= 'Battery';
	g_dng_fdx_nv_name_lq       	constant varchar2(30)             	:= 'LimitedQuantities';
	g_dng_fdx_nv_name_eq       	constant varchar2(30)             	:= 'SmallQuantityException';
--
-- Private variable declarations
	g_log				varchar2(10) 				:= cnl_sys.cnl_util_pck.get_system_profile_f('-ROOT-_USER_PRINTING_CTO-PP-LOG_ENABLE');
	g_pck				varchar2(30) 				:= 'cnl_centiro_pck';
	g_print_id			integer;
	g_file_name			varchar2(100);
	g_rtn				varchar2(30);
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create Message header for interface file to Centiro
------------------------------------------------------------------------------------------------
  procedure create_cto_file ( p_cto_file_type_i in  varchar2
                            , p_cto_out_dir_i   in  varchar2
                            , p_msg_id_i        in  varchar2
                            , p_site_id_i       in  varchar2
                            , p_client_id_i     in  varchar2
                            , p_order_id_i      in  varchar2 := null
                            , p_container_id_i  in  varchar2 := null
                            , p_content_i       in  varchar2
                            , p_filename_o      out varchar2
                            )
  is
    l_file_type       utl_file.file_type;
    l_filename        varchar2(100);
    l_tmp_fexists     boolean;
    l_arc_fexists     boolean;
    l_file_length     number;
    l_block_size      binary_integer;
    l_reference       varchar2(20);
  begin
    -- get Alphanumeric characters only from Order/Container ID
    select regexp_replace( nvl(p_container_id_i, p_order_id_i), '[^[:alnum:]]+')
    into   l_reference
    from   dual;                          
    -- create filename
    l_filename := p_cto_file_type_i                      || '_' 
               || p_site_id_i                            || '_' 
               || p_client_id_i                          || '_' 
               || l_reference                            || '_'
               || to_char(sysdate,'YYYYMMDD"_"HH24MISS') || '_'
               || p_msg_id_i
               || '.xml'
               ;
    -- open/create file in tmp
    l_file_type := utl_file.fopen ( location     => g_centiro_tmp_dir
                                  , filename     => l_filename
                                  , open_mode    => 'w'
                                  , max_linesize => 32767
                                  );

    -- write xml encoding to file
    utl_file.put_line ( file   => l_file_type
                      , buffer => g_xml_encoding
                      );
    -- write message header to file
    utl_file.put_line ( file   => l_file_type
                      , buffer => p_content_i
                      );
    -- close file
    if utl_file.is_open (file => l_file_type)
    then
       utl_file.fclose (file => l_file_type);
    end if;
    -- copy file from tmp to archive
    utl_file.fgetattr ( location    => g_centiro_tmp_dir 
                      , filename    => l_filename
                      , fexists     => l_tmp_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size
                      );
    if l_tmp_fexists
    then
       utl_file.fcopy ( src_location  => g_centiro_tmp_dir
                      , src_filename  => l_filename
                      , dest_location => g_centiro_arc_dir
                      , dest_filename => l_filename
                      );
    end if;                                          
    -- move file from tmp to out
    utl_file.fgetattr ( location    => g_centiro_arc_dir 
                      , filename    => l_filename
                      , fexists     => l_arc_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size 
                      );
    if l_arc_fexists
    then
       utl_file.frename ( src_location  => g_centiro_tmp_dir
                        , src_filename  => l_filename
                        , dest_location => p_cto_out_dir_i
                        , dest_filename => l_filename
                        , overwrite     => true
                        );
      -- add filename to table to be able to cleanup directory
      insert into cnl_files_archive( application
                                   , location
                                   , filename
                                   )
      values                       ( 'CENTIRO'
                                   , g_centiro_arc_dir
                                   , l_filename
                                   );
      commit;
    end if;
    --
    p_filename_o := l_filename;
    --
    utl_file.fclose (file => l_file_type);
    --
  exception
    when utl_file.invalid_path
    then
      raise_application_error( -20100
                             , 'Invalid path'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_mode
    then
      raise_application_error( -20100
                             , 'Invalid Mode'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_filehandle
    then
      raise_application_error( -20100
                             , 'Invalid File Handle'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_operation
    then
      raise_application_error( -20100
                             , 'Invalid Operation'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.read_error
    then
      raise_application_error( -20100
                             , 'Read Error'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.write_error
    then
      raise_application_error( -20100
                             , 'Write Error'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.internal_error
    then
      raise_application_error( -20100
                             , 'Internal Error'
                             );
      utl_file.fclose (file => l_file_type);
    when no_data_found
    then
      raise_application_error( -20100
                             , 'No Data Found'
                             );
      utl_file.fclose (file => l_file_type);
    when value_error
    then
      raise_application_error( -20100
                             , 'Value Error'
                             );
      utl_file.fclose (file => l_file_type);
    when others
    then
      raise_application_error( -20100
                             , sqlerrm
                             );
      utl_file.fclose (file => l_file_type);
  end create_cto_file;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create file for Centiro
------------------------------------------------------------------------------------------------
  procedure open_cto_file ( p_cto_file_type_i in  varchar2
                          , p_msg_id_i        in  varchar2
                          , p_site_id_i       in  varchar2
                          , p_client_id_i     in  varchar2
                          , p_order_id_i      in  varchar2 := null
                          , p_container_id_i  in  varchar2 := null
                          , p_file_type_o     out utl_file.file_type
                          , p_file_name_o     out varchar2
                          )
  is
    l_file_type       utl_file.file_type;
    l_csl_file_id_i   varchar2(10);
    l_filename        varchar2(100);
    l_reference       varchar2(20);
  begin
    -- get Alphanumeric characters only from Order/Container ID
    select regexp_replace( nvl(p_container_id_i, p_order_id_i), '[^[:alnum:]]+')
    into   l_reference
    from   dual;                          
    -- create filename
    l_filename := p_cto_file_type_i                      || '_' 
               || p_site_id_i                            || '_' 
               || p_client_id_i                          || '_' 
               || l_reference                            || '_'
               || to_char(sysdate,'YYYYMMDD"_"HH24MISS') || '_'
               || p_msg_id_i
               || '.xml'
               ;
    -- open/create file in tmp
    l_file_type := utl_file.fopen ( location     => g_centiro_tmp_dir
                                  , filename     => l_filename
                                  , open_mode    => 'w'
                                  , max_linesize => 32767
                                  );
    -- write xml encoding to file
    utl_file.put_line ( file   => l_file_type
                      , buffer => g_xml_encoding
                      );
    --
    p_file_type_o := l_file_type;
    p_file_name_o := l_filename;

  exception
    when utl_file.invalid_path
    then
      raise_application_error( -20100
                             , 'Invalid path'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_mode
    then
      raise_application_error( -20100
                             , 'Invalid Mode'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_filehandle
    then
      raise_application_error( -20100
                             , 'Invalid File Handle'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_operation
    then
      raise_application_error( -20100
                             , 'Invalid Operation'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.read_error
    then
      raise_application_error( -20100
                             , 'Read Error'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.write_error
    then
      raise_application_error( -20100
                             , 'Write Error'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.internal_error
    then
      raise_application_error( -20100
                             , 'Internal Error'
                             );
      utl_file.fclose (file => l_file_type);
    when no_data_found
    then
      raise_application_error( -20100
                             , 'No Data Found'
                             );
      utl_file.fclose (file => l_file_type);
    when value_error
    then
      raise_application_error( -20100
                             , 'Value Error'
                             );
      utl_file.fclose (file => l_file_type);
    when others
    then
      raise_application_error( -20100
                             , sqlerrm
                             );
      utl_file.fclose (file => l_file_type);
  end open_cto_file;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 20-06-2016
-- Purpose : Add line to file
------------------------------------------------------------------------------------------------
  procedure write_line( p_file_type_i in  utl_file.file_type
                      , p_content_i   in  varchar2
                      )
  is
  begin
    -- write line to file
    utl_file.put_line ( file   => p_file_type_i
                      , buffer => p_content_i
                      );
  exception
    when utl_file.invalid_path
    then
      raise_application_error( -20100
                             , 'Invalid path '
                             );
    when utl_file.invalid_mode
    then
      raise_application_error( -20100
                             , 'Invalid Mode '
                             );
    when utl_file.invalid_filehandle
    then
      raise_application_error( -20100
                             , 'Invalid File Handle '
                             );
    when utl_file.invalid_operation
    then
      raise_application_error( -20100
                             , 'Invalid Operation '
                             );
    when utl_file.read_error
    then
      raise_application_error( -20100
                             , 'Read Error '
                             );
    when utl_file.write_error
    then
      raise_application_error( -20100
                             , 'Write Error '
                             );
    when utl_file.internal_error
    then
      raise_application_error( -20100
                             , 'Internal Error '
                             );
    when no_data_found
    then
      raise_application_error( -20100
                             , 'No Data Found '
                             );
    when value_error
    then
      raise_application_error( -20100
                             , 'Value Error '
                             );
    when others
    then
      raise_application_error( -20100
                             , sqlerrm
                             );
  end write_line;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Close, archive and move file to out directory for Customs Streamliner
------------------------------------------------------------------------------------------------
  procedure close_cto_file ( p_file_type_i   in utl_file.file_type
                           , p_file_name_i   in varchar2
                           , p_cto_out_dir_i in varchar2
                           )
  is
    l_file_type       utl_file.file_type;
    l_filename        varchar2(100);
    l_tmp_fexists     boolean;
    l_arc_fexists     boolean;
    l_file_length     number;
    l_block_size      binary_integer;
  begin
    l_file_type := p_file_type_i;
    l_filename  := p_file_name_i;
    -- close file
    if utl_file.is_open (file => l_file_type)
    then
      utl_file.fclose (file => l_file_type);
    end if;
    -- copy file from tmp to out *archive
    utl_file.fgetattr ( location    => g_centiro_tmp_dir
                      , filename    => l_filename
                      , fexists     => l_tmp_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size
                      );
    if l_tmp_fexists
    then
      utl_file.fcopy ( src_location  => g_centiro_tmp_dir
                     , src_filename  => l_filename
                     , dest_location => p_cto_out_dir_i--g_centiro_arc_dir
                     , dest_filename => l_filename
                     );
    end if;
    -- move file from tmp to archive *out
    utl_file.fgetattr ( location    => g_centiro_tmp_dir--g_centiro_arc_dir
                      , filename    => l_filename
                      , fexists     => l_arc_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size
                      );
    if l_arc_fexists
    then
      utl_file.frename ( src_location  => g_centiro_tmp_dir
                       , src_filename  => l_filename
                       , dest_location => g_centiro_arc_dir--p_cto_out_dir_i
                       , dest_filename => l_filename
                       , overwrite     => true
                       );
      -- add filename to table to be able to cleanup directory
      insert into cnl_files_archive( application
                                   , location
                                   , filename
                                   )
      values                       ( 'CENTIRO'
                                   , g_centiro_arc_dir
                                   , l_filename
                                   );
      commit;
    end if;
    --
    utl_file.fclose (file => l_file_type);
    --
  exception
    when utl_file.invalid_path
    then
      raise_application_error( -20100
                             , 'Invalid path'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_mode
    then
      raise_application_error( -20100
                             , 'Invalid Mode'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_filehandle
    then
      raise_application_error( -20100
                             , 'Invalid File Handle'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.invalid_operation
    then
      raise_application_error( -20100
                             , 'Invalid Operation'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.read_error
    then
      raise_application_error( -20100
                             , 'Read Error'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.write_error
    then
      raise_application_error( -20100
                             , 'Write Error'
                             );
      utl_file.fclose (file => l_file_type);
    when utl_file.internal_error
    then
      raise_application_error( -20100
                             , 'Internal Error'
                             );
      utl_file.fclose (file => l_file_type);
    when no_data_found
    then
      raise_application_error( -20100
                             , 'No Data Found'
                             );
      utl_file.fclose (file => l_file_type);
    when value_error
    then
      raise_application_error( -20100
                             , 'Value Error'
                             );
      utl_file.fclose (file => l_file_type);
    when others
    then
      raise_application_error( -20100
                             , sqlerrm
                             );
      utl_file.fclose (file => l_file_type);
  end close_cto_file;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create MESSAGEHEADER segment for interface file to Centiro
------------------------------------------------------------------------------------------------
	function add_message_header( p_msg_name_i      in  varchar2
				   , p_msg_version_i   in  varchar2
				   , p_msg_id_i        in  varchar2
				   , p_site_id_i       in  varchar2
				   , p_source_system_i in  varchar2
				   , p_dest_system_i   in  varchar2
				   , p_cto_client_id_i in  varchar2
				   )
		return varchar2                              
	is
		-- Create message header
		cursor c_msg
		is
			select	xmltype.getClobVal ( xmlelement ( "MESSAGEHEADER" 
				,	xmlattributes ( to_char(sysdate,'YYYY-MM-DD"T"HH24:MI:SS') || '+01:00'  	as "MESSAGETIMESTAMP"
				, 	p_msg_name_i                                            			as "MESSAGENAME"
				, 	p_msg_version_i                                         			as "MESSAGEVERSION"
				, 	p_msg_id_i                                             			 	as "MESSAGEID"
				, 	p_site_id_i                                             			as "SITE"
				, 	p_source_system_i                                       			as "SOURCESYSTEM" 
				, 	p_dest_system_i                                         			as "DESTINATIONSYSTEM"
				, 	p_cto_client_id_i                                       			as "CLIENTID"
				)))
			from   dual
		;
		--
		l_retval 	varchar2(32767);
		l_rtn		varchar2(30) := 'add_message_header';
	begin
		-- add log record
		if 	g_log = 'ON'
		and	g_rtn = 'create_packparcel'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding message header'
								   , p_code_parameters_i 	=> '"msg_name" "'||p_msg_name_i||'" '
												|| '"msg_version" "'||p_msg_version_i||'" '
												|| '"msg_id" "'||p_msg_id_i||'" '
												|| '"source_system" "'||p_source_system_i||'" '
												|| '"dest_system" "'||p_dest_system_i||'" '
												|| '"cto_client_id" "'||p_cto_client_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		open	c_msg;
		fetch 	c_msg
		into  	l_retval;
		close 	c_msg;
		-- add log record
		if 	g_log = 'ON'
		and	g_rtn = 'create_packparcel'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding message header'
								   , p_code_parameters_i 	=> '"msg_name" "'||p_msg_name_i||'" '
												|| '"msg_version" "'||p_msg_version_i||'" '
												|| '"msg_id" "'||p_msg_id_i||'" '
												|| '"source_system" "'||p_source_system_i||'" '
												|| '"dest_system" "'||p_dest_system_i||'" '
												|| '"cto_client_id" "'||p_cto_client_id_i||'" '
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		return l_retval;

  end add_message_header;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Jun-2016
-- Purpose : Create BUSINESSRULES segment for interface file to Centiro
------------------------------------------------------------------------------------------------
  function add_businessrules ( p_br_name01_i  in  varchar2
                             , p_br_value01_i in  varchar2
                             , p_br_name02_i  in  varchar2 :=null
                             , p_br_value02_i in  varchar2 :=null
                             , p_br_name03_i  in  varchar2 :=null
                             , p_br_value03_i in  varchar2 :=null
                             , p_br_name04_i  in  varchar2 :=null
                             , p_br_value04_i in  varchar2 :=null
                             , p_br_name05_i  in  varchar2 :=null
                             , p_br_value05_i in  varchar2 :=null
                             )
    return varchar2                              
  is
    cursor c_brs
    is
      select xmltype.getClobVal ( xmlforest ( xmlforest ( decode ( p_br_value01_i, null, null
                                                                                , xmlforest ( p_br_name01_i  as "NAME"
                                                                                            , p_br_value01_i as "VALUE"
                                                                                            )
                                                                 )
                                                                                            as "BUSINESSRULE"
                                                        , decode ( p_br_value02_i, null, null
                                                                                , xmlforest ( p_br_name02_i  as "NAME"
                                                                                            , p_br_value02_i as "VALUE"
                                                                                            )
                                                                 )
                                                                                            as "BUSINESSRULE"
                                                        , decode ( p_br_value03_i, null, null
                                                                                , xmlforest ( p_br_name03_i  as "NAME"
                                                                                            , p_br_value03_i as "VALUE"
                                                                                            )
                                                                 )
                                                                                            as "BUSINESSRULE"
                                                        , decode ( p_br_value04_i, null, null
                                                                                , xmlforest ( p_br_name04_i  as "NAME"
                                                                                            , p_br_value04_i as "VALUE"
                                                                                            )
                                                                 )
                                                                                            as "BUSINESSRULE"
                                                        , decode ( p_br_value05_i, null, null
                                                                                , xmlforest ( p_br_name05_i  as "NAME"
                                                                                            , p_br_value05_i as "VALUE"
                                                                                            )
                                                                 )
                                                                                            as "BUSINESSRULE"
                                                        )
                                                        as "BUSINESSRULES"
                                            )
                                )
      from   dual
      ;

    l_retval varchar2(32767);

  begin
    open  c_brs;
    fetch c_brs
    into  l_retval;
    close c_brs;

    if instr( l_retval, 'NAME') = 0
    then
      l_retval := null;
    end if;

    return l_retval;

  end add_businessrules;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Jun-2016
-- Purpose : Create ADDRESSES segment for interface file to Centiro
------------------------------------------------------------------------------------------------
	function add_addresses( p_colpoint_yn_i		in  varchar2
			      , p_sender_yn_i         	in  varchar2
			      , p_3rdparty_yn_i       	in  varchar2
			      , p_taxduty_payer_yn_i  	in  varchar2
			      , p_cnee_account_i      	in  varchar2
			      , p_transport_account_i 	in  varchar2
			      , p_taxduty_account_i   	in  varchar2
			      , p_site_id_i           	in  varchar2
			      , p_client_id_i         	in  varchar2
			      , p_order_id_i          	in  varchar2
			      )
		return varchar2                              
	is
		-- Create address XML
		cursor c_ads( b_ads_type          in varchar2 := g_receiver
			    , b_cnee_account      in varchar2 
			    , b_transport_account in varchar2
			    , b_taxduty_account   in varchar2
			    , b_site_id           in varchar2
			    , b_client_id         in varchar2
			    , b_order_id          in varchar2
			    )
		is
			select	xmltype.getClobVal( xmlelement( "ADDRESS" , xmlattributes( b_ads_type as "TYPE")
							      , xmlforest( decode( b_ads_type, g_receiver, ohr.customer_id, g_buyer, ads.address_id, g_transportpayer, ads.address_id, g_taxanddutiespayer, ads.address_id, g_sender, ohr.hub_address_id, g_collectionpoint, ohr.hub_address_id
								                 ) 		as "CODE"
									 , null			as "COMPANYNAME"
									 , xmlcdata ( decode( b_ads_type, g_receiver, ohr.name, g_buyer, ads.name, g_transportpayer, ads.name, g_taxanddutiespayer, ads.name, g_sender, ohr.hub_name, g_collectionpoint, ohr.hub_name)
										    )		as "FIRSTNAME"
									 , null 		as "LASTNAME")
									 , xmlelement( "STREETADDRESS", xmlcdata( decode( b_ads_type, g_receiver, substr(ohr.address1,1,50), g_buyer, substr(ads.address1,1,50), g_transportpayer, substr(ads.address1,1,50), g_taxanddutiespayer, substr(ads.address1,1,50), g_sender, substr(ohr.hub_address1,1,50), g_collectionpoint  , substr(ohr.hub_address1,1,50))))
										, xmlforest( xmlcdata( decode( b_ads_type, g_receiver, substr(ohr.address2,1,50), g_buyer, substr(ads.address2,1,50), g_transportpayer, substr(ads.address2,1,50), g_taxanddutiespayer	, substr(ads.address2,1,50), g_sender, substr(ohr.hub_address2,1,50), g_collectionpoint  	, substr(ohr.hub_address2,1,50))
												     ) 		as "COADDRESS"
											   , null       	as "EXTRAADDRESS1"
											   , null		as "EXTRAADDRESS2"
											   , null		as "EXTRAADDRESS3")
									 , xmlelement( "ZIPCODE", decode( b_ads_type, g_receiver, ohr.postcode, g_buyer, ads.postcode, g_transportpayer, ads.postcode, g_taxanddutiespayer, ads.postcode, g_sender, ohr.hub_postcode, g_collectionpoint  , ohr.hub_postcode))
									 , xmlelement( "CITY", xmlcdata( decode( b_ads_type, g_receiver, ohr.town, g_buyer, ads.town, g_transportpayer, ads.town, g_taxanddutiespayer, ads.town, g_sender, ohr.hub_town, g_collectionpoint, ohr.hub_town)))
										, xmlforest( decode( b_ads_type, g_receiver, ohr.county, g_buyer, ads.county, g_transportpayer, ads.county, g_taxanddutiespayer, ads.county, g_sender, ohr.hub_county, g_collectionpoint , ohr.hub_county
											           )	as "STATE") 
									 , xmlelement ( "ISOCOUNTRY", ( select iso2_id from dcsdba.country where iso3_id = decode( b_ads_type, g_receiver, ohr.country, g_buyer, ads.country, g_transportpayer, ads.country, g_taxanddutiespayer, ads.country, g_sender, ohr.hub_country, g_collectionpoint, ohr.hub_country)))
										, xmlforest ( xmlcdata ( decode( b_ads_type, g_receiver, ohr.contact, g_buyer, ads.contact, g_transportpayer, ads.contact, g_taxanddutiespayer, ads.contact, g_sender, ohr.hub_contact, g_collectionpoint, ohr.hub_contact)
												       ) 	as "CONTACT"
											    , null       	as "PHONEPREFIX"
											    , xmlcdata ( regexp_replace(decode( b_ads_type, g_receiver, ohr.contact_phone, g_buyer, ads.contact_phone, g_transportpayer, ads.contact_phone, g_taxanddutiespayer, ads.contact_phone, g_sender, ohr.hub_contact_phone, g_collectionpoint  , ohr.hub_contact_phone), '[^[:digit:]]+')
												       ) 	as "PHONE"
											    , xmlcdata ( regexp_replace(decode( b_ads_type, g_receiver, ohr.contact_fax, g_buyer, ads.contact_fax, g_transportpayer, ads.contact_fax, g_taxanddutiespayer, ads.contact_fax, g_sender, ohr.hub_contact_fax, g_collectionpoint, ohr.hub_contact_fax), '[^[:digit:]]+')
												       ) 	as "FAX"
                                                         , null       as "CELLPHONEPREFIX"
                                                         , xmlcdata ( regexp_replace(
                                                                                    decode( b_ads_type, g_receiver         , ohr.contact_mobile
                                                                                                      , g_buyer            , ads.contact_mobile
                                                                                                      , g_transportpayer   , ads.contact_mobile
                                                                                                      , g_taxanddutiespayer, ads.contact_mobile
                                                                                                      , g_sender           , ohr.hub_contact_mobile
                                                                                                      , g_collectionpoint  , ohr.hub_contact_mobile
                                                                                          )
                                                                                    , '[^[:digit:]]+'
                                                                                    )
                                                                    ) as "CELLPHONE"
                                                         , xmlcdata ( decode( b_ads_type, g_receiver         , ohr.contact_email
                                                                                        , g_buyer            , ads.contact_email
                                                                                        , g_transportpayer   , ads.contact_email
                                                                                        , g_taxanddutiespayer, ads.contact_email
                                                                                        , g_sender           , ohr.hub_contact_email
                                                                                        , g_collectionpoint  , ohr.hub_contact_email
                                                                            )
                                                                    ) as "EMAIL"
                                                         , xmlcdata ( decode( g_centiro_billingcode_yn, g_no, null
                                                                                                            , decode( b_ads_type, g_receiver         , b_cnee_account
                                                                                                                                , g_buyer            , null
                                                                                                                                , g_transportpayer   , b_transport_account
                                                                                                                                , g_taxanddutiespayer, b_taxduty_account
                                                                                                                                , g_sender           , null
                                                                                                                                , g_collectionpoint  , null
                                                                                                                    )
                                                                            )
                                                                    ) as "CARRIERACCOUNT"
                                                         )
                                             )
                                )
      from   dcsdba.order_header ohr
      ,      dcsdba.address      ads
      where  ohr.from_site_id    = b_site_id
      and    ohr.client_id       = b_client_id
      and    ohr.order_id        = b_order_id
      and    ohr.client_id       = ads.client_id (+)
      and    nvl(ohr.hub_vat_number, nvl(b_transport_account, b_taxduty_account)) = ads.address_id (+)
      and    '3rdParty'          = ads.address_type (+)      
      ;               
    cursor c_addresses ( b_ads_rec in varchar2
                       , b_ads_buy in varchar2
                       , b_ads_tpr in varchar2
                       , b_ads_tdr in varchar2
                       , b_ads_sen in varchar2
                       , b_ads_col in varchar2
                       )
    is
      select xmltype.getClobVal ( xmlelement ( "ADDRESSES"
                                             , decode( b_ads_rec, null, null
                                                                      , xmltype ( b_ads_rec)
                                                     )
                                             , decode( b_ads_buy, null, null
                                                                      , xmltype ( b_ads_buy)
                                                     )
                                             , decode( b_ads_tpr, null, null
                                                                      , xmltype ( b_ads_tpr)
                                                     )
                                             , decode( b_ads_tdr, null, null
                                                                      , xmltype ( b_ads_tdr)
                                                     )
                                             , decode( b_ads_sen, null, null
                                                                      , xmltype ( b_ads_sen)
                                                     )
                                             , decode( b_ads_col, null, null
                                                                      , xmltype ( b_ads_col)
                                                     )
                                             )
                                )
      from   dual  
      ;                          

		l_ads_rec 	varchar2(32767);
		l_ads_buy 	varchar2(32767);
		l_ads_tpr 	varchar2(32767);
		l_ads_tdr 	varchar2(32767);
		l_ads_sen 	varchar2(32767);
		l_ads_col 	varchar2(32767);
		l_retval  	varchar2(32767);
		l_rtn		varchar2(30) := 'add_addresses';
	begin
		-- add log record
		if 	g_log = 'ON'
		and	g_rtn = 'create_packparcel'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding addresses to XML'
								   , p_code_parameters_i 	=> '"collection_point" "'||p_colpoint_yn_i||'" '
												|| '"sender_id" "'||p_sender_yn_i||'" '
												|| '"3rdparty_yn" "'||p_3rdparty_yn_i||'" '
												|| '"taxduty_payer_yn" "'||p_taxduty_payer_yn_i||'" '
												|| '"consignee_account" "'||p_cnee_account_i||'" '
												|| '"transport_account" "'||p_transport_account_i||'" '
												|| '"taxduty_account" "'||p_taxduty_account_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- add RECEIVER
		open  c_ads( b_ads_type          => g_receiver
			   , b_cnee_account      => p_cnee_account_i 
			   , b_transport_account => null
			   , b_taxduty_account   => null
			   , b_site_id           => p_site_id_i
			   , b_client_id         => p_client_id_i
			   , b_order_id          => p_order_id_i
			   );
		fetch	c_ads
		into  	l_ads_rec;
		close 	c_ads;

		-- add BUYER or TRANSPORTPAYER
		if 	p_3rdparty_yn_i = g_yes
		then
			if	g_centiro_billingcode_yn = g_yes
			then
				open  	c_ads( b_ads_type          => g_transportpayer
					     , b_cnee_account      => null 
					     , b_transport_account => p_transport_account_i
					     , b_taxduty_account   => null
					     , b_site_id           => p_site_id_i
					     , b_client_id         => p_client_id_i
					     , b_order_id          => p_order_id_i
					     );
				fetch 	c_ads
				into  	l_ads_tpr;
				close 	c_ads;
			else
				open  	c_ads( b_ads_type          => g_buyer
					     , b_cnee_account      => null 
					     , b_transport_account => null
					     , b_taxduty_account   => null
					     , b_site_id           => p_site_id_i
					     , b_client_id         => p_client_id_i
					     , b_order_id          => p_order_id_i
					     );
				fetch 	c_ads
				into  	l_ads_buy;
				close 	c_ads;
			end if;
		end if;

		-- add TAXANDDUTIESPAYER
		if 	p_taxduty_payer_yn_i = g_yes
		then
			if 	g_centiro_billingcode_yn = g_yes
			then
				open	c_ads( b_ads_type          => g_taxanddutiespayer
					     , b_cnee_account      => null 
					     , b_transport_account => null
					     , b_taxduty_account   => p_taxduty_account_i
					     , b_site_id           => p_site_id_i
					     , b_client_id         => p_client_id_i
					     , b_order_id          => p_order_id_i
					     );
				fetch 	c_ads
				into	l_ads_tdr;
				close	c_ads;
			end if;
		end if;

		-- add SENDER
		if 	p_sender_yn_i = g_yes
		then
			open	c_ads( b_ads_type          => g_sender
				     , b_cnee_account      => null 
				     , b_transport_account => null
				     , b_taxduty_account   => null
				     , b_site_id           => p_site_id_i
				     , b_client_id         => p_client_id_i
				     , b_order_id          => p_order_id_i
				     );
			fetch 	c_ads
			into  	l_ads_sen;
			close 	c_ads;
		end if;

		-- add COLLECTIONPOINT
		if 	p_colpoint_yn_i = g_yes
		then
			open	c_ads( b_ads_type          => g_collectionpoint
				     , b_cnee_account      => null 
				     , b_transport_account => null
				     , b_taxduty_account   => null
				     , b_site_id           => p_site_id_i
				     , b_client_id         => p_client_id_i
				     , b_order_id          => p_order_id_i
				     );
			fetch 	c_ads
			into  	l_ads_col;
			close 	c_ads;
		end if;

		-- create the ADRESSES now, only when RECEIVER address is valid
		if  	l_ads_rec is null
		then
			l_retval := null;
		else
			open	c_addresses( b_ads_rec => l_ads_rec  -- RECEIVER
					   , b_ads_buy => l_ads_buy  -- BUYER
					   , b_ads_tpr => l_ads_tpr  -- TRANSPORTPAYER
					   , b_ads_tdr => l_ads_tdr  -- TAXANDDUTIESPAYER
					   , b_ads_sen => l_ads_sen  -- SENDER
					   , b_ads_col => l_ads_col  -- COLLECTIONPOINT
					   );
			fetch 	c_addresses
			into  	l_retval;
			close 	c_addresses;
		end if;

		-- add log record
		if 	g_log = 'ON'
		and	g_rtn = 'create_packparcel'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding addresses to XML'
								   , p_code_parameters_i 	=> '"collection_point" "'||p_colpoint_yn_i||'" '
												|| '"sender_id" "'||p_sender_yn_i||'" '
												|| '"3rdparty_yn" "'||p_3rdparty_yn_i||'" '
												|| '"taxduty_payer_yn" "'||p_taxduty_payer_yn_i||'" '
												|| '"consignee_account" "'||p_cnee_account_i||'" '
												|| '"transport_account" "'||p_transport_account_i||'" '
												|| '"taxduty_account" "'||p_taxduty_account_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		return l_retval;

	end add_addresses;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Jun-2016
-- Purpose : Create ATTRIBUTES segment for interface file to Centiro
------------------------------------------------------------------------------------------------
	function add_attributes ( p_at_name01_i  in  varchar2
				, p_at_value01_i in  varchar2
				, p_at_name02_i  in  varchar2 :=null
				, p_at_value02_i in  varchar2 :=null
				, p_at_name03_i  in  varchar2 :=null
				, p_at_value03_i in  varchar2 :=null
				, p_at_name04_i  in  varchar2 :=null
				, p_at_value04_i in  varchar2 :=null
				, p_at_name05_i  in  varchar2 :=null
				, p_at_value05_i in  varchar2 :=null
				, p_at_name06_i  in  varchar2 :=null
				, p_at_value06_i in  varchar2 :=null
				, p_at_name07_i  in  varchar2 :=null
				, p_at_value07_i in  varchar2 :=null
				, p_at_name08_i  in  varchar2 :=null
				, p_at_value08_i in  varchar2 :=null
				, p_at_name09_i  in  varchar2 :=null
				, p_at_value09_i in  varchar2 :=null
				, p_at_name10_i  in  varchar2 :=null
				, p_at_value10_i in  varchar2 :=null
				)
		return varchar2                              
	is
		cursor c_att
		is
		      select xmltype.getClobVal ( xmlforest ( xmlforest ( decode ( p_at_value01_i, null, null
												, xmlforest ( p_at_name01_i  as "NAME"
													    , xmlcdata ( p_at_value01_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value02_i, null, null
												, xmlforest ( p_at_name02_i  as "NAME"
													    , xmlcdata ( p_at_value02_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value03_i, null, null
												, xmlforest ( p_at_name03_i  as "NAME"
													    , xmlcdata ( p_at_value03_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value04_i, null, null
												, xmlforest ( p_at_name04_i  as "NAME"
													    , xmlcdata ( p_at_value04_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value05_i, null, null
												, xmlforest ( p_at_name05_i  as "NAME"
													    , xmlcdata ( p_at_value05_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value06_i, null, null
												, xmlforest ( p_at_name06_i  as "NAME"
													    , xmlcdata ( p_at_value06_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value07_i, null, null
												, xmlforest ( p_at_name07_i  as "NAME"
													    , xmlcdata ( p_at_value07_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value08_i, null, null
												, xmlforest ( p_at_name08_i  as "NAME"
													    , xmlcdata ( p_at_value08_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value09_i, null, null
												, xmlforest ( p_at_name09_i  as "NAME"
													    , xmlcdata ( p_at_value09_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									, decode ( p_at_value10_i, null, null
												, xmlforest ( p_at_name10_i  as "NAME"
													    , xmlcdata ( p_at_value10_i) as "VALUE"
													    )
										 )
													    as "ATTRIBUTE"
									)
									as "ATTRIBUTES"
							    )
						)
		      from   dual
		;
		--
		l_rtn	varchar2(30) := 'add_attributes';
		l_retval varchar2(32767);
	begin
		-- add log record
		if 	g_log = 'ON'
		and	g_rtn = 'create_packparcel'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding atributes'
								   , p_code_parameters_i 	=> null
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		open  	c_att;
		fetch 	c_att
		into  	l_retval;
		close 	c_att;

		if 	instr( l_retval, 'NAME') = 0
		then
			l_retval := null;
		end if;

		-- add log record
		if 	g_log = 'ON'
		and	g_rtn = 'create_packparcel'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding atributes'
								   , p_code_parameters_i 	=> null
								   , p_order_id_i		=> null
								   , p_client_id_i		=> null
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> null
								   );
		end if;

		return l_retval;

	end add_attributes;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Jun-2016
-- Purpose : Create NAMED_VALUES segment within SERVICE_ATTRIBUTES for interface file to Centiro
------------------------------------------------------------------------------------------------
  function add_sa_namedvalues ( p_nv_name01_i  in  varchar2
                              , p_nv_value01_i in  varchar2
                              , p_nv_name02_i  in  varchar2 :=null
                              , p_nv_value02_i in  varchar2 :=null
                              , p_nv_name03_i  in  varchar2 :=null
                              , p_nv_value03_i in  varchar2 :=null
                              , p_nv_name04_i  in  varchar2 :=null
                              , p_nv_value04_i in  varchar2 :=null
                              , p_nv_name05_i  in  varchar2 :=null
                              , p_nv_value05_i in  varchar2 :=null
                              , p_nv_name06_i  in  varchar2 :=null
                              , p_nv_value06_i in  varchar2 :=null
                              , p_nv_name07_i  in  varchar2 :=null
                              , p_nv_value07_i in  varchar2 :=null
                              , p_nv_name08_i  in  varchar2 :=null
                              , p_nv_value08_i in  varchar2 :=null
                              , p_nv_name09_i  in  varchar2 :=null
                              , p_nv_value09_i in  varchar2 :=null
                              , p_nv_name10_i  in  varchar2 :=null
                              , p_nv_value10_i in  varchar2 :=null
                              )
    return varchar2                              
  is
    cursor c_nvs
    is
      select xmltype.getClobVal ( xmlforest ( xmlforest ( decode ( p_nv_value01_i, null, null
                                                                              , xmlforest ( p_nv_name01_i  as "NAME"
                                                                                          , p_nv_value01_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value02_i, null, null
                                                                              , xmlforest ( p_nv_name02_i  as "NAME"
                                                                                          , p_nv_value02_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value03_i, null, null
                                                                              , xmlforest ( p_nv_name03_i  as "NAME"
                                                                                          , p_nv_value03_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value04_i, null, null
                                                                              , xmlforest ( p_nv_name04_i  as "NAME"
                                                                                          , p_nv_value04_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value05_i, null, null
                                                                              , xmlforest ( p_nv_name05_i  as "NAME"
                                                                                          , p_nv_value05_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value06_i, null, null
                                                                              , xmlforest ( p_nv_name06_i  as "NAME"
                                                                                          , p_nv_value06_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value07_i, null, null
                                                                              , xmlforest ( p_nv_name07_i  as "NAME"
                                                                                          , p_nv_value07_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value08_i, null, null
                                                                              , xmlforest ( p_nv_name08_i  as "NAME"
                                                                                          , p_nv_value08_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value09_i, null, null
                                                                              , xmlforest ( p_nv_name09_i  as "NAME"
                                                                                          , p_nv_value09_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      , decode ( p_nv_value10_i, null, null
                                                                              , xmlforest ( p_nv_name10_i  as "NAME"
                                                                                          , p_nv_value10_i as "VALUE"
                                                                                          )
                                                               )
                                                                                          as "NAMEDVALUE"
                                                      )
                                                      as "NAMEDVALUES"
                                            )
                                )
      from   dual
      ;

    l_retval varchar2(32767);

  begin
    open  c_nvs;
    fetch c_nvs
    into  l_retval;
    close c_nvs;

    if instr( l_retval, 'NAME') = 0
    then
      l_retval := null;
    end if;

    return l_retval;

  end add_sa_namedvalues;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-Jun-2016
-- Purpose : Create SERVICEATTRIBUTES segment for interface file to Centiro
------------------------------------------------------------------------------------------------
  function add_serviceattributes ( p_sa_code01_i     in  varchar2 := null
                                 , p_sa_value01_i    in  varchar2 := null
                                 , p_sa_namedval01_i in  varchar2 := null
                                 , p_sa_code02_i     in  varchar2 := null
                                 , p_sa_value02_i    in  varchar2 := null
                                 , p_sa_namedval02_i in  varchar2 := null
                                 , p_sa_code03_i     in  varchar2 := null
                                 , p_sa_value03_i    in  varchar2 := null
                                 , p_sa_namedval03_i in  varchar2 := null
                                 , p_sa_code04_i     in  varchar2 := null
                                 , p_sa_value04_i    in  varchar2 := null
                                 , p_sa_namedval04_i in  varchar2 := null
                                 , p_sa_code05_i     in  varchar2 := null
                                 , p_sa_value05_i    in  varchar2 := null
                                 , p_sa_namedval05_i in  varchar2 := null
                                 , p_sa_code06_i     in  varchar2 := null
                                 , p_sa_value06_i    in  varchar2 := null
                                 , p_sa_namedval06_i in  varchar2 := null
                                 , p_sa_code07_i     in  varchar2 := null
                                 , p_sa_value07_i    in  varchar2 := null
                                 , p_sa_namedval07_i in  varchar2 := null
                                 , p_sa_code08_i     in  varchar2 := null
                                 , p_sa_value08_i    in  varchar2 := null
                                 , p_sa_namedval08_i in  varchar2 := null
                                 , p_sa_code09_i     in  varchar2 := null
                                 , p_sa_value09_i    in  varchar2 := null
                                 , p_sa_namedval09_i in  varchar2 := null
                                 , p_sa_code10_i     in  varchar2 := null
                                 , p_sa_value10_i    in  varchar2 := null
                                 , p_sa_namedval10_i in  varchar2 := null
                                 )
    return varchar2                              
  is
    cursor c_sat
    is
      select xmltype.getClobVal ( xmlelement ( "SERVICEATTRIBUTES"
                                             , decode ( p_sa_code01_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code01_i, null, null
                                                                                                                      , xmlforest ( p_sa_code01_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value01_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval01_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval01_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code02_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code02_i, null, null
                                                                                                                      , xmlforest ( p_sa_code02_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value02_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval02_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval02_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code03_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code03_i, null, null
                                                                                                                      , xmlforest ( p_sa_code03_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value03_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval03_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval03_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code04_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code04_i, null, null
                                                                                                                      , xmlforest ( p_sa_code04_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value04_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval04_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval04_i)
                                                                                                 )

                                                                                        )
                                                      )
                                             , decode ( p_sa_code05_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code05_i, null, null
                                                                                                                      , xmlforest ( p_sa_code05_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value05_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval05_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval05_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code06_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code06_i, null, null
                                                                                                                      , xmlforest ( p_sa_code06_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value06_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval06_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval06_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code07_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code07_i, null, null
                                                                                                                      , xmlforest ( p_sa_code07_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value07_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval07_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval07_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code08_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code08_i, null, null
                                                                                                                      , xmlforest ( p_sa_code08_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value08_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval08_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval08_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code09_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code09_i, null, null
                                                                                                                      , xmlforest ( p_sa_code09_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value09_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval09_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval09_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             , decode ( p_sa_code10_i, null, null
                                                                           , xmlelement ( "SERVICEATTRIBUTE"
                                                                                        , decode ( p_sa_code10_i, null, null
                                                                                                                      , xmlforest ( p_sa_code10_i  as "CODE"
                                                                                                                                  , xmlcdata ( p_sa_value10_i) as "VALUE"
                                                                                                                                  )
                                                                                                 )
                                                                                        , decode ( p_sa_namedval10_i, null, null
                                                                                                                          , xmltype ( p_sa_namedval10_i)
                                                                                                 )
                                                                                        )
                                                      )
                                             )
                                )
      from   dual
      ;

    l_retval varchar2(32767);

  begin
    open  c_sat;
    fetch c_sat
    into  l_retval;
    close c_sat;

    if instr( l_retval, 'CODE') = 0
    then
      l_retval := null;
    end if;

    return l_retval;

  end add_serviceattributes;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create PARCELS segment for interface file to Centiro
------------------------------------------------------------------------------------------------
	function add_parcels( p_site_id_i      in  varchar2
			    , p_client_id_i    in  varchar2
			    , p_order_id_i     in  varchar2
			    , p_parcel_id_i    in  varchar2
			    , p_pallet_type_i  in  varchar2
			    , p_weight_i       in  number
			    , p_depth_i        in  number
			    , p_height_i       in  number
			    , p_width_i        in  number
			    , p_copies_i       in  number
			    , p_goods_desc_i   in  varchar2
			    , p_attributes_i   in  varchar2
			    )
		return varchar2                              
	is
		cursor c_pcl( b_site_id      in varchar2
			    , b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_parcel_id    in varchar2
			    , b_pallet_type  in varchar2
			    , b_weight       in number
			    , b_depth        in number
			    , b_height       in number
			    , b_width        in number
			    , b_copies       in number
			    , b_goods_desc   in varchar2
			    , b_attributes   in varchar2
			    )
		is
			select xmltype.getClobVal ( xmlelement ( "PARCEL"
                                             , xmlattributes ( b_parcel_id as "PARCELID")
                                             , xmlforest ( null                                              as "UNITLEVEL"
                                                         , b_order_id                                        as "ORDERNO"
                                                         , nvl(b_copies, 1)                                  as "NUMBEROFTAGS"
                                                         , 1                                                 as "NUMBEROFPACKAGES"
                                                         , b_pallet_type                                     as "TYPEOFPACKAGE"
                                                         , nvl(b_goods_desc, 'Goods Description')            as "TYPEOFGOODS"
                                                         , xmlcdata (ohr.order_reference)                    as "SHIPPERREFERENCE1"
                                                         , null                                              as "SHIPPERREFERENCE2"
                                                         , xmlcdata (ohr.purchase_order)                     as "RECEIVERREFERENCE1"
                                                         , null                                              as "RECEIVERREFERENCE2"
                                                         , null                                              as "SIGNATURE"
                                                         , null                                              as "DELIVERYINSTRUCTION1"
                                                         , null                                              as "DELIVERYINSTRUCTION2"
                                                         , null                                              as "DELIVERYINSTRUCTION3"
                                                         , null                                              as "DELIVERYINSTRUCTION4"
                                                         , to_char(nvl(b_weight,0.01),'fm99999990.90')       as "WEIGHT"  -- KG
                                                         , null                                              as "VOLUME"  -- CM3 > M3
                                                         , to_char(nvl((b_depth ),0.01),'fm99999990.90')     as "LENGTH"  -- CM > M
                                                         , to_char(nvl((b_height),0.01),'fm99999990.90')     as "HEIGHT"  -- CM > M
                                                         , to_char(nvl((b_width ),0.01),'fm99999990.90')     as "WIDTH"   -- CM > M
                                                         , null                                              as "CHILDPACKAGES"
                                                         )
                                             -- ATTRIBUTES
                                             , decode( b_attributes, null, null
                                                                         , xmltype ( b_attributes)
                                                     )
                                             )
                                )
			from   dcsdba.order_header    ohr
			where  ohr.from_site_id = b_site_id
			and    ohr.client_id    = b_client_id
			and    ohr.order_id     = b_order_id
		;
		--
		cursor c_dim( b_depth  in number
			    , b_width  in number
			    , b_height in number
			    )
		is
			select 	rownum
			,      	dim_type
			,      	dim
			from   	(
				select 'depth'              dim_type 
				,      nvl( b_depth, 0.01)  dim
				from   dual
				union  all
				select 'width'              dim_type 
				,      nvl( b_width, 0.01)  dim
				from   dual
				union  all
				select 'height'             dim_type 
				,      nvl( b_height, 0.01) dim
				from   dual
				order  by dim desc
				)    
		;

		r_dim                     	c_dim%rowtype;

		l_large_dim               	number;
		l_middle_dim              	number;
		l_small_dim               	number;
		l_depth                   	number;
		l_width                   	number;
		l_height                  	number;
		l_retval                  	varchar2(32767);
		l_centiro_max_square_size 	number;
		l_rtn				varchar2(30) := 'add_parcels';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding Parcel'
								   , p_code_parameters_i 	=> '"parcel_id" "'||p_parcel_id_i||'" '
												|| '"parcel_type" "'||p_pallet_type_i||'" '
												|| '"weight" "'||p_weight_i||'" '
												|| '"depth" "'||p_depth_i||'" '
												|| '"height" "'||p_height_i||'" '
												|| '"width" "'||p_width_i||'" '
												|| '"copies" "'||p_copies_i||'" '
												|| '"goods_description" "'||p_goods_desc_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- Set dimension in right order for best dimensions/girth calculation
		for 	r_dim in c_dim( b_depth  => p_depth_i
				      , b_width  => p_width_i
				      , b_height => p_height_i
				      )
		loop
			case	r_dim.rownum
			when 	1
			then
				l_large_dim  := r_dim.dim;
			when 	2
			then
				l_middle_dim := r_dim.dim;
			when 	3
			then
				l_small_dim  := r_dim.dim;
			else
				exit;
			end case;
		end loop;

		-- if square size is larger than set size (Constant) then don't change dims as it might be a pallet
		l_centiro_max_square_size := to_number( g_centiro_max_square_size) / 100;  -- Constant value is in CM so set to M

		if	l_large_dim + l_middle_dim < l_centiro_max_square_size
		then
			l_depth  := l_large_dim;  -- length always largest value
			l_width  := l_middle_dim; -- width always middle value
			l_height := l_small_dim;  -- height always smalles value
		else
			l_depth  := p_depth_i;
			l_width  := p_width_i;
			l_height := p_height_i;
		end if;

		open  c_pcl( b_site_id      => p_site_id_i     
			   , b_client_id    => p_client_id_i   
			   , b_order_id     => p_order_id_i    
			   , b_parcel_id    => p_parcel_id_i
			   , b_pallet_type  => p_pallet_type_i
			   , b_weight       => p_weight_i
			   , b_depth        => nvl( l_depth,  p_depth_i)  
			   , b_height       => nvl( l_height, p_height_i) 
			   , b_width        => nvl( l_width,  p_width_i)  
			   , b_copies       => p_copies_i      
			   , b_goods_desc   => p_goods_desc_i  
			   , b_attributes   => p_attributes_i  
			   );
		fetch 	c_pcl
		into  	l_retval;
		close 	c_pcl;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding Parcel'
								   , p_code_parameters_i 	=> '"parcel_id" "'||p_parcel_id_i||'" '
												|| '"parcel_type" "'||p_pallet_type_i||'" '
												|| '"weight" "'||p_weight_i||'" '
												|| '"depth" "'||p_depth_i||'" '
												|| '"height" "'||p_height_i||'" '
												|| '"width" "'||p_width_i||'" '
												|| '"copies" "'||p_copies_i||'" '
												|| '"goods_description" "'||p_goods_desc_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> null
								   , p_container_id_i		=> null
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		return l_retval;

	end add_parcels;                                
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 26-Jun-2016
-- Purpose : Update CNL_CONTAINER_DATA with details
------------------------------------------------------------------------------------------------
	procedure update_container_data ( p_container_id_i     in  varchar2
					, p_container_type_i   in  varchar2
					, p_pallet_id_i        in  varchar2
					, p_pallet_type_i      in  varchar2
					, p_container_n_of_n_i in  number
					, p_site_id_i          in  varchar2
					, p_client_id_i        in  varchar2
					, p_owner_id_i         in  varchar2
					, p_order_id_i         in  varchar2
					, p_customer_id_i      in  varchar2
					, p_carrier_id_i       in  varchar2
					, p_service_level_i    in  varchar2
					, p_wms_weight_i       in  number
					, p_wms_height_i       in  number
					, p_wms_width_i        in  number
					, p_wms_depth_i        in  number
					, p_wms_database_i     in  varchar2
					, p_cto_enabled_yn     in  varchar2
					, p_cto_pp_filename_i  in  varchar2
					, p_cto_pp_dstamp_i    in  cnl_container_data.cto_pp_dstamp%type
					, p_cto_carrier_i      in  varchar2
					, p_cto_service_i      in  varchar2
					)
	is
		-- Fetch cnl_container_data
		cursor c_cda( b_client_id    in varchar2
			    , b_order_id     in varchar2
			    , b_pallet_id    in varchar2
			    , b_container_id in varchar2
			    , b_wms_database in varchar2
			    )
		is
			select	cda.*
			from   	cnl_container_data cda
			where  	cda.client_id      = b_client_id
			and    	cda.order_id       = b_order_id
			and    	cda.pallet_id      = b_pallet_id
			and    	cda.container_id   = b_container_id
			and    	cda.wms_database   = b_wms_database
		;
		--
		r_cda           c_cda%rowtype;
		l_rtn		varchar2(30) := 'update_container_data';
	begin
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding/updating cnl_container data'
								   , p_code_parameters_i 	=> '"container_type" "'||p_container_type_i||'" '
												|| '"pallet_type" "'||p_pallet_type_i||'" '
												|| '"container_n_of_n" "'||p_container_n_of_n_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		open 	c_cda( b_client_id    => p_client_id_i
			     , b_order_id     => p_order_id_i
			     , b_pallet_id    => p_pallet_id_i
			     , b_container_id => p_container_id_i
			     , b_wms_database => p_wms_database_i
			     );
		fetch 	c_cda
		into  	r_cda;
		if 	c_cda%notfound
		then
			-- insert cda record
			insert	into	
				cnl_container_data 
				( container_id
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
				, cto_enabled_yn
				, cto_pp_filename
				, cto_pp_dstamp
				, cto_carrier
				, cto_service
				)
			values	( p_container_id_i
				, p_container_type_i
				, p_pallet_id_i
				, p_pallet_type_i
				, p_container_n_of_n_i
				, p_site_id_i
				, p_client_id_i
				, p_owner_id_i
				, p_order_id_i
				, p_customer_id_i
				, p_carrier_id_i
				, p_service_level_i
				, p_wms_weight_i
				, p_wms_height_i
				, p_wms_width_i
				, p_wms_depth_i
				, p_wms_database_i
				, p_cto_enabled_yn
				, p_cto_pp_filename_i
				, p_cto_pp_dstamp_i
				, p_cto_carrier_i
				, p_cto_service_i
				);
		else
			-- update cda record with centiro data
			update	cnl_container_data      cda
			set    	cda.cto_pp_filename     = p_cto_pp_filename_i
			,      	cda.cto_pp_dstamp       = p_cto_pp_dstamp_i
			,      	cda.cto_carrier         = p_cto_carrier_i
			,      	cda.cto_service         = p_cto_service_i
			where  	cda.client_id           = p_client_id_i
			and    	cda.order_id            = p_order_id_i
			and    	cda.pallet_id           = p_pallet_id_i
			and    	cda.container_id        = p_container_id_i
			and    	cda.wms_database        = p_wms_database_i
			;
		end if;
		close	c_cda;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding/updating cnl_container data'
								   , p_code_parameters_i 	=> '"container_type" "'||p_container_type_i||'" '
												|| '"pallet_type" "'||p_pallet_type_i||'" '
												|| '"container_n_of_n" "'||p_container_n_of_n_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
  end update_container_data;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create SaveOrder interface file to Centiro
------------------------------------------------------------------------------------------------
  procedure create_saveorder ( p_site_id_i   in  varchar2
                             , p_client_id_i in  varchar2
                             , p_order_id    in  varchar2
                             )
  is
    cursor c_ord ( b_site_id   varchar2
                 , b_client_id varchar2
                 , b_order_id  varchar2
                 )
    is
      select ohr.from_site_id
      ,      ohr.client_id
      ,      ohr.order_id
      ,      ohr.customer_id
      ,      nvl(ohr.cod, g_no)                                    cod
      ,      decode( nvl(ohr.cod, g_no), g_yes, g_true
                                       , g_no , g_false
                   )                                               cod_tf
      ,      ohr.dispatch_method
      ,      ohr.user_def_type_1                                   misc
      ,      ohr.hub_carrier_id
      ,      lower(ohr.freight_charges)                            freight_charges
      from   dcsdba.order_header ohr
      where  ohr.from_site_id = b_site_id
      and    ohr.client_id    = b_client_id
      and    ohr.order_id     = b_order_id
      ;

    cursor c_orders ( b_site_id       in varchar2
                    , b_client_id     in varchar2
                    , b_order_id      in varchar2
                    , b_businessrules in varchar2
                    , b_addresses     in varchar2
                    , b_attributes    in varchar2
                    )
    is
      select xmltype.getClobVal ( xmlelement ( "ORDERS"
                                             -- ORDER
                                             , xmlelement ( "ORDER"
                                                          , xmlattributes ( nvl(ohr.tm_stop_seq, 0) + 1                          as "ORDERVERSION"
                                                                          , 'ORDERCREATED'                                       as "ORDERSTATUS"
                                                                          , ohr.client_id || '@' || decode(
										ohr.from_site_id,'GBMIK01','GBASF02',
										ohr.from_site_id)				as "CLIENTID"
                                                                          , ohr.order_id                                         as "ORDERNO"
                                                                          )
                                                          , xmlforest ( null                                                     as "CARRIER"
                                                                      , null                                                     as "SERVICE"
                                                                      , null                                                     as "SERVICELEVEL"
                                                                      , ohr.purchase_order                                       as "PURCHASEORDERNO"
                                                                      , ohr.order_reference                                      as "CUSTOMERORDERNO"
                                                                      , ohr.order_type                                           as "ORDERTYPE"
                                                                      , null                                                     as "CUSTNO"
                                                                      , to_char(ohr.order_date,'YYYY-MM-DD"T"HH24:MI:SS')
                                                                                            || '+01:00'                          as "ORDERDATE"      --2016-05-23T01:01:01+01:00
                                                                      , null                                                     as "DELIVERYDATE"   --2016-09-09
                                                                      , to_char(decode(to_char(nvl(ohr.ship_by_date, sysdate),'DY'),'SUN', nvl(ohr.ship_by_date, sysdate)+1
                                                                                                                                   ,'SAT', nvl(ohr.ship_by_date, sysdate)+2
                                                                                                                                   ,'FRI', decode(least(to_char(nvl(ohr.ship_by_date, sysdate),'HH24'),19), 19, nvl(ohr.ship_by_date, sysdate)+3
                                                                                                                                                                                                          , nvl(ohr.ship_by_date, sysdate)
                                                                                                                                                 ) 
                                                                                                                                         , decode(least(to_char(nvl(ohr.ship_by_date, sysdate),'HH24'),19), 19, nvl(ohr.ship_by_date, sysdate)+1
                                                                                                                                                                                                          , nvl(ohr.ship_by_date, sysdate)
                                                                                                                                                 )
                                                                                       )
                                                                               ,'YYYY-MM-DD'
                                                                               )                                                 as "SHIPDATE" --2016-09-08
                                                                      , null                                                     as "EARLYSHIPDATE"  --1900-01-01
                                                                      , null                                                     as "LATESHIPDATE>"  --2016-09-01
                                                                      , decode(lower(ohr.freight_charges), 'prepaid'  , 'SENDER'
                                                                                                         , 'collect'  , 'CONSIGNEE'
                                                                                                         , '3rd party', 'THIRDPART'
                                                                                                         , 'p'        , 'SENDER'
                                                                                                         , 'pp'       , 'SENDER'
                                                                                                         , 'pc'       , 'SENDER'
                                                                                                         , 'pb'       , 'SENDER'
                                                                                                         , 'c'        , 'CONSIGNEE'
                                                                                                         , 'cc'       , 'CONSIGNEE'
                                                                                                         , 'cb'       , 'CONSIGNEE'
                                                                                                         , 'b'        , 'THIRDPART'
                                                                                                         , 'bb'       , 'THIRDPART'
                                                                                                                      , 'SENDER'
                                                                              )                                                  as "TERMSOFPAYMENT"
                                                                      , ohr.tod                                                  as "TERMSOFDELIVERY"
                                                                      , xmlcdata (ohr.tod_place)                                 as "TERMSOFDELIVERYLOCATION"
                                                                      , to_char(nvl(ohr.inv_total_1,0.01),'fm999999990.90')      as "ORDERVALUE"
                                                                      , nvl(ohr.inv_currency,'EUR')                              as "ORDERVALUECURRENCY"
                                                                      , null                                                     as "CUSTOMSID"
                                                                      , null                                                     as "MRN"
                                                                      , xmlcdata (ohr.order_id)                                  as "SHIPPERREFERENCE1"
                                                                      , null                                                     as "SHIPPERREFERENCE2"
                                                                      , xmlcdata (nvl( ohr.purchase_order
                                                                                     , ohr.order_reference
                                                                                     )
                                                                                 )                                               as "RECEIVERREFERENCE1"
                                                                      , null                                                     as "RECEIVERREFERENCE2"
                                                                      , null                                                     as "DELIVERYINSTRUCTION1"
                                                                      , null                                                     as "DELIVERYINSTRUCTION2"
                                                                      , null                                                     as "DELIVERYINSTRUCTION3"
                                                                      , null                                                     as "DELIVERYINSTRUCTION4"
                                                                      , null                                                     as "CARRIERACCOUNTNUMBER"
                                                                      , to_char(nvl(ohr.order_weight,0.01),'fm999990.90')        as "WEIGHT"                   --100.0
                                                                      , to_char(nvl(ohr.order_volume,0.01),'fm999990.90')        as "VOLUME"                   --1.0
                                                                      )
                                                          -- BUSINESSRULES
                                                          , decode( b_businessrules, null, null
                                                                                     , xmltype ( b_businessrules)
                                                                  )
                                                          -- ADDRESSES
                                                          , decode( b_addresses, null, null
                                                                                     , xmltype ( b_addresses)
                                                                  )
                                                          -- ATTRIBUTES
                                                          , decode( b_attributes, null, null
                                                                                     , xmltype ( b_attributes)
                                                                  )
                                                          )
                                             )
                               )
      from    dcsdba.order_header ohr
      where   ohr.from_site_id = b_site_id
      and     ohr.client_id    = b_client_id
      and     ohr.order_id     = b_order_id
      ; 
    cursor c_so ( b_msg_header   varchar2
                , b_orders       varchar2
                )
    is
      select xmltype.getClobVal ( xmlelement ( "SAVEORDER"
                                             , xmlattributes ( g_xmlns as "xmlns")
                                             , xmltype ( b_msg_header)
                                             , decode( b_orders, null, null
                                                                     , xmltype ( b_orders)
                                                     )
                                             )
                                )
      from   dual  
      ;                          
    cursor c_oay ( b_client_id   varchar2
                 , b_order_id    varchar2
                 , b_accessorial varchar2
                 )
    is
      select substr(oay.accessorial, 1, 10) accessorial 
      from   dcsdba.order_accessory oay
      where  oay.client_id   = b_client_id
      and    oay.order_id    = b_order_id
      and    oay.accessorial = nvl( b_accessorial, oay.accessorial)
      order  by accessorial
      ;
    cursor c_haz ( b_client_id in varchar2
                 , b_order_id in varchar2
                 )
    is
      select sku.client_id
      ,      sku.sku_id
      ,      sku.hazmat                                          hazmat_yn
      ,      hmt.hazmat_id        
      ,      hhs.hazmat_class                                    un_class
      ,      hmt.notes                                           un_desc
      ,      hmt.user_def_type_1                                 un_code
      ,      hmt.user_def_type_2                                 cto_type
      ,      hmt.user_def_type_3                                 un_pack_grp
      ,      hmt.user_def_type_4                                 un_pack_instr
      ,      hmt.user_def_note_1                                 cto_carrier_desc
      from   dcsdba.order_line          ole
      ,      dcsdba.sku                 sku
      ,      dcsdba.hazmat              hmt
      ,      dcsdba.hazmat_hazmat_class hhs    
      where  sku.hazmat           = g_yes
      and    sku.hazmat_id        = hmt.hazmat_id
      and    hmt.hazmat_id        = hhs.hazmat_id
      and    ole.client_id        = sku.client_id
      and    ole.sku_id           = sku.sku_id 
      and    ole.client_id        = b_client_id
      and    ole.order_id         = b_order_id
      order  by hmt.hazmat_id
      ;
    cursor c_clt ( b_client_id varchar2)
    is
      select nvl(user_def_type_2, g_false)                  consolidation_tf
      ,      nvl(user_def_type_4, g_centiro_def_goods_desc) goods_desc
      from   dcsdba.client
      where  client_id = b_client_id
      ;

    r_ord              c_ord%rowtype;
    r_oay              c_oay%rowtype;
    r_clt              c_clt%rowtype;
    r_haz              c_haz%rowtype; 

    l_cto_msg_id       varchar2(20);
    l_msg_header       varchar2(8192);
    l_businessrules    varchar2(8192);
    l_addresses        varchar2(8192);
    l_attributes       varchar2(8192);
    l_orders           varchar2(32767); 
    l_saveorder        varchar2(32767);
    l_filename         varchar2(100); 
    l_accessorial      varchar2(10);
    l_consolidation_tf varchar2(5);
    l_goods_desc       varchar2(50); 
    l_dng               varchar2(5);               
    l_dnglb             varchar2(5);
    l_dng_type          varchar2(5);      
    l_dng_un            varchar2(30);
    l_dng_description   varchar2(200);
    l_dng_class         varchar2(30);
    l_dng_packagegroup  varchar2(30);
    l_doa_result        integer;
    l_doa_merge_error   varchar2(40);
    l_sender_yn         varchar2(1) := g_no;
    l_3rdparty_yn       varchar2(1) := g_no;
    l_site_translated	dcsdba.site.site_id%type;
  begin
	g_rtn := 'create_saveorder';
	if	p_site_id_i = 'RCLEHV'
	then	
		l_site_translated := 'NLSBR01';
	elsif	p_site_id_i = 'RCLTLB'
	then	
		l_site_translated := 'NLTLG01';
	elsif	p_site_id_i = 'GBMIK01'
	then	
		l_site_translated := 'GBASF02';
	else
		l_site_translated := p_site_id_i;
	end if;

    -- fetch order data
    open  c_ord ( b_site_id   => p_site_id_i
                , b_client_id => p_client_id_i
                , b_order_id  => p_order_id
                );
    fetch c_ord
    into  r_ord;
    close c_ord;

    -- fetch client data
    open  c_clt ( b_client_id => p_client_id_i);
    fetch c_clt
    into  r_clt;
    close c_clt;

    l_consolidation_tf := r_clt.consolidation_tf;
    l_goods_desc       := r_clt.goods_desc;

    -- delete all existing Order Accessorials to prevent invalid Carrier Add-On's
    delete dcsdba.order_accessory
    where  client_id = p_client_id_i
    and    order_id  = p_order_id
    ;

    -- fetch id for file and msg_id
    select lpad(to_char(cnl_cto_msg_id_seq1.nextval),10,0)
    into   l_cto_msg_id
    from   dual;

    -- get content
    -- messageheader segment
    l_msg_header := add_message_header ( p_msg_name_i      => g_saveorder
                                       , p_msg_version_i   => g_msg_version
                                       , p_msg_id_i        => l_cto_msg_id
                                       , p_site_id_i       => l_site_translated
                                       , p_source_system_i => g_centiro_wms_source
                                       , p_dest_system_i   => g_centiro_wms_dest
                                       , p_cto_client_id_i => r_ord.client_id || '@' || l_site_translated
                                       );

    -- businessrules segment
    l_businessrules := add_businessrules ( p_br_name01_i  => g_acss
                                         , p_br_value01_i => g_true
                                         , p_br_name02_i  => g_acsslogg
                                         , p_br_value02_i => g_centiro_acss_logg_yn
                                         , p_br_name03_i  => g_consolidation
                                         , p_br_value03_i => l_consolidation_tf
                                         , p_br_name04_i  => null
                                         , p_br_value04_i => null
                                         , p_br_name05_i  => null
                                         , p_br_value05_i => null
                                         );
    -- addresses segment
    if r_ord.hub_carrier_id in ('OLL','OLH')
    then
      l_sender_yn := g_yes;
    else
      l_sender_yn := g_no;
    end if;

    if r_ord.freight_charges in ('3rd party', 'b', 'bb')
    then
      l_3rdparty_yn := g_yes;
    else
      l_3rdparty_yn := g_no;
    end if;

    l_addresses := add_addresses ( p_colpoint_yn_i       => g_no
                                 , p_sender_yn_i         => l_sender_yn
                                 , p_3rdparty_yn_i       => l_3rdparty_yn
                                 , p_taxduty_payer_yn_i  => g_no
                                 , p_cnee_account_i      => null
                                 , p_transport_account_i => null
                                 , p_taxduty_account_i   => null
                                 , p_site_id_i           => p_site_id_i
                                 , p_client_id_i         => p_client_id_i
                                 , p_order_id_i          => p_order_id
                                 );

    -- check if order contains DG 
    open  c_haz ( b_client_id => r_ord.client_id
                , b_order_id  => r_ord.order_id
                );
    fetch c_haz
    into  r_haz;
    --
    if c_haz%found
    then
      if r_haz.cto_type = g_lb
      then
        l_dnglb := g_true;
      else
        l_dng   := g_true;
      end if;
      l_dng_type         := r_haz.cto_type;
      l_dng_un           := r_haz.un_code;
      l_dng_description  := r_haz.cto_carrier_desc;
      l_dng_class        := r_haz.un_class;
      l_dng_packagegroup := r_haz.un_pack_grp;
    else
      l_dng      := null;
      l_dnglb    := null;
      l_dng_type := null;
    end if;
    close c_haz;

    -- check if Order Accessorial exists for DG Type and add if not
    open  c_oay ( b_client_id   => r_ord.client_id
                , b_order_id    => r_ord.order_id
                , b_accessorial => l_dng_type
                );
    fetch c_oay
    into  r_oay;
    --
    if c_oay%notfound
    then
      l_doa_result := dcsdba.libmergeorderaccessory.directorderaccessory ( p_mergeerror   => l_doa_merge_error
                                                                         , p_toupdatecols => null
                                                                         , p_mergeaction  => 'A'
                                                                         , p_clientid     => r_ord.client_id
                                                                         , p_orderid      => r_ord.order_id
                                                                         , p_accessorial  => l_dng_type
                                                                         , p_timezonename => 'Europe/Amsterdam'
                                                                         );
    end if;
    --
    close c_oay;

    -- Get Order Accessorials from WMS
    for r_oay in c_oay ( b_client_id   => r_ord.client_id
                       , b_order_id    => r_ord.order_id
                       , b_accessorial => null
                       )
    loop
      if length( l_accessorial) + length( r_oay.accessorial) > 10
      then
        exit;
      else  
        l_accessorial := substr( l_accessorial || r_oay.accessorial, 1, 10);
      end if;
    end loop;

    -- attributes segment
    l_attributes := add_attributes ( p_at_name01_i  => g_customer
                                   , p_at_value01_i => r_ord.customer_id
                                   , p_at_name02_i  => g_cod
                                   , p_at_value02_i => r_ord.cod_tf
                                   , p_at_name03_i  => g_dispatchmethod
                                   , p_at_value03_i => r_ord.dispatch_method
                                   , p_at_name04_i  => g_misc
                                   , p_at_value04_i => r_ord.misc
                                   , p_at_name05_i  => g_addonservice
                                   , p_at_value05_i => l_accessorial
                                   , p_at_name06_i  => null
                                   , p_at_value06_i => null
                                   , p_at_name07_i  => null
                                   , p_at_value07_i => null
                                   , p_at_name08_i  => null
                                   , p_at_value08_i => null
                                   , p_at_name09_i  => null
                                   , p_at_value09_i => null
                                   , p_at_name10_i  => null
                                   , p_at_value10_i => null
                                   );
    -- orders segment
    open  c_orders ( b_site_id       => p_site_id_i
                   , b_client_id     => p_client_id_i
                   , b_order_id      => p_order_id
                   , b_businessrules => l_businessrules
                   , b_addresses     => l_addresses
                   , b_attributes    => l_attributes
                   );
    fetch c_orders
    into  l_orders;
    close c_orders;

    -- get the complete content together
    open  c_so (b_msg_header => l_msg_header
               ,b_orders     => l_orders
               );
    fetch c_so
    into  l_saveorder;
    close c_so;

    -- create file with content
    create_cto_file ( p_cto_file_type_i => g_saveorder
                    , p_cto_out_dir_i   => g_centiro_so_dir
                    , p_msg_id_i        => l_cto_msg_id
                    , p_site_id_i       => l_site_translated
                    , p_client_id_i     => r_ord.client_id
                    , p_order_id_i      => r_ord.order_id
                    , p_container_id_i  => null
                    , p_content_i       => l_saveorder
                    , p_filename_o      => l_filename
                    );

    -- update order_header with "OrderVersion", increase by 1
    update dcsdba.order_header ohr
    set    ohr.tm_stop_seq = nvl(ohr.tm_stop_seq, 0) + 1
    where  ohr.client_id   = r_ord.client_id
    and    ohr.order_id    = r_ord.order_id
    ;

  end create_saveorder;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create PackParcel interface file to Centiro
------------------------------------------------------------------------------------------------
	procedure create_packparcel( p_site_id_i	in  varchar2
				   , p_client_id_i    	in  varchar2
				   , p_order_id_i     	in  varchar2
				   , p_pallet_id_i    	in  varchar2 := null
				   , p_container_id_i 	in  varchar2 := null
				   , p_printer_i      	in  varchar2
				   , p_copies_i       	in  number 
				   , p_print2file_i   	in  varchar2
				   , p_rtk_key_i	in  integer
				   , p_run_task_i	in  dcsdba.run_task%rowtype
				   )
	is
		-- Collect order_container order data
		cursor	c_ocr_con( b_site_id		in varchar2
				 , b_client_id    	in varchar2
				 , b_order_id     	in varchar2
				 , b_container_id 	in varchar2
				 )
		is
			select ohr.from_site_id
			,      ohr.client_id
			,      ohr.owner_id
			,      ohr.order_id
			,      ohr.hub_carrier_id
			,      lower(ohr.freight_charges)                    	freight_charges
			,      nvl(ohr.freight_terms, ohr.hub_vat_number)    	transport_account
			,      ohr.letter_of_credit                          	taxduties_account
			,      cty.ce_eu_type                                	cty_eu_type
			,      ocr.container_id
			,      ocr.container_type
			,      ocr.pallet_id
			,      ocr.config_id                                 	pallet_type
			,      ocr.container_n_of_n
			,      ohr.customer_id
			,      nvl(ohr.cod, g_no)                            	cod
			,      decode( nvl(ohr.cod, g_no), g_yes, g_true
							 , g_no , g_false)	cod_tf
			,      ohr.carrier_id
			,      ohr.service_level
			,      nvl( ocr.container_weight, ocr.pallet_weight) 	weight
			,      nvl( ocr.container_height, ocr.pallet_height) 	height
			,      nvl( ocr.container_width,  ocr.pallet_width)  	width
			,      nvl( ocr.container_depth,  ocr.pallet_depth)  	depth
			,      ohr.tax_amount_5                              	cto_nacex_copies
			,      'JDA2016'                                     	wms_database
			from   dcsdba.order_header    ohr
			,      dcsdba.order_container ocr
			,      dcsdba.country         cty
			where  ohr.client_id    = ocr.client_id
			and    ohr.order_id     = ocr.order_id
			and    ohr.country      = cty.iso3_id
			and    ohr.from_site_id = b_site_id
			and    ohr.client_id    = b_client_id
			and    ohr.order_id     = b_order_id
			and    ocr.container_id = nvl( b_container_id, ocr.container_id)
		;

		-- Collect order container pallet order data
		cursor	c_ocr_pal( b_site_id   	in varchar2
				 , b_client_id 	in varchar2
				 , b_order_id  	in varchar2
				 , b_pallet_id 	in varchar2
				 )
		is
			select	distinct -- 1 record only
				ohr.from_site_id
			,      	ohr.client_id
			,      	ohr.owner_id
			,      	ohr.order_id
			,      	ohr.hub_carrier_id
			,      	lower(ohr.freight_charges)                    	freight_charges
			,      	nvl(ohr.freight_terms, ohr.hub_vat_number)    	transport_account
			,      	ohr.letter_of_credit                          	taxduties_account
			,      	cty.ce_eu_type                                	cty_eu_type
			,      	null                                          	container_id
			,      	null                                          	container_type
			,      	ocr.pallet_id
			,      	ocr.config_id                                 	pallet_type
			,      	null                                          	container_n_of_n
			,      	ohr.customer_id
			,      	nvl(ohr.cod, g_no)                            	cod
			,      	decode( nvl(ohr.cod, g_no), g_yes, 'TRUE'
			 				  , g_no , 'FALSE')	cod_tf
			,      	ohr.carrier_id
			,      	ohr.service_level
			,      	max( nvl( ocr.pallet_weight, 1))              	weight
			,      	max( nvl( ocr.pallet_height, 1))              	height
			,      	max( nvl( ocr.pallet_width,  1))              	width
			,      	max( nvl( ocr.pallet_depth,  1))              	depth
			,      	ohr.tax_amount_5                              	cto_nacex_copies
			,      	'JDA2016'                                     	wms_database
			from   	dcsdba.order_header    ohr
			,      	dcsdba.order_container ocr
			,      	dcsdba.country         cty
			where  	ohr.client_id    = ocr.client_id
			and    	ohr.order_id     = ocr.order_id
			and    	ohr.country      = cty.iso3_id
			and    	ohr.from_site_id = b_site_id
			and    	ohr.client_id    = b_client_id
			and    	ohr.order_id     = b_order_id
			and    	ocr.pallet_id    = nvl( b_pallet_id, ocr.pallet_id)
			group  
			by 	ohr.from_site_id
			,      	ohr.client_id
			,      ohr.owner_id
			,      ohr.order_id
			,      ohr.hub_carrier_id
			,      ohr.freight_charges
			,      nvl(ohr.freight_terms, ohr.hub_vat_number)
			,      ohr.letter_of_credit
			,      cty.ce_eu_type
			,      ocr.pallet_id
			,      ocr.config_id
			,      ohr.customer_id
			,      nvl(ohr.cod, g_no)
			,      ohr.carrier_id
			,      ohr.service_level 
			,      ohr.tax_amount_5   
		;

		-- Get shipment data ready for CTO
		cursor	c_shipment( b_site_id		in varchar2
				  , b_client_id         in varchar2
				  , b_order_id          in varchar2
				  , b_printer           in varchar2
				  , b_dws_labels_path   in varchar2
				  , b_departureid       in varchar2
				  )
		is
			select	xmltype.getClobVal 
			( 	xmlelement 
				(	"SHIPMENT"
				, 	xmlforest 
					(	ohr.client_id || '@' || decode(ohr.from_site_id,
						'GBMIK01','GBASF02',ohr.from_site_id)				as "CLIENTID"
					, 	lower( ohr.carrier_id)                                 		as "CARRIER"
					, 	ohr.service_level                                      		as "SERVICE"
					, 	ohr.order_id                                           		as "ORDERNO"
                                        , 	b_printer                                              		as "PRINTERNODE"
					, 	decode( instr( b_printer, 'PRL'), 0, 'LASER', 'ZEBRA')  	as "PRINTERTYPE"
					, 	decode( b_dws_labels_path, null, null, lower( g_true))		as "PRINTTOFILE"
					, 	b_dws_labels_path                                      		as "PRINTTOFOLDER"
					, 	lpad( nvl( ohr.uploaded_ws2pc_id, ohr.psft_order_id),10,0)	as "SHIPMENTID"
					, 	'REGULAR'                                              		as "SHIPMENTTYPE"
					, 	to_char(greatest(decode(to_char(sysdate,'DY'),'SUN', sysdate+1
											     ,'SAT', sysdate+2
                                                                                             ,'FRI', 
						decode(least(to_char(sysdate,'HH24'),19),19,sysdate+3,sysdate), 
						decode(least(to_char(sysdate,'HH24'),19),19,sysdate+1,sysdate)),
						decode(to_char(nvl(ship_by_date,sysdate),'DY'),'SUN', 
						nvl(ship_by_date,sysdate)+1,'SAT', nvl(ship_by_date,sysdate)+2,
						'FRI', decode(least(to_char(nvl(ship_by_date,sysdate),
						'HH24'),19), 19, nvl(ship_by_date,sysdate)+3, 
						nvl(ship_by_date,sysdate)), 
						decode(least(to_char(nvl(sysdate,sysdate),'HH24'),19),
						19, nvl(ship_by_date,sysdate)+1, nvl(ship_by_date,sysdate))))
						,'YYYY-MM-DD')                        				as "SHIPDATE" --2016-09-08
					, 	xmlcdata (ohr.order_id)                                		as "SHIPPERREFERENCE1"
					, 	null                                                   		as "SHIPPERREFERENCE2"
					, 	xmlcdata (ohr.order_reference)                         		as "RECEIVERREFERENCE1"
					, 	xmlcdata (ohr.purchase_order)                          		as "RECEIVERREFERENCE2"
					, 	ohr.tod                                                		as "TERMSOFDELIVERY"
					, 	xmlcdata (ohr.tod_place)                               		as "TERMSOFDELIVERYLOCATION"
					, 	decode( g_centiro_billingcode_yn, g_no, null, 
						decode( cty.ce_eu_type, 'EU', decode(lower(ohr.freight_charges),
						'prepaid'  , 'P', 
						'collect'  , 'C', 
						'3rd party', 'B',
						'p'        , 'P', 
						'c'        , 'C', 
						'b'        , 'B', 
						'pp'       , 'P',
						'pc'       , 'P', 
						'pb'       , 'P', 
						'cc'       , 'C', 
						'cb'       , 'C', 
						'bb'       , 'B', 
						'P'),decode(lower(ohr.freight_charges), 
						'prepaid'  , 'PP', 
						'collect'  , 'CC', 
						'3rd party', 'BB', 
						'pp'       , 'PP', 
						'pc'       , 'PC', 
						'pb'       , 'PB', 
						'cc'       , 'CC', 
						'cb'       , 'CB', 
						'bb'       , 'BB', 
						'p'        , 'PP', 
						'c'        , 'CC', 
						'b'        , 'BB', 
						'PP')))                                                		as "BILLINGCODE"
					, 	decode( g_centiro_billingcode_yn, g_yes, null, 
						decode(lower(ohr.freight_charges), 
						'prepaid'  , 'SENDER', 
						'collect'  , 'CONSIGNEE', 
						'3rd party', 'THIRDPART', 'SENDER'))				as "TRANSPORTPAYER"
					, 	decode( g_centiro_billingcode_yn, g_yes, null, 
						decode(lower(ohr.freight_charges), 
						'3rd party', ohr.hub_vat_number, null))				as "TRPPAYERCUSTNOSENDER"
					, 	decode( g_centiro_billingcode_yn, g_yes, null, 
						decode(lower(ohr.freight_charges), 
						'collect'  , ohr.hub_vat_number, null))				as "TRPPAYERCUSTNOCONSIGNEE"
					, 	decode( ohr.cod, g_yes, to_char(ohr.cod_value,
						'fm99999990.90'), null)						as "CODAMOUNT"
					, 	decode( ohr.cod, g_yes, xmlcdata (ohr.inv_reference), null)	as "CODREFERENCE"
					, 	decode( ohr.cod, g_yes, ohr.cod_currency, null)			as "CODCURRENCY"
					, 	substr(trim(regexp_replace(ohr.instructions,
						'[[:punct:]]',' ')),  1,50) 					as "DELIVERYINSTRUCTION1"
					, 	substr(trim(regexp_replace(ohr.instructions,
						'[[:punct:]]',' ')), 51,50) 					as "DELIVERYINSTRUCTION2"
					, 	substr(trim(regexp_replace(ohr.instructions,
						'[[:punct:]]',' ')),101,50) 					as "DELIVERYINSTRUCTION3"
					, 	to_char(nvl(ohr.inv_total_1,0.01),'fm999999990.90')       	as "SHIPMENTVALUE"
					, 	nvl(ohr.inv_currency,'EUR')                            		as "SHIPMENTVALUECURRENCY"
					, 	nvl(b_departureid, ohr.consignment)                    		as "DEPARTUREID"
					, 	null                                                   		as "WEIGHT"                   --100.0
					, 	null                                                   		as "VOLUME"                   --1.0
					, 	null                                                   		as "LOADINGMEASURE"
					)
				)
			)
			from	dcsdba.order_header ohr
			,       dcsdba.country      cty
			where   ohr.country         = cty.iso3_id
			and     ohr.from_site_id    = b_site_id
			and     ohr.client_id       = b_client_id
			and     ohr.order_id        = b_order_id
		;

		-- Create PP envelope
		cursor	c_pp( b_msg_header	varchar2
			    , b_shipment     	varchar2
			    )
		is
			select 	replace
				( 	replace
					( 	xmltype.getClobVal 
						( 	xmlelement 
							( 	"PACKPARCEL"
							, 	xmlattributes	( g_xmlns 	as "xmlns")
							, 	xmltype 	( b_msg_header)
							, 	decode( b_shipment, null, null, xmltype ( b_shipment))
							)
						)
					, 	chr(10) || '</SHIPMENT>'
					)
				, 	chr(10) || '</PACKPARCEL>'
				) data
			from   	dual  
		;

		-- Fetch ordre accessorials
		cursor	c_oay( b_client_id	varchar2
			     , b_order_id    	varchar2
			     , b_accessorial 	varchar2
			     )
		is
			select	substr(oay.accessorial, 1, 10) accessorial
			from   	dcsdba.order_accessory oay
			where  	oay.client_id          = b_client_id
			and    	oay.order_id           = b_order_id
			and    	oay.accessorial        = nvl( b_accessorial, oay.accessorial)
			union  	-- Add DTP for DHL, non-EU, DDP, non-BBX shipments
			select 	'DTP'                  accessorial
			from   	dcsdba.order_header    ohr
			,      	dcsdba.country         cty
			where  	ohr.client_id          = b_client_id
			and    	ohr.order_id           = b_order_id
			and    	(
				ohr.country            = cty.iso3_id
				or
				ohr.country            = cty.iso2_id
				)
			and    	nvl(cty.ce_eu_type, '|^|') != 'EU'
			and    	upper(ohr.carrier_id)  = 'DHL.COM'
			and    	ohr.service_level      != 'BBX'
			and    	ohr.tod                = 'DDP'
			and     ohr.client_id not in ('FLIRS', 'FLIRB', 'IROECOM')
			order  
			by 	accessorial
		;

		-- Get client information
		cursor	c_clt( b_client_id	varchar2)
		is
			select	nvl(user_def_type_2, g_false)                  consolidation_tf
			,      	nvl(user_def_type_4, g_centiro_def_goods_desc) goods_desc
			from   	dcsdba.client
			where  client_id = b_client_id
		;

		-- Get carrier user defined details
		cursor	c_crr( b_site_id	in varchar2
			     , b_client_id     	in varchar2
			     , b_carrier_id    	in varchar2
			     , b_service_level 	in varchar2
			     )
		is
			select	crr.user_def_type_5	cto_open_smt_tf
			,      	crr.user_def_type_6     cto_departureid
			,      	crr.user_def_type_7     cto_use_dws_tf
			,      	crr.user_def_type_8     cto_nacex_crw
			from   	dcsdba.carriers   crr
			where  	(
				site_id       = b_site_id
				or
				site_id       is null
				)
			and    	crr.client_id     = b_client_id
			and    	crr.carrier_id    = b_carrier_id
			and    	crr.service_level = b_service_level
		;

		-- Get hazmat details for inventory
		cursor	c_haz( b_client_id	in varchar2
			     , b_parcel_id 	in varchar2
			     )
		is
			select	sku.client_id
			,      	sku.sku_id
			,      	sku.hazmat                                          hazmat_yn
			,      	hmt.hazmat_id        
			,      	hhs.hazmat_class                                    un_class
			,      	hmt.notes                                           un_desc
			,      	hmt.user_def_type_1                                 un_code
			,      	hmt.user_def_type_2                                 cto_type
			,      	hmt.user_def_type_3                                 un_pack_grp
			,      	hmt.user_def_type_4                                 un_pack_instr
			,      	hmt.user_def_type_5                                 un_accessibility
			,      	hmt.user_def_note_1                                 cto_carrier_desc
			,      	sum(nvl(sku.each_weight, 0) * mvt.qty_to_move/*ivy.qty_on_hand*/)      tot_sku_weight
			,      	sum(mvt.qty_to_move/*ivy.qty_on_hand*/)                                tot_sku_qty
			from   	dcsdba.move_task	   mvt--dcsdba.inventory           ivy
			,      	dcsdba.sku                 sku
			,      	dcsdba.hazmat              hmt
			,      	dcsdba.hazmat_hazmat_class hhs    
			where  	sku.hazmat           = g_yes
			and    	sku.hazmat_id        = hmt.hazmat_id
			and    	hmt.hazmat_id        = hhs.hazmat_id
			and    	hmt.user_def_type_1  is not null    -- UN Code
			and    	hmt.user_def_type_2  is not null    -- DG Type
			and    	mvt/*ivy*/.client_id        = sku.client_id
			and    	mvt/*ivy*/.sku_id           = sku.sku_id 
			and    	mvt/*ivy*/.client_id        = b_client_id
			and    	(
				mvt/*ivy*/.pallet_id        = b_parcel_id
				or
				mvt/*ivy*/.container_id     = b_parcel_id
				or 
				mvt.to_container_id	    = b_parcel_id
				or 
				mvt.to_pallet_id	    = b_parcel_id
				)
			group  
			by 	sku.client_id
			,      	sku.sku_id
			,      	sku.hazmat         
			,      	hmt.hazmat_id      
			,      	hhs.hazmat_class   
			,      	hmt.notes          
			,      	hmt.user_def_type_1
			,      	hmt.user_def_type_2
			,      	hmt.user_def_type_3
			,      	hmt.user_def_type_4
			,      	hmt.user_def_type_5
			,      	hmt.user_def_note_1
			order  	
			by 	hmt.hazmat_id
		;

		-- Get line hazmat details
		cursor	c_haz_ord( b_client_id 	in varchar2
				 , b_order_id  	in varchar2
				 )
		is
			select	distinct
				decode( hmt.user_def_type_2, 'HZ', 0, 'LB', 1, 'LQ', 2, 'EQ', 3, 4)  dg_sorter
			,      	hmt.user_def_type_2  cto_type
			,      	hmt.user_def_type_5  dg_accessibility
			from   	dcsdba.order_line    ole
			,      	dcsdba.sku           sku
			,      	dcsdba.hazmat        hmt    
			where  	sku.hazmat           = g_yes
			and    	sku.hazmat_id        = hmt.hazmat_id
			and    	hmt.user_def_type_1  is not null    -- UN Code
			and    	hmt.user_def_type_2  is not null    -- DG Type
			and    	ole.client_id        = sku.client_id
			and    	ole.sku_id           = sku.sku_id 
			and    	ole.client_id        = b_client_id
			and    	ole.order_id         = b_order_id
			order  
			by 	dg_sorter asc
		;

		-- Dry Ice
		cursor	c_dry_ocr( b_client_id 	in varchar2
				 , b_order_id  	in varchar2
				 )
		is 
			select	1
			from   	dcsdba.order_container ocr
			where  	ocr.client_id = b_client_id
			and    	ocr.order_id  = b_order_id
			and   	substr(ocr.config_id, 1, 6) = g_dryice
		;

		-- Dry ice weight
		cursor 	c_dry( b_client_id   in varchar2
			     , b_pallet_type in varchar2
			     )
		is
			select	round(pcg.packaging_weight) dry_ice_weight
			from   	dcsdba.pallet_config pcg
			where  	(
				pcg.client_id = b_client_id
				or
				pcg.client_id is null
				)
			and    	pcg.config_id = b_pallet_type
		;

		-- Centiro Pallet type
		cursor	c_pcg( b_client_id   in varchar2
			     , b_pallet_type in varchar2
			     )
		is
			select 	nvl(pcg.pallet_type_group, b_pallet_type) pallet_type
			from   	dcsdba.pallet_config pcg
			where  	(
				pcg.client_id = b_client_id
				or
				pcg.client_id is null
				)
			and    	pcg.config_id = b_pallet_type
			order  
			by 	client_id nulls last
			,      	pallet_type
		;

		-- Centiro pallet type
		cursor	c_ptg( b_client_id in varchar2
			     , b_group     in varchar2
			     )
		is
			select	nvl(ptg.notes, b_group) pallet_type_group
			from   	dcsdba.pallet_type_grp ptg
			where  	(
				ptg.client_id = b_client_id
				or
				ptg.client_id is null
				)
			and    	ptg.pallet_type_group = b_group
			order 
			by 	ptg.client_id nulls last
			,	ptg.pallet_type_group
		;

		--
		cursor	c_bbx( b_client_id in varchar2
			     , b_order_id  in varchar2
			     )
		is
			select 	substr( 
				'DHL-BBX-'	||     
				cty.iso2_id	||
				'-'             ||   -- Add DHL country hubs when shipping to other countries
				decode(cty.iso2_id, 'NO', decode(ohr.delivery_point, 'SVG', 'SVG', 'OSL', 'ONL', 'OSL'), 'NOHUB')||     
				'-'		||     
				to_char(sysdate, 'YYYYMMDD'),1,30)
			from	dcsdba.order_header ohr
			,      	dcsdba.country      cty
			where  	ohr.country = cty.iso3_id
			and    	client_id   = b_client_id
			and    	order_id    = b_order_id
		;

		-- BDS-5898 new IATA dg regulations
		-- To check if container contains DG plus female
		cursor c_iata( b_client_id 	in varchar2
			     , b_order_id	in varchar2
			     , b_container_id	in varchar2
			     , b_pallet_id	in varchar2
			     )
		is
			select	m.sku_id
			,	s.hazmat_id
			,	s.gender
			,	sum(m.qty_to_move) dg_qty
			from	dcsdba.move_task m
			inner
			join	dcsdba.sku s
			on	m.sku_id	= s.sku_id
			and	m.client_id	= s.client_id
			where	m.task_id 	= b_order_id
			and	m.client_id	= b_client_id
			and	(	(	m.to_container_id	= b_container_id
					or	m.container_id		= b_container_id
					)
				or	(	m.to_pallet_id		= b_pallet_id
					or	m.pallet_id		= b_pallet_id
					)
				)
			group
			by	m.sku_id
			,	s.hazmat_id
			,	s.gender
		;			

		-- BDS-5898 new IATA dg regulations
		-- To fetch specific hazmat id details
		cursor c_iata_dg( b_hazmat_id	in varchar2)
		is
			select	h.user_def_type_2 cto_type
			,	h.user_def_type_5 dg_accessibility
			from	dcsdba.hazmat h
			where	h.hazmat_id	= b_hazmat_id
		;

		-- BDS-5898 new IATA dg regulations
		-- Fetch DG details
		cursor c_iata_haz( b_client_id	in varchar2
				 , b_qty	in number
				 , b_sku	in varchar2
				 , b_hazmat	in varchar2
				 )
		is
			select	sku.client_id
			,      	b_sku
			,      	sku.hazmat					hazmat_yn
			,      	b_hazmat  					hazmat_id      
			,      	hhs.hazmat_class				un_class
			,      	hmt.notes					un_desc
			,      	hmt.user_def_type_1				un_code
			,      	hmt.user_def_type_2				cto_type
			,      	hmt.user_def_type_3				un_pack_grp
			,      	hmt.user_def_type_4				un_pack_instr
			,      	hmt.user_def_type_5				un_accessibility
			,      	hmt.user_def_note_1				cto_carrier_desc
			,      	sum(nvl(sku.each_weight, 0) * b_qty)		tot_sku_weight
			,      	b_qty                                		tot_sku_qty
			from   	dcsdba.sku                 sku
			,      	dcsdba.hazmat              hmt
			,      	dcsdba.hazmat_hazmat_class hhs    
			where  	sku.sku_id		= b_sku
--			and	sku.hazmat_id		= b_hazmat
			and    	hmt.hazmat_id		= b_hazmat
			and	hhs.hazmat_id		= b_hazmat
			and    	hmt.user_def_type_1  is not null    -- UN Code
			and    	hmt.user_def_type_2  is not null    -- DG Type
			and    	sku.client_id        	= b_client_id
			group  
			by 	sku.client_id
			,      	b_sku
			,      	sku.hazmat         
			,      	b_hazmat      
			,      	hhs.hazmat_class   
			,      	hmt.notes          
			,      	hmt.user_def_type_1
			,      	hmt.user_def_type_2
			,      	hmt.user_def_type_3
			,      	hmt.user_def_type_4
			,      	hmt.user_def_type_5
			,      	hmt.user_def_note_1
			,	b_qty
			order  	
			by 	hmt.hazmat_id
		;

		-- BDS-5898 new IATA DG regulations 
		l_iata_hazmat_id	dcsdba.sku.hazmat_id%type;
		l_iata_gender		dcsdba.sku.gender%type;
		l_iata_sku_id		dcsdba.sku.sku_id%type;
		l_iata_tmp_sku_id	dcsdba.sku.sku_id%type;
		l_iata_dg_qty		dcsdba.move_task.qty_to_move%type;
		l_iata_tmp_dg_qty	dcsdba.move_task.qty_to_move%type;

		-- Cursor variables
		r_ocr_con           	c_ocr_con%rowtype;
		r_ocr_pal           	c_ocr_pal%rowtype;
		r_ocr_pcl           	c_ocr_con%rowtype;
		r_oay               	c_oay%rowtype;
		r_clt               	c_clt%rowtype; 
		r_crr               	c_crr%rowtype; 
		r_haz               	c_haz%rowtype;
		r_haz_ord           	c_haz_ord%rowtype;

		-- standard variables
		l_from_site_id      	varchar2(20); 
		l_client_id         	varchar2(20);
		l_owner_id          	varchar2(20);
		l_order_id          	varchar2(20);
		l_hub_carrier_id    	varchar2(25);
		l_freight_charges   	varchar2(10);
		l_container_id      	varchar2(20);
		l_container_type    	varchar2(15);
		l_pallet_id         	varchar2(20);
		l_pallet_type       	varchar2(15);
		l_pallet_type_group 	varchar2(50);
		l_package_type      	varchar2(15);
		l_container_n_of_n  	number;
		l_customer_id       	varchar2(15);
		l_cod               	varchar2(1);
		l_cod_tf            	varchar2(5);
		l_carrier_id        	varchar2(25);
		l_service_level     	varchar2(40);
		l_consignee_account 	varchar2(35);
		l_transport_account 	varchar2(35);
		l_taxduties_account 	varchar2(35);
		l_tmp_trans_account 	varchar2(35);
		l_tmp_tduty_account 	varchar2(35);
		l_cty_eu_type       	varchar2(2);
		l_weight            	number;
		l_height            	number;
		l_width             	number;
		l_depth             	number;
		l_wms_database      	varchar2(20);
		l_cto_msg_id        	varchar2(20);
		l_file_type         	utl_file.file_type;
		l_content           	varchar2(8192);
		l_pp_tlr            	varchar2(8192) := '</SHIPMENT>'||chr(10)||'</PACKPARCEL>';        
		l_msg_header        	varchar2(8192);
		l_businessrules     	varchar2(8192);
		l_addresses         	varchar2(8192);
		l_parcel           	varchar2(8192);
		l_pcl_attributes    	varchar2(8192);
		l_smt_attributes    	varchar2(8192);
		l_sa_namedvalues    	varchar2(8192);
		l_serviceattributes 	varchar2(8192);
		l_shipment          	varchar2(32767); 
		l_packparcel        	varchar2(32767);
		l_filename          	varchar2(100);
		l_nv_name01_i       	varchar2(50); 
		l_nv_value01_i      	varchar2(50);
		l_nv_name02_i       	varchar2(50); 
		l_nv_value02_i      	varchar2(50);
		l_nv_name03_i       	varchar2(50); 
		l_nv_value03_i      	varchar2(50);
		l_nv_name04_i       	varchar2(50); 
		l_nv_value04_i      	varchar2(50);
		l_nv_name05_i       	varchar2(50); 
		l_nv_value05_i      	varchar2(50);
		l_nv_name06_i       	varchar2(50); 
		l_nv_value06_i      	varchar2(50);
		l_nv_name07_i       	varchar2(50); 
		l_nv_value07_i      	varchar2(50);
		l_nv_name08_i       	varchar2(50); 
		l_nv_value08_i      	varchar2(50);
		l_nv_name09_i       	varchar2(50); 
		l_nv_value09_i      	varchar2(50);
		l_nv_name10_i       	varchar2(50); 
		l_nv_value10_i      	varchar2(50);
		l_sa_code01_i       	varchar2(10); 
		l_sa_value01_i      	varchar2(50);
		l_sa_namedval01_i   	varchar2(8192);
		l_sa_code02_i       	varchar2(10); 
		l_sa_value02_i      	varchar2(50);
		l_sa_namedval02_i   	varchar2(8192);
		l_sa_code03_i       	varchar2(10); 
		l_sa_value03_i      	varchar2(50);
		l_sa_namedval03_i   	varchar2(8192);
		l_sa_code04_i       	varchar2(10); 
		l_sa_value04_i      	varchar2(50);
		l_sa_namedval04_i   	varchar2(8192);
		l_sa_code05_i       	varchar2(10); 
		l_sa_value05_i      	varchar2(50);
		l_sa_namedval05_i   	varchar2(8192);
		l_sa_code06_i       	varchar2(10); 
		l_sa_value06_i      	varchar2(50);
		l_sa_namedval06_i   	varchar2(8192);
		l_sa_code07_i       	varchar2(10); 
		l_sa_value07_i      	varchar2(50);
		l_sa_namedval07_i   	varchar2(8192);
		l_sa_code08_i       	varchar2(10); 
		l_sa_value08_i      	varchar2(50);
		l_sa_namedval08_i   	varchar2(8192);
		l_sa_code09_i       	varchar2(10); 
		l_sa_value09_i      	varchar2(50);
		l_sa_namedval09_i   	varchar2(8192);
		l_sa_code10_i       	varchar2(10); 
		l_sa_value10_i      	varchar2(50);
		l_sa_namedval10_i   	varchar2(8192);
		l_oay_count         	number;
		l_accessorial       	varchar2(10);
		l_consolidation_tf  	varchar2(5);
		l_goods_desc        	varchar2(50);
		l_cto_enabled_yn    	varchar2(1) := g_yes; 
		l_cto_nacex_crw     	varchar2(30);
		l_cto_nacex_copies  	number;
		l_cto_departureid   	varchar2(30);
		l_cto_depid_bbx     	varchar2(30);
		l_cto_open_smt_tf   	varchar2(5);
		l_dws_labels_path   	varchar2(100);
		l_dng               	varchar2(5);               
		l_dnglb             	varchar2(5);
		l_dng_type          	varchar2(5);      
		l_dng_type_fdx      	varchar2(5);      
		l_dng_un            	varchar2(30);
		l_dng_description   	varchar2(200);
		l_dng_class         	varchar2(30);
		l_dng_packagegroup  	varchar2(30);
		l_dng_packageinstr  	varchar2(30);
		l_dng_accessibility 	varchar2(30);
		l_dng_netweight     	number;
		l_dng_quantity      	number;
		l_dng_di_wgt_desc   	varchar2(50);
		l_doa_result        	integer;
		l_doa_merge_error   	varchar2(40);
		l_sender_yn         	varchar2(1) := g_no;
		l_3rdparty_yn       	varchar2(1) := g_no;
		l_taxduty_payer_yn  	varchar2(1) := g_no;
		l_integer           	integer;
		l_site_translated	dcsdba.site.site_id%type;
		l_rtn			varchar2(30) := 'create_packparcel';
		l_pp_dir		integer;
	begin
		g_rtn := l_rtn; -- Required to exclude logging for save order that is using the same sub procedures
		-- Set global print id for logging
		g_print_id := p_rtk_key_i;

		-- Set time zone
		execute immediate 'alter session set time_zone = ''Europe/Berlin''';

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> null
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start centiro packparcek procedure'
								   , p_code_parameters_i 	=> '"printer" "'||p_printer_i||'" '
												|| '"copies" "'||p_copies_i||'" '
												|| '"print2file" "'||p_print2file_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- fetch client data
		open	c_clt ( b_client_id => p_client_id_i);
		fetch 	c_clt
		into  	r_clt;
		close 	c_clt;

		l_consolidation_tf := r_clt.consolidation_tf;
		l_goods_desc       := r_clt.goods_desc;

		-- fetch order data
		-- Pack Parcel by Container ID
		if	p_pallet_id_i 		is null 
		and 	p_container_id_i 	is not null
		then
			open	c_ocr_con( b_site_id      => p_site_id_i
					 , b_client_id    => p_client_id_i
					 , b_order_id     => p_order_id_i
					 , b_container_id => p_container_id_i
					 );
			fetch 	c_ocr_con
			into  	r_ocr_con;
			close 	c_ocr_con;

			l_from_site_id		:= r_ocr_con.from_site_id;
			l_client_id         	:= r_ocr_con.client_id;
			l_owner_id          	:= r_ocr_con.owner_id;
			l_order_id          	:= r_ocr_con.order_id;
			l_hub_carrier_id    	:= r_ocr_con.hub_carrier_id;
			l_freight_charges   	:= r_ocr_con.freight_charges;
			l_tmp_trans_account 	:= r_ocr_con.transport_account;
			l_tmp_tduty_account 	:= r_ocr_con.taxduties_account;
			l_cty_eu_type       	:= r_ocr_con.cty_eu_type;
			l_container_id      	:= r_ocr_con.container_id;
			l_container_type    	:= r_ocr_con.container_type;
			l_pallet_id         	:= r_ocr_con.pallet_id;
			l_pallet_type       	:= r_ocr_con.pallet_type;
			l_container_n_of_n  	:= r_ocr_con.container_n_of_n;
			l_customer_id       	:= r_ocr_con.customer_id;
			l_cod               	:= r_ocr_con.cod;
			l_cod_tf            	:= r_ocr_con.cod_tf;
			l_carrier_id        	:= r_ocr_con.carrier_id;
			l_service_level     	:= r_ocr_con.service_level;
			l_weight            	:= r_ocr_con.weight;
			l_height            	:= r_ocr_con.height;
			l_width             	:= r_ocr_con.width;
			l_depth             	:= r_ocr_con.depth;
			l_wms_database      	:= r_ocr_con.wms_database;

			if	l_carrier_id 	= g_nacex
			then
				l_cto_nacex_copies	:= r_ocr_con.cto_nacex_copies;
			end if;

			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> null
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Pack parcel by container id'
									   , p_code_parameters_i 	=> '"printer" "'||p_printer_i||'" '
													|| '"copies" "'||p_copies_i||'" '
													|| '"print2file" "'||p_print2file_i||'" '
													|| '"owner_id" "'||l_owner_id||'" '
													|| '"hub_carrier_id" "'||l_hub_carrier_id||'" '
													|| '"freight_charges" "'||l_freight_charges||'" '
													|| '"tmp_trans_account" "'||l_tmp_trans_account||'" '
													|| '"tmp_tduty_account" "'||l_tmp_tduty_account||'" '
													|| '"cty_eu_type" "'||l_cty_eu_type||'" '
													|| '"container_type" "'||l_container_type||'" '
													|| '"pallet_type" "'||l_pallet_type  ||'" '
													|| '"container_n_of_n" "'||l_container_n_of_n||'" '
													|| '"customer_id" "'||l_customer_id||'" '
													|| '"cod" "'||l_cod||'" '
													|| '"cod_tf" "'||l_cod_tf||'" '
													|| '"carrier_id" "'||l_carrier_id||'" '
													|| '"service_level" "'||l_service_level||'" '
													|| '"weight" "'||l_weight||'" '
													|| '"height" "'||l_height||'" '
													|| '"width" "'||l_width||'" '
													|| '"depth" "'||l_depth||'" '
													|| '"consolidation_tf" "'||l_consolidation_tf||'" '
													|| '"goods description" "'||l_goods_desc||'" '
													|| '"cto_nacex_copies" "'||l_cto_nacex_copies||'" '
									   , p_order_id_i		=> l_order_id
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> l_pallet_id
									   , p_container_id_i		=> l_container_id
									   , p_site_id_i		=> p_site_id_i
									   );
			end if;
		end if;

		-- Pack Parcel by Pallet ID
		if	p_pallet_id_i 		is not null
		and 	p_container_id_i 	is null
		then
			open	c_ocr_pal( b_site_id   => p_site_id_i
					 , b_client_id => p_client_id_i
					 , b_order_id  => p_order_id_i
					 , b_pallet_id => p_pallet_id_i
					 );
			fetch 	c_ocr_pal
			into  	r_ocr_pal;
			close 	c_ocr_pal;

			l_from_site_id		:= r_ocr_pal.from_site_id;
			l_client_id         	:= r_ocr_pal.client_id;
			l_owner_id          	:= r_ocr_pal.owner_id;
			l_order_id          	:= r_ocr_pal.order_id;
			l_hub_carrier_id    	:= r_ocr_pal.hub_carrier_id;
			l_freight_charges   	:= r_ocr_pal.freight_charges;
			l_tmp_trans_account 	:= r_ocr_pal.transport_account;
			l_tmp_tduty_account 	:= r_ocr_pal.taxduties_account;
			l_cty_eu_type       	:= r_ocr_pal.cty_eu_type;
			l_container_id      	:= r_ocr_pal.container_id;
			l_container_type    	:= r_ocr_pal.container_type;
			l_pallet_id         	:= r_ocr_pal.pallet_id;
			l_pallet_type       	:= r_ocr_pal.pallet_type;
			l_container_n_of_n  	:= r_ocr_pal.container_n_of_n;
			l_customer_id       	:= r_ocr_pal.customer_id;
			l_cod               	:= r_ocr_pal.cod;
			l_cod_tf            	:= r_ocr_pal.cod_tf;
			l_carrier_id        	:= r_ocr_pal.carrier_id;
			l_service_level 	:= r_ocr_pal.service_level;
			l_weight            	:= r_ocr_pal.weight;
			l_height            	:= r_ocr_pal.height;
			l_width             	:= r_ocr_pal.width;
			l_depth             	:= r_ocr_pal.depth;
			l_wms_database      	:= r_ocr_pal.wms_database;

			if	l_carrier_id = g_nacex
			then
				l_cto_nacex_copies := r_ocr_con.cto_nacex_copies;
			end if;

			-- add log record
			if 	g_log = 'ON'
			then
				cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
									   , p_file_name_i		=> null
									   , p_source_package_i		=> g_pck
									   , p_source_routine_i		=> l_rtn
									   , p_routine_step_i		=> 'Pack parcel by container id'
									   , p_code_parameters_i 	=> '"printer" "'||p_printer_i||'" '
													|| '"copies" "'||p_copies_i||'" '
													|| '"print2file" "'||p_print2file_i||'" '
													|| '"owner_id" "'||l_owner_id||'" '
													|| '"hub_carrier_id" "'||l_hub_carrier_id||'" '
													|| '"freight_charges" "'||l_freight_charges||'" '
													|| '"tmp_trans_account" "'||l_tmp_trans_account||'" '
													|| '"tmp_tduty_account" "'||l_tmp_tduty_account||'" '
													|| '"cty_eu_type" "'||l_cty_eu_type||'" '
													|| '"container_type" "'||l_container_type||'" '
													|| '"pallet_type" "'||l_pallet_type  ||'" '
													|| '"container_n_of_n" "'||l_container_n_of_n||'" '
													|| '"customer_id" "'||l_customer_id||'" '
													|| '"cod" "'||l_cod||'" '
													|| '"cod_tf" "'||l_cod_tf||'" '
													|| '"carrier_id" "'||l_carrier_id||'" '
													|| '"service_level" "'||l_service_level||'" '
													|| '"weight" "'||l_weight||'" '
													|| '"height" "'||l_height||'" '
													|| '"width" "'||l_width||'" '
													|| '"depth" "'||l_depth||'" '
													|| '"consolidation_tf" "'||l_consolidation_tf||'" '
													|| '"goods description" "'||l_goods_desc||'" '
													|| '"cto_nacex_copies" "'||l_cto_nacex_copies||'" '
									   , p_order_id_i		=> l_order_id
									   , p_client_id_i		=> p_client_id_i
									   , p_pallet_id_i		=> l_pallet_id
									   , p_container_id_i		=> l_container_id
									   , p_site_id_i		=> p_site_id_i
									   );
			end if;
		end if;

		-- fetch Centiro Carrier/Service
		open	c_crr( b_site_id       => l_from_site_id
			     , b_client_id     => l_client_id
			     , b_carrier_id    => l_carrier_id
			     , b_service_level => l_service_level
			     );
		fetch 	c_crr
		into  	r_crr;
		close 	c_crr;

		l_cto_open_smt_tf 	:= r_crr.cto_open_smt_tf;
		l_cto_departureid	:= r_crr.cto_departureid;
		l_cto_nacex_crw    	:= r_crr.cto_nacex_crw;

		-- fetch id for file and msg_id
		select	lpad(to_char(cnl_cto_msg_id_seq1.nextval),10,0)
		into   	l_cto_msg_id
		from   	dual;

		-- open cto file
		if	p_site_id_i = 'RCLEHV'
		then	
			l_site_translated := 'NLSBR01';
		elsif	p_site_id_i = 'RCLTLB'
		then	
			l_site_translated := 'NLTLG01';
		elsif	p_site_id_i = 'GBMIK01'
		then	
			l_site_translated := 'GBASF02';
		else
			l_site_translated := p_site_id_i;
		end if;

		open_cto_file( p_cto_file_type_i => g_packparcel
			     , p_msg_id_i        => l_cto_msg_id
			     , p_site_id_i       => l_site_translated
			     , p_client_id_i     => l_client_id
			     , p_order_id_i      => l_order_id
			     , p_container_id_i  => l_container_id
			     , p_file_type_o     => l_file_type
			     , p_file_name_o     => l_filename
			     );
		g_file_name := l_filename;
		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'New file opened and site translated'
								   , p_code_parameters_i 	=> null
								   , p_order_id_i		=> l_order_id
								   , p_client_id_i		=> l_client_id
								   , p_pallet_id_i		=> l_pallet_id
								   , p_container_id_i		=> l_container_id
								   , p_site_id_i		=> l_site_translated
								   );
		end if;

		-- get content
		-- messageheader segment
		l_msg_header := add_message_header( p_msg_name_i      => g_packparcel
						  , p_msg_version_i   => g_msg_version
						  , p_msg_id_i        => l_cto_msg_id
						  , p_site_id_i       => l_site_translated
						  , p_source_system_i => g_centiro_wms_source
						  , p_dest_system_i   => g_centiro_wms_dest
						  , p_cto_client_id_i => l_client_id || '@' || l_site_translated
					          );

		-- get path for DWS labels if applicable
		if	p_print2file_i = g_yes
		then
			l_dws_labels_path := g_centiro_dws_labels_path;
		else
			l_dws_labels_path := null;
		end if;

		-- define DepartureID in case DHL Breakbulk shipment
		if 	l_carrier_id    = g_dhl
		and 	l_service_level = g_bbx
		then
			open	c_bbx( b_client_id => l_client_id
				     , b_order_id  => l_order_id
				     );
			fetch 	c_bbx
			into  	l_cto_depid_bbx;
			close	c_bbx;
		end if;

		if	l_cto_depid_bbx is not null
		then
			l_cto_departureid := l_cto_depid_bbx;
		end if;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Start adding shipment details'
								   , p_code_parameters_i 	=> '"printer" "'||p_printer_i||'" '
												|| '"departure_id" "'||l_cto_departureid||'" '
								   , p_order_id_i		=> l_order_id
								   , p_client_id_i		=> l_client_id
								   , p_pallet_id_i		=> l_pallet_id
								   , p_container_id_i		=> l_container_id
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- shipment segment
		open  	c_shipment( b_site_id           => l_from_site_id
				  , b_client_id         => l_client_id
				  , b_order_id          => l_order_id
				  , b_printer           => p_printer_i
				  , b_dws_labels_path   => l_dws_labels_path
				  , b_departureid       => l_cto_departureid
				  );
		fetch 	c_shipment
		into  	l_shipment;
		close 	c_shipment;

		-- add log record
		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished adding shipment details'
								   , p_code_parameters_i 	=> '"printer" "'||p_printer_i||'" '
												|| '"departure_id" "'||l_cto_departureid||'" '
								   , p_order_id_i		=> l_order_id
								   , p_client_id_i		=> l_client_id
								   , p_pallet_id_i		=> l_pallet_id
								   , p_container_id_i		=> l_container_id
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;

		-- packparcel segment
		if	l_shipment is not null
		then
			-- get the complete content together
			open	c_pp( b_msg_header => l_msg_header
				    , b_shipment   => l_shipment
				    );
			fetch 	c_pp
			into  	l_packparcel;
			close 	c_pp;

			-- write packparcel content to cto file
			write_line( p_file_type_i => l_file_type
				  , p_content_i   => trim(l_packparcel)
				  );

		end if;

		-- addresses segment
		if	l_hub_carrier_id in ('OLL','OLH')
		then
			l_sender_yn := g_yes;
		else
			l_sender_yn := g_no;
		end if;

		-- select address types and carrier accounts for collect, 3rd party and tax and duty payer
		-- 
		-- Consignee pays transport and/or tax and duties
		--
		case	l_freight_charges
		when 	'pc'
		then 
			l_consignee_account := l_tmp_tduty_account;
		when 	'collect'
		then
			l_consignee_account := l_tmp_trans_account;
		when 	'c'
		then
			l_consignee_account := l_tmp_trans_account;
		when 	'cc'
		then
			l_consignee_account := l_tmp_trans_account;
		when 	'cb'
		then
			l_consignee_account := l_tmp_trans_account;
		else
			l_consignee_account := null;
		end case;
		--
		-- Third Party transport
		--
		case	l_freight_charges 
		when 	'3rd party'
		then
			l_3rdparty_yn       := g_yes;
			l_transport_account := l_tmp_trans_account;
		when 	'b'
		then
			l_3rdparty_yn       := g_yes;
			l_transport_account := l_tmp_trans_account;
		when 	'bb'
		then
			l_3rdparty_yn       := g_yes;
			l_transport_account := l_tmp_trans_account;
		else
			l_3rdparty_yn       := g_no;
			l_transport_account := null;
		end case;
		--
		-- Third Party tax and duties
		--
		case 	l_freight_charges 
		when 	'3rd party'
		then
			if	l_cty_eu_type = 'EU'
			then
				l_taxduty_payer_yn  := g_no;
				l_taxduties_account := null;
			else
				l_taxduty_payer_yn  := g_yes;
				l_taxduties_account := nvl(l_tmp_tduty_account, l_tmp_trans_account);
			end if;
		when 	'pb'
		then
			l_taxduty_payer_yn  := g_yes;
			l_taxduties_account := l_tmp_tduty_account;
		when 	'cb'
		then
			l_taxduty_payer_yn  := g_yes;
			l_taxduties_account := l_tmp_tduty_account;
		when 	'bb'
		then
			l_taxduty_payer_yn  := g_yes;
			l_taxduties_account := nvl(l_tmp_tduty_account, l_tmp_trans_account);
		else
			l_taxduty_payer_yn  := g_no;
			l_taxduties_account := null;
		end case;

		l_addresses	:= add_addresses( p_colpoint_yn_i       => g_no
						, p_sender_yn_i         => l_sender_yn
						, p_3rdparty_yn_i       => l_3rdparty_yn
						, p_taxduty_payer_yn_i  => l_taxduty_payer_yn
						, p_cnee_account_i      => l_consignee_account
						, p_transport_account_i => l_transport_account
						, p_taxduty_account_i   => l_taxduties_account
						, p_site_id_i           => p_site_id_i
						, p_client_id_i         => p_client_id_i
						, p_order_id_i          => p_order_id_i
						);

		-- write addresses content to cto file
		if	l_addresses is not null
		then
			write_line( p_file_type_i => l_file_type
				  , p_content_i   => trim(l_addresses)
				  );
		end if;

		-- BDS 5998 New IATA DG regulations
		-- Check if container / pallet contains DG and femalefor specific hazmat id's
		<<iata_loop>>
		for i in c_iata( p_client_id_i, p_order_id_i, l_container_id, l_pallet_id)
		loop
			if	i.gender = 'F'
			then
				l_iata_gender := 'F';
			end if;

			if	i.hazmat_id like 'RHSUN3480%'
			then
				l_iata_hazmat_id 	:= 'RHSUN3481P';
				l_iata_tmp_sku_id	:= i.sku_id;
				l_iata_tmp_dg_qty	:= i.dg_qty;
			elsif	i.hazmat_id like 'RHSUN3090%'
			then
				l_iata_hazmat_id := 'RHSUN3091P';
				l_iata_tmp_sku_id	:= i.sku_id;
				l_iata_tmp_dg_qty	:= i.dg_qty;
			end if;

			if	l_iata_gender = 'F'
			and	l_iata_hazmat_id in ('RHSUN3091P','RHSUN3481P')
			then
				l_iata_sku_id	:= l_iata_tmp_sku_id;
				l_iata_dg_qty	:= l_iata_tmp_dg_qty;

				open	c_iata_dg(l_iata_hazmat_id);
				fetch	c_iata_dg
				into	l_dng_type
				,	l_dng_accessibility;
				close	c_iata_dg;

				if	l_dng_type = g_lb
				then
					l_dnglb := g_true;
				else
					l_dng   := g_true;
				end if;	
				exit iata_loop;
			end if;
		end loop;

		-- parcels segment
		-- check if order contains DG, dg_type accounts for whole order/shipment
		if l_iata_sku_id is null
		then -- Container does not contain DG plus female
			open 	c_haz_ord( b_client_id => p_client_id_i
					 , b_order_id  => p_order_id_i
					 );
			fetch	c_haz_ord
			into  	r_haz_ord;
			--
			if 	c_haz_ord%found
			then
				if	r_haz_ord.cto_type = g_lb
				then
					l_dnglb := g_true;
				else
					l_dng   := g_true;
				end if;
				l_dng_type          := r_haz_ord.cto_type;
				l_dng_accessibility := r_haz_ord.dg_accessibility;
				close c_haz_ord;
			else
				close c_haz_ord;
				-- no DG, check for Dry-Ice
				open	c_dry_ocr( b_client_id => p_client_id_i
						 , b_order_id  => p_order_id_i
						 );
				fetch 	c_dry_ocr
				into  	l_integer;
				--
				if	c_dry_ocr%found
				then
					l_dng      := g_true;
					l_dng_type := g_di;
					close	c_dry_ocr;
				else
					close	c_dry_ocr;
					l_dng      := null;
					l_dnglb    := null;
					l_dng_type := null;
				end if;
			end if;
		end if;

		-- Now create Parcel segments
		-- In case carrier is FedEx then loop through Parcels and get all data for all parcels
		-- write <PARCELS> segment start
		write_line( p_file_type_i => l_file_type
			  , p_content_i   => '<PARCELS>'
			  );
		-- now get the PARCEL segments
		-- check if open shipments are allowed, if FALSE create PackParcel with all parcels, if TRUE proceed as usual with the single parcel (PalletID/ContainerID)
		if	l_cto_open_smt_tf = g_false
		then
			-- check if cursor c_ocr_con is open and close if this is the case
			if	c_ocr_con%isopen
			then
				close	c_ocr_con;
			end if;
			-- loop through all parcels
			for	r_ocr_pcl in c_ocr_con( b_site_id      => l_from_site_id
						      , b_client_id    => l_client_id
						      , b_order_id     => l_order_id
						      , b_container_id => null
						      )
			loop
				l_pallet_id        := r_ocr_pcl.pallet_id;
				l_pallet_type      := r_ocr_pcl.pallet_type;
				l_container_id     := r_ocr_pcl.container_id;
				l_container_type   := r_ocr_pcl.container_type;
				l_container_n_of_n := r_ocr_pcl.container_n_of_n;
				l_weight           := r_ocr_pcl.weight;
				l_height           := r_ocr_pcl.height;
				l_width            := r_ocr_pcl.width;
				l_depth            := r_ocr_pcl.depth;

				-- check if parcel contains DG 
				open	c_haz( b_client_id => p_client_id_i
				             , b_parcel_id => l_container_id
					     );
				fetch 	c_haz
				into  	r_haz;
				--
				if	c_haz%found
				then
					l_dng_un            := r_haz.un_code;
					if    	l_carrier_id = g_dhl_freight
					then
						l_dng_description   := r_haz.un_desc;
					else
						l_dng_description   := r_haz.cto_carrier_desc;
					end if;
					l_dng_class         := r_haz.un_class;
					l_dng_packagegroup  := r_haz.un_pack_grp;
					l_dng_packageinstr  := r_haz.un_pack_instr;
					l_dng_accessibility := r_haz.un_accessibility;
					l_dng_di_wgt_desc   := g_dng_netweight;
					close c_haz;
					-- now loop through c_haz to get total quantity and weight
					for	r_haz in c_haz( b_client_id => p_client_id_i
							      , b_parcel_id => l_container_id
							      )
					loop
						l_dng_netweight := nvl( l_dng_netweight, 0) + nvl( r_haz.tot_sku_weight, 0);
						l_dng_quantity  := nvl( l_dng_quantity, 0)  + nvl( r_haz.tot_sku_qty, 0);
					end loop;       
				else
					close c_haz;
					-- clear the DNG variables to prevent non DG parcels inheriting DG data
					l_dng_un            := null;
					l_dng_description   := null;
					l_dng_class         := null;
					l_dng_packagegroup  := null;
					l_dng_packageinstr  := null;
					l_dng_accessibility := null;
					l_dng_di_wgt_desc   := null;
					l_dng_netweight     := null;
					l_dng_quantity      := null;

					-- check if parcel contains Dry-Ice
					if	substr( nvl( l_container_type, l_pallet_type), 1, 6) = g_dryice
					then
						open	c_dry ( b_client_id   => p_client_id_i 
							, b_pallet_type => nvl( l_container_type, l_pallet_type)
							);
						fetch	c_dry
						into  	l_dng_netweight;
						close 	c_dry;
					else
						l_dng_netweight := null;
					end if;
				end if;

				-- If FedEx then don't use Parcel Attributes for DG shipments
				-- The Service Attributes Named Values should be used instead
				-- Only for Dry-Ice Parcel Attributes should be used but with different attribute name
				if	l_carrier_id = g_fedex
				then
					if 	l_dng_type = g_di
					then
						-- parcel attributes segment DG Shipments FedEx (Dry-Ice only)
						l_pcl_attributes 	:= add_attributes( p_at_name01_i  => g_dng_dryiceweight -- specific Dry-Ice Weight attribute name for FedEx      
											 , p_at_value01_i => round( l_dng_netweight, 2)
											 , p_at_name02_i  => null
											 , p_at_value02_i => null
											 , p_at_name03_i  => null
											 , p_at_value03_i => null
											 , p_at_name04_i  => null
											 , p_at_value04_i => null
											 , p_at_name05_i  => null
											 , p_at_value05_i => null
											 , p_at_name06_i  => null
											 , p_at_value06_i => null
											 , p_at_name07_i  => null
											 , p_at_value07_i => null
											 , p_at_name08_i  => null
											 , p_at_value08_i => null
											 , p_at_name09_i  => null
											 , p_at_value09_i => null
											 , p_at_name10_i  => null
											 , p_at_value10_i => null
											 );
					end if;
				else
					-- parcel attributes segment DG Shipments non FedEx
					l_pcl_attributes 	:= add_attributes( p_at_name01_i  => g_dng_un
										 , p_at_value01_i => l_dng_un
										 , p_at_name02_i  => g_dng_description
										 , p_at_value02_i => l_dng_description
										 , p_at_name03_i  => g_dng_class
										 , p_at_value03_i => l_dng_class
										 , p_at_name04_i  => g_dng_packagegroup
										 , p_at_value04_i => l_dng_packagegroup
										 , p_at_name05_i  => g_dng_packageinstructions
										 , p_at_value05_i => l_dng_packageinstr
										 , p_at_name06_i  => g_dng_netweight -- default attribute name
										 , p_at_value06_i => round( l_dng_netweight, 2)
										 , p_at_name07_i  => g_dng_quantity
										 , p_at_value07_i => round( l_dng_quantity, 0)
										 , p_at_name08_i  => null
										 , p_at_value08_i => null
										 , p_at_name09_i  => null
										 , p_at_value09_i => null
										 , p_at_name10_i  => null
										 , p_at_value10_i => null
										 );
				end if;

				-- check if Order Accessorial exists for DG Type and add if not
				open	c_oay( b_client_id   => l_client_id
					     , b_order_id    => l_order_id
					     , b_accessorial => l_dng_type
					     );
				fetch 	c_oay
				into  	r_oay;
				if 	c_oay%notfound
				then
					l_doa_result := dcsdba.libmergeorderaccessory.directorderaccessory( p_mergeerror   => l_doa_merge_error
													  , p_toupdatecols => null
													  , p_mergeaction  => 'A'
													  , p_clientid     => l_client_id
													  , p_orderid      => l_order_id
													  , p_accessorial  => l_dng_type
													  , p_timezonename => 'Europe/Amsterdam'
													  );
				end if;
				close c_oay;

				-- define package type
				open	c_pcg( b_client_id   => l_client_id
					     , b_pallet_type => nvl( l_container_type, l_pallet_type)
					     );
				fetch 	c_pcg
				into  	l_pallet_type_group;--l_package_type;
				close 	c_pcg;
				if    	l_pallet_type_group is null
				then
					l_package_type := nvl( l_container_type, l_pallet_type);
				else
					open   	c_ptg( b_client_id 	=> l_client_id
						     , b_group		=> l_pallet_type_group
						     );
					fetch	c_ptg 
					into 	l_package_type;
					close  	c_ptg;
				end if;
				if 	l_package_type is null
				then
					l_package_type := nvl( l_container_type, l_pallet_type);
				end if;

				l_parcel := add_parcels( p_site_id_i      => l_from_site_id
						       , p_client_id_i    => l_client_id
						       , p_order_id_i     => l_order_id
						       , p_parcel_id_i    => nvl( l_container_id, l_pallet_id)
						       , p_pallet_type_i  => l_package_type
						       , p_weight_i       => l_weight
						       , p_depth_i        => l_depth
						       , p_height_i       => l_height
						       , p_width_i        => l_width
						       , p_copies_i       => p_copies_i
						       , p_goods_desc_i   => l_goods_desc
						       , p_attributes_i   => l_pcl_attributes
						       ); 

				-- write parcel content to cto file
				if 	l_parcel is not null
				then
					write_line( p_file_type_i => l_file_type
						  , p_content_i   => trim(l_parcel)
						  );
				end if;

				-- update CNL_CONTAINER_DATA
				update_container_data( p_container_id_i     => nvl(l_container_id, '0000000000')
						     , p_container_type_i   => l_container_type
						     , p_pallet_id_i        => l_pallet_id
						     , p_pallet_type_i      => l_pallet_type
						     , p_container_n_of_n_i => l_container_n_of_n
						     , p_site_id_i          => l_from_site_id
						     , p_client_id_i        => l_client_id
						     , p_owner_id_i         => l_owner_id
						     , p_order_id_i         => l_order_id
						     , p_customer_id_i      => l_customer_id
						     , p_carrier_id_i       => l_carrier_id
						     , p_service_level_i    => l_service_level
						     , p_wms_weight_i       => l_weight
						     , p_wms_height_i       => l_height
						     , p_wms_width_i        => l_width
						     , p_wms_depth_i        => l_depth
						     , p_wms_database_i     => l_wms_database
						     , p_cto_enabled_yn     => l_cto_enabled_yn
						     , p_cto_pp_filename_i  => l_filename
						     , p_cto_pp_dstamp_i    => current_timestamp
						     , p_cto_carrier_i      => l_carrier_id
						     , p_cto_service_i      => l_service_level
						     );
			end loop;

		else
			if 	l_iata_sku_id is null
			then
				-- check if parcel contains DG 
				open	c_haz( b_client_id => p_client_id_i
					     , b_parcel_id => l_pallet_id
					     );
				fetch 	c_haz
				into  	r_haz;
				if 	c_haz%found
				then
					l_dng_un            := r_haz.un_code;
					if    	l_carrier_id = g_dhl_freight
					then
						l_dng_description   := r_haz.un_desc;
					else
						l_dng_description   := r_haz.cto_carrier_desc;
					end if;
					l_dng_class         := r_haz.un_class;
					l_dng_packagegroup  := r_haz.un_pack_grp;
					l_dng_packageinstr  := r_haz.un_pack_instr;
					l_dng_accessibility := r_haz.un_accessibility;
					l_dng_di_wgt_desc   := g_dng_netweight;
					close c_haz;
					-- now loop through c_haz to get total quantity and weight
					for	r_haz in c_haz( b_client_id => p_client_id_i
							      , b_parcel_id => l_pallet_id
							      )
					loop
						l_dng_netweight := nvl( l_dng_netweight, 0) + nvl( r_haz.tot_sku_weight, 0);
						l_dng_quantity  := nvl( l_dng_quantity, 0)  + nvl( r_haz.tot_sku_qty, 0);
					end loop;       
				else
					close c_haz;
					-- clear the DNG variables to prevent non DG parcels inheriting DG data
					l_dng_un            := null;
					l_dng_description   := null;
					l_dng_class         := null;
					l_dng_packagegroup  := null;
					l_dng_packageinstr  := null;
					l_dng_accessibility := null;
					l_dng_di_wgt_desc   := null;
					l_dng_netweight     := null;
					l_dng_quantity      := null;

					-- check if parcel contains Dry-Ice
					if	substr( nvl( l_container_type, l_pallet_type), 1, 6) = g_dryice
					then
						open 	c_dry( b_client_id   => p_client_id_i 
							     , b_pallet_type => nvl( l_container_type, l_pallet_type)
							     );
						fetch 	c_dry
						into  	l_dng_netweight;
						close 	c_dry;
					else
						l_dng_netweight := null;
					end if;
				end if;
			else
				open 	c_iata_haz( p_client_id_i, l_iata_dg_qty, l_iata_sku_id,l_iata_hazmat_id );
				fetch	c_iata_haz
				into 	r_haz;
				close 	c_iata_haz;

				l_dng_un            := r_haz.un_code;
				if    	l_carrier_id = g_dhl_freight
				then
					l_dng_description   := r_haz.un_desc;
				else
					l_dng_description   := r_haz.cto_carrier_desc;
				end if;
				l_dng_class		:= r_haz.un_class;
				l_dng_packagegroup	:= r_haz.un_pack_grp;
				l_dng_packageinstr	:= r_haz.un_pack_instr;
				l_dng_accessibility	:= r_haz.un_accessibility;
				l_dng_di_wgt_desc	:= g_dng_netweight;
				l_dng_netweight		:= nvl( r_haz.tot_sku_weight, 0);
				l_dng_quantity  	:= nvl( r_haz.tot_sku_qty, 0);
			end if;

			-- If FedEx then don't use Parcel Attributes for DG shipments
			-- The Service Attributes Named Values should be used instead
			-- Only for Dry-Ice Parcel Attributes should be used but with different attribute name
			if	l_carrier_id = g_fedex
			then
				if	l_dng_type = g_di
				then
					-- parcel attributes segment DG Shipments FedEx (Dry-Ice only)
					l_pcl_attributes := add_attributes( p_at_name01_i  => g_dng_dryiceweight -- specific Dry-Ice Weight attribute name for FedEx      
									  , p_at_value01_i => round( l_dng_netweight, 2)
									  , p_at_name02_i  => null
									  , p_at_value02_i => null
									  , p_at_name03_i  => null
									  , p_at_value03_i => null
									  , p_at_name04_i  => null
									  , p_at_value04_i => null
									  , p_at_name05_i  => null
									  , p_at_value05_i => null
									  , p_at_name06_i  => null
									  , p_at_value06_i => null
									  , p_at_name07_i  => null
									  , p_at_value07_i => null
									  , p_at_name08_i  => null
									  , p_at_value08_i => null
									  , p_at_name09_i  => null
									  , p_at_value09_i => null
									  , p_at_name10_i  => null
									  , p_at_value10_i => null
									  );
				end if;
			else
				-- parcel attributes segment DG Shipments non FedEx
				l_pcl_attributes := add_attributes( p_at_name01_i  => g_dng_un
								  , p_at_value01_i => l_dng_un
								  , p_at_name02_i  => g_dng_description
								  , p_at_value02_i => l_dng_description
								  , p_at_name03_i  => g_dng_class
								  , p_at_value03_i => l_dng_class
								  , p_at_name04_i  => g_dng_packagegroup
								  , p_at_value04_i => l_dng_packagegroup
								  , p_at_name05_i  => g_dng_packageinstructions
								  , p_at_value05_i => l_dng_packageinstr
								  , p_at_name06_i  => g_dng_netweight -- default attribute name
								  , p_at_value06_i => round( l_dng_netweight, 2)
								  , p_at_name07_i  => g_dng_quantity
								  , p_at_value07_i => round( l_dng_quantity, 0)
								  , p_at_name08_i  => null
								  , p_at_value08_i => null
								  , p_at_name09_i  => null
								  , p_at_value09_i => null
								  , p_at_name10_i  => null
								  , p_at_value10_i => null
								  );
			end if;

			-- check if Order Accessorial exists for DG Type and add if not
			open	c_oay( b_client_id   => l_client_id
				     , b_order_id    => l_order_id
				     , b_accessorial => l_dng_type
				     );
			fetch 	c_oay
			into  	r_oay;	
			if 	c_oay%notfound
			then
				l_doa_result := dcsdba.libmergeorderaccessory.directorderaccessory( p_mergeerror   => l_doa_merge_error
												  , p_toupdatecols => null
												  , p_mergeaction  => 'A'
												  , p_clientid     => l_client_id
												  , p_orderid      => l_order_id
												  , p_accessorial  => l_dng_type
												  , p_timezonename => 'Europe/Amsterdam'
												  );
			end if;
			close c_oay;

			-- define package type
			open	c_pcg( b_client_id   => l_client_id
				     , b_pallet_type => nvl( l_container_type, l_pallet_type)
				     );
			fetch 	c_pcg
			into  	l_pallet_type_group;--l_package_type;
			close 	c_pcg;
			if	l_pallet_type_group is null
			then
				l_package_type := nvl( l_container_type, l_pallet_type);
			else
				open	c_ptg( b_client_id => l_client_id
					     , b_group	=> l_pallet_type_group
					     );
				fetch  	c_ptg 
				into 	l_package_type;
				close  c_ptg;
			end if;
			if 	l_package_type is null
			then
				l_package_type := nvl( l_container_type, l_pallet_type);
			end if;

			l_parcel := add_parcels( p_site_id_i      => l_from_site_id
					       , p_client_id_i    => l_client_id
					       , p_order_id_i     => l_order_id
					       , p_parcel_id_i    => nvl( l_container_id, l_pallet_id)
					       , p_pallet_type_i  => l_package_type
					       , p_weight_i       => l_weight
					       , p_depth_i        => l_depth
					       , p_height_i       => l_height
					       , p_width_i        => l_width
					       , p_copies_i       => p_copies_i
					       , p_goods_desc_i   => l_goods_desc
					       , p_attributes_i   => l_pcl_attributes
					       ); 

			-- write parcel content to cto file
			if	l_parcel is not null
			then
				write_line( p_file_type_i => l_file_type
					  , p_content_i   => trim(l_parcel)
					  );
			end if;

			-- update CNL_CONTAINER_DATA
			update_container_data( p_container_id_i     => nvl(l_container_id, '0000000000')
					     , p_container_type_i   => l_container_type
					     , p_pallet_id_i        => l_pallet_id
					     , p_pallet_type_i      => l_pallet_type
					     , p_container_n_of_n_i => l_container_n_of_n
					     , p_site_id_i          => l_from_site_id
					     , p_client_id_i        => l_client_id
					     , p_owner_id_i         => l_owner_id
					     , p_order_id_i         => l_order_id
					     , p_customer_id_i      => l_customer_id
					     , p_carrier_id_i       => l_carrier_id
					     , p_service_level_i    => l_service_level
					     , p_wms_weight_i       => l_weight
					     , p_wms_height_i       => l_height
					     , p_wms_width_i        => l_width
					     , p_wms_depth_i        => l_depth
					     , p_wms_database_i     => l_wms_database
					     , p_cto_enabled_yn     => l_cto_enabled_yn
					     , p_cto_pp_filename_i  => l_filename
					     , p_cto_pp_dstamp_i    => current_timestamp
					     , p_cto_carrier_i      => l_carrier_id
					     , p_cto_service_i      => l_service_level
					     );

		end if;

		-- write </PARCELS> segment start
		write_line( p_file_type_i => l_file_type
			  , p_content_i   => '</PARCELS>'
			  );

		-- shipment attributes segment
		-- Nacex exception, when Nacex copies > 4 then Nacex CRW = 'R'
		if	l_cto_nacex_copies > 4
		then
			l_cto_nacex_crw := 'R';
		end if;

		l_smt_attributes := add_attributes( p_at_name01_i  => g_nacex_crw
					          , p_at_value01_i => l_cto_nacex_crw
						  , p_at_name02_i  => g_nacex_copies_spl
						  , p_at_value02_i => l_cto_nacex_copies
						  , p_at_name03_i  => g_dng
						  , p_at_value03_i => l_dng
						  , p_at_name04_i  => g_dnglb
						  , p_at_value04_i => l_dnglb
						  , p_at_name05_i  => null
						  , p_at_value05_i => null
						  , p_at_name06_i  => null
						  , p_at_value06_i => null
						  , p_at_name07_i  => null
						  , p_at_value07_i => null
						  , p_at_name08_i  => null
						  , p_at_value08_i => null
						  , p_at_name09_i  => null
						  , p_at_value09_i => null
						  , p_at_name10_i  => null
						  , p_at_value10_i => null
						  );

		-- write shipment attributes segment
		if	l_smt_attributes is not null
		then
			write_line( p_file_type_i => l_file_type
				  , p_content_i   => trim(l_smt_attributes)
				  );
		end if;

		-- businessrules segment
		if 	l_consolidation_tf = g_false
		then
			l_consolidation_tf := null;
		end if;

		l_businessrules := add_businessrules( p_br_name01_i  => g_consolidation
						    , p_br_value01_i => l_consolidation_tf
						    , p_br_name02_i  => null
						    , p_br_value02_i => null
						    , p_br_name03_i  => null
						    , p_br_value03_i => null
						    , p_br_name04_i  => null
						    , p_br_value04_i => null
						    , p_br_name05_i  => null
						    , p_br_value05_i => null
						    );

		-- write business rules segment
		if 	l_businessrules is not null
		then
			write_line( p_file_type_i => l_file_type
				  , p_content_i   => trim(l_businessrules)
				  );
		end if;

		-- service attributes segment
		-- Add Service Attribute if COD
		l_oay_count := 1;

		if	l_cod = g_yes
		then
			l_sa_code01_i := g_cod;
			l_oay_count   := l_oay_count + 1;
		end if;

		-- Add Service Attribute if Dry-Ice or Dangerous Goods parcel
		if 	l_dng_type is not null
		then
			-- Different values and segments required by Centiro for FedEx
			if	l_carrier_id = g_fedex
			then
				if	l_dng_type in ( g_lb, g_lq, g_eq)
				then
					-- Centiro expects "DNG" as ServiceAttribute for FedEx instead of LB, LQ or EQ
					l_dng_type_fdx := g_dng;
					-- Accessibility
					l_nv_name01_i  := g_dng_fdx_nv_name_access;
					if 	l_dng_accessibility = g_adg
					then
						l_nv_value01_i := g_true;
					else
						l_nv_value01_i := g_false;
					end if;
					-- DG Type
					case l_dng_type
					when g_lb
					then
						l_nv_name02_i  := g_dng_fdx_nv_name_lb;
						l_nv_value02_i := g_true;
					when g_lq
					then
						l_nv_name02_i  := g_dng_fdx_nv_name_lq;
						l_nv_value02_i := g_true;
					when g_eq
					then
						l_nv_name02_i  := g_dng_fdx_nv_name_eq;
						l_nv_value02_i := g_true;
					else
						null;
					end case; 
					-- Get Named Values for FedEx
					l_sa_namedvalues := add_sa_namedvalues( p_nv_name01_i  => l_nv_name01_i
									      , p_nv_value01_i => l_nv_value01_i
									      , p_nv_name02_i  => l_nv_name02_i
									      , p_nv_value02_i => l_nv_value02_i
									      , p_nv_name03_i  => l_nv_name03_i
									      , p_nv_value03_i => l_nv_value03_i
									      , p_nv_name04_i  => l_nv_name04_i
									      , p_nv_value04_i => l_nv_value04_i
									      , p_nv_name05_i  => l_nv_name05_i
									      , p_nv_value05_i => l_nv_value05_i
									      , p_nv_name06_i  => l_nv_name06_i
									      , p_nv_value06_i => l_nv_value06_i
									      , p_nv_name07_i  => l_nv_name07_i
									      , p_nv_value07_i => l_nv_value07_i
									      , p_nv_name08_i  => l_nv_name08_i
									      , p_nv_value08_i => l_nv_value08_i
									      , p_nv_name09_i  => l_nv_name09_i
									      , p_nv_value09_i => l_nv_value09_i
									      , p_nv_name10_i  => l_nv_name10_i
									      , p_nv_value10_i => l_nv_value10_i
									      );
				end if;

				if 	l_oay_count = 1
				then
					l_sa_code01_i     := l_dng_type_fdx;
					l_sa_namedval01_i := l_sa_namedvalues;
					l_oay_count       := l_oay_count + 1;
				else
					l_sa_code02_i     := l_dng_type_fdx;
					l_sa_namedval02_i := l_sa_namedvalues;
					l_oay_count       := l_oay_count + 1;
				end if;
			else
				if	l_oay_count = 1
				then
					l_sa_code01_i := l_dng_type;
					l_oay_count   := l_oay_count + 1;
				else
					l_sa_code02_i := l_dng_type;
					l_oay_count   := l_oay_count + 1;
				end if;
			end if;
		end if;

		-- Get Order Accessorials from WMS
		for	r_oay in c_oay( b_client_id   => l_client_id
				      , b_order_id    => l_order_id
				      , b_accessorial => null
				      )
		loop
			if	upper(r_oay.accessorial) = l_dng_type
			then
				l_accessorial := null;
			else
				l_accessorial := upper(r_oay.accessorial);
			end if;

			case	l_oay_count
			when 	1
			then
				l_sa_code01_i := l_accessorial;
			when 	2
			then
				l_sa_code02_i := l_accessorial;
			when 	3
			then
				l_sa_code03_i := l_accessorial;
			when 	4
			then
				l_sa_code04_i := l_accessorial;
			when 	5
			then
				l_sa_code05_i := l_accessorial;
			when 	6
			then
				l_sa_code06_i := l_accessorial;
			when 	7
			then
				l_sa_code07_i := l_accessorial;
			when 	8
			then
				l_sa_code08_i := l_accessorial;
			when 	9
			then
				l_sa_code09_i := l_accessorial;
			when 	10
			then
				l_sa_code10_i := l_accessorial;
			else
				exit;
			end case;
			l_oay_count := l_oay_count + 1;
		end loop;
		-- serviceattributes segment
		l_serviceattributes := add_serviceattributes( p_sa_code01_i     => l_sa_code01_i 
							    , p_sa_value01_i    => l_sa_value01_i
							    , p_sa_namedval01_i => l_sa_namedval01_i
							    , p_sa_code02_i     => l_sa_code02_i 
							    , p_sa_value02_i    => l_sa_value02_i
							    , p_sa_namedval02_i => l_sa_namedval02_i
							    , p_sa_code03_i     => l_sa_code03_i 
							    , p_sa_value03_i    => l_sa_value03_i
							    , p_sa_namedval03_i => l_sa_namedval03_i
							    , p_sa_code04_i     => l_sa_code04_i 
							    , p_sa_value04_i    => l_sa_value04_i
							    , p_sa_namedval04_i => l_sa_namedval04_i
							    , p_sa_code05_i     => l_sa_code05_i 
							    , p_sa_value05_i    => l_sa_value05_i
							    , p_sa_namedval05_i => l_sa_namedval05_i
							    , p_sa_code06_i     => l_sa_code06_i 
							    , p_sa_value06_i    => l_sa_value06_i
							    , p_sa_namedval06_i => l_sa_namedval06_i
							    , p_sa_code07_i     => l_sa_code07_i 
							    , p_sa_value07_i    => l_sa_value07_i
							    , p_sa_namedval07_i => l_sa_namedval07_i
							    , p_sa_code08_i     => l_sa_code08_i 
							    , p_sa_value08_i    => l_sa_value08_i
							    , p_sa_namedval08_i => l_sa_namedval08_i
							    , p_sa_code09_i     => l_sa_code09_i 
							    , p_sa_value09_i    => l_sa_value09_i
							    , p_sa_namedval09_i => l_sa_namedval09_i
							    , p_sa_code10_i     => l_sa_code10_i 
							    , p_sa_value10_i    => l_sa_value10_i
							    , p_sa_namedval10_i => l_sa_namedval10_i
							    );
		-- write service attributes segment
		if	l_serviceattributes is not null
		then
			write_line( p_file_type_i => l_file_type
				  , p_content_i   => trim(l_serviceattributes)
				  );
		end if;

		-- add remaining xml tags
		l_content := l_pp_tlr;
		-- write trailer content to file
		write_line( p_file_type_i => l_file_type
			  , p_content_i   => trim(l_content)
			  );

		--l_pp_dir := g_centiro_pp_dir||cnl_cto_packparcel_dir_seq1.nextval;

		-- close and move file
		close_cto_file( p_file_type_i   => l_file_type
			      , p_file_name_i   => l_filename
			      , p_cto_out_dir_i => /*l_pp_dir--*/g_centiro_pp_dir
			      );


		if 	g_log = 'ON'
		then
			cnl_sys.cnl_logging_pck.add_print_log_rec_p( p_print_id_i		=> g_print_id
								   , p_file_name_i		=> g_file_name
								   , p_source_package_i		=> g_pck
								   , p_source_routine_i		=> l_rtn
								   , p_routine_step_i		=> 'Finished centiro packparcel procedure'
								   , p_code_parameters_i 	=> '"printer" "'||p_printer_i||'" '
												|| '"copies" "'||p_copies_i||'" '
												|| '"print2file" "'||p_print2file_i||'" '
								   , p_order_id_i		=> p_order_id_i
								   , p_client_id_i		=> p_client_id_i
								   , p_pallet_id_i		=> p_pallet_id_i
								   , p_container_id_i		=> p_container_id_i
								   , p_site_id_i		=> p_site_id_i
								   );
		end if;
	exception
		when others
		then
			case
			when c_ocr_con%isopen
			then
				close c_ocr_con;
			when c_ocr_pal%isopen
			then
				close c_ocr_pal;
			when c_shipment%isopen
			then
				close c_shipment;
			when c_pp%isopen
			then
				close c_pp;
			when c_oay%isopen
			then
				close c_oay;
			when c_clt%isopen
			then
				close c_clt;
			when c_haz%isopen
			then
				close c_haz;
			when c_haz_ord%isopen
			then
				close c_haz_ord;
			when c_dry%isopen
			then
				close c_dry;
			when c_dry_ocr%isopen
			then
				close c_dry_ocr;
			when c_bbx%isopen
			then
				close c_bbx;
			else
				null;
			end case;

			raise;

  end create_packparcel;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 08-Jun-2016
-- Purpose : Create CancelParcel interface file to Centiro
------------------------------------------------------------------------------------------------
  procedure create_cancelparcel ( p_site_id_i    in  varchar2
                                , p_client_id_i  in  varchar2
                                , p_order_id_i   in  varchar2
                                , p_parcel_id    in  varchar2
                                )
  is
    cursor c_cp ( b_msg_header in varchar2
                , b_parcel_id  in varchar2
                )
    is
      select xmltype.getClobVal ( xmlelement ( "CANCELPARCEL"
                                             , xmlattributes ( g_xmlns as "xmlns")
                                             , xmltype ( b_msg_header)
                                             , xmlelement ( "PARCELS"
                                                          , xmlelement ( "PARCEL"
                                                                       , xmlforest ( b_parcel_id as "PARCELID") 
                                                                       )
                                                          )
                                             )
                                )
      from   dual  
      ;                          

    l_cto_msg_id      varchar2(20);
    l_msg_header      varchar2(8192);
    l_cancelparcel    varchar2(32767); 
    l_filename        varchar2(100);
    l_site_translated	dcsdba.site.site_id%type;
  begin
	if	p_site_id_i = 'RCLEHV'
	then	
		l_site_translated := 'NLSBR01';
	elsif	p_site_id_i = 'RCLTLB'
	then	
		l_site_translated := 'NLTLG01';
	elsif	p_site_id_i = 'GBMIK01'
	then	
		l_site_translated := 'GBASF02';
	else
		l_site_translated := p_site_id_i;
	end if;

    -- fetch id for file and msg_id
    select lpad(to_char(cnl_cto_msg_id_seq1.nextval),10,0)
    into   l_cto_msg_id
    from   dual;

    -- get content
    l_msg_header := add_message_header ( p_msg_name_i      => g_cancelparcel
                                       , p_msg_version_i   => g_msg_version
                                       , p_msg_id_i        => l_cto_msg_id
                                       , p_site_id_i       => l_site_translated
                                       , p_source_system_i => g_centiro_wms_source
                                       , p_dest_system_i   => g_centiro_wms_dest
                                       , p_cto_client_id_i => p_client_id_i || '@' || l_site_translated
                                       );
    --
    open  c_cp ( b_msg_header => l_msg_header
               , b_parcel_id  => p_parcel_id
               );
    fetch c_cp
    into  l_cancelparcel;
    close c_cp;

    -- create file with content
    create_cto_file ( p_cto_file_type_i => g_cancelparcel
                    , p_cto_out_dir_i   => g_centiro_cp_dir
                    , p_msg_id_i        => l_cto_msg_id
                    , p_site_id_i       => l_site_translated
                    , p_client_id_i     => p_client_id_i
                    , p_order_id_i      => null
                    , p_container_id_i  => p_parcel_id
                    , p_content_i       => l_cancelparcel
                    , p_filename_o      => l_filename
                    );
    -- Update CNL_CONTAINER_DATA and set CTO_CP_.. fields
    update cnl_container_data cda
    set    cda.cto_cp_filename = substr( l_filename, 100)
    ,      cda.cto_cp_dstamp   = current_timestamp
    where  cda.site_id         = p_site_id_i
    and    cda.client_id       = p_client_id_i
    and    cda.order_id        = p_order_id_i
    and    (
           cda.container_id    = p_parcel_id
           or
           cda.pallet_id       = p_parcel_id
           )
    ;                
  end create_cancelparcel;
--
--
begin
  -- Initialization
  null;
end cnl_centiro_pck;