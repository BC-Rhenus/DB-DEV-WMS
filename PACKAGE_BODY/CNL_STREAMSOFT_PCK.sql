CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_STREAMSOFT_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functionality for the integration with Stream Software's Customs Streamliner (Customs Management System)
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--
  g_yes                              constant varchar2(1)              := 'Y';
  g_no                               constant varchar2(1)              := 'N';
  g_true                             constant varchar2(20)             := 'TRUE';
  g_false                            constant varchar2(20)             := 'FALSE';
  g_low                              constant varchar2(20)             := 'LOW';
  g_high                             constant varchar2(20)             := 'HIGH';
  g_xml_encoding                     constant varchar2(1024)           := '<?xml version="1.0" encoding="UTF-8"?>';
  g_xmlns                            constant varchar2(50)             := 'http://www.w3.org/2001/XMLSchema-instance';
  g_streamsoft_tmp_dir               constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSOFT_TMP_DIR');
  g_streamsoft_arc_dir               constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSOFT_ARCHIVE_DIR');
  g_streamsoft_out_dir               constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSOFT_OUTPUT_DIR');
  g_streamsoft_env_type              constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSOFT_ENV_TYPE');
  g_streamsoft_inb_msg_type          constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSOFT_INB_MSG_TYPE');
  g_cli_ceu_auto_free_yn             constant cnl_constants.value%type := cnl_util_pck.get_constant( p_name_i => 'STREAMSOFT_CLI_CEU_AUTO_FREE_YN');
  g_pldarct                          constant varchar2(20)             := 'PLDARCT';
  g_pldaajt                          constant varchar2(20)             := 'PLDAAJT';
  g_pldaodr                          constant varchar2(20)             := 'PLDAODR';
  g_pldaarc                          constant varchar2(20)             := 'PLDAARC';
  g_pldafis_vat                      constant varchar2(20)             := 'PLDAFIS_VAT';
  g_pldafis_cbs                      constant varchar2(20)             := 'PLDAFIS_CBS';
  g_ballist                          constant varchar2(20)             := 'BALLIST';
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create Message header for interface file to Stream Software
------------------------------------------------------------------------------------------------
  procedure create_ssw_file ( p_ssw_file_type_i in  varchar2
                            , p_msg_id_i        in  varchar2
                            , p_company_i       in  varchar2
                            , p_bu_ppl_i        in  varchar2
                            , p_reference_id_i  in  varchar2
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
  begin                            
    -- create filename
    l_filename := p_ssw_file_type_i                      || '_' 
               || p_company_i                            || '_' 
               || p_bu_ppl_i                             || '_' 
               || p_reference_id_i                       || '_'
               || to_char(sysdate,'YYYYMMDD"_"HH24MISS') || '_'
               || p_msg_id_i
               || '.xml'
               ;
    -- open/create file in tmp
    l_file_type := utl_file.fopen ( location     => g_streamsoft_tmp_dir
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
    utl_file.fgetattr ( location    => g_streamsoft_tmp_dir 
                      , filename    => l_filename
                      , fexists     => l_tmp_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size
                      );
    if l_tmp_fexists
    then
       utl_file.fcopy ( src_location  => g_streamsoft_tmp_dir
                      , src_filename  => l_filename
                      , dest_location => g_streamsoft_arc_dir
                      , dest_filename => l_filename
                      );
    end if;                                          
    -- move file from tmp to out
    utl_file.fgetattr ( location    => g_streamsoft_arc_dir 
                      , filename    => l_filename
                      , fexists     => l_arc_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size 
                      );
    if l_arc_fexists
    then
       utl_file.frename ( src_location  => g_streamsoft_tmp_dir
                        , src_filename  => l_filename
                        , dest_location => g_streamsoft_out_dir
                        , dest_filename => l_filename
                        , overwrite     => true
                        );
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
  end create_ssw_file;
  --
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Jun-2016
-- Purpose : Create file for Customs Streamliner
------------------------------------------------------------------------------------------------
  procedure open_csl_file ( p_msg_type_i     in  varchar2
                          , p_company_i      in  varchar2
                          , p_bu_ppl_i       in  varchar2
                          , p_reference_id_i in  varchar2
                          , p_file_type_o    out utl_file.file_type
                          , p_file_name_o    out varchar2
                          )
  is
    l_file_type       utl_file.file_type;
    l_csl_file_id_i   varchar2(10);
    l_filename        varchar2(100);
    l_reference       varchar2(20);
  begin
    -- fetch id for file
    select lpad( to_char( cnl_csl_file_id_seq1.nextval), 10, 0)
    into   l_csl_file_id_i
    from   dual;
    -- get Alphanumeric characters only from Reference ID
    select regexp_replace( p_reference_id_i, '[^[:alnum:]]+')
    into   l_reference
    from   dual;                          
    -- create filename
    l_filename := p_msg_type_i     || '_'
               || p_company_i      || '_'
               || p_bu_ppl_i       || '_'
               || l_reference      || '_'
               || l_csl_file_id_i  
               || '.xml'
               ;
    -- open/create file in tmp
    l_file_type := utl_file.fopen ( location     => g_streamsoft_tmp_dir
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
  end open_csl_file;
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
  procedure close_csl_file ( p_file_type_i in utl_file.file_type
                           , p_file_name_i in varchar2
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
    -- copy file from tmp to archive
    utl_file.fgetattr ( location    => g_streamsoft_tmp_dir
                      , filename    => l_filename
                      , fexists     => l_tmp_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size
                      );
    if l_tmp_fexists
    then
       utl_file.fcopy ( src_location  => g_streamsoft_tmp_dir
                      , src_filename  => l_filename
                      , dest_location => g_streamsoft_arc_dir
                      , dest_filename => l_filename
                      );
    end if;
    -- move file from tmp to out
    utl_file.fgetattr ( location    => g_streamsoft_arc_dir
                      , filename    => l_filename
                      , fexists     => l_arc_fexists
                      , file_length => l_file_length
                      , block_size  => l_block_size
                      );
    if l_arc_fexists
    then
       utl_file.frename ( src_location  => g_streamsoft_tmp_dir
                        , src_filename  => l_filename
                        , dest_location => g_streamsoft_out_dir
                        , dest_filename => l_filename
                        , overwrite     => true
                        );
      -- add filename to table to be able to cleanup directory
      insert into cnl_files_archive( application
                                   , location
                                   , filename
                                   )
      values                       ( 'STREAMSOFT'
                                   , g_streamsoft_arc_dir
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
  end close_csl_file;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Oct-2016
-- Purpose : Get Company for Customs Streamliner
------------------------------------------------------------------------------------------------
  function get_company ( p_client_id_i in  varchar2
                       )
  return varchar2
  is
    cursor c_spe ( b_client_id in varchar2)
    is
      select text_data 
      from   dcsdba.system_profile
      where  profile_id = '-ROOT-_USER_STREAMLINER_COMPANY_' || b_client_id
      ;
    l_company varchar2(20);
  begin
    open  c_spe ( b_client_id => p_client_id_i);
    fetch c_spe
    into  l_company;
    close c_spe;

    return l_company;

  end get_company;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Oct-2016
-- Purpose : Get BusinessUnit for Customs Streamliner
------------------------------------------------------------------------------------------------
  function get_businessunit ( p_client_id_i in  varchar2
                            )
  return varchar2
  is
    cursor c_spe ( b_client_id in varchar2)
    is
      select text_data
      from   dcsdba.system_profile
      where  profile_id = '-ROOT-_USER_STREAMLINER_BUSINESSUNIT_' || b_client_id
      ;                                     
    l_businessunit varchar2(20);
  begin
    open  c_spe ( b_client_id => p_client_id_i);
    fetch c_spe
    into  l_businessunit;
    close c_spe;

    return l_businessunit;

  end get_businessunit;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Oct-2016
-- Purpose : Get BusinessUnit for Customs Streamliner
------------------------------------------------------------------------------------------------
  function get_client_from_bu ( p_businessunit_i in  varchar2
                              )
  return varchar2
  is
    cursor c_spe ( b_businessunit_i in varchar2)
    is
      select replace(profile_id, '-ROOT-_USER_STREAMLINER_BUSINESSUNIT_', null) client_id 
      from   dcsdba.system_profile
      where  text_data = b_businessunit_i
      ;                                     
    l_client varchar2(20);
  begin
    open  c_spe ( b_businessunit_i => p_businessunit_i);
    fetch c_spe
    into  l_client;
    close c_spe;

    return l_client;

  end get_client_from_bu;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Oct-2016
-- Purpose : Get License for Customs Streamliner
------------------------------------------------------------------------------------------------
  function get_license ( p_client_id_i in  varchar2
                            )
  return varchar2
  is
    cursor c_spe ( b_client_id in varchar2)
    is
      select text_data 
      from   dcsdba.system_profile
      where  profile_id = '-ROOT-_USER_STREAMLINER_LICENSE_' || b_client_id
      ;                                     
    l_license varchar2(20);
  begin
    open  c_spe ( b_client_id => p_client_id_i);
    fetch c_spe
    into  l_license;
    close c_spe;

    return l_license;

  end get_license;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 06-Dec-2018
-- Purpose : Get Additional Commodity Code for High/Low Tax Rate
------------------------------------------------------------------------------------------------
  function get_additionalcode ( p_commodity_code_i in varchar2
                              , p_tax_high_low_i   in varchar2
                              )
  return varchar2
  is
    cursor c_tax (b_commodity in varchar2)
    is
      select tnl.code
      ,      tfl.additionalcode
      ,      tfl.valuestring
      ,      abs(to_number(replace(trim(tfl.valuestring),'%',null))) abs_vat_perc
      from   customs_basic.v_taric_full@csl_rcl             tfl
      ,      customs_basic.v_taric_nomeclature_decl@csl_rcl tnl 
      where  tfl.type          = 'VAT' 
      and    tfl.issuecountry  = 'NL' 
      and    tfl.country       = 'NL' 
      and    tfl.goodscode     in ( tnl.code 
                                  , substr(tnl.code, 1, 8) || '00'
                                  , substr(tnl.code, 1, 6) || '0000'
                                  , substr(tnl.code, 1, 4) || '000000'
                                  , substr(tnl.code, 1, 2) || '00000000' 
                                  )
      and    to_char(sysdate, 'yyyymmdd') between tfl.startdate and tfl.enddate
      and    nvl(tfl.additionalcode, ' ') != ' '
      and    tnl.code          = b_commodity
      order  by abs(to_number(replace(trim(tfl.valuestring),'%',null)))
      ;

    r_tax      c_tax%rowtype;

    l_add_code varchar2(4);
    l_vat_perc number := 0;
  begin
    -- check if additional codes are available at all
    open  c_tax(b_commodity => p_commodity_code_i);
    fetch c_tax
    into  r_tax;
    if c_tax%found
    then
      close c_tax;
      -- 
      -- if LOW is the one we're looking for we have fetched the value so return that
      case p_tax_high_low_i
      when g_low
      then
        l_add_code := r_tax.additionalcode;
      when g_high
      then
        -- if HIGH we need to get the largest value and return that one
        -- cursor sorted on vat_perc ascending so lowest first
        -- simply loop completely through and fetch the additionalcode every time so we have the correct one after the loop is completed
        for r_tax in c_tax (b_commodity => p_commodity_code_i)
        loop
          l_vat_perc := r_tax.abs_vat_perc;
          l_add_code := r_tax.additionalcode;  
        end loop;
      else
        l_add_code := null;
      end case;
    else
      close c_tax;
      l_add_code := null;
    end if;

    return l_add_code;

  exception
    when others
    then
      case
      when c_tax%isopen
      then
        close c_tax;
      else
        null;
      end case;

    l_add_code := null;

    return l_add_code;

  end get_additionalcode;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Inbound Receipt file for Customs Streamliner
------------------------------------------------------------------------------------------------
  procedure create_inbound_receipt ( p_site_id_i      in  varchar2       
                                   , p_client_id_i    in  varchar2
                                   , p_reference_id_i in  varchar2
                                   )
  is
    cursor c_plda_hdr ( b_company in varchar2) 
    is
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company                                  as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                , xmlelement ( "MessageDetails"
                                                                                             , xmlforest ( g_streamsoft_inb_msg_type as "type"
                                                                                                         )
                                                                                             )
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;
    cursor c_plda_gdn ( b_businessunit in varchar2)
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'I'                                                 as "type"
                                                         , p_reference_id_i                                    as "linkId"
                                                         , b_businessunit                                      as "businessUnit"
                                                         )
                                             )
                                ) data
      from   dual
      ;
    cursor c_plda_gim ( b_client_id    in varchar2
                      , b_reference_id in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( itn.line_id                         as "linkId"
                                                         )
                                             , xmlelement ( "Inbound"
                                                          , xmlforest ( 'F'                    as "requireValidation"
                                                                      )
                                                          )
                                             , xmlelement ( "Product"
                                                          , xmlforest ( xmlcdata ( itn.sku_id) as "code"
                                                                      , to_char( (nvl( sku.each_weight, 0) * 0.9)
                                                                               , 'fm999999999999990D990'
                                                                               )               as "netmass"
                                                                      )
                                                          )
                                             , xmlforest ( nvl(itn.origin_id,'US')             as "originCountry" -- Default US as per request of customs
                                                         , pae.qty_due                         as "pieces"
                                                         , sum( nvl( itn.update_qty, 0))       as "stockAmount"
                                                         , itn.reference_id                    as "additionalRangeField1"
                                                         , decode( b_client_id, 'CRLBB', decode( substr( itn.tag_id, 1, 1), 'X', substr( itn.tag_id, 5)
                                                                                                                               , itn.tag_id
                                                                                             )
                                                                                     , itn.tag_id
                                                                 )                             as "serialnumber"
                                                         )
                                             )
                                ) data
      from   dcsdba.inventory_transaction itn
      ,      dcsdba.pre_advice_line       pae
      ,      dcsdba.sku                   sku
      where  itn.client_id    = b_client_id
      and    itn.reference_id = b_reference_id
      and    itn.client_id    = pae.client_id
      and    itn.reference_id = pae.pre_advice_id
      and    itn.line_id      = pae.line_id
      and    itn.client_id    = sku.client_id
      and    itn.sku_id       = sku.sku_id
      and    itn.code         in ('Receipt', 'Receipt Reverse')
      having sum(itn.update_qty) > 0  
      group  by itn.client_id
      ,      itn.reference_id
      ,      itn.line_id
      ,      itn.sku_id
      ,      sku.each_weight
      ,      itn.origin_id
      ,      pae.qty_due
      ,      itn.tag_id
      order by itn.reference_id
      ,     lpad(itn.line_id, 6, 0)
      ,     itn.tag_id
      ;

    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;

    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_content        varchar2(8192);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
    l_principal      varchar2(20);
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
  begin
    -- get company
    l_company := get_company ( p_client_id_i => p_client_id_i
                             );
    if l_company is null
    then
      l_company := p_site_id_i;
    end if;

    -- get businessunit
    l_businessunit := get_businessunit ( p_client_id_i => p_client_id_i
                                       ); 
    if l_businessunit is null
    then
      l_businessunit := p_client_id_i;
    end if;

    -- open csl file
    open_csl_file ( p_msg_type_i     => g_pldarct
                  , p_company_i      => l_company
                  , p_bu_ppl_i       => l_businessunit
                  , p_reference_id_i => p_reference_id_i
                  , p_file_type_o    => l_file_type
                  , p_file_name_o    => l_file_name
                  );

    -- fetch hdr data
    open  c_plda_hdr ( b_company => l_company);
    fetch c_plda_hdr
    into  l_content;
    close c_plda_hdr;
    -- write hdr content to file
    write_line ( p_file_type_i => l_file_type
               , p_content_i   => l_content
               );

    -- fetch gdn data
    open  c_plda_gdn ( b_businessunit => l_businessunit);
    fetch c_plda_gdn
    into  l_content;
    close c_plda_gdn;
    -- write gdn content to file
    write_line ( p_file_type_i => l_file_type
               , p_content_i   => l_content
               );

    -- fetch gim data
    for r_plda_gim in c_plda_gim ( b_client_id    => p_client_id_i
                                 , b_reference_id => p_reference_id_i
                                 )
    loop
      l_content := r_plda_gim.data;
      -- write gim content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );
    end loop;

    -- add remaining xml tags
    l_content := l_plda_tlr;
    -- write hdr content to file
    write_line ( p_file_type_i => l_file_type
               , p_content_i   => l_content
               );

    -- close and move file
    close_csl_file ( p_file_type_i => l_file_type
                   , p_file_name_i => l_file_name
                   );

  exception
    when others
    then
      case
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_inbound_receipt;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Inbound file for Intrastat/BTW purpose for Customs Streamliner based on outbound order
------------------------------------------------------------------------------------------------
  procedure create_inbound_wlgvat( p_site_id_i      in  varchar2       
                                 , p_client_id_i    in  varchar2
                                 , p_reference_id_i in  varchar2
                                 )
  is
    cursor c_smt ( b_site_id   in varchar2
                 , b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select 1
      from   dcsdba.shipping_manifest smt
      where  smt.site_id   = b_site_id
      and    smt.client_id = b_client_id
      and    smt.order_id  = b_order_id
      ;
    cursor c_plda_hdr ( b_company in varchar2) 
    is
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company             as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                , xmlelement ( "MessageDetails"
                                                                                             , xmlforest ( 'INITIAL' as "type"
                                                                                                         )
                                                                                             )
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;
    cursor c_plda_gdn ( b_businessunit in varchar2
                      , b_client_id    in varchar2
                      , b_order_id     in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'I'                                                                as "type"
                                                         , ohr.order_id                                                       as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( 'FISCAL'                                   as "arrivedStockType"
                                                                                 , 'FISCAL'                                   as "stockType"
                                                                                 , to_char( smt.shipped_dstamp, 'YYYY-MM-DD') as "arrivedDate"
                                                                                 , to_char( smt.shipped_dstamp, 'HH24:MI:SS') as "arrivedTime"
                                                                                 )
                                                                     as "Inbound"
                                                                     )
                                             , xmlforest ( to_char( smt.shipped_dstamp, 'YYYY-MM-DD')                         as "dossierDate"
                                                         , xmlcdata( ohr.order_reference)                                     as "commercialReference"
                                                         , xmlforest ( to_char( nvl( ohr.inv_total_1, 0)
                                                                              , 'fm999999990D90'
                                                                              )                                               as "invoiceAmount"
                                                                     , xmlforest ( nvl( upper( ohr.inv_currency), 'EUR')      as "currency"
                                                                                 )
                                                                                 as "ExchangeRate"
                                                                     )
                                                                     as "Invoice"
                                                         , xmlforest ( decode( b_client_id, 'WLGSALES' , 3    
                                                                                          , 'WLS'      , 3
                                                                                                       , 1
                                                                             )                                                as "transactionNature1"
                                                                     , decode( b_client_id, 'WLGSALES' , 1 -- Changed on request of Streamsoft    
                                                                                          , 'WLS'      , 1 -- Changed on request of Streamsoft 
                                                                                                       , 1
                                                                             )                                                as "transactionNature2"
                                                                     )
                                                                     as "TransactionNature"
                                                         , xmlforest ( 'PK'                                                   as "packageType"
                                                                     )
                                                                     as "TotalPackaging"
                                                         , b_businessunit || ohr.order_reference                              as "overview1"
                                                         , b_businessunit                                                     as "businessUnit"
                                                         )
                                             )
                                ) data
      from   dcsdba.order_header ohr
      ,      ( 
             select client_id
             ,      order_id
             ,      max( shipped_dstamp) shipped_dstamp
             from   dcsdba.shipping_manifest
             group  by client_id
             ,      order_id
             ) smt
      where  ohr.client_id   = smt.client_id
      and    ohr.order_id    = smt.order_id
      and    ohr.client_id   = b_client_id
      and    ohr.order_id    = b_order_id
      and    ohr.status      in ('Shipped','Delivered')
      ;
    cursor c_plda_gim ( b_businessunit in varchar2
                      , b_client_id    in varchar2
                      , b_order_id     in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( lpad( nvl( smt.user_def_num_3, smt.line_id), 7, 0)     as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( 'NLTLG01_VAT'                  as "customLicensing"
                                                                                 )
                                                                     as "CustomProcedure"
                                                                     )
                                                         , xmlforest ( xmlforest ( xmlcdata ( smt.sku_id)         as "code"
                                                                                 , xmlcdata ( sku.description)    as "description"
                                                                                 , xmlforest ( 'SI'               as "typeDeclaration"
                                                                                             , sku.commodity_code as "commodityCode"
                                                                                             )
                                                                                             as "CommodityImport"
                                                                                 , 'PCE'                          as "stockLevel"
                                                                                 , 'PKG'                          as "declareLevel"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( xmlforest ( cnl_streamsoft_pck.get_additionalcode( sku.commodity_code
                                                                                                            , sku.user_def_type_3
                                                                                                            )     as "nationalAdditionalCommodity1"
                                                                     )
                                                                     as "AdditionalCommodity"
                                                         , smt.origin_id                                          as "originCountry"
                                                         , to_char( (nvl( sku.each_weight, 0.001) * 0.9) * sum(smt.qty_shipped)
                                                                  , 'fm999999999999990D990'
                                                                  )                                               as "netMass"
                                                         , to_char( (nvl( sku.each_weight, 0.001) ) * sum(smt.qty_shipped)
                                                                  , 'fm999999999999990D990'
                                                                  )                                               as "grossMass"
                                                         , sum( smt.qty_shipped)                                  as "stockAmount"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( ( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * sum( smt.qty_shipped)
                                                                                          , 'fm999999990D90'
                                                                                          )                       as "price"
                                                                                 )
                                                                     as "Price"            
                                                                     )
                                             , xmlforest ( b_businessunit || ohr.order_reference || lpad( smt.user_def_num_3, 7, 0) as "serialnumber"
                                                         )
                                             )
                                ) data
      from   dcsdba.shipping_manifest smt
      ,      dcsdba.order_header      ohr
      ,      dcsdba.order_line        ole
      ,      dcsdba.sku               sku
      where  smt.client_id            = ohr.client_id
      and    smt.order_id             = ohr.order_id
      and    smt.client_id            = ole.client_id
      and    smt.order_id             = ole.order_id
      and    smt.line_id              = ole.line_id
      and    smt.client_id            = sku.client_id
      and    smt.sku_id               = sku.sku_id
      and    smt.client_id            = b_client_id
      and    smt.order_id             = b_order_id
      and    smt.shipped              = g_yes
      group  by smt.user_def_num_3
      ,      smt.line_id
      ,      smt.sku_id
      ,      sku.description
      ,      sku.commodity_code
      ,      sku.user_def_type_3
      ,      smt.origin_id
      ,      sku.each_weight
      ,      ole.line_value
      ,      ole.qty_ordered
      ,      ohr.order_reference
      order  by smt.line_id
      ;

    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;

    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_integer        integer;
    l_content        varchar2(8192);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
    l_principal      varchar2(20);
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
  begin
    open  c_smt ( b_site_id   => p_site_id_i
                , b_client_id => p_client_id_i
                , b_order_id  => p_reference_id_i
                );
    fetch c_smt
    into  l_integer;

    if c_smt%found
    then
      -- get company
      select decode( p_site_id_i, 'RCLTLB', 'FISC_' || substr( 'NLTLG01', -5)
                                , 'RCLEHV', 'FISC_' || substr( 'NLSBR01', -5)
                                          , 'FISC_' || substr( p_site_id_i, -5)
                   )
      into l_company
      from dual
      ;
      -- get businessunit
      l_businessunit := 'WLG';  -- decided with Customs Dept. for WLGore and WLGSales "WLG" is used as BU in Streamliner 

      -- open csl file
      open_csl_file ( p_msg_type_i     => g_pldafis_vat
                    , p_company_i      => l_company
                    , p_bu_ppl_i       => l_businessunit
                    , p_reference_id_i => p_reference_id_i
                    , p_file_type_o    => l_file_type
                    , p_file_name_o    => l_file_name
                    );

      -- fetch hdr data
      open  c_plda_hdr ( b_company => l_company);
      fetch c_plda_hdr
      into  l_content;
      close c_plda_hdr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- fetch gdn data
      open  c_plda_gdn ( b_businessunit => l_businessunit
                       , b_client_id    => p_client_id_i
                       , b_order_id     => p_reference_id_i
                       );
      fetch c_plda_gdn
      into  l_content;
      close c_plda_gdn;
      -- write gdn content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- fetch gim data
      for r_plda_gim in c_plda_gim ( b_businessunit => l_businessunit
                                   , b_client_id    => p_client_id_i
                                   , b_order_id     => p_reference_id_i
                                   )
      loop
        l_content := r_plda_gim.data;
        -- write gim content to file
        write_line ( p_file_type_i => l_file_type
                   , p_content_i   => l_content
                   );
      end loop;

      -- add remaining xml tags
      l_content := l_plda_tlr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- close and move file
      close_csl_file ( p_file_type_i => l_file_type
                     , p_file_name_i => l_file_name
                     );

    end if;
    close c_smt;

  exception
    when others
    then
      case
      when c_smt%isopen
      then
        close c_smt;
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_inbound_wlgvat;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Outbound Adjustment file for Customs Streamliner
------------------------------------------------------------------------------------------------
  procedure create_adjustment_minus ( p_site_id_i   in  varchar2       
                                    , p_client_id_i in  varchar2
                                    , p_key_i       in  varchar2
                                    )
  is
    cursor c_plda_hdr ( b_company in varchar2) 
    is
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company             as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;
    cursor c_plda_gdn ( b_businessunit in varchar2
                      , b_key          in integer
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'O'                                                        as "type"
                                                         , itn.key                                                    as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( itn.dstamp, 'YYYY-MM-DD') as "departedDate"
                                                                                 , to_char( itn.dstamp, 'HH24:MI:SS') as "departedTime"
                                                                                 , 'NL'                               as "destinationCountry"
                                                                                 )
                                                                     as "Outbound"
                                                                     )
                                             , xmlforest ( to_char( itn.dstamp, 'YYYY-MM-DD')                         as "dossierDate"
                                                         , b_businessunit                                             as "businessUnit"
                                                         , upper( itn.code)                                           as "AdditionalScenarioSmartDeclare"
                                                         )
                                             )
                                ) data
      from   dcsdba.inventory_transaction itn
      where  itn.key = b_key
      ;
    cursor c_plda_gim ( b_client_id in varchar2
                      , b_key       in integer
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( lpad( itn.line_id, 6, 0) || '_' || itn.key             as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( decode( itn.user_def_chk_4, 'Y', 'BONDED'
                                                                                                                            , 'FREE'
                                                                                         )                        
                                                                                         as "stockType"
                                                                                 )
                                                                     as "Outbound"            
                                                                     )
                                                         , xmlforest ( xmlforest ( xmlcdata ( itn.sku_id)         as "code"
                                                                                 , xmlforest ( sku.commodity_code as "commodityCode"
                                                                                             )
                                                                                 as "CommodityImport"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( itn.origin_id                                          as "originCountry"
                                                         , abs( itn.update_qty)                                   as "stockAmount"
                                                         , itn.reference_id                                       as "additionalRangeField1"
                                                         , decode( b_client_id, 'CRLBB', decode( substr( itn.tag_id, 1, 1), 'X', substr( itn.tag_id, 5)
                                                                                                                               , itn.tag_id
                                                                                               )
                                                                                     , itn.tag_id
                                                                 )                                                as "serialnumber"
                                                         )
                                             )
                                ) data
      from   dcsdba.inventory_transaction itn
      ,      dcsdba.sku                   sku
      where  itn.client_id = sku.client_id
      and    itn.sku_id    = sku.sku_id
      and    itn.client_id = b_client_id
      and    itn.key       = b_key
      ;

    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;

    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_content        varchar2(8192);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
    l_principal      varchar2(20);
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
  begin
    -- get company
    l_company := get_company ( p_client_id_i => p_client_id_i
                             );
    if l_company is null
    then
      l_company := p_site_id_i;
    end if;

    -- get businessunit
    l_businessunit := get_businessunit ( p_client_id_i => p_client_id_i
                                       ); 
    if l_businessunit is null
    then
      l_businessunit := p_client_id_i;
    end if;

    -- open csl file
    open_csl_file ( p_msg_type_i     => g_pldaajt
                  , p_company_i      => l_company
                  , p_bu_ppl_i       => l_businessunit
                  , p_reference_id_i => p_key_i
                  , p_file_type_o    => l_file_type
                  , p_file_name_o    => l_file_name
                  );

    -- fetch hdr data
    open  c_plda_hdr ( b_company => l_company);
    fetch c_plda_hdr
    into  l_content;
    close c_plda_hdr;
    -- write hdr content to file
    write_line ( p_file_type_i => l_file_type
               , p_content_i   => l_content
               );

    -- fetch gdn data
    open  c_plda_gdn ( b_businessunit => l_businessunit
                     , b_key          => p_key_i
                     );
    fetch c_plda_gdn
    into  l_content;
    close c_plda_gdn;
    -- write gdn content to file
    write_line ( p_file_type_i => l_file_type
               , p_content_i   => l_content
               );

    -- fetch gim data
    for r_plda_gim in c_plda_gim ( b_client_id => p_client_id_i
                                 , b_key       => p_key_i
                                 )
    loop
      l_content := r_plda_gim.data;
      -- write gim content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );
    end loop;

    -- add remaining xml tags
    l_content := l_plda_tlr;
    -- write hdr content to file
    write_line ( p_file_type_i => l_file_type
               , p_content_i   => l_content
               );

    -- close and move file
    close_csl_file ( p_file_type_i => l_file_type
                   , p_file_name_i => l_file_name
                   );

  exception
    when others
    then
      case
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_adjustment_minus;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Outbound Entrepot file for Customs Streamliner
------------------------------------------------------------------------------------------------
 procedure create_outbound_entrepot ( p_site_id_i      in  varchar2       
                                     , p_client_id_i    in  varchar2
                                     , p_reference_id_i in  varchar2
                                     )
  is
    cursor c_mtk ( b_site_id   in varchar2
                 , b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select 1
      from   dcsdba.move_task mtk
      where  mtk.site_id   = b_site_id
      and    mtk.client_id = b_client_id
      and    mtk.task_id   = b_order_id
      ;
    cursor c_plda_hdr ( b_company in varchar2) 
    is
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company             as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;
    cursor c_plda_gdn ( b_businessunit in varchar2
                      , b_client_id    in varchar2
                      , b_order_id     in varchar2
		, b_company      in varchar2
		, b_licensing in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'O'                                                        as "type"
                                                         , ohr.order_id                                               as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( mtk.picked_dstamp, 'YYYY-MM-DD')                         as "departedDate"
                                                                                 , to_char( mtk.picked_dstamp, 'HH24:MI:SS')                         as "departedTime"
                                                                                 , xmlforest ( xmlforest ( substr( ohr.vat_number, 1, 2)             as "country"
                                                                                                         , decode ( ohr.vat_number, null, null
                                                                                                                                        , '000'
                                                                                                                  )                                  as "identifier"
                                                                                                         , substr( ohr.vat_number, 3)                as "operatorIdentity"
                                                                                                         )
                                                                                                         as "OperatorIdentity"
                                                                                             , xmlforest ( xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.inv_name
                                                                                                                                                                   , ohr.name
                                                                                                                                        ) )                     as "operatorName"
                                                                                                         , xmlforest ( decode( ohr.hub_carrier_id, 'SUB', ohr.inv_postcode
                                                                                                                                                                   ,ohr.postcode)                  as "postalCode"
                                                                                                                     , xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.inv_address1, ohr.address1) )     as "streetAndNumber1"
                                                                                                                     , xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.inv_address2, ohr.address2) )     as "streetAndNumber2"
                                                                                                                     , xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.inv_town, ohr.town) )         as "city"
                                                                                                                     , decode( ohr.hub_carrier_id, 'SUB', cty_inv.iso2_id, cty_sto.iso2_id )              as "country"
                                                                                                                     )
                                                                                                                     as "OperatorAddress" 
                                                                                                         )
                                                                                                         as "Operator"
                                                                                             , decode( ohr.hub_carrier_id, 'SUB', ohr.inv_address_id, ohr.customer_id)                                       as "erpId"
                                                                                             )
                                                                                             as "Consignee"
                                                                                 , xmlforest ( b_businessunit                                        as "erpId" 
                                                                                             )
                                                                                             as "ConsigneeImport"
										 , decode( substr(ohr.postcode,1,2), 'BT' , decode(ohr.country, 'GBR', 'XI',cty_sto.iso2_id), cty_sto.iso2_id)	as  "destinationCountry"
                                                                                 --, cty_sto.iso2_id                                                   as "destinationCountry"
                                                                                 , decode( substr( ohr.order_type, 1, 1), 'D', decode( cty_sto.ce_eu_type, 'EU', 'T'
                                                                                                                                                               , 'F'
                                                                                                                                     )
                                                                                                                             , 'F'
                                                                                         )                                                           as "transitFlag"
                                                                                 )
                                                                     as "Outbound"
                                                                     )
                                             , xmlforest ( to_char( sysdate /*mtk.picked_dstamp*/, 'YYYY-MM-DD')                     as "dossierDate"
                                                         , xmlcdata( ohr.purchase_order)                                 as "commercialReference"
                                                         , xmlforest ( xmlcdata ( ohr.inv_reference )                    as "invoiceNumber"
                                                                     , to_char( ohr.inv_dstamp, 'YYYY-MM-DD')            as "invoiceDate"
                                                                     , to_char( nvl( ohr.inv_total_1, 0)
                                                                              , 'fm999999990D90'
                                                                              )                                          as "invoiceAmount"
                                                                     , xmlforest ( nvl( upper( ohr.inv_currency), 'EUR') as "currency"
                                                                                 )
                                                                                 as "ExchangeRate"
                                                                     )
                                                                     as "Invoice"
                                                         , xmlforest ( xmlforest ( to_char( nvl( ohr.freight_cost, 0), 'fm999999990D90') as "transportInsuranceCharges"
                                                                                 , xmlforest ( nvl( upper( ohr.inv_currency), 'EUR')     as "currency"
                                                                                             )
                                                                                             as "ExchangeRate"
                                                                                 , xmlforest ( to_char( nvl( ohr.insurance_cost, 0), 'fm999999990D90') as "insuranceCharges"
                                                                                             , xmlforest ( nvl( upper( ohr.inv_currency), 'EUR')       as "currency"
                                                                                                         )
                                                                                                         as "ExchangeRate"
                                                                                             )
                                                                                             as "InsuranceCharges" 
                                                                                 )
                                                                                 as "CustomsCharges"
                                                                     )
                                                                     as "TotalCharges"
																	
														 , xmlforest ( '1'                               as "transactionNature1"
																	 , '1'                               as "transactionNature2"
																	 )
																	 as "TransactionNature"
														 , xmlforest ( b_company             as "erpid"
                                                                     )
																	 as "WarehouseLocation"
														 , xmlforest (b_licensing             as "customLicensing"
                                                                     )
																	 as "CustomProcedure"
																	 
														, xmlforest ( xmlforest ( ohr.tod                            as "deliveryTerms"
                                                                                 , xmlcdata ( ohr.tod_place )         as "deliveryTermsPlace"
                                                                                 )
                                                                                 as "DeliveryTerms"
                                                                      , csl.tracking_number         as "borderIdentity"  
                                                                     , '3' as  "borderMode"
								     , 'AUTO' as                        "departureIdentity"
																	 )
                                                                     as "TransportMeans"
                                                         , xmlforest ( xmlforest ( substr( ohr.inv_vat_number, 1, 2)             as "country"
                                                                                 , decode ( ohr.inv_vat_number, null, null
                                                                                                                    , '000'
                                                                                          )                                      as "identifier"
                                                                                 , substr( ohr.inv_vat_number, 3)                as "operatorIdentity"
                                                                                 )
                                                                                 as "OperatorIdentity" 
                                                                     , xmlforest ( xmlcdata ( ohr.inv_name )                     as "operatorName"
                                                                                 , xmlforest ( ohr.inv_postcode                  as "postalCode"
                                                                                             , xmlcdata ( ohr.inv_address1 )     as "streetAndNumber1"
                                                                                             , xmlcdata ( ohr.inv_address2 )     as "streetAndNumber2"
                                                                                             , xmlcdata ( ohr.inv_town )         as "city"
                                                                                             , cty_inv.iso2_id                   as "country"
                                                                                             )
                                                                                             as "OperatorAddress" 
                                                                                 )
                                                                                 as "Operator"
                                                                     , ohr.inv_address_id                                        as "erpId"
                                                                     )
                                                                     as "Customer"
                                                         , b_businessunit                                                        as "businessUnit"
                                                         , decode( substr( ohr.order_type, 3, 2), 'CO', 'CORRECTIE'
                                                                                                , 'OV', 'OVERDRACHT'
                                                                                                , 'VM', 'ADJUSTMENT'
                                                                                                , 'VN', 'VERNIETIGING'
                                                                                                      , null
                                                                 )                                                               as "AdditionalScenarioSmartDeclare"
                                                         )
                                             )
                                ) data
      from   dcsdba.order_header ohr
       ,	 ( 
             select client_id
             ,      order_id
             ,     min( tracking_number) as tracking_number
             from   cnl_sys.cnl_cto_ship_labels
              group  by client_id
             ,      order_id
             )	 csl 
      ,      dcsdba.country      cty_sto
      ,      dcsdba.country      cty_inv
      ,      ( 
             select client_id
             ,      task_id
             ,      max( dstamp) picked_dstamp
             from   dcsdba.move_task
             group  by client_id
             ,      task_id
             ) mtk
      where  ohr.client_id   = mtk.client_id
      and    ohr.order_id    = mtk.task_id
      and    ohr.country     = cty_sto.iso3_id (+)
      and    ohr.inv_country = cty_inv.iso3_id (+)
       and    ohr.client_id   = csl.client_id
      and    ohr.order_id    = csl.order_id  
      and    ohr.client_id   = b_client_id
      and    ohr.order_id    = b_order_id
      ;
    cursor c_plda_gim ( b_client_id in varchar2
                      , b_order_id  in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( lpad( mtk.line_id, 6, 0) || '_' || mtk.key        as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( decode( inv.user_def_chk_4, 'Y' , 'BONDED'
                                                                                                                             , 'FREE'
                                                                                         )                   
                                                                                         as "stockType"
                                                                                 )
                                                                     as "Outbound"            
                                                                     )
                                                         , xmlforest ( xmlforest ( xmlcdata ( mtk.sku_id)    as "code"
                                                                                 , xmlforest ( sku.commodity_code as "commodityCode"
                                                                                             )
                                                                                             as "CommodityImport"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( inv.origin_id                                     as "originCountry"
                                                         , to_char( (nvl( sku.each_weight, 0) * 0.9)
                                                                  , 'fm999999999999990D990'
                                                                  )                                          as "netMass1"
                                                         , to_char( (nvl( sku.each_weight, 0) * 0.9) * mtk.qty_to_move
                                                                  , 'fm999999999999990D990'
                                                                  )                                          as "netMass"
                                                         , mtk.qty_to_move                                   as "stockAmount"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( ( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * mtk.qty_to_move
                                                                                          , 'fm999999990D90'
                                                                                          )                  as "price"
                                                                                 )
                                                                     as "Price"            
                                                                     )
                                             , xmlforest ( inv.receipt_id                                    as "additionalRangeField1"
                                                         , decode( b_client_id, 'CRLBB', decode( substr( mtk.tag_id, 1, 1), 'X', substr( mtk.tag_id, 5)
                                                                                                                               , mtk.tag_id
                                                                                               )
                                                                                       , mtk.tag_id
                                                                 )                                           as "serialnumber"
                                                         )
                                             )
                                ) data
      from   dcsdba.move_task         mtk
      ,      dcsdba.order_line        ole
      ,      dcsdba.sku               sku
      ,      (
             select distinct
                    client_id
             ,      sku_id
             ,      tag_id
             ,      receipt_id
             ,      condition_id
             ,      user_def_chk_4
             ,      origin_id
             from   dcsdba.inventory
             ) inv 
      where  mtk.client_id            = ole.client_id
      and    mtk.task_id              = ole.order_id
      and    mtk.line_id              = ole.line_id
      and    mtk.client_id            = sku.client_id
      and    mtk.sku_id               = sku.sku_id
      and    mtk.client_id            = inv.client_id
      and    mtk.sku_id               = inv.sku_id
      and    mtk.tag_id               = inv.tag_id
      and    mtk.client_id            = b_client_id
      and    mtk.task_id              = b_order_id
      order  by mtk.line_id
      ,      mtk.key
      ;
--Outbound Lines for Client_group CSLENTKIT
cursor c_plda_gim_1 ( b_client_id in varchar2
                      , b_order_id  in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( lpad( ole.line_id, 6, 0) || '_' || ole.order_id        as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( decode (ole.user_def_type_2, 'K', 'BONDED', decode( inv.user_def_chk_4, 'Y' , 'BONDED'
                                                                                                                             , 'FREE'
                                                                                         ) 
                                                                                         )
                                                                                         as "stockType"
                                                                                 )
                                                                     as "Outbound"            
                                                                     )
							 , xmlforest ( decode( ole.user_def_type_2, 'K','EX_RECON','') as "additionalScenario")	     
                                                         , xmlforest ( xmlforest ( xmlcdata ( ole.sku_id)    as "code"
                                                                                 , xmlforest ( sku.commodity_code as "commodityCode"
                                                                                             )
                                                                                             as "CommodityImport"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( decode( ole.user_def_type_2, 'K', ole.origin_id
                                                                                                                             , inv.origin_id
                                                                                         )                                      as "originCountry"
                                                         , to_char( (nvl( decode( ole.user_def_type_2, 'K',ole.catch_weight, sku.each_weight), 0) * 0.9)
                                                                  , 'fm999999999999990D990'
                                                                  )                                          as "netMass1"
                                                         , to_char( (nvl( decode( ole.user_def_type_2, 'K',ole.catch_weight, sku.each_weight), 0) * 0.9 * nvl (mtk.qty_to_move, ole.qty_ordered))
                                                                  , 'fm999999999999990D990'
                                                                  )                                          as "netMass"
                                                         , nvl (mtk.qty_to_move, ole.qty_ordered)                                   as "stockAmount"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( ( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * nvl( mtk.qty_to_move, ole.qty_ordered)
                                                                                          , 'fm999999990D90'
                                                                                          )                  as "price"
                                                                                 )
                                                                     as "Price"            
                                                                     )
                                             , xmlforest ( nvl (inv.receipt_id , 'NL_RECON_'||ole.order_id||'_'||ole.line_id)                                   as "additionalRangeField1"
                                                         , nvl (mtk.tag_id, 'NL_RECON_'||ole.order_id||'_'||ole.line_id)                                         as "serialnumber"
                                                         )
                                             )
                                ) data

-- select ole.line_id, ole.sku_id , ole.user_def_type_2 , ole.host_line_id , ole.qty_ordered , ole.qty_tasked, ole.line_value, mtk.key, mtk.tag_id ,mtk.qty_to_move, sku.commodity_code , nvl (mtk.qty_to_move, ole.qty_ordered)

FROM dcsdba.order_line ole
LEFT OUTER JOIN dcsdba.move_task mtk ON ole.client_id = mtk.client_id
	AND ole.sku_id = mtk.sku_id
	AND ole.line_id = mtk.line_id
	AND ole.order_id = mtk.task_id
JOIN dcsdba.sku sku ON ole.sku_id = sku.sku_id
	AND ole.client_id = sku.client_id
LEFT OUTER JOIN (
	SELECT DISTINCT client_id
		,sku_id
		,tag_id
		,receipt_id
		,condition_id
		,user_def_chk_4
		,origin_id
	FROM dcsdba.inventory
	) inv ON Ole.sku_id = inv.sku_id
	AND Mtk.tag_id = inv.tag_id
	AND Ole.client_id = inv.client_id
WHERE ole.client_id = b_client_id
AND ole.order_id = b_order_id
	AND ole.user_def_type_2 != 'C'
ORDER BY ole.line_id

      ;

    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;
    r_plda_gim_1     c_plda_gim_1%rowtype;

    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_integer        integer;
    l_content        varchar2(8192);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
	l_licensing	     varchar2(20);
    l_principal      varchar2(20);
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
    l_client_group_  varchar2(10) := 'N';
  begin
    open  c_mtk ( b_site_id   => p_site_id_i
                , b_client_id => p_client_id_i
                , b_order_id  => p_reference_id_i
                );
    fetch c_mtk
    into  l_integer;

    if c_mtk%found
    then
      -- get company
      l_company := get_company ( p_client_id_i => p_client_id_i
                               );
      if l_company is null
      then
        l_company := p_site_id_i;
      end if;

      -- get businessunit
      l_businessunit := get_businessunit ( p_client_id_i => p_client_id_i
                                         ); 
      if l_businessunit is null
      then
        l_businessunit := p_client_id_i;
      end if;
	  
	   --fetch Licensing
	   l_licensing := get_license ( p_client_id_i => p_client_id_i
								  );
		if l_licensing is null
		then
			l_licensing := p_client_id_i;
		end if;

      -- open csl file
      open_csl_file ( p_msg_type_i     => g_pldaodr
                    , p_company_i      => l_company
                    , p_bu_ppl_i       => l_businessunit
                    , p_reference_id_i => p_reference_id_i
                    , p_file_type_o    => l_file_type
                    , p_file_name_o    => l_file_name
                    );

      -- fetch hdr data
      open  c_plda_hdr ( b_company => l_company);
      fetch c_plda_hdr
      into  l_content;
      close c_plda_hdr;
	  
	 
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- fetch gdn data
      open  c_plda_gdn ( b_businessunit => l_businessunit
                       , b_client_id    => p_client_id_i
                       , b_order_id     => p_reference_id_i
						, b_company => l_company
						, b_licensing => l_licensing
                       );
      fetch c_plda_gdn
      into  l_content;
      close c_plda_gdn;
      -- write gdn content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );
      begin
        select distinct nvl('Y','N')
        into   l_client_group_
        from   dcsdba.client_group_clients
        where  client_id = p_client_id_i
        and    client_group = 'CSLENTKIT';
        -- Currently we only have 'VARIAN', but for the future, we can have more
      exception 
         when others then
        l_client_group_ := 'N';
        end
      ;


      if l_client_group_ = 'Y' then

      -- fetch gim data for client VARIAN
      for r_plda_gim_1 in c_plda_gim_1 ( b_client_id => p_client_id_i
                                   , b_order_id  => p_reference_id_i
                                   )
      loop
        l_content := r_plda_gim_1.data;
        -- write gim content to file
        write_line ( p_file_type_i => l_file_type
                   , p_content_i   => l_content
                   );
      end loop;
     else
      -- fetch gim data
        for r_plda_gim in c_plda_gim ( b_client_id => p_client_id_i
                                     , b_order_id  => p_reference_id_i
                                     )
        loop
          l_content := r_plda_gim.data;
          -- write gim content to file
          write_line ( p_file_type_i => l_file_type
                     , p_content_i   => l_content
                     );
        end loop;
        end if;

      -- add remaining xml tags
      l_content := l_plda_tlr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- close and move file
      close_csl_file ( p_file_type_i => l_file_type
                     , p_file_name_i => l_file_name
                     );

    end if;
    close c_mtk;

  exception
    when others
    then
      case
      when c_mtk%isopen
      then
        close c_mtk;
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_outbound_entrepot;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Outbound Export file for Customs Streamliner
------------------------------------------------------------------------------------------------
  procedure create_outbound_export ( p_site_id_i      in  varchar2       
                                   , p_client_id_i    in  varchar2
                                   , p_reference_id_i in  varchar2
                                   )
  is
    cursor c_mtk ( b_site_id   in varchar2
                 , b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select 1
      from   dcsdba.move_task mtk
      where  mtk.site_id   = b_site_id
      and    mtk.client_id = b_client_id
      and    mtk.task_id   = b_order_id
      ;
    cursor c_plda_hdr ( b_company in varchar2) 
    is
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company             as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;
    cursor c_plda_gdn ( b_businessunit in varchar2
                      , b_client_id    in varchar2
                      , b_order_id     in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'O'                                                        as "type"
                                                         , ohr.order_id || 'EXP'                                      as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( mtk.picked_dstamp, 'YYYY-MM-DD')                         as "departedDate"
                                                                                 , to_char( mtk.picked_dstamp, 'HH24:MI:SS')                         as "departedTime"
                                                                                 , xmlforest ( xmlforest ( substr( ohr.vat_number, 1, 2)             as "country"
                                                                                                         , decode ( ohr.vat_number, null, null
                                                                                                                                        , '000'
                                                                                                                  )                                  as "identifier"
                                                                                                         , substr( ohr.vat_number, 3)                as "operatorIdentity"
                                                                                                         )
                                                                                                         as "OperatorIdentity"
                                                                                             , xmlforest ( xmlcdata ( ohr.name )                     as "operatorName"
                                                                                                         , xmlforest ( ohr.postcode                  as "postalCode"
                                                                                                                     , xmlcdata ( ohr.address1 )     as "streetAndNumber1"
                                                                                                                     , xmlcdata ( ohr.address2 )     as "streetAndNumber2"
                                                                                                                     , xmlcdata ( ohr.town )         as "city"
                                                                                                                     , cty_sto.iso2_id               as "country"
                                                                                                                     )
                                                                                                                     as "OperatorAddress" 
                                                                                                         )
                                                                                                         as "Operator"
                                                                                             , ohr.customer_id                                       as "erpId"
                                                                                             )
                                                                                             as "Consignee"
                                                                                 , xmlforest ( b_businessunit                                        as "erpId" 
                                                                                             )
                                                                                             as "ConsigneeImport"
                                                                                 , cty_sto.iso2_id                                                   as "destinationCountry"
                                                                                 )
                                                                     as "Outbound"
                                                                     )
                                             , xmlforest ( to_char( sysdate /*mtk.picked_dstamp*/, 'YYYY-MM-DD')                     as "dossierDate"
                                                         , xmlcdata( ohr.purchase_order)                                 as "commercialReference"
                                                         , xmlforest ( xmlcdata ( ohr.inv_reference )                    as "invoiceNumber"
                                                                     , to_char( ohr.inv_dstamp, 'YYYY-MM-DD')            as "invoiceDate"
                                                                     , to_char( nvl( ohr.inv_total_1, 0)
                                                                              , 'fm999999990D90'
                                                                              )                                          as "invoiceAmount"
                                                                     , xmlforest ( nvl( upper( ohr.inv_currency), 'EUR') as "currency"
                                                                                 )
                                                                                 as "ExchangeRate"
                                                                     )
                                                                     as "Invoice"
                                                         , xmlforest ( 1                                     as "transactionNature1"
                                                                     , 1                                     as "transactionNature2"
                                                                     )
                                                                     as "TransactionNature"
                                                         , xmlforest ( decode( ohr.from_site_id, 'RCLEHV', 'NLSBR01'
                                                                                               , 'RCLTLB', 'NLTLG01'
                                                                                                         , ohr.from_site_id
                                                                             ) as "erpId"
                                                                     )
                                                                     as "WarehouseLocation"
                                                         , xmlforest ( decode( ohr.from_site_id, 'NLSBR01', 'NLSBR01'
                                                                                               , 'NLTLG01', 'NLTLG01'
                                                                                                         , ohr.from_site_id
                                                                             ) 
                                                                             || '-NOSTOCK'                                  as "customLicensing"
                                                                     )
                                                                     as "CustomProcedure"
                                                         , xmlforest ( xmlforest ( ohr.tod                            as "deliveryTerms"
                                                                                 , xmlcdata ( ohr.tod_place )         as "deliveryTermsPlace"
                                                                                 )
                                                                                 as "DeliveryTerms"
                                                                     , 3                                              as "borderMode" 
                                                                     , 'AUTO'                                         as "borderIdentity"  /*odc.carrier_consignment_id*/  --For future change "borderIdentity"
                                                                     , 'AUTO'                                         as "departureIdentity" 
                                                                     )
                                                                     as "TransportMeans"
                                                         , xmlforest ( xmlforest ( substr( ohr.inv_vat_number, 1, 2)             as "country"
                                                                                 , decode ( ohr.inv_vat_number, null, null
                                                                                                                    , '000'
                                                                                          )                                      as "identifier"
                                                                                 , substr( ohr.inv_vat_number, 3)                as "operatorIdentity"
                                                                                 )
                                                                                 as "OperatorIdentity" 
                                                                     , xmlforest ( xmlcdata ( ohr.inv_name )                     as "operatorName"
                                                                                 , xmlforest ( ohr.inv_postcode                  as "postalCode"
                                                                                             , xmlcdata ( ohr.inv_address1 )     as "streetAndNumber1"
                                                                                             , xmlcdata ( ohr.inv_address2 )     as "streetAndNumber2"
                                                                                             , xmlcdata ( ohr.inv_town )         as "city"
                                                                                             , cty_inv.iso2_id                   as "country"
                                                                                             )
                                                                                             as "OperatorAddress" 
                                                                                 )
                                                                                 as "Operator"
                                                                     , ohr.inv_address_id                                        as "erpId"
                                                                     )
                                                                     as "Customer"
                                                         , b_businessunit                                                        as "businessUnit"
                                                         )
                                             )
                                ) data
      from   dcsdba.order_header ohr
	  /*,		( 
             select client_id
             ,      order_id
             ,      min( carrier_consignment_id) carrier_consignment_id
             from   dcsdba.order_container
             group  by client_id
             ,      order_id
             )	 odc  */  --For future change "borderIdentity"
      ,      dcsdba.country      cty_sto
      ,      dcsdba.country      cty_inv
      ,      ( 
             select client_id
             ,      task_id
             ,      max( dstamp) picked_dstamp
             from   dcsdba.move_task
             group  by client_id
             ,      task_id
             ) mtk
      where  ohr.client_id   = mtk.client_id
      and    ohr.order_id    = mtk.task_id
      and    ohr.country     = cty_sto.iso3_id (+)
      and    ohr.inv_country = cty_inv.iso3_id (+)
	/*  and    ohr.client_id   = odc.client_id
      and    ohr.order_id    = odc.order_id */  --For future change "borderIdentity"
      and    ohr.client_id   = b_client_id
      and    ohr.order_id    = b_order_id
      ;
    cursor c_plda_gim ( b_client_id in varchar2
                      , b_order_id  in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( lpad( mtk.line_id, 6, 0)                          as "sequence"
                                                         )
                                                         , xmlforest ( xmlforest (decode( inv2.user_def_chk_4, 'Y'           , 'BONDED'
                                                                                                                             , 'FREE'
                                                                                         )                    
                                                                                 as "stockType"
                                                                                 )
                                                                     as "Outbound"            
                                                                     )
                                                         , xmlforest ( xmlforest ( sku.sku_id             as "code"
                                                                                 , sku.description             as "description"
                                                                                 , xmlforest ( 'SI'               as "typeDeclaration"
                                                                                             , sku.commodity_code as "commodityCode"
                                                                                             )
                                                                                 as "CommodityImport"
                                                                                 , 'PCE'                                      as "stockLevel" 
                                                                                 , 'PKG'                                      as "declareLevel"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( mtk.origin_id                                          as "originCountry"
                                                         , sum( mtk.qty_to_move)                                   as "pieces"
                                                         , to_char( (nvl( sku.each_weight, 0.001) * 0.9) * sum( mtk.qty_to_move)
                                                                  , 'fm999999999999990D990'
                                                                  )                                               as "netMass"
                                                         , to_char( nvl( sku.each_weight, 0.001)         * sum( mtk.qty_to_move)
                                                                  , 'fm999999999999990D990'
                                                                  )                                               as "grossMass"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( ( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * sum( mtk.qty_to_move)
                                                                                          , 'fm999999990D90'
                                                                                          )                  as "price"
                                                                                 )
                                                                     as "Price"            
                                                                     )
                                             )
                                ) data
      from   dcsdba.move_task         mtk
      ,      dcsdba.order_line        ole
      ,      dcsdba.sku               sku
      ,      dcsdba.inventory         inv2
      where  mtk.client_id            = ole.client_id
      and    mtk.task_id              = ole.order_id
      and    mtk.line_id              = ole.line_id
      and    mtk.client_id            = sku.client_id
      and    mtk.sku_id               = sku.sku_id
      and    mtk.sku_id               = inv2.sku_id
      and    mtk.tag_id               = inv2.tag_id
      and    mtk.client_id            = inv2.client_id
      and    mtk.client_id            = b_client_id
      and    mtk.task_id              = b_order_id
      group  by mtk.line_id
      ,      inv2.user_def_chk_4
      ,      sku.sku_id
      ,      sku.description
      ,      sku.commodity_code
      ,      mtk.origin_id
      ,      sku.each_weight
      ,      ole.line_value
      ,      ole.qty_ordered
      order  by mtk.line_id
      ;

    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;

    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_integer        integer;
    l_content        varchar2(8192);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
    l_principal      varchar2(20);
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
  begin
    open  c_mtk ( b_site_id   => p_site_id_i
                , b_client_id => p_client_id_i
                , b_order_id  => p_reference_id_i
                );
    fetch c_mtk
    into  l_integer;

    if c_mtk%found
    then
      -- get businessunit
      l_businessunit := get_businessunit ( p_client_id_i => p_client_id_i
                                         ); 
      if l_businessunit is null
      then
        l_businessunit := p_client_id_i;
      end if;

      -- set company
      select decode( p_site_id_i, 'RCLEHV', 'NLSBR01'
                                , 'RCLTLB', 'NLTLG01'
                                          , p_site_id_i
                   )
      into l_company
      from dual
      ;
      -- open csl file
      open_csl_file ( p_msg_type_i     => g_pldaodr
                    , p_company_i      => l_company
                    , p_bu_ppl_i       => l_businessunit
                    , p_reference_id_i => p_reference_id_i
                    , p_file_type_o    => l_file_type
                    , p_file_name_o    => l_file_name
                    );

      -- fetch hdr data
      open  c_plda_hdr ( b_company => l_company);
      fetch c_plda_hdr
      into  l_content;
      close c_plda_hdr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- fetch gdn data
      open  c_plda_gdn ( b_businessunit => l_businessunit
                       , b_client_id    => p_client_id_i
                       , b_order_id     => p_reference_id_i
                       );
      fetch c_plda_gdn
      into  l_content;
      close c_plda_gdn;
      -- write gdn content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- fetch gim data
      for r_plda_gim in c_plda_gim ( b_client_id => p_client_id_i
                                   , b_order_id  => p_reference_id_i
                                   )
      loop
        l_content := r_plda_gim.data;
        -- write gim content to file
        write_line ( p_file_type_i => l_file_type
                   , p_content_i   => l_content
                   );
      end loop;

      -- add remaining xml tags
      l_content := l_plda_tlr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- close and move file
      close_csl_file ( p_file_type_i => l_file_type
                     , p_file_name_i => l_file_name
                     );

    end if;
    close c_mtk;

  exception
    when others
    then
      case
      when c_mtk%isopen
      then
        close c_mtk;
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_outbound_export;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 13-Dec-2018
-- Purpose : Create Outbound file for Intrastat/BTW purpose for Customs Streamliner based on outbound order
------------------------------------------------------------------------------------------------
  procedure create_outbound_cbs( p_site_id_i      in  varchar2       
                               , p_client_id_i    in  varchar2
                               , p_reference_id_i in  varchar2
                               , p_csl_bu_i       in  varchar2
                               , p_trans_type_i   in  integer  := null
                               )
  is
    cursor c_smt ( b_site_id   in varchar2
                 , b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select 1
      from   dcsdba.shipping_manifest smt
      where  smt.site_id   = b_site_id
      and    smt.client_id = b_client_id
      and    smt.order_id  = b_order_id
      and    smt.shipped   = g_yes
      ;
    cursor c_ole ( b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select distinct
             nvl( ole.user_def_num_1, 1) trans_code
      from   dcsdba.order_line ole
      where  ole.client_id = b_client_id
      and    ole.order_id  = b_order_id
      order  by 1
      ;
    cursor c_plda_hdr ( b_company in varchar2) 
    is
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company             as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                , xmlelement ( "MessageDetails"
                                                                                             , xmlforest ( null as "type" -- Streamsoft requested to leave this empty
                                                                                                         )
                                                                                             )
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;
    cursor c_plda_gdn ( b_businessunit in varchar2
                      , b_client_id    in varchar2
                      , b_order_id     in varchar2
                      , b_trans_type   in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'O'                                                        as "type"
                                                         , ohr.order_id                                               as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( smt.shipped_dstamp, 'YYYY-MM-DD')                        as "departedDate"
                                                                                 , to_char( smt.shipped_dstamp, 'HH24:MI:SS')                        as "departedTime"
                                                                                 , xmlforest ( xmlforest ( decode ( ohr.hub_carrier_id, 'SUB', substr( ohr.hub_vat_number, 1, 2)
                                                                                                                                             , substr( ohr.inv_vat_number, 1, 2)
                                                                                                                  )                                                            as "country"
                                                                                                         , decode ( decode ( ohr.hub_carrier_id, 'SUB', ohr.hub_vat_number
                                                                                                                                                      , ohr.inv_vat_number
                                                                                                                           )
                                                                                                                  , null, null
                                                                                                                        , '000'
                                                                                                                  )                                                            as "identifier"
                                                                                                         , decode ( ohr.hub_carrier_id, 'SUB', substr( ohr.hub_vat_number, 3)
                                                                                                                                             , substr( ohr.inv_vat_number, 3)
                                                                                                                  )                                                            as "operatorIdentity"
                                                                                                         )
                                                                                                         as "OperatorIdentity"
                                                                                             , xmlforest ( decode ( ohr.hub_carrier_id, 'SUB', substr( ohr.hub_vat_number, 1, 2)
                                                                                                                                             , substr( ohr.inv_vat_number, 1, 2)
                                                                                                                  )                                                            as "country"
                                                                                                         , decode ( decode ( ohr.hub_carrier_id, 'SUB', ohr.hub_vat_number
                                                                                                                                                      , ohr.inv_vat_number
                                                                                                                           )
                                                                                                                  , null, null
                                                                                                                        , '005'
                                                                                                                  )                                                            as "identifier"
                                                                                                         , decode ( ohr.hub_carrier_id, 'SUB', substr( ohr.hub_vat_number, 3)
                                                                                                                                             , substr( ohr.inv_vat_number, 3)
                                                                                                                  )                                                            as "operatorIdentity"
                                                                                                         )
                                                                                                         as "OperatorIdentity"
                                                                                             , xmlforest ( xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.hub_name
                                                                                                                                                       , ohr.inv_name 
                                                                                                                            )
                                                                                                                    )                                                          as "operatorName"
                                                                                                         , xmlforest ( decode( ohr.hub_carrier_id, 'SUB', ohr.hub_postcode
                                                                                                                                                        , ohr.inv_postcode
                                                                                                                             )                                                 as "postalCode"
                                                                                                                     , xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.hub_address1
                                                                                                                                                                   , ohr.inv_address1
                                                                                                                                        )
                                                                                                                                )                                              as "streetAndNumber1"
                                                                                                                     , xmlcdata ( decode( ohr.hub_carrier_id, 'SUB', ohr.hub_town
                                                                                                                                                                   , ohr.inv_town
                                                                                                                                        ) 
                                                                                                                                )                                              as "city"
                                                                                                                     , decode( ohr.hub_carrier_id, 'SUB', cty_hub.iso2_id
                                                                                                                                                        , cty_inv.iso2_id
                                                                                                                             )                                                 as "country"
                                                                                                                     )
                                                                                                                     as "OperatorAddress" 
                                                                                                         )
                                                                                                         as "Operator"
                                                                                             , decode( ohr.hub_carrier_id, 'SUB', nvl(ohr.hub_address_id, ohr.country)
                                                                                                                                , nvl(ohr.inv_address_id, ohr.country)
                                                                                                     )                                                                         as "erpId"
                                                                                             )
                                                                                             as "Consignee"
                                                                                 , cty.iso2_id                                                       as "destinationCountry"
                                                                                 )
                                                                     as "Outbound"
                                                                     )
                                             , xmlforest ( to_char( smt.shipped_dstamp, 'YYYY-MM-DD')                                                as "dossierDate"
                                                         , xmlcdata( ohr.purchase_order)                                                             as "commercialReference"
                                                         , xmlforest ( xmlcdata ( ohr.inv_reference )                                                as "invoiceNumber"
                                                                     , to_char( nvl( ohr.inv_dstamp, smt.shipped_dstamp), 'YYYY-MM-DD')              as "invoiceDate"
                                                                     , to_char( nvl( ohr.inv_total_1, 0)
                                                                              , 'fm999999990D90'
                                                                              )                                                                      as "invoiceAmount"
                                                                     , xmlforest ( nvl( upper( ohr.inv_currency), 'EUR')                             as "currency"
                                                                                 )
                                                                                 as "ExchangeRate"
                                                                     )
                                                                     as "Invoice"
                                                         , xmlforest ( b_trans_type                                                                  as "transactionNature1"
                                                                     , decode( b_trans_type, 3 , 1 -- Was null, adjusted on request of Stream    
                                                                                               , 1
                                                                             )                                                                       as "transactionNature2"
                                                                     )
                                                                     as "TransactionNature"
                                                         , xmlforest ( decode( ohr.from_site_id, 'RCLEHV', 'NLSBR01'
                                                                                               , 'RCLTLB', 'NLTLG01'
                                                                                                         , ohr.from_site_id
                                                                             )                                                                       as "erpId"
                                                                     )
                                                                     as "WarehouseLocation"
                                                         , xmlforest ( 'NLTLG01_CBS'                                                                 as "customLicensing"
                                                                     )
                                                                     as "CustomProcedure"
                                                         , xmlforest ( 'PK'                                                                          as "packageType"
                                                                     )
                                                                     as "TotalPackaging"
                                                         , xmlforest ( xmlforest ( ohr.tod                                                           as "deliveryTerms"
                                                                                 , xmlcdata ( nvl( ohr.tod_place, ohr.town) )                        as "deliveryTermsPlace"
                                                                                 )
                                                                                 as "DeliveryTerms"
                                                                     , 3                                                                             as "borderMode" 
                                                                     , 'AUTO'                                                                        as "borderIdentity" 
                                                                     , 'AUTO'                                                                        as "departureIdentity" 
                                                                     )
                                                                     as "TransportMeans"
                                                         , xmlforest ( b_businessunit                                                                as "erpId"
                                                                     )
                                                                     as "SupplierIntrastat"
                                                         , xmlforest ( b_businessunit                                                                as "erpId"
                                                                     )
                                                                     as "RepresentedParty"
                                                         , ohr.order_reference                                                                       as "overview1"
                                                         , ohr.client_id                                                                             as "overview2"
                                                         , ohr.order_id                                                                              as "overview3"
                                                         , b_businessunit                                                                            as "businessUnit"
                                                         )
                                             )
                                ) data   
      from   dcsdba.order_header ohr
      ,      dcsdba.country      cty
      ,      dcsdba.country      cty_inv
      ,      dcsdba.country      cty_hub
      ,      ( 
             select client_id
             ,      order_id
             ,      max( shipped_dstamp) shipped_dstamp
             from   dcsdba.shipping_manifest
             group  by client_id
             ,      order_id
             ) smt
      where  ohr.client_id   = smt.client_id
      and    ohr.order_id    = smt.order_id
      and    ohr.country     = cty.iso3_id (+)
      and    ohr.country     = cty_inv.iso3_id (+)
      and    ohr.country     = cty_hub.iso3_id (+)
      and    ohr.client_id   = b_client_id
      and    ohr.order_id    = b_order_id
      and    ohr.status      in ('Shipped','Delivered')
      ;
    cursor c_plda_gim ( b_client_id    in varchar2
                      , b_order_id     in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( lpad( smt.line_id, 7, 0)                               as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( 'FISCAL'                       as "stockType"
                                                                                 )
                                                                     as "Outbound"
                                                                     )
                                                         , xmlforest ( xmlforest ( xmlcdata ( smt.sku_id)         as "code"
                                                                                 , xmlcdata ( sku.description)    as "description"
                                                                                 , xmlforest ( 'SI'               as "typeDeclaration"
                                                                                             , decode( sku.commodity_code, null, null
                                                                                                                               , rpad( sku.commodity_code, 10, '0') 
                                                                                                     )            as "commodityCode"
                                                                                             )
                                                                                             as "CommodityImport"
                                                                                 , 'PCE'                          as "stockLevel"
                                                                                 , 'PKG'                          as "declareLevel"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( nvl(smt.origin_id,'US')                                as "originCountry" -- Default US as per request of Customs
                                                         , to_char( (nvl( sku.each_weight, 0.001) * 0.9) * sum(smt.qty_shipped)
                                                                  , 'fm999999999999990D990'
                                                                  )                                               as "netMass"
                                                         , to_char( (nvl( sku.each_weight, 0.001) ) * sum(smt.qty_shipped)
                                                                  , 'fm999999999999990D990'
                                                                  )                                               as "grossMass"
                                                         , sum( smt.qty_shipped)                                  as "stockAmount"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( decode(( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * sum( smt.qty_shipped)
                                                                                                   ,0,0.01
                                                                                                   ,( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * sum( smt.qty_shipped))
                                                                                          , 'fm999999990D90'
                                                                                          )                       as "price"
                                                                                 )
                                                                     as "Price"            
                                                                     )
                                                        , xmlforest ( xmlforest ( 1                                           as "packages"
                                                                                , 'PK'                                        as "packageType"
                                                                                )
                                                                    as "Packaging"
                                                                    )
                                             )
                                ) data
      from   dcsdba.shipping_manifest smt
      ,      dcsdba.order_header      ohr
      ,      dcsdba.order_line        ole
      ,      dcsdba.sku               sku
      where  smt.client_id            = ohr.client_id
      and    smt.order_id             = ohr.order_id
      and    smt.client_id            = ole.client_id
      and    smt.order_id             = ole.order_id
      and    smt.line_id              = ole.line_id
      and    smt.client_id            = sku.client_id
      and    smt.sku_id               = sku.sku_id
      and    smt.shipped              = g_yes
      and    smt.client_id            = b_client_id
      and    smt.order_id             = b_order_id
      group  by smt.line_id
      ,      smt.sku_id
      ,      sku.description
      ,      sku.commodity_code
      ,      smt.origin_id
      ,      sku.each_weight
      ,      ole.line_value
      ,      ole.qty_ordered
      order  by smt.line_id
      ;

    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;

    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_integer        integer;
    l_content        varchar2(8192);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
    l_principal      varchar2(20);
    l_trans_type     number;
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
  begin
    open  c_smt ( b_site_id   => p_site_id_i
                , b_client_id => p_client_id_i
                , b_order_id  => p_reference_id_i
                );
    fetch c_smt
    into  l_integer;

    if c_smt%found
    then
      -- get company
      select decode( p_site_id_i, 'RCLTLB', 'FISC_' || substr( 'NLTLG01', -5)
                                , 'RCLEHV', 'FISC_' || substr( 'NLSBR01', -5)
                                          , 'FISC_' || substr( p_site_id_i, -5)
                   )
      into l_company
      from dual
      ;
      -- get businessunit
      l_businessunit := p_csl_bu_i; 

      -- open csl file
      open_csl_file ( p_msg_type_i     => g_pldafis_cbs
                    , p_company_i      => l_company
                    , p_bu_ppl_i       => l_businessunit
                    , p_reference_id_i => p_reference_id_i
                    , p_file_type_o    => l_file_type
                    , p_file_name_o    => l_file_name
                    );

      -- fetch hdr data
      open  c_plda_hdr ( b_company => l_company);
      fetch c_plda_hdr
      into  l_content;
      close c_plda_hdr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- define transaction code
      if p_trans_type_i is null
      then
        open  c_ole ( b_client_id => p_client_id_i
                    , b_order_id  => p_reference_id_i
                    );
        fetch c_ole
        into  l_trans_type;
        close c_ole;
      else
        l_trans_type := p_trans_type_i;
      end if;

      -- fetch gdn data
      open  c_plda_gdn ( b_businessunit => l_businessunit
                       , b_client_id    => p_client_id_i
                       , b_order_id     => p_reference_id_i
                       , b_trans_type   => l_trans_type
                       );
      fetch c_plda_gdn
      into  l_content;
      close c_plda_gdn;
      -- write gdn content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- fetch gim data
      for r_plda_gim in c_plda_gim ( b_client_id    => p_client_id_i
                                   , b_order_id     => p_reference_id_i
                                   )
      loop
        l_content := r_plda_gim.data;
        -- write gim content to file
        write_line ( p_file_type_i => l_file_type
                   , p_content_i   => l_content
                   );
      end loop;

      -- add remaining xml tags
      l_content := l_plda_tlr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );

      -- close and move file
      close_csl_file ( p_file_type_i => l_file_type
                     , p_file_name_i => l_file_name
                     );

    end if;
    close c_smt;

  exception
    when others
    then
      case
      when c_smt%isopen
      then
        close c_smt;
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_outbound_cbs;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 21-Jun-2016
-- Purpose : Create Stock List file for Customs Streamliner (Stock Reconciliation)
------------------------------------------------------------------------------------------------
procedure create_stock_list( p_site_id_i      in  varchar2       
                           , p_client_id_i    in  varchar2
                           )
is
	cursor 	c_blt_hdr( b_company      in varchar2
                         , b_businessunit in varchar2
			 ) 
	is
		select	replace( xmltype.getClobVal( xmlelement( "BalanceList" , xmlattributes ( g_xmlns    as "xmlns:xsi")
                                                               , xmlforest( to_char(sysdate, 'YYYY-MM-DD') as "date"
									  , to_char(sysdate, 'HH24:MI:SS') as "time"
									  , b_company                      as "company"
									  , b_businessunit                 as "businessUnit"
									  )
							       , xmlelement( "AdditionalBalances")
                                                               )
                                                   ) 
			       , '</AdditionalBalances></BalanceList>'
			       , null
                               ) data
		from   dual
	;
	--
	cursor c_blt_abe( b_site_id   	in varchar2
                        , b_client_id 	in varchar2
                        )
	is
		select	xmltype.getClobVal( xmlforest( xmlforest( i.sku_id                              	as "productCode"
							        , i.origin_id                           	as "countryOfOrigin"
								, decode( i.user_def_chk_4,
									 'Y', 'BONDED'
									, 'FREE'
									)					as "stockType"
								, decode( b_client_id
									,'CRLBB', decode( substr(i.tag_id, 1, 1)
											, 'X'
											, substr( i.tag_id, 5)
											, i.tag_id
											)
									, i.tag_id
									)					as "serialnumber"
								, i.receipt_id                          	as "additionalRangeField1"
								, sum( nvl( i.qty_on_hand, 0))          	as "stockamount"
								)						as "AdditionalBalance"
						     )
					  ) 									data
		from	dcsdba.inventory i
		inner
		join	dcsdba.location  l
		on	l.location_id	= i.location_id
		and	l.site_id	= i.site_id
		and	l.loc_type     	not in ('ShipDock')--,'Suspense')
		and     l.zone_1        not like '%DOCK%'
		where  	i.site_id      	= b_site_id
		and    	i.client_id    	= b_client_id
		and	(	i.container_id 	is null
			or	i.container_id 	not in (
						       select	c.container_id
						       from	dcsdba.order_container c
						       where	c.status 		= 'PClosed'
						       and	c.container_id 		= i.container_id
						       and	c.client_id		= i.client_id
						       and	c.order_id||c.client_id	= (	
											  select	o.order_id||o.client_id
											  from		dcsdba.order_header o
											  where		o.order_id 	= c.order_id
											  and		o.client_id 	= c.client_id
											  and		o.status 	= 'Ready to Load'
											  )
						       )
			)
		group  
		by 	i.sku_id
		,      	i.origin_id
		,      	i.user_def_chk_4
		,      	i.tag_id
		,      	i.receipt_id
		order  
		by 	i.sku_id
		,      	i.tag_id
	;   
	--
	r_blt_hdr	c_blt_hdr%rowtype;
	r_blt_abe	c_blt_abe%rowtype;
	--
	l_file_type	utl_file.file_type;
	l_file_name	varchar2(100);
	l_content	varchar2(8192);
	l_company	varchar2(20);
	l_businessunit	varchar2(20);
	l_principal	varchar2(20);
	l_blt_tlr	varchar2(8192) := '</AdditionalBalances></BalanceList>';        
begin
	-- get company
	l_company 	:= get_company( p_client_id_i => p_client_id_i);
	if	l_company is null
	then
		l_company	:= p_site_id_i;
	end if;

	-- get businessunit
	l_businessunit	:= get_businessunit( p_client_id_i => p_client_id_i); 
	if 	l_businessunit is null
	then
		l_businessunit	:= p_client_id_i;
	end if;

	-- open csl file
	open_csl_file( p_msg_type_i	=> g_ballist
                     , p_company_i	=> l_company
		     , p_bu_ppl_i	=> l_businessunit
		     , p_reference_id_i	=> to_char( sysdate, 'YYYYMMDD')
		     , p_file_type_o	=> l_file_type
		     , p_file_name_o	=> l_file_name
		     );

	-- fetch hdr data
	open	c_blt_hdr( b_company		=> l_company
                         , b_businessunit	=> l_businessunit
                         );
	fetch 	c_blt_hdr
	into  	l_content;
	close 	c_blt_hdr;

	-- write hdr content to file
	write_line( p_file_type_i	=> l_file_type
                  , p_content_i		=> l_content
                  );

	-- fetch abe data
	for	r_blt_abe in c_blt_abe( b_site_id	=> p_site_id_i
                                      , b_client_id	=> p_client_id_i
                                      )
	loop
		l_content	:= r_blt_abe.data;

		-- write gim content to file
		write_line( p_file_type_i	=> l_file_type
			  , p_content_i		=> l_content
			  );
	end loop;

	-- add remaining xml tags
	l_content 	:= l_blt_tlr;

	-- write hdr content to file
	write_line( p_file_type_i	=> l_file_type
		  , p_content_i		=> l_content
		  );

	-- close and move file
	close_csl_file( p_file_type_i	=> l_file_type
		      , p_file_name_i	=> l_file_name
		      );

exception
	when others
	then
		case
		when 	c_blt_hdr%isopen
		then
			close	c_blt_hdr;
		when 	c_blt_abe%isopen
		then
			close 	c_blt_abe;
		else
			null;
		end 	case;
	raise;
end create_stock_list;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 11-Nov-2016
-- Purpose : Process In/Outbound Release interface from Customs Streamliner in WMS2009
------------------------------------------------------------------------------------------------
  procedure process_csl_release ( p_message_type_i       in  varchar2
                                , p_date_i               in  varchar2
                                , p_time_i               in  varchar2
                                , p_company_i            in  varchar2
                                , p_businessunit_i       in  varchar2
                                , p_warehouse_i          in  varchar2
                                , p_dossier_type_i       in  varchar2
                                , p_dossier_id_i         in  varchar2
                                , p_dossier_status_i     in  varchar2
                                , p_linkid_i             in  varchar2
                                , p_error_o              out integer
                                , p_errortext_o          out varchar2
                                )
  is
    cursor c_ivy ( b_site_id     in varchar2
                 , b_client_id   in varchar2
                 , b_receipt_id  in varchar2
                 )
    is
      select distinct
             ivy.site_id
      ,      ivy.client_id
      ,      ivy.owner_id
      ,      ivy.sku_id
      ,      ivy.tag_id
      ,      ivy.condition_id
      ,      ivy.origin_id
      ,      ivy.lock_code
      ,      ivy.lock_status
      from   dcsdba.inventory ivy
      where  ivy.site_id      = b_site_id
      and    ivy.client_id    = b_client_id
      and    ivy.receipt_id   = b_receipt_id
      and    ivy.lock_code    = 'CUST HOLD'
      ;
    cursor c_ltt (b_mergeerror in varchar2)
    is
      select b_mergeerror
      ||     ' - '
      ||     text
      from   dcsdba.language_text
      where  language = 'EN_GB'
      and    label    = b_mergeerror
      ;

    r_ivy        c_ivy%rowtype;       
    r_ltt        c_ltt%rowtype;

    l_err        integer := 1; -- 1 = OK, 0 = Error
    l_err_code   varchar2(20);
    l_err_txt    varchar2(500);
    l_site_id    varchar2(10);
    l_client_id  varchar2(10);
    l_integer    integer := 0;
  begin
    case p_dossier_type_i
    when 'I'
    then
      -- get site_id
      select decode ( p_warehouse_i, 'RCLEHV', 'NLSBR01'
                                   , 'RCLTLB', 'NLTLG01'
                                             , p_warehouse_i
                    )
      into l_site_id
      from dual
      ;
      -- get client_id
      l_client_id := get_client_from_bu( p_businessunit_i => p_businessunit_i);
      --
      -- update PreAdvice, set Receipt_Closed = 'Y'
      update dcsdba.pre_advice_header par
      set    par.receipt_closed = g_yes
      where  par.site_id        = l_site_id
      and    par.client_id      = l_client_id
      and    par.pre_advice_id  = p_linkid_i
      and    (
             par.status         = 'Complete'
             or
             par.finish_dstamp  is not null
             )
      ;
      commit;
      -- update inventory, unlock from 'CUST HOLD'
      for r_ivy in c_ivy ( b_site_id     => l_site_id
                         , b_client_id   => l_client_id
                         , b_receipt_id  => p_linkid_i
                         )
      loop
        dcsdba.libsession.setsessionuserid (userid => 'SEEBURGER');
        dcsdba.libsession.setsessionworkstation (stationid => 'SEEBURGER');
        l_err := dcsdba.libmergeinvupdate.directinventoryupdate ( p_MergeError   => l_err_code
                                                                , p_ToUpdateCols => null
                                                                , p_MergeAction  => 'U'
                                                                , p_OwnerID      => r_ivy.owner_id
                                                                , p_TagID        => r_ivy.tag_id
                                                                , p_ClientID     => r_ivy.client_id
                                                                , p_SKUID        => r_ivy.sku_id
                                                                , p_BatchID      => null
                                                                , p_ConditionID  => r_ivy.condition_id
                                                                , p_OriginID     => r_ivy.origin_id
                                                                , p_LockStatus   => 'UnLocked'
                                                                , p_LockCode     => null
                                                                , p_QCStatus     => null
                                                                , p_ExpiryDStamp => null
                                                                , p_TimeZoneName => 'Europe/Amsterdam'
                                                                , p_SupplierID   => null
                                                                , p_SiteID       => l_site_id
                                                                , p_ReasonID     => 'CUSTRELEAS'
                                                                , p_Notes        => 'Inventory released by Streamsoft'
                                                                );
                                                                commit;
        if l_err = 0
        then
          if l_err_code is not null
          then
            open  c_ltt (b_mergeerror => l_err_code);
            fetch c_ltt
            into  l_err_txt;
            close c_ltt;
          end if;
        end if;
      end loop;
    --
    when 'O'
    then
      null;
    end case;

    p_error_o     := l_err;
    p_errortext_o := l_err_txt;

  exception
    when others
    then

      l_err         := 0;
      l_err_txt     := substr( sqlerrm, 1, 500);

      p_error_o     := l_err;
      p_errortext_o := l_err_txt;

  end process_csl_release;
------------------------------------------------------------------------------------------------
-- Author  : W. Zhou, 01-Sep-2021
-- Purpose : Create Outbound Reconditioning file for Customs Streamliner
------------------------------------------------------------------------------------------------
  procedure create_outbound_reconditioning ( p_site_id_i      in  varchar2       
                                     , p_client_id_i    in  varchar2
                                     , p_reference_id_i in  varchar2
                                     )
  is

  --list all kits in one order

  cursor c_files( b_site_id   in varchar2
                 , b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
  is
    SELECT ole.order_id
    	,ole.host_line_id
    	,odh.from_site_id AS site_id
    	,ole.client_id
    FROM dcsdba.order_line ole
    JOIN dcsdba.order_header odh ON ole.client_id = odh.client_id
    	AND ole.order_id = odh.order_id
    JOIN dcsdba.country cty_sto ON odh.country = cty_sto.iso3_id
    WHERE ole.client_id = b_client_id
    	AND ole.order_id = b_order_id
    	AND odh.from_site_id = b_site_id
    	AND ole.user_def_type_2 IN ('K')
  ;



    cursor c_mtk ( b_site_id   in varchar2
                 , b_client_id in varchar2
                 , b_order_id  in varchar2
                 )
    is
      select 1
      from   dcsdba.move_task mtk
      where  mtk.site_id   = b_site_id
      and    mtk.client_id = b_client_id
      and    mtk.task_id   = b_order_id
	  and    mtk.client_id = 'VARIAN'
      ;
    cursor c_plda_hdr ( b_company in varchar2) 
    is 
      select replace( xmltype.getClobVal ( xmlelement ( "PldaSswDeclaration" , xmlattributes ( g_xmlns as "xmlns:xsi")
                                                      , xmlforest ( 'PD'                               as "typeDeclaration"
                                                                  , '1.0'                              as "version"
                                                                  , to_char(sysdate, 'YYYY-MM-DD')     as "dateCreation"
                                                                  , to_char(sysdate, 'HH24:MI:SS')     as "timeCreation"
                                                                  )
                                                      , xmlelement ( "CustomsStreamliner"
                                                                   , xmlforest ( b_company                                  as "company"
                                                                               )
                                                                   )
                                                      , xmlelement ( "MessageBody"
                                                                   , xmlelement ( "SADDossier"
                                                                                )
                                                                   )
                                                      )
                                         ) 
                    , '</SADDossier></MessageBody></PldaSswDeclaration>'
                    , null
                    ) data
      from   dual
      ;  

    cursor c_plda_gdn ( b_businessunit in varchar2
                      , b_client_id    in varchar2
                      , b_order_id     in varchar2
					  , b_host_line_id in varchar2
					  , b_type in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsDeclaration"
                                             , xmlforest ( 'I'                                                        as "type"
                                                         , 'NL_RECON_'||ole.order_id||'_'||ole.line_id                                               as "linkId"
                                                         )
                                             , xmlforest ( xmlforest ( to_char( sysdate, 'YYYY-MM-DD')  as "arrivedDate"
                                                                       , to_char( sysdate, 'HH24:MI:SS')    as "arrivedTime"
                                                                       , 'T'					 as "isRepack"
                                                                                 , xmlforest (  'NL_RECON_'||ole.order_id||'_'||ole.line_id         as "bomUniqueReference"
                                                                                             , (select XMLAGG( 
                                                                                                    XMLELEMENT("Item",
                                                                                                          xmlforest (  
       																										  ole1.sku_id       as "itemNumber"
                                                                                                                     , mtk1.qty_to_move	as "quantity"
                                                                                                                     , 'VUS_SBR01-C'    as "customLicensing"
                                                                                                                     , 'BONDED' as "stockType"
                                                                                                                     , mtk1.tag_id        as "serialnumber"
																													 , inv1.origin_id 	as "originCountry"
                                                                                                                     , inv1.receipt_id  as "additionalRangeField1"
                                                                                                                     )
                                                                                                               )
                                                                                                            )
				-- select ole.line_id, ole.sku_id , ole.user_def_type_2 , ole.host_line_id , ole.qty_ordered , ole.qty_tasked, ole.line_value, mtk.key, mtk.tag_id ,mtk.qty_to_move, sku.commodity_code , nvl (mtk.qty_to_move, ole.qty_ordered)
                                                                                              from dcsdba.order_line ole1
																							  LEFT OUTER JOIN dcsdba.move_task mtk1 ON ole1.client_id = mtk1.client_id
																								AND ole1.sku_id = mtk1.sku_id
																								AND ole1.line_id = mtk1.line_id
																								AND ole1.order_id = mtk1.task_id
																								JOIN dcsdba.sku sku1 ON ole1.sku_id = sku1.sku_id
																									AND ole1.client_id = sku1.client_id
																								LEFT OUTER JOIN (
																									SELECT DISTINCT client_id
																										,sku_id
																										,tag_id
																										,receipt_id
																										,condition_id
																										,user_def_chk_4
																										,origin_id
																									FROM dcsdba.inventory
																									) inv1 ON Ole1.sku_id = inv1.sku_id
																									AND mtk1.tag_id = inv1.tag_id
																									AND ole1.client_id = inv1.client_id
																											 where    ole1.host_line_id like b_host_line_id||'%'
                                                                                                             and ole1.order_id = b_order_id
																											 and ole1.user_def_type_2 = 'C'
                                                                                             ) "Items"
                                                                                             )
                                                                                             as "Bom"
                                                                                 , xmlforest ( 'VARIAN'                                        as "erpId" 
                                                                                             )
                                                                                             as "ConsigneeImport"
                                                                                 , 'NL'                                                  as "destinationCountry"
                                                                                 , decode( substr( odh.order_type, 1, 1), 'D', decode( cty_sto.ce_eu_type, 'EU', 'T'
                                                                                                                                                               , 'F'
                                                                                                                                     )
                                                                                                                             , 'F'
                                                                                         )                                                           as "transitFlag"
                                                                                 )
                                                                     as "Inbound"
                                                                     )



                                             , xmlforest ( to_char( sysdate /*mtk.picked_dstamp*/, 'YYYY-MM-DD')                     as "dossierDate"
                                                         , xmlcdata( odh.purchase_order)                                 as "commercialReference"
                                                         , xmlforest ( 'RECON-INVOICE'                    as "invoiceNumber"  --xmlcdata ( odh.inv_reference )
                                                                     , to_char( sysdate, 'YYYY-MM-DD')            as "invoiceDate"
                                                                     , to_char( ( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * ole.qty_ordered
                                                                                          , 'fm999999990D90'
                                                                                          )                                          as "invoiceAmount"
                                                                     , xmlforest ( nvl( upper( odh.inv_currency), 'EUR') as "currency"
                                                                                 )
                                                                                 as "ExchangeRate"
                                                                     )
                                                                     as "Invoice"
														 , to_char(sysdate, 'YYYY-MM-DD')     as "expectedDate"
                                                         , to_char(sysdate, 'HH24:MI:SS')     as "expectedTime" 
														 , xmlforest ( 'VUS_SBR01-C'    as "customLicensing"
                                                                     )
																	as CustomProcedure

														, '' as totalGrossMass
														, xmlforest ( 'NL'                        as "dispatchCountry",
                                                                      xmlforest ( 'CIP'                            as "deliveryTerms"
                                                                                 , 'VARIAN'         as "deliveryTermsPlace"
                                                                                 )
                                                                                 as "DeliveryTerms",
                                                                     'Auto'         as "borderIdentity" 
                                                                     )
                                                                     as "TransportMeans"
														, 'VARIAN'               as "businessUnit"														 


														 )			 

														) 
														 )  data



--select ole.order_id, ole.line_id, ole.qty_tasked, ole.sku_id, ole.user_def_type_2, ole.host_line_id, odh.client_id, odh.country, cty_sto.iso2_id, odh.order_type
      from   dcsdba.order_line ole
      join dcsdba.order_header odh
      on
      ole.client_id = odh.client_id
        and ole.order_id = odh.order_id
        join dcsdba.country   cty_sto   
        on odh.country     = cty_sto.iso3_id

where      
                ole.client_id   = b_client_id --'VARIAN'
      and    ole.order_id    = b_order_id --'0083059071'
      and    ole.host_line_id like  b_host_line_id||'%' -- '015100%'  
 --     and    odh.from_site_id = b_from_site_id --'NLSBR01'
	  and   ole.user_def_type_2 = 'K' --b_type

      ;





    cursor c_plda_gim ( b_client_id in varchar2
                      , b_order_id  in varchar2
					  , b_host_line_id in varchar2
                      )
    is
      select xmltype.getClobVal ( xmlelement ( "GoodsItem"
                                             , xmlforest ( ole.line_id        as "linkId"
                                                         )
                                                         , xmlforest ( xmlforest ( nvl(ole.condition_id,'Bonded')          as "stockType"
                                                                                 )
                                                                     as "Inbound"            
                                                                     )
														, xmlforest ( xmlforest ('VUS_SBR01-C'               as "customLicensing"
                                                                     )
                                                                     as "CustomProcedure"
																	 )
                                                         , xmlforest ( xmlforest ( xmlcdata ( ole.sku_id)    as "code"
                                                                                 , '1'    as "attributeProductType"
                                                                                 )
                                                                     as "Product"
                                                                     )
                                             , xmlforest ( nvl(ole.origin_id, 'US')                                 as "originCountry"
                                                         , ole.qty_ordered                                          as "pieces"
                                                         , to_char( (nvl( ole.catch_weight * 0.9 , 0.001) ) * ole.qty_ordered
                                                                  , 'fm999999999999990D990'
                                                                  )                                          as "netMass"
                                                         , to_char( (nvl( ole.catch_weight * 0.9 , 0.001) ) * ole.qty_ordered
                                                                  , 'fm999999999999990D990'
                                                                  )                                          as "grossMass"
                                                         , ole.qty_ordered                                   as "stockAmount"
                                                         )
                                                         , xmlforest ( xmlforest ( to_char( ( nvl( ole.line_value, 0) / nvl( ole.qty_ordered, 1) ) * ole.qty_ordered
                                                                                          , 'fm999999990D90'
                                                                                          )                  as "price"
                                                                                 )
                                                                     as "Price"            
                                                                     )
                                             , xmlforest ( 'NL_RECON_'||ole.order_id||'_'||ole.line_id            as "additionalRangeField1"
                                                         , 'NL_RECON_'||ole.order_id||'_'||ole.line_id            as "serialnumber"
                                                         )
                                             )
                                ) data
--  select ole.order_id, ole.host_line_id, ole.site_id, ole.client_id, ole.origin_id, ole.catch_weight, ole.qty_ordered, ole.origin_id
      from   dcsdba.order_line ole
      join dcsdba.order_header odh
      on
      ole.client_id = odh.client_id
        and ole.order_id = odh.order_id
        join dcsdba.country   cty_sto   
        on odh.country     = cty_sto.iso3_id


where      
                ole.client_id   = b_client_id
      and    ole.order_id    = b_order_id
	  and    ole.host_line_id = b_host_line_id
      and    ole.user_def_type_2 in ('K')
      ;


    r_plda_hdr       c_plda_hdr%rowtype;
    r_plda_gdn       c_plda_gdn%rowtype;
    r_plda_gim       c_plda_gim%rowtype;





    l_file_type      utl_file.file_type;
    l_file_name      varchar2(100);
    l_integer        integer;
    l_content        varchar2(32767);
    l_company        varchar2(20);
    l_businessunit   varchar2(20);
    l_principal      varchar2(20);
    l_plda_tlr       varchar2(8192) := '</SADDossier></MessageBody></PldaSswDeclaration>';        
    l_client_group_  varchar2(10) := 'N';


 begin
 --loop one order into multiple files in case there are multiple kits.
    for r_files in c_files ( b_site_id   => p_site_id_i
                , b_client_id => p_client_id_i
                , b_order_id  => p_reference_id_i
                ) loop



    open  c_mtk ( b_site_id   => r_files.site_id
                , b_client_id => r_files.client_id
                , b_order_id  => r_files.order_id
                );
    fetch c_mtk
    into  l_integer;

    if c_mtk%found
    then
      -- get company
      l_company := get_company ( p_client_id_i => p_client_id_i
                               );
      if l_company is null
      then
        l_company := p_site_id_i;
      end if;

      -- get businessunit
      l_businessunit := get_businessunit ( p_client_id_i => p_client_id_i
                                         ); 
      if l_businessunit is null
      then
        l_businessunit := p_client_id_i;
      end if;

      -- open csl file
      open_csl_file ( p_msg_type_i     => g_pldaarc
                    , p_company_i      => l_company
                    , p_bu_ppl_i       => l_businessunit
                    , p_reference_id_i => p_reference_id_i
                    , p_file_type_o    => l_file_type
                    , p_file_name_o    => l_file_name
                    );

      -- fetch hdr data
      open  c_plda_hdr ( b_company => l_company);
      fetch c_plda_hdr
      into  l_content;
      close c_plda_hdr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );
      -- fetch gdn data
      open  c_plda_gdn ( b_businessunit => l_businessunit
                       , b_client_id    => r_files.client_id
                       , b_order_id     => r_files.order_id					   
					   , b_host_line_id => r_files.host_line_id
					   , b_type         => 'C'
                       );
      fetch c_plda_gdn
      into  l_content;
      close c_plda_gdn;
      -- write gdn content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );
     -- fetch gim data
      for r_plda_gim in c_plda_gim ( b_client_id    => r_files.client_id
                       , b_order_id     => r_files.order_id					   
					   , b_host_line_id => r_files.host_line_id
		--			   ,  b_type          => 'C'
                                   )
      loop
        l_content := r_plda_gim.data;
        -- write gim content to file
        write_line ( p_file_type_i => l_file_type
                   , p_content_i   => l_content
                   );
      end loop;

      -- add remaining xml tags
      l_content := l_plda_tlr;
      -- write hdr content to file
      write_line ( p_file_type_i => l_file_type
                 , p_content_i   => l_content
                 );
      -- close and move file
      close_csl_file ( p_file_type_i => l_file_type
                     , p_file_name_i => l_file_name
                     );

    end if;
    close c_mtk;
end loop;
  exception
    when others
    then
      case
      when c_mtk%isopen
      then
        close c_mtk;
      when c_plda_hdr%isopen
      then
        close c_plda_hdr;
      when c_plda_gdn%isopen
      then
        close c_plda_gdn;
      when c_plda_gim%isopen
      then
        close c_plda_gim;
      else
        null;
      end case;

      raise;

  end create_outbound_reconditioning;




begin
  -- Initialization
  null;
end cnl_streamsoft_pck;