CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_TASKRELDAE_PCK" is
    
	Procedure release_lists_p( p_site_i 		in varchar2
                                 , p_client_group_i	in varchar2
				 );
	--
end	cnl_wms_taskreldae_pck;