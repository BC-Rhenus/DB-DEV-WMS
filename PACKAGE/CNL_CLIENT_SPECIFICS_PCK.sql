CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_CLIENT_SPECIFICS_PCK" is
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
	procedure return_relocate_p( p_client_id_i 	in dcsdba.client.client_id%type
				   , p_shelf_zone_i	in dcsdba.location.zone_1%type
				   , p_shelf_sub1_i	in dcsdba.location.subzone_1%type
				   , p_shelf_oversub1_i in dcsdba.location.subzone_1%type
				   , p_shelf_retzone_i	in dcsdba.location.zone_1%type
				   , p_batch_mix_i	varchar2
				   );
	--
	procedure set_order_back_to_shipped_p( p_client_id_i 	dcsdba.client.client_id%type
					     , p_site_id_i	dcsdba.site.site_id%type
					     );
	--
	function get_crl_sscc_box_nr( p_ordered_qty_i  number
				    , p_full_box_qty_i number
				    ) 
	return varchar2;
	--
	procedure replace_receive_all_p( p_client_id_i		varchar2
				       , p_site_id_i		varchar2
				       , p_receipt_loc_i	varchar2
				       );
	procedure create_inv_backup_p(p_client_id_i varchar2);
	--
	procedure search_non_pf_zero_demand_p(	p_from_zone_i		dcsdba.location.zone_1		%type
					     , 	p_from_subzone_1_i	dcsdba.location.subzone_1	%type 	default null
					     ,	p_to_zone_i		dcsdba.location.zone_1		%type	default null
					     ,	p_pallet_type_i		dcsdba.pallet_config.config_id	%type 	default null
					     ,	p_location_level_i	dcsdba.location.levels		%type 	default null
					     ,	p_zero_alloc_i		dcsdba.address.user_def_chk_2	%type
					     ,	p_zero_demand_i		dcsdba.address.user_def_chk_1	%type
					     ,	p_exclude_rule_id_i	dcsdba.allocation_rule.rule_id	%type 	default null
					     ,	p_max_nr_relocates_i	dcsdba.address.user_def_num_3	%type 	default null
					     ,	p_client_id_i		dcsdba.client.client_id		%type
					     ,  p_loc_height_i		dcsdba.address.user_def_num_4	%type 	default null
					     );
end cnl_client_specifics_pck;