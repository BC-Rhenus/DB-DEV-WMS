CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_WMS_SKURELDAE_PCK" is
  procedure jda_ProcessSkuRelocation  ( p_SiteID in dcsdba.SKU_Relocation.Site_ID%type,
                                        p_ToZone in dcsdba.SKU_Relocation.To_Zone%type
                                      );
  procedure cnl_ProcessSkuRelocation_p( p_siteid in dcsdba.SKU_Relocation.Site_ID%type);
end cnl_wms_Skureldae_pck;