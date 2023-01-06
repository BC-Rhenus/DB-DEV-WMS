CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_SKURELDAE_PCK" is
  
  function jda_GetNoOfTags  ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type,
                              p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type,
                              p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type,
                              p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type,
                              p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type,
                              p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type,
                              p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type,
                              p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type,
                              p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                            )
      return integer;
  --

  function jda_GetQuantityOfStock ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type,
                                    p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type,
                                    p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type,
                                    p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type,
                                    p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type,
                                    p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type,
                                    p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type,
                                    p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type,
                                    p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                                  )
      return number;
  --

  function jda_CheckForReleasedTasks  ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type,
                                        p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type,
                                        p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type,
                                        p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type,
                                        p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type,
                                        p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type,
                                        p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type,
                                        p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type,
                                        p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                                      )
      return integer;
  --

  function jda_CheckForReleasedQuantity ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type,
                                          p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type,
                                          p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type,
                                          p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type,
                                          p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type,
                                          p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type,
                                          p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type,
                                          p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type,
                                          p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                                        )
      return number;
  --

  function jda_GenerateTagRelocation  ( p_SiteID            in dcsdba.SKU_Relocation.Site_ID%type,
                                        p_ClientID          in dcsdba.SKU_Relocation.Client_ID%type,
                                        p_SKUID             in dcsdba.SKU_Relocation.SKU_ID%type,
                                        p_OwnerID           in dcsdba.SKU_Relocation.Owner_ID%type,
                                        p_ConditionID       in dcsdba.SKU_Relocation.Condition_ID%type,
                                        p_OriginID          in dcsdba.SKU_Relocation.Origin_ID%type,
                                        p_ToZone            in dcsdba.SKU_Relocation.To_Zone%type,
                                        p_ToSubZone1        in dcsdba.SKU_Relocation.To_SubZone_1%type,
                                        p_ToSubZone2        in dcsdba.SKU_Relocation.To_SubZone_2%type,
                                        p_FromZone          in dcsdba.SKU_Relocation.From_Zone%type,
                                        p_FromSubZone1      in dcsdba.SKU_Relocation.From_SubZone_1%type,
                                        p_FromSubZone2      in dcsdba.SKU_Relocation.From_SubZone_2%type,
                                        p_Algorithm         in dcsdba.SKU_Relocation.Algorithm%type,
                                        p_TaskID            in dcsdba.Move_Task.Task_ID%type,
                                        p_DisTagSwap        in dcsdba.Move_Task.Disallow_Tag_Swap%type,
                                        p_RequiredTags      in integer,
                                        p_AllowedInvStatus  in dcsdba.SKU_Relocation.Allowed_Inv_Status%type,
                                        p_LockedCode        in dcsdba.SKU_Relocation.Lock_Code%type default null
                                      )
      return boolean;
  --

  function jda_GenerateStockRelocation  ( p_SiteID            in dcsdba.SKU_Relocation.Site_ID%type,
                                          p_ClientID          in dcsdba.SKU_Relocation.Client_ID%type,
                                          p_SKUID             in dcsdba.SKU_Relocation.SKU_ID%type,
                                          p_OwnerID           in dcsdba.SKU_Relocation.Owner_ID%type,
                                          p_ConditionID       in dcsdba.SKU_Relocation.Condition_ID%type,
                                          p_OriginID          in dcsdba.SKU_Relocation.Origin_ID%type,
                                          p_ToZone            in dcsdba.SKU_Relocation.To_Zone%type,
                                          p_ToSubZone1        in dcsdba.SKU_Relocation.To_SubZone_1%type,
                                          p_ToSubZone2        in dcsdba.SKU_Relocation.To_SubZone_2%type,
                                          p_FromZone          in dcsdba.SKU_Relocation.From_Zone%type,
                                          p_FromSubZone1      in dcsdba.SKU_Relocation.From_SubZone_1%type,
                                          p_FromSubZone2      in dcsdba.SKU_Relocation.From_SubZone_2%type,
                                          p_Algorithm         in dcsdba.SKU_Relocation.Algorithm%type,
                                          p_TaskID            in dcsdba.Move_Task.Task_ID%type,
                                          p_DisTagSwap        in dcsdba.Move_Task.Disallow_Tag_Swap%type,
                                          p_RequiredStock     in number,
                                          p_AllowedInvStatus  in dcsdba.SKU_Relocation.Allowed_Inv_Status%type,
                                          p_LockedCode        in dcsdba.SKU_Relocation.Lock_Code%type default null
                                        )
      return boolean;
  --

  function jda_CheckRelocationSuitable  ( p_SiteID        in dcsdba.Inventory.Site_ID%type,
                                          p_ClientID      in dcsdba.Inventory.Client_ID%type,
                                          p_SKUID         in dcsdba.Inventory.SKU_ID%type,
                                          p_OwnerID       in dcsdba.Inventory.Owner_ID%type,
                                          p_ConditionID   in dcsdba.Inventory.Condition_ID%type,
                                          p_OriginID      in dcsdba.Inventory.Origin_ID%type,
                                          p_LocationID    in dcsdba.Inventory.Location_ID%type,
                                          p_PalletID      in dcsdba.Inventory.Pallet_ID%type,
                                          p_TagID         in dcsdba.Inventory.Tag_ID%type,
                                          p_CERotationID  in dcsdba.Inventory.CE_Rotation_ID%type,
                                          p_CEUnderBond   in dcsdba.Inventory.CE_Under_Bond%type,
                                          p_CEAvailStatus in dcsdba.Inventory.CE_Avail_Status%type,
                                          p_Key           in dcsdba.Inventory.Key%type
                                        )
      return boolean;
  --
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   ProcessSkuRelocation                                  */
/*                                                                            */
/*     DESCRIPTION:     Process SKU relocation.                               */
/*                                                                            */
/*     RETURN VALUES:   void                                                  */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 600  22/09/00 MJT  DCS    NEW5117  Automatic SKU Relocation Daemon    */
/*  dcs 600  07/11/00 JAH  DCS    SYS5233  Be able to relocate >1 tag per call*/
/*  dcs 700  15/06/01 RAM  DCS    NEW5488  Client ID                          */
/*  dcs 800  28/03/03 RMC  DCS    PDR8938  Also find locs that are not empty  */
/*  dcs 900  31/01/05 JRL  DCS    NEW7657  Sku relocation for non-tagged stock*/
/*  2006.1.0 07/04/06 EW   DCS    BUG2453  SKU relocation tasks go to error   */
/*  2010.2-b 19/03/10 MJT  DCS    DSP3271  Relocation Tag Swapping            */
/*  2010.2.0 30/07/10 JH   DCS    DSP3591  Sku relocation tag problem         */
/*  2011.2.1 09/11/11 JH   DCS    DSP4589  SKU reloc daemon priority order    */
/*  2011.2.2 05/12/11 JH   DCS    DSP4776  Allow dynamic task id              */
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/*  2013.2-b 28/10/13 SD   DCS    DSP6606  SKU Relocation enhancements        */
/******************************************************************************/
  procedure jda_ProcessSkuRelocation  ( p_SiteID in dcsdba.SKU_Relocation.Site_ID%type,
                                        p_ToZone in dcsdba.SKU_Relocation.To_Zone%type
                                      )
    is
      cursor SkuRelocSearch ( p_SiteID in dcsdba.SKU_Relocation.Site_ID%type,
                              p_ToZone in dcsdba.SKU_Relocation.To_Zone%type
                            )
        is
          SELECT    Client_ID
          ,         Site_ID
          ,         Sku_id
          ,         Condition_ID
          ,         Origin_ID
          ,         Owner_ID
          ,         NVL(No_tags, 0) No_Tags
          ,         To_Zone
          ,         To_SubZone_1
          ,         To_SubZone_2
          ,         From_Zone
          ,         From_SubZone_1
          ,         From_SubZone_2
          ,         Algorithm
          ,         NVL(Qty_Required, 0) Qty_Required
          ,         NVL(Trigger_Qty, 0) Trigger_Qty
          ,         nvl(Disallow_Tag_Swap, 'N') Disallow_Tag_Swap
          ,         nvl(Task_ID, dcsdba.LibMoveTaskID.GetRelocateTaskID) Task_ID
          ,         Allowed_Inv_Status
          ,         Lock_Code
          FROM      dcsdba.SKU_Relocation
          WHERE     (To_Zone = p_ToZone OR p_ToZone IS NULL)
          AND       (Site_ID = p_SiteID OR p_SiteID IS NULL)
          AND       (task_id != 'SKURELOCATE' or task_id is null)                                 -- Condition added for Rhenus purposes. 
          ORDER BY  Site_ID
          ,         SKU_ID
          ,         Priority
          ,         To_Zone;
      --

      l_RequiredTags integer := 0;
      l_TotalTags integer := 0;
      l_RequiredStock number := 0.0;
      l_TotalStock number := 0.0;
      l_Continue boolean := true;
  begin
    /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - start with p_SiteID ' || p_SiteID || ', p_ToZone ' || p_ToZone, 5);*/
    for SkuRelocRow in SkuRelocSearch(p_SiteID, p_ToZone) loop
      l_Continue := true;
      dcsdba.LibLocationZone.TransformWildCardsProc(SkuRelocRow.From_Zone);
      /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - read sku_relocation record with Client_Id ' || SkuRelocRow.Client_ID || ', Site_ID ' || SkuRelocRow.Site_ID || ', Sku_Id ' || SkuRelocRow.Sku_Id || ', Condition_ID ' || SkuRelocRow.Condition_ID || ', Origin_ID ' || SkuRelocRow.Origin_ID || ', Owner_ID ' || SkuRelocRow.Owner_ID || ', No_Tags ' || SkuRelocRow.No_Tags || ', To_Zone ' || SkuRelocRow.To_Zone || ', To_SubZone_1 ' || SkuRelocRow.To_SubZone_1 || ', To_SubZone_2 ' || SkuRelocRow.To_SubZone_2 || ', From_Zone ' || SkuRelocRow.From_Zone || ', From_SubZone_1 ' || SkuRelocRow.From_SubZone_1 || ', From_SubZone_2 ' || SkuRelocRow.From_SubZone_2 || ', Algorithm ' || SkuRelocRow.Algorithm || ', Qty_Required ' || SkuRelocRow.Qty_Required || ', Trigger_Qty ' || SkuRelocRow.Trigger_Qty || ', Disallow_Tag_Swap ' || SkuRelocRow.Disallow_Tag_Swap || ', Task_ID ' || SkuRelocRow.Task_ID || ',Allowed_Inv_Status '||SkuRelocRow.Allowed_Inv_Status||',Lock_Code '||SkuRelocRow.Lock_Code, 5);*/
      if (SkuRelocRow.No_Tags > 0) then
      /*If no of tags specified, find out how many tags are already in to locations*/
        l_TotalTags := jda_GetNoOfTags ( SkuRelocRow.Site_ID
                                        , SkuRelocRow.Client_ID
                                        , SkuRelocRow.SKU_ID
                                        , SkuRelocRow.Owner_ID
                                        , SkuRelocRow.Condition_ID
                                        , SkuRelocRow.Origin_ID
                                        , SkuRelocRow.To_Zone
                                        , SkuRelocRow.To_SubZone_1
                                        , SkuRelocRow.To_SubZone_2
                                        );
        if (l_TotalTags < 0) then
        /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_Id ' || SkuRelocRow.Client_ID || ', Sku_Id ' || SkuRelocRow.Sku_Id || ' - failed to get number of tags', 4);*/
        l_Continue := false;
        end if;
      else
        /*If a quantity is specified, find out how much stock is already in to locations*/
        l_TotalStock := jda_GetQuantityOfStock  ( SkuRelocRow.Site_ID
                                                , SkuRelocRow.Client_ID
                                                , SkuRelocRow.SKU_ID
                                                , SkuRelocRow.Owner_ID
                                                , SkuRelocRow.Condition_ID
                                                , SkuRelocRow.Origin_ID
                                                , SkuRelocRow.To_Zone
                                                , SkuRelocRow.To_SubZone_1
                                                , SkuRelocRow.To_SubZone_2
                                                );
        if (l_TotalStock < 0.0) then
          /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_Id ' || SkuRelocRow.Client_ID || ', Sku_Id ' || SkuRelocRow.Sku_Id || ' - failed to get quantity of stock', 4);*/
          l_Continue := false;
        end if;
      end if;

      if (l_Continue = true) then
        if (SkuRelocRow.No_Tags > 0) then
          /*Check how many tags are required for the relocation*/
          l_RequiredTags := SkuRelocRow.No_Tags - l_TotalTags;
          /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - No_Tags ' || SkuRelocRow.No_Tags || ', l_TotalTags ' || l_TotalTags || ', l_RequiredTags ' || l_RequiredTags, 4);*/
          if (l_RequiredTags <= 0) then
            /*Enough stock, does not need replenishing*/
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_ID ' || SkuRelocRow.Client_ID || ', SKU_ID ' || SkuRelocRow.SKU_ID || ' does not need replenishing', 4);*/
            l_Continue := false;
          end if;
        else
          /*Check how much stock is required for the relocation*/
          l_RequiredStock := SkuRelocRow.Qty_Required - l_TotalStock;
          /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Qty_Required ' || SkuRelocRow.Qty_Required || ', l_TotalStock ' || l_TotalStock || ', l_RequiredStock ' || l_RequiredStock || ', Trigger_Qty ' || SkuRelocRow.Trigger_Qty, 4);*/
          if (l_RequiredStock <= 0.0) then
            /*Enough stock, does not need replenishing*/
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_ID ' || SkuRelocRow.Client_ID || ', SKU_ID ' || SkuRelocRow.SKU_ID || ' does not need replenishing', 4);*/
            l_Continue := false;
          elsif (l_TotalStock > SkuRelocRow.Trigger_Qty) then
            /*Required qty does not exceed trigger*/
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_ID ' || SkuRelocRow.Client_ID || ', SKU_ID ' || SkuRelocRow.SKU_ID || ' does not meet trigger level', 4);*/
            l_Continue := false;
          end if;
        end if;
      end if;

      if (l_Continue = true) then
        if (SkuRelocRow.No_Tags > 0) then
          /*Check for existing relocation tasks for tagged inventory*/
          l_RequiredTags := l_RequiredTags - jda_CheckForReleasedTasks  ( SkuRelocRow.Site_ID
                                                                        , SkuRelocRow.Client_ID
                                                                        , SkuRelocRow.SKU_ID
                                                                        , SkuRelocRow.Owner_ID
                                                                        , SkuRelocRow.Condition_ID
                                                                        , SkuRelocRow.Origin_ID
                                                                        , SkuRelocRow.To_Zone
                                                                        , SkuRelocRow.To_SubZone_1
                                                                        , SkuRelocRow.To_SubZone_2
                                                                        );
          /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - No_Tags ' || SkuRelocRow.No_Tags || ', l_TotalTags ' || l_TotalTags || ', l_RequiredTags ' || l_RequiredTags, 4);*/
          if (l_RequiredTags <= 0) then
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_ID ' || SkuRelocRow.Client_ID || ', SKU_ID ' || SkuRelocRow.SKU_ID || ' - released tasks exist for relocation', 4);*/
            l_Continue := false;
          end if;
        else
          /* Check for existing reloc tasks for non-tag stock */
          l_RequiredStock := l_RequiredStock - jda_CheckForReleasedQuantity ( SkuRelocRow.Site_ID
                                                                            , SkuRelocRow.Client_ID
                                                                            , SkuRelocRow.SKU_ID
                                                                            , SkuRelocRow.Owner_ID
                                                                            , SkuRelocRow.Condition_ID
                                                                            , SkuRelocRow.Origin_ID
                                                                            , SkuRelocRow.To_Zone
                                                                            , SkuRelocRow.To_SubZone_1
                                                                            , SkuRelocRow.To_SubZone_2
                                                                            );
          /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Qty_Required ' || SkuRelocRow.Qty_Required || ', l_TotalStock ' || l_TotalStock || ', l_RequiredStock ' || l_RequiredStock, 4);*/
          if (l_RequiredStock <= 0.0) then
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - Client_ID ' || SkuRelocRow.Client_ID || ', SKU_ID ' || SkuRelocRow.SKU_ID || ' - released task exists for relocation', 4);*/
            l_Continue := false;
          end if;
        end if;
      end if;

      if (l_Continue = true) then
        if (SkuRelocRow.No_Tags > 0) then
          /*Generate required relocation task for tagged inventory*/
          if (jda_GenerateTagRelocation ( SkuRelocRow.Site_ID
                                        , SkuRelocRow.Client_ID
                                        , SkuRelocRow.SKU_ID
                                        , SkuRelocRow.Owner_ID
                                        , SkuRelocRow.Condition_ID
                                        , SkuRelocRow.Origin_ID
                                        , SkuRelocRow.To_Zone
                                        , SkuRelocRow.To_SubZone_1
                                        , SkuRelocRow.To_SubZone_2
                                        , SkuRelocRow.From_Zone
                                        , SkuRelocRow.From_SubZone_1
                                        , SkuRelocRow.From_SubZone_2
                                        , SkuRelocRow.Algorithm
                                        , SkuRelocRow.Task_ID
                                        , SkuRelocRow.Disallow_Tag_Swap
                                        , l_RequiredTags
                                        , SkuRelocRow.Allowed_Inv_Status
                                        , SkuRelocRow.Lock_Code) = false
                                        ) then
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - failed to generate tagged relocation task', 4);*/
            l_Continue := false;
          end if;
        else
          /*Generate a relocation task for the required non-tagged stock*/
          if (jda_GenerateStockRelocation ( SkuRelocRow.Site_ID
                                          , SkuRelocRow.Client_ID
                                          , SkuRelocRow.SKU_ID
                                          , SkuRelocRow.Owner_ID
                                          , SkuRelocRow.Condition_ID
                                          , SkuRelocRow.Origin_ID
                                          , SkuRelocRow.To_Zone
                                          , SkuRelocRow.To_SubZone_1
                                          , SkuRelocRow.To_SubZone_2
                                          , SkuRelocRow.From_Zone
                                          , SkuRelocRow.From_SubZone_1
                                          , SkuRelocRow.From_SubZone_2
                                          , SkuRelocRow.Algorithm
                                          , SkuRelocRow.Task_ID
                                          , SkuRelocRow.Disallow_Tag_Swap
                                          , l_RequiredStock
                                          , SkuRelocRow.Allowed_Inv_Status
                                          , SkuRelocRow.Lock_Code) = false
                                          ) then
            /*LibMQSDebug.print('LibSkureldae.ProcessSkuRelocation - failed to generate stock relocation task', 4);*/
            l_Continue := false;
          end if;
        end if;
      end if;

      if (l_Continue = true) then
        commit;
      end if;

    end loop;

  end jda_ProcessSkuRelocation;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   GetNoOfTags                                           */
/*                                                                            */
/*     DESCRIPTION:     Search for the number of tags in the to_zone,	      */
/*			to_subzone_1, to_subzone_2 and return the number.     */
/*                                                                            */
/*     RETURN VALUES:   Number of tags				              */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 600  22/09/00 MJT  DCS    NEW5117  Automatic SKU Relocation Daemon    */
/*  dcs 700  15/06/01 RAM  DCS    NEW5488  Client ID                          */
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/******************************************************************************/
  function jda_GetNoOfTags  ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type
                            , p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type
                            , p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type
                            , p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type
                            , p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type
                            , p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type
                            , p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type
                            , p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type
                            , p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                            )
    return integer is
      cursor InventorySearch
      is
        SELECT    COUNT(*)
        FROM      dcsdba.Inventory I
        ,         dcsdba.Location L
        WHERE     I.Site_ID = p_SiteID
        AND       I.Site_ID = l.Site_ID
        AND       I.Location_ID = L.Location_ID
        AND       I.Client_ID = p_ClientID
        AND       I.SKU_ID = p_SKUID
        AND       I.Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND I.Condition_ID IS NULL) OR (p_ConditionID = I.Condition_ID))
        AND       ((p_OriginID IS NULL AND I.Origin_ID IS NULL) OR (p_OriginID = I.Origin_ID))
        AND       I.Zone_1 = p_ToZone
        AND       (p_ToSubZone1 IS NULL OR L.SubZone_1 = p_ToSubZone1)
        AND       (p_ToSubZone2 IS NULL OR L.SubZone_2 = p_ToSubZone2);
      --

      l_Records integer := -1;

  begin
    open  InventorySearch;
    fetch InventorySearch into l_Records;
    close InventorySearch;
    return l_Records;

  end jda_GetNoOfTags;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   GetQuantityOfStock				      */
/*                                                                            */
/*     DESCRIPTION:     Get the quantity of available stock for relocation    */
/*			in the to_zone area.				      */
/*                                                                            */
/*     RETURN VALUES:   Quantity of stock                                     */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 900   31/01/05 JRL DCS    NEW7657  Sku relocation for non-tagged stock*/
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/******************************************************************************/
  function jda_GetQuantityOfStock ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type
                                  , p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type
                                  , p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type
                                  , p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type
                                  , p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type
                                  , p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type
                                  , p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type
                                  , p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type
                                  , p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                                  )
    return number is
      cursor InventorySearch
      is
        SELECT  NVL(SUM(qty_on_hand), 0.0)
        FROM    dcsdba.Inventory I
        ,       dcsdba.Location L
        WHERE   I.Site_ID = p_SiteID
        AND     I.Site_ID = l.Site_ID
        AND     I.Location_ID = L.Location_ID
        AND     I.Client_ID = p_ClientID
        AND     I.SKU_ID = p_SKUID
        AND     I.Owner_ID = p_OwnerID
        AND     ((p_ConditionID IS NULL AND I.Condition_ID IS NULL) OR (p_ConditionID = I.Condition_ID))
        AND     ((p_OriginID IS NULL AND I.Origin_ID IS NULL) OR (p_OriginID = I.Origin_ID))
        AND     I.Zone_1 = p_ToZone
        AND     (p_ToSubZone1 IS NULL OR L.SubZone_1 = p_ToSubZone1)
        AND     (p_ToSubZone2 IS NULL OR L.SubZone_2 = p_ToSubZone2);
    --

    l_Quantity number := -1.0;

  begin
    open    InventorySearch;
    fetch   InventorySearch into l_Quantity;
    close   InventorySearch;
    return  l_Quantity;

  end jda_GetQuantityOfStock;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   CheckForReleasedTasks				      */
/*                                                                            */
/*     DESCRIPTION:     Search for released move tasks which relocating SKUs  */
/*			to the 'to' zone.				      */
/*                                                                            */
/*     RETURN VALUES:   Number of tasks       				      */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 600  22/09/00 MJT  DCS    NEW5117  Automatic SKU Relocation Daemon    */
/*  dcs 600  06/11/00 JAH  DCS    SYS5233  Use indicator variable             */
/*  dcs 700  15/06/01 RAM  DCS    NEW5488  Client ID                          */
/*  dcs 840  22/06/04 RMC  DCS    PDR9362  Skureldae problems                 */
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/******************************************************************************/
  function jda_CheckForReleasedTasks  ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type
                                      , p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type
                                      , p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type
                                      , p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type
                                      , p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type
                                      , p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type
                                      , p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type
                                      , p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type
                                      , p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                                      )
    return integer is
      cursor RelocationSearch
      is
        SELECT    Final_loc_ID
        FROM      dcsdba.Move_Task
        WHERE     Site_ID = p_SiteID
        AND       Client_ID = p_ClientID
        AND       Sku_id = p_SKUID
        AND       Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND Condition_ID IS NULL) OR (p_ConditionID = Condition_ID))
        AND       ((p_OriginID IS NULL AND Origin_ID IS NULL) OR (p_OriginID = Origin_ID))
        AND       Task_Type = 'M'
        AND       Status IN ('Released', 'Complete', 'In Progress');
      --

      cursor ZoneSearch ( p_LocationID in dcsdba.Location.Location_ID%type)
      is
        SELECT    Zone_1
        ,         SubZone_1
        ,         SubZone_2
        FROM      dcsdba.Location
        WHERE     Location_id = p_LocationID
        AND       (Site_ID = p_SiteID OR Site_ID IS NULL);
      --

      l_ZoneRow   ZoneSearch%rowtype;
      l_TaskCount integer := 0;

  begin
    /*LibMQSDebug.print('LibSkureldae.CheckForReleasedTasks - p_SiteID ' || p_SiteID || ',  p_ClientID ' || p_ClientID || ', p_SKUID ' || p_SKUID || ', p_OwnerID ' || p_OwnerID || ', p_ConditionID ' || p_ConditionID || ', p_OriginID ' || p_OriginID, 4);*/
    for RelocationRow in RelocationSearch loop
      if (RelocationRow.Final_loc_ID is not null) then
        open  ZoneSearch(RelocationRow.Final_loc_ID);
        fetch ZoneSearch into l_ZoneRow;
        if (ZoneSearch%found) then
          if ((l_ZoneRow.Zone_1 = p_ToZone)
          and (l_ZoneRow.SubZone_1 = p_ToSubZone1 or p_ToSubZone1 is null)
          and (l_ZoneRow.SubZone_2 = p_ToSubZone2 or p_ToSubZone2 is null)) then
            l_TaskCount := l_TaskCount + 1;
          end if;
        end if;
        close ZoneSearch;
      end if;
  end loop;
  /*LibMQSDebug.print('LibSkureldae.CheckForReleasedTasks - return ' || l_TaskCount, 4);*/
  return l_TaskCount;
end jda_CheckForReleasedTasks;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   CheckForReleasedQuantity                              */
/*                                                                            */
/*     DESCRIPTION:     Search for released move tasks which relocating SKUs  */
/*                      to the 'to' zone.                                     */
/*                                                                            */
/*     RETURN VALUES:   Quantity of tasks                                     */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 900  31/01/05 JRL  DCS    NEW7657  Sku relocation for non-tagged stock*/
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/******************************************************************************/
  function jda_CheckForReleasedQuantity ( p_SiteID      in dcsdba.SKU_Relocation.Site_ID%type
                                        , p_ClientID    in dcsdba.SKU_Relocation.Client_ID%type
                                        , p_SKUID       in dcsdba.SKU_Relocation.SKU_ID%type
                                        , p_OwnerID     in dcsdba.SKU_Relocation.Owner_ID%type
                                        , p_ConditionID in dcsdba.SKU_Relocation.Condition_ID%type
                                        , p_OriginID    in dcsdba.SKU_Relocation.Origin_ID%type
                                        , p_ToZone      in dcsdba.SKU_Relocation.To_Zone%type
                                        , p_ToSubZone1  in dcsdba.SKU_Relocation.To_SubZone_1%type
                                        , p_ToSubZone2  in dcsdba.SKU_Relocation.To_SubZone_2%type
                                        )
    return number is
      cursor RelocationSearch
      is
        SELECT    Final_loc_ID
        ,         Qty_To_Move
        FROM      dcsdba.Move_Task
        WHERE     Site_ID = p_SiteID
        AND       Client_ID = p_ClientID
        AND       Sku_id = p_SKUID
        AND       Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND Condition_ID IS NULL) OR (p_ConditionID = Condition_ID))
        AND       ((p_OriginID IS NULL AND Origin_ID IS NULL) OR (p_OriginID = Origin_ID))
        AND       Task_Type = 'M'
        AND       Status IN ('Released', 'Complete', 'In Progress');
      --

      cursor ZoneSearch ( p_LocationID in dcsdba.Location.Location_ID%type)
      is
        SELECT    Zone_1
        ,         SubZone_1
        ,         SubZone_2
        FROM      dcsdba.Location
        WHERE     Location_id = p_LocationID
        AND       (Site_ID = p_SiteID OR Site_ID IS NULL);
      --

      l_ZoneRow       ZoneSearch%rowtype;
      l_TaskQuantity  number := 0;
  begin
    /*LibMQSDebug.print('LibSkureldae.CheckForReleasedQuantity - p_SiteID ' || p_SiteID || ',  p_ClientID ' || p_ClientID || ', p_SKUID ' || p_SKUID || ', p_OwnerID ' || p_OwnerID || ', p_ConditionID ' || p_ConditionID || ', p_OriginID ' || p_OriginID, 4);*/
    for RelocationRow in RelocationSearch loop
      if (RelocationRow.Final_loc_ID is not null) then
        open  ZoneSearch(RelocationRow.Final_loc_ID);
        fetch ZoneSearch into l_ZoneRow;
        if (ZoneSearch%found) then
          if  ((l_ZoneRow.Zone_1 = p_ToZone)
          and (l_ZoneRow.SubZone_1 = p_ToSubZone1 or p_ToSubZone1 is null)
          and (l_ZoneRow.SubZone_2 = p_ToSubZone2 or p_ToSubZone2 is null)) then
            l_TaskQuantity := l_TaskQuantity + RelocationRow.Qty_To_Move;
          end if;
        end if;
        close ZoneSearch;
      end if;
    end loop;
    /*LibMQSDebug.print('LibSkureldae.CheckForReleasedQuantity - return ' || l_TaskQuantity, 4);*/
    return l_TaskQuantity;
  end jda_CheckForReleasedQuantity;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   GenerateTagRelocation                                 */
/*                                                                            */
/*     DESCRIPTION:     Generates tag relocations.                            */
/*                                                                            */
/*     RETURN VALUES:   Whether successful                                    */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 900   01/02/05 JRL DCS    NEW7657  Sku relocation for non-tagged stock*/
/*  2005.2   23/03/05 CJD  DCS    NEW7425  Dynamic pick face enhancements     */
/*  2006.2-b 02/08/06 SL   DCS    ENH1831  Set Aisle/Bay/Level/Position null  */
/*  2010.2-b 19/03/10 MJT  DCS    DSP3271  Relocation Tag Swapping            */
/*  2011.2.2 05/12/11 JH   DCS    DSP4776  Allow dynamic task id              */
/*  2012.1.0 26/03/12 JH   DCS    DSP5039  Date format problem                */
/*  2012.2.1 12/10/12 JH   DCS    DSP5685  Inventory skipping problem         */
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/*  2013.2-b 28/10/13 SD   DCS    DSP6606  SKU Relocation enhancements	      */
/******************************************************************************/
  function jda_GenerateTagRelocation  ( p_SiteID            in dcsdba.SKU_Relocation.Site_ID%type
                                      , p_ClientID          in dcsdba.SKU_Relocation.Client_ID%type
                                      , p_SKUID             in dcsdba.SKU_Relocation.SKU_ID%type
                                      , p_OwnerID           in dcsdba.SKU_Relocation.Owner_ID%type
                                      , p_ConditionID       in dcsdba.SKU_Relocation.Condition_ID%type
                                      , p_OriginID          in dcsdba.SKU_Relocation.Origin_ID%type
                                      , p_ToZone            in dcsdba.SKU_Relocation.To_Zone%type
                                      , p_ToSubZone1        in dcsdba.SKU_Relocation.To_SubZone_1%type
                                      , p_ToSubZone2        in dcsdba.SKU_Relocation.To_SubZone_2%type
                                      , p_FromZone          in dcsdba.SKU_Relocation.From_Zone%type
                                      , p_FromSubZone1      in dcsdba.SKU_Relocation.From_SubZone_1%type
                                      , p_FromSubZone2      in dcsdba.SKU_Relocation.From_SubZone_2%type
                                      , p_Algorithm         in dcsdba.SKU_Relocation.Algorithm%type
                                      , p_TaskID            in dcsdba.Move_Task.Task_ID%type
                                      , p_DisTagSwap        in dcsdba.Move_Task.Disallow_Tag_Swap%type
                                      , p_RequiredTags      in integer
                                      , p_AllowedInvStatus  in dcsdba.SKU_Relocation.Allowed_Inv_Status%type
                                      , p_LockedCode        in dcsdba.SKU_Relocation.Lock_Code%type default null
                                      )
    return boolean is
      cursor CreationSearch
      is
        SELECT    I.*
        FROM      dcsdba.Inventory I
        ,         dcsdba.Location L
        WHERE     I.Site_ID = p_SiteID
        AND       I.Site_ID = L.Site_ID
        AND       I.Location_ID = L.Location_ID
        AND       L.Loc_Type NOT IN ('ShipDock', 'Trailer')
        AND       I.Tag_ID IS NOT NULL
        AND       I.Client_ID = p_ClientID
        AND       I.SKU_ID = p_SKUID
        AND       I.Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND I.Condition_ID IS NULL) OR (p_ConditionID = I.Condition_ID))
        AND       ((p_OriginID IS NULL AND I.Origin_ID IS NULL) OR (p_OriginID = I.Origin_ID))
        AND       ((p_FromZone IS NULL AND I.Zone_1 NOT LIKE p_ToZone) OR (p_FromZone IS NOT NULL AND I.Zone_1 LIKE p_FromZone))
        AND       (p_FromSubZone1 IS NULL OR L.SubZone_1 LIKE p_FromSubZone1)
        AND       (p_FromSubZone2 IS NULL OR L.SubZone_2 LIKE p_FromSubZone2)
        AND       ((I.Lock_Status = 'UnLocked' AND   p_AllowedInvStatus = 'UnLocked') 
              OR  (I.Lock_Status = 'Locked' AND  p_AllowedInvStatus = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode))
              OR  (p_AllowedInvStatus = 'Both' AND ((I.Lock_Status = 'Locked' AND ( p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode)) OR I.Lock_Status = 'UnLocked' ))
                  )
        AND       L.Lock_Status IN ('UnLocked', 'InLocked')
        AND       ((I.Qty_On_Hand > I.Qty_Allocated) OR 1 < ( SELECT    COUNT(Task_ID)  
                                                              FROM      dcsdba.Move_Task M
                                                              WHERE     M.Site_ID = p_SiteID
                                                              AND       M.Client_ID = p_ClientID
                                                              AND       M.SKU_ID = p_SKUID
                                                              AND       M.Owner_ID = p_OwnerID
                                                              AND       M.Condition_ID = p_ConditionID
                                                              AND       M.Origin_ID = p_OriginID
                                                              AND       M.From_Loc_ID = I.Location_ID
                                                              AND       M.Tag_ID = I.Tag_ID
                                                              AND       M.CE_Rotation_ID = I.CE_Rotation_ID
                                                              AND       NVL(M.CE_Under_Bond, 'N') = NVL(I.CE_Under_Bond, 'N')
                                                              AND       NVL(M.CE_Avail_Status, 'N') = NVL(I.CE_Avail_Status, 'N')
                                                              AND       NVL(M.Pallet_Grouped, 'N') = 'N'
                                                            )
                  )
        ORDER BY  I.Receipt_DStamp
        ,         I.Move_DStamp
        ,         (I.Qty_On_Hand - I.Qty_Allocated);
      --

      cursor ExpirySearch
      is
        SELECT    I.*
        FROM      dcsdba.Inventory I
        ,         dcsdba.Location L
        WHERE     I.Site_ID = p_SiteID
        AND       I.Site_ID = L.Site_ID
        AND       I.Location_ID = L.Location_ID
        AND       L.Loc_Type NOT IN ('ShipDock', 'Trailer')
        AND       I.Tag_ID IS NOT NULL
        AND       I.Client_ID = p_ClientID
        AND       I.SKU_ID = p_SKUID
        AND       I.Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND I.Condition_ID IS NULL) OR (p_ConditionID = I.Condition_ID))
        AND       ((p_OriginID IS NULL AND I.Origin_ID IS NULL) OR (p_OriginID = I.Origin_ID))
        AND       ((p_FromZone IS NULL AND I.Zone_1 NOT LIKE p_ToZone) OR (p_FromZone IS NOT NULL AND I.Zone_1 LIKE p_FromZone))
        AND       (p_FromSubZone1 IS NULL OR L.SubZone_1 LIKE p_FromSubZone1)
        AND       (p_FromSubZone2 IS NULL OR L.SubZone_2 LIKE p_FromSubZone2)
        AND       ((I.Lock_Status = 'UnLocked' AND p_AllowedInvStatus = 'UnLocked') 
              OR  (I.Lock_Status = 'Locked' AND p_AllowedInvStatus = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode))
              OR  (p_AllowedInvStatus = 'Both' AND ((I.Lock_Status = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode)) OR I.Lock_Status = 'UnLocked'))
                  )
        AND       L.Lock_Status IN ('UnLocked', 'InLocked')
        AND       ((I.Qty_On_Hand > I.Qty_Allocated) OR 1 < (   SELECT  COUNT(Task_ID)
                                                                FROM    dcsdba.Move_Task M
                                                                WHERE   M.Site_ID = p_SiteID
                                                                AND     M.Client_ID = p_ClientID
                                                                AND     M.SKU_ID = p_SKUID
                                                                AND     M.Owner_ID = p_OwnerID
                                                                AND     M.Condition_ID = p_ConditionID
                                                                AND     M.Origin_ID = p_OriginID
                                                                AND     M.From_Loc_ID = I.Location_ID
                                                                AND     M.Tag_ID = I.Tag_ID
                                                                AND     M.CE_Rotation_ID = I.CE_Rotation_ID
                                                                AND     NVL(M.CE_Under_Bond, 'N') = NVL(I.CE_Under_Bond, 'N')
                                                                AND     NVL(M.CE_Avail_Status, 'N') = NVL(I.CE_Avail_Status, 'N')
                                                                AND     NVL(M.Pallet_Grouped, 'N') = 'N'
                                                              )
                    )
        ORDER BY    I.Expiry_DStamp
        ,           I.Move_DStamp
        ,           (I.Qty_On_Hand - I.Qty_Allocated);
      --

      l_RequiredTags  integer := 0;
      l_InventoryRow  CreationSearch%rowtype;
      l_NewLocId      dcsdba.Location.Location_ID%type;
      l_FaceKey       dcsdba.Pick_Face.Key%type;
      l_Result        integer := 0;
      l_Continue      boolean := true;

  begin
    /*LibMQSDebug.print('LibSkureldae.GenerateTagRelocation - start with p_ClientID ' || p_ClientID || ', p_SKUID ' || p_SKUID || ', p_Algorithm ' || p_Algorithm, 4);*/
    if (p_Algorithm = 'FIFO by Creation Date') then
      open CreationSearch;
    else
      open ExpirySearch;
    end if;
    l_RequiredTags := p_RequiredTags;
    while (l_RequiredTags > 0) loop
      l_Continue := true;
      if (p_Algorithm = 'FIFO by Creation Date') then
        fetch CreationSearch into l_InventoryRow;
        if (CreationSearch%notfound) then
          /*LibMQSDebug.print('LibSkureldae.GenerateTagRelocation - no more tags to relocate', 4);*/
          exit;
        end if;
      else
        fetch ExpirySearch into l_InventoryRow;
        if (ExpirySearch%notfound) then
          /*LibMQSDebug.print('LibSkureldae.GenerateTagRelocation - no more tags to relocate', 4);*/
          exit;
        end if;
      end if;

      if (l_Continue = true) then
        if (jda_CheckRelocationSuitable ( l_InventoryRow.Site_ID
                                        , l_InventoryRow.Client_ID
                                        , l_InventoryRow.SKU_ID
                                        , l_InventoryRow.Owner_ID
                                        , l_InventoryRow.Condition_ID
                                        , l_InventoryRow.Origin_ID
                                        , l_InventoryRow.Location_ID
                                        , l_InventoryRow.Pallet_ID
                                        , l_InventoryRow.Tag_ID
                                        , l_InventoryRow.CE_Rotation_ID
                                        , l_InventoryRow.CE_Under_Bond
                                        , l_InventoryRow.CE_Avail_Status
                                        , l_InventoryRow.Key
                                        ) = false) then
          l_Continue := false;
        end if;
      end if;

      if (l_Continue = true) then
        dcsdba.LibPutSearch.FindPutawayLocation ( l_Result
                                                , l_NewLocID
                                                , l_FaceKey
                                                , l_InventoryRow.Client_ID
                                                , l_InventoryRow.SKU_ID
                                                , l_InventoryRow.Tag_ID
                                                , l_InventoryRow.Pallet_ID
                                                , l_InventoryRow.Config_ID
                                                , l_InventoryRow.Qty_On_Hand
                                                , l_InventoryRow.Batch_ID
                                                , l_InventoryRow.Condition_ID
                                                , l_InventoryRow.Site_ID
                                                , l_InventoryRow.Location_ID
                                                , 0.0 /* EnteredHeight   */
                                                , 'N' /* PalletPutSearch */
                                                , l_InventoryRow.Pallet_Volume
                                                , l_InventoryRow.Pallet_Height
                                                , l_InventoryRow.Receipt_DStamp
                                                , l_InventoryRow.Owner_ID
                                                , l_InventoryRow.Origin_ID
                                                , l_InventoryRow.Pallet_Depth
                                                , l_InventoryRow.Pallet_Width
                                                , l_InventoryRow.Pallet_Weight
                                                , l_InventoryRow.Pallet_Config
                                                , l_InventoryRow.User_Def_Type_1
                                                , l_InventoryRow.User_Def_Type_2
                                                , l_InventoryRow.User_Def_Type_3
                                                , l_InventoryRow.User_Def_Type_4
                                                , l_InventoryRow.User_Def_Type_5
                                                , l_InventoryRow.User_Def_Type_6
                                                , l_InventoryRow.User_Def_Type_7
                                                , l_InventoryRow.User_Def_Type_8
                                                , l_InventoryRow.User_Def_Chk_1
                                                , l_InventoryRow.User_Def_Chk_2
                                                , l_InventoryRow.User_Def_Chk_3
                                                , l_InventoryRow.User_Def_Chk_4
                                                , 'Z'
                                                , l_InventoryRow.Lock_Code
                                                , null /* Hazmat	*/
                                                , null /* InventoryKey	*/
                                                , l_InventoryRow.CE_Receipt_Type
                                                , l_InventoryRow.CE_Under_Bond
                                                , p_ToZone
                                                , p_ToSubZone1
                                                , p_ToSubZone2
                                                );
        if (l_Result != 1) then
        /*LibMQSDebug.print('LibSkureldae.GenerateTagRelocation - could not find putaway location so skip', 4);*/
        l_Continue := false;
        end if;
      end if;

      if (l_Continue = true) then
        l_Result := dcsdba.LibMoveTask.CreateMoveTask ( p_TaskID
                                                      , null /* Line ID */
                                                      , l_InventoryRow.Client_ID
                                                      , l_InventoryRow.SKU_ID
                                                      , l_InventoryRow.Tag_ID
                                                      , l_InventoryRow.Location_ID
                                                      , l_NewLocId
                                                      , 'Released'
                                                      , l_InventoryRow.Qty_On_Hand
                                                      , l_InventoryRow.Config_ID
                                                      , l_InventoryRow.Description
                                                      , null /* Work Group */
                                                      , null /* Consignment */
                                                      , 'M'
                                                      , null /* Container ID */
                                                      , null /* Pallet ID */
                                                      , null /* Consol Link */
                                                      , null /* Priority */
                                                      , null
                                                      , 'N' /* Allocation */
                                                      , l_InventoryRow.Site_ID
                                                      , l_InventoryRow.Condition_ID
                                                      , null /* FirstKey */
                                                      , null /* PalletGrouped */
                                                      , null /* PalletVolume */
                                                      , l_InventoryRow.Owner_ID
                                                      , null /* KitSKUID */
                                                      , null /* KitLineID */
                                                      , null /* KitLink */
                                                      , null /* KitRatio */
                                                      , l_InventoryRow.Origin_ID
                                                      , null /* OriginalDStamp */
                                                      , null /* PalletConfig */
                                                      , null /* CustomerID */
                                                      , null /* DueTaskID */
                                                      , null /* DueLineID */
                                                      , null /* DueType */
                                                      , null /* Repack */
                                                      , null /* BOL ID */
                                                      , 'N' /* PickFaceAutoEx */
                                                      , null /* PrintLabel */
                                                      , null /* OldTaskID */
                                                      , 'N' /* RepackQCDone */
                                                      , null /* CatchWeight */
                                                      , 'N' /* UsePickToGrid */
                                                      , null /* PickReAllocFlag */
                                                      , null /* StageRouteID */
                                                      , null /* StageRouteSeq */
                                                      , 'N' /* Labelling */
                                                      , null /* PFConsolLink */
                                                      , null /* SerialNumber */
                                                      , 'N' /* IgnoreStage */
                                                      , null /* ShipmentGroup */
                                                      , l_InventoryRow.CE_Under_Bond /* CEUnderBond */
                                                      , null /* KitPlanID */
                                                      , null /* PlanSequence */
                                                      , null /* ContainerConfig */
                                                      , p_DisTagSwap
                                                      , CERotationID => l_InventoryRow.CE_Rotation_ID
                                                      , CEAvailStatus => l_InventoryRow.CE_Avail_Status
                                                      );
        if (l_Result = 0) then
          /*LibMQSDebug.print('LibSkureldae.GenerateTagRelocation - failed to generate relocation task so exit', 4);*/
          exit;
        end if;
        /*LibMQSDebug.print('LibSkureldae.GenerateTagRelocation - generated relocation task', 4);*/
        l_RequiredTags := l_RequiredTags - 1;
      end if;
    end loop;
    if (p_Algorithm = 'FIFO by Creation Date') then
      close CreationSearch;
    else
      close ExpirySearch;
    end if;
    return true;
  end jda_GenerateTagRelocation;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   GenerateStockRelocation                               */
/*                                                                            */
/*     DESCRIPTION:     Generates stock relocations.                          */
/*                                                                            */
/*     RETURN VALUES:   Whether successful                                    */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  dcs 900   01/02/05 JRL DCS    NEW7657  Sku relocation for non-tagged stock*/
/*  2005.2   23/03/05 CJD  DCS    NEW7425  Dynamic pick face enhancements     */
/*  2006.2-b 02/08/06 SL   DCS    ENH1831  Set Aisle/Bay/Level/Position null  */
/*  2010.2-b 19/03/10 MJT  DCS    DSP3271  Relocation Tag Swapping            */
/*  2011.2.2 05/12/11 JH   DCS    DSP4776  Allow dynamic task id              */
/*  2012.1.0 26/03/12 JH   DCS    DSP5039  Date format problem                */
/*  2012.2.1 12/10/12 JH   DCS    DSP5685  Inventory skipping problem         */
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/*  2013.2-b 28/10/13 SD   DCS    DSP6606  SKU Relocation enhancements	      */
/******************************************************************************/
  function jda_GenerateStockRelocation  ( p_SiteID            in dcsdba.SKU_Relocation.Site_ID%type
                                        , p_ClientID          in dcsdba.SKU_Relocation.Client_ID%type
                                        , p_SKUID             in dcsdba.SKU_Relocation.SKU_ID%type
                                        , p_OwnerID           in dcsdba.SKU_Relocation.Owner_ID%type
                                        , p_ConditionID       in dcsdba.SKU_Relocation.Condition_ID%type
                                        , p_OriginID          in dcsdba.SKU_Relocation.Origin_ID%type
                                        , p_ToZone            in dcsdba.SKU_Relocation.To_Zone%type
                                        , p_ToSubZone1        in dcsdba.SKU_Relocation.To_SubZone_1%type
                                        , p_ToSubZone2        in dcsdba.SKU_Relocation.To_SubZone_2%type
                                        , p_FromZone          in dcsdba.SKU_Relocation.From_Zone%type
                                        , p_FromSubZone1      in dcsdba.SKU_Relocation.From_SubZone_1%type
                                        , p_FromSubZone2      in dcsdba.SKU_Relocation.From_SubZone_2%type
                                        , p_Algorithm         in dcsdba.SKU_Relocation.Algorithm%type
                                        , p_TaskID            in dcsdba.Move_Task.Task_ID%type
                                        , p_DisTagSwap        in dcsdba.Move_Task.Disallow_Tag_Swap%type
                                        , p_RequiredStock     in number
                                        , p_AllowedInvStatus  in dcsdba.SKU_Relocation.Allowed_Inv_Status%type
                                        , p_LockedCode        in dcsdba.SKU_Relocation.Lock_Code%type default null
                                        )
    return boolean is
      cursor CreationSearch
      is
        SELECT    I.*
        FROM      dcsdba.Inventory I
        ,         dcsdba.Location L
        WHERE     I.Site_ID = p_SiteID
        AND       I.Site_ID = L.Site_ID
        AND       I.Location_ID = L.Location_ID
        AND       L.Loc_Type NOT IN ('ShipDock', 'Trailer')
        AND       I.Client_ID = p_ClientID
        AND       I.SKU_ID = p_SKUID
        AND       I.Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND I.Condition_ID IS NULL) OR (p_ConditionID = I.Condition_ID))
        AND       ((p_OriginID IS NULL AND I.Origin_ID IS NULL) OR (p_OriginID = I.Origin_ID))
        AND       ((p_FromZone IS NULL AND I.Zone_1 NOT LIKE p_ToZone) OR (p_FromZone IS NOT NULL AND I.Zone_1 LIKE p_FromZone))
        AND       (p_FromSubZone1 IS NULL OR L.SubZone_1 LIKE p_FromSubZone1)
        AND       (p_FromSubZone2 IS NULL OR L.SubZone_2 LIKE p_FromSubZone2)
        AND       ((I.Lock_status = 'UnLocked' AND p_AllowedInvStatus = 'UnLocked')
              OR  (I.Lock_Status = 'Locked' AND p_AllowedInvStatus = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode))
              OR  (p_AllowedInvStatus = 'Both' AND ((I.Lock_Status = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode)) OR I.Lock_Status = 'UnLocked' ))  
                  )
        AND       L.Lock_Status IN ('UnLocked', 'InLocked')
        AND       I.Qty_On_Hand > I.Qty_Allocated
        AND       (0 = (  SELECT    COUNT(Task_ID)
                          FROM      dcsdba.Move_Task M
                          WHERE     M.Site_ID = p_SiteID
                          AND       M.Client_ID = p_ClientID
                          AND       M.SKU_ID = p_SKUID
                          AND       M.Owner_ID = p_OwnerID
                          AND       (p_ConditionID IS NULL OR M.Condition_ID = p_ConditionID)
                          AND       (p_OriginID IS NULL OR M.Origin_ID = p_OriginID)
                          AND       M.From_Loc_ID = I.Location_ID
                          AND       NVL(M.Pallet_Grouped, 'N') = 'N'
                        )
                    )
        ORDER BY    I.Receipt_DStamp
        ,           I.Move_DStamp
        ,           (I.Qty_On_Hand - I.Qty_Allocated);
      --

      cursor ExpirySearch
      is
        SELECT    I.*
        FROM      dcsdba.Inventory I
        ,         dcsdba.Location L
        WHERE     I.Site_ID = p_SiteID
        AND       I.Site_ID = L.Site_ID
        AND       I.Location_ID = L.Location_ID
        AND       L.Loc_Type NOT IN ('ShipDock', 'Trailer')
        AND       I.Client_ID = p_ClientID
        AND       I.SKU_ID = p_SKUID
        AND       I.Owner_ID = p_OwnerID
        AND       ((p_ConditionID IS NULL AND I.Condition_ID IS NULL) OR (p_ConditionID = I.Condition_ID))
        AND       ((p_OriginID IS NULL AND I.Origin_ID IS NULL) OR (p_OriginID = I.Origin_ID))
        AND       ((p_FromZone IS NULL AND I.Zone_1 NOT LIKE p_ToZone) OR (p_FromZone IS NOT NULL AND I.Zone_1 LIKE p_FromZone))
        AND       (p_FromSubZone1 IS NULL OR L.SubZone_1 LIKE p_FromSubZone1)
        AND       (p_FromSubZone2 IS NULL OR L.SubZone_2 LIKE p_FromSubZone2)
        AND       ((I.Lock_Status = 'UnLocked' AND p_AllowedInvStatus = 'UnLocked')
              OR  (I.Lock_Status = 'Locked' AND p_AllowedInvStatus = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode))
              OR  ( p_AllowedInvStatus = 'Both' AND ((I.Lock_Status = 'Locked' AND (p_LockedCode IS NULL OR I.Lock_Code = p_LockedCode)) OR I.Lock_Status = 'UnLocked'))
                  )
        AND       L.Lock_Status IN ('UnLocked', 'InLocked')
        AND       I.Qty_On_Hand > I.Qty_Allocated
        AND       (0 = (  SELECT  COUNT(Task_ID)
                          FROM    dcsdba.Move_Task M
                          WHERE   M.Site_ID = p_SiteID
                          AND     M.Client_ID = p_ClientID
                          AND     M.SKU_ID = p_SKUID
                          AND     M.Owner_ID = p_OwnerID
                          AND     (p_ConditionID IS NULL OR M.Condition_ID = p_ConditionID)
                          AND     (p_OriginID IS NULL OR M.Origin_ID = p_OriginID)
                          AND     M.From_Loc_ID = I.Location_ID
                          AND     NVL(M.Pallet_Grouped, 'N') = 'N'
                        )
                    )
        ORDER BY    I.Expiry_DStamp
        ,           I.Move_DStamp
        ,           (I.Qty_On_Hand - I.Qty_Allocated);
      --

      l_RequiredStock number := 0.0;
      l_QtyCanMove    number := 0.0;
      l_QtyToMove     number := 0.0;
      l_InventoryRow  CreationSearch%rowtype;
      l_NewLocId      dcsdba.Location.Location_ID%type;
      l_FaceKey       dcsdba.Pick_Face.Key%type;
      l_Result        integer := 0;
      l_Continue      boolean := true;

  begin
    /*LibMQSDebug.print('LibSkureldae.GenerateStockRelocation - start with p_ClientID ' || p_ClientID || ', p_SKUID ' || p_SKUID || ', p_Algorithm ' || p_Algorithm, 4);*/
    if (p_Algorithm = 'FIFO by Creation Date') then
      open CreationSearch;
    else
      open ExpirySearch;
    end if;

    l_RequiredStock := p_RequiredStock;
    while (l_RequiredStock > 0) loop
      l_Continue := true;
      if (p_Algorithm = 'FIFO by Creation Date') then
        fetch CreationSearch into l_InventoryRow;
        if (CreationSearch%notfound) then
          /*LibMQSDebug.print('LibSkureldae.GenerateStockRelocation - no more tags to relocate', 4);*/
          exit;
        end if;
      else
        fetch ExpirySearch into l_InventoryRow;
        if (ExpirySearch%notfound) then
          /*LibMQSDebug.print('LibSkureldae.GenerateStockRelocation - no more tags to relocate', 4);*/
          exit;
        end if;
      end if;

      if (l_Continue = true) then
        if (jda_CheckRelocationSuitable ( l_InventoryRow.Site_ID
                                        , l_InventoryRow.Client_ID
                                        , l_InventoryRow.SKU_ID
                                        , l_InventoryRow.Owner_ID
                                        , l_InventoryRow.Condition_ID
                                        , l_InventoryRow.Origin_ID
                                        , l_InventoryRow.Location_ID
                                        , l_InventoryRow.Pallet_ID
                                        , l_InventoryRow.Tag_ID
                                        , l_InventoryRow.CE_Rotation_ID
                                        , l_InventoryRow.CE_Under_Bond
                                        , l_InventoryRow.CE_Avail_Status
                                        , l_InventoryRow.Key) = false) then
          l_Continue := false;
        end if;
      end if;

      if (l_Continue = true) then
        l_QtyCanMove := l_InventoryRow.Qty_On_Hand;
        if (l_QtyCanMove > l_RequiredStock) then
          l_QtyToMove := l_RequiredStock;
        else
          l_QtyToMove := l_QtyCanMove;
        end if;
        dcsdba.LibPutSearch.FindPutawayLocation ( l_Result
                                                , l_NewLocID
                                                , l_FaceKey
                                                , l_InventoryRow.Client_ID
                                                , l_InventoryRow.SKU_ID
                                                , l_InventoryRow.Tag_ID
                                                , l_InventoryRow.Pallet_ID
                                                , l_InventoryRow.Config_ID
                                                , l_QtyToMove
                                                , l_InventoryRow.Batch_ID
                                                , l_InventoryRow.Condition_ID
                                                , l_InventoryRow.Site_ID
                                                , l_InventoryRow.Location_ID
                                                , 0.0 /* EnteredHeight   */
                                                , 'N' /* PalletPutSearch */
                                                , l_InventoryRow.Pallet_Volume
                                                , l_InventoryRow.Pallet_Height
                                                , l_InventoryRow.Receipt_DStamp
                                                , l_InventoryRow.Owner_ID
                                                , l_InventoryRow.Origin_ID
                                                , l_InventoryRow.Pallet_Depth
                                                , l_InventoryRow.Pallet_Width
                                                , l_InventoryRow.Pallet_Weight
                                                , l_InventoryRow.Pallet_Config
                                                , l_InventoryRow.User_Def_Type_1
                                                , l_InventoryRow.User_Def_Type_2
                                                , l_InventoryRow.User_Def_Type_3
                                                , l_InventoryRow. User_Def_Type_4
                                                , l_InventoryRow. User_Def_Type_5
                                                , l_InventoryRow. User_Def_Type_6
                                                , l_InventoryRow. User_Def_Type_7
                                                , l_InventoryRow. User_Def_Type_8
                                                , l_InventoryRow. User_Def_Chk_1
                                                , l_InventoryRow. User_Def_Chk_2
                                                , l_InventoryRow. User_Def_Chk_3
                                                , l_InventoryRow. User_Def_Chk_4
                                                , 'Z'
                                                , l_InventoryRow. Lock_Code
                                                , null /* Hazmat	*/
                                                , null /* InventoryKey	*/
                                                , l_InventoryRow. CE_Receipt_Type
                                                , l_InventoryRow. CE_Under_Bond
                                                , p_ToZone
                                                , p_ToSubZone1
                                                , p_ToSubZone2
                                                );
        if (l_Result != 1) then
          /*LibMQSDebug.print('LibSkureldae.GenerateStockRelocation - could not find putaway location so skip', 4);*/
          l_Continue := false;
        end if;
      end if;

      if (l_Continue = true) then
        l_Result := dcsdba.LibMoveTask.CreateMoveTask ( p_TaskID
                                                      , null /* Line ID */
                                                      , l_InventoryRow. Client_ID
                                                      , l_InventoryRow. SKU_ID
                                                      , l_InventoryRow. Tag_ID
                                                      , l_InventoryRow. Location_ID
                                                      , l_NewLocId
                                                      , 'Released'
                                                      , l_QtyToMove
                                                      , l_InventoryRow. Config_ID
                                                      , l_InventoryRow. Description
                                                      , null /* Work Group */
                                                      , null /* Consignment */
                                                      , 'M'
                                                      , null /* Container ID */
                                                      , null /* Pallet ID */
                                                      , null /* Consol Link */
                                                      , null /* Priority */
                                                      , null
                                                      , 'N' /* Allocation */
                                                      , l_InventoryRow. Site_ID
                                                      , l_InventoryRow. Condition_ID
                                                      , null /* FirstKey */
                                                      , null /* PalletGrouped */
                                                      , null /* PalletVolume */
                                                      , l_InventoryRow. Owner_ID
                                                      , null /* KitSKUID */
                                                      , null /* KitLineID */
                                                      , null /* KitLink */
                                                      , null /* KitRatio */
                                                      , l_InventoryRow. Origin_ID
                                                      , null /* OriginalDStamp */
                                                      , null /* PalletConfig */
                                                      , null /* CustomerID */
                                                      , null /* DueTaskID */
                                                      , null /* DueLineID */
                                                      , null /* DueType */
                                                      , null /* Repack */
                                                      , null /* BOL ID */
                                                      , 'N' /* PickFaceAutoEx */
                                                      , null /* PrintLabel */
                                                      , null /* OldTaskID */
                                                      , 'N' /* RepackQCDone */
                                                      , null /* CatchWeight */
                                                      , 'N' /* UsePickToGrid */
                                                      , null /* PickReAllocFlag */
                                                      , null /* StageRouteID */
                                                      , null /* StageRouteSeq */
                                                      , 'N' /* Labelling */
                                                      , null /* PFConsolLink */
                                                      , null /* SerialNumber */
                                                      , 'N' /* IgnoreStage */
                                                      , null /* ShipmentGroup */
                                                      , l_InventoryRow. CE_Under_Bond /* CEUnderBond */
                                                      , null /* KitPlanID */
                                                      , null /* PlanSequence */
                                                      , null /* ContainerConfig */
                                                      , p_DisTagSwap
                                                      , CERotationID => l_InventoryRow.CE_Rotation_ID
                                                      , CEAvailStatus => l_InventoryRow.CE_Avail_Status
                                                      );
        if (l_Result = 0) then
          /*LibMQSDebug.print('LibSkureldae.GenerateStockRelocation - failed to generate relocation task so exit', 4);*/
          exit;
        end if;
        /*LibMQSDebug.print('LibSkureldae.GenerateStockRelocation - generated relocation task', 4);*/
        l_RequiredStock := l_RequiredStock - l_QtyToMove;
      end if;
    end loop;
    if (p_Algorithm = 'FIFO by Creation Date') then
      close CreationSearch;
    else
      close ExpirySearch;
    end if;
    return true;
  end jda_GenerateStockRelocation;
/******************************************************************************/
/*                                                                            */
/*     FUNCTION NAME:   CheckRelocationSuitable                               */
/*                                                                            */
/*     DESCRIPTION:     Checks relocation is suitable - no existing           */
/*  			relocations or relocating from a mixed pallet.        */
/*                                                                            */
/*     RETURN VALUES:   Whether it is suitable                                */
/*                                                                            */
/*  RELEASE   DATE     BY  PROJ   ID       DESCRIPTION                        */
/*  ======== ======== ==== ====== ======== =============                      */
/*  2013.1-b 09/11/12 JH   DCS    DSP5778  Migrate skureldae to package       */
/******************************************************************************/
  function jda_CheckRelocationSuitable  ( p_SiteID        in dcsdba.Inventory.Site_ID%type
                                        , p_ClientID      in dcsdba.Inventory.Client_ID%type
                                        , p_SKUID         in dcsdba.Inventory.SKU_ID%type
                                        , p_OwnerID       in dcsdba.Inventory.Owner_ID%type
                                        , p_ConditionID   in dcsdba.Inventory.Condition_ID%type
                                        , p_OriginID      in dcsdba.Inventory.Origin_ID%type
                                        , p_LocationID    in dcsdba.Inventory.Location_ID%type
                                        , p_PalletID      in dcsdba.Inventory.Pallet_ID%type
                                        , p_TagID         in dcsdba.Inventory.Tag_ID%type
                                        , p_CERotationID  in dcsdba.Inventory.CE_Rotation_ID%type
                                        , p_CEUnderBond   in dcsdba.Inventory.CE_Under_Bond%type
                                        , p_CEAvailStatus in dcsdba.Inventory.CE_Avail_Status%type
                                        , p_Key           in dcsdba.Inventory.Key%type
                                        )
    return boolean is
      cursor RelocationSearch
      is
        SELECT    Tag_ID
        FROM      dcsdba.Move_Task
        WHERE     Site_ID = p_SiteID
        AND       Tag_ID = p_TagID
        AND       CE_Rotation_ID = p_CERotationID
        AND       NVL(CE_Under_Bond, 'N') = NVL(p_CEUnderBond, 'N')
        AND       NVL(CE_Avail_Status, 'N') = NVL(p_CEAvailStatus, 'N')
        AND       Client_ID = p_ClientID
        AND       SKU_ID = p_SKUID
        AND       ((p_ConditionID IS NULL AND Condition_ID IS NULL) OR (p_ConditionID = Condition_ID))
        AND       ((p_OriginID IS NULL AND Origin_ID IS NULL) OR (p_OriginID = Origin_ID))
        AND       Owner_ID = p_OwnerID
        AND       From_Loc_Id = p_LocationID
        AND       Task_Type = 'M'
        AND       RowNum = 1;
      --

      cursor PalletSearch
      is
        SELECT    Pallet_ID
        FROM      dcsdba.Inventory
        WHERE     Key <> p_Key
        AND       (p_SiteID IS NULL OR Site_ID = p_SiteID)
        AND       Location_ID = p_LocationID
        AND       Pallet_ID = p_PalletID
        AND       RowNum = 1;
      --

      l_TagID     dcsdba.Move_Task.Tag_ID%type;
      l_PalletID  dcsdba.Move_Task.Pallet_ID%type;

  begin
    /*LibMQSDebug.print('LibSkureldae.CheckRelocationSuitable - start', 4);*/
    /*Check no relocations already exist*/
    open  RelocationSearch;
    fetch RelocationSearch into l_TagID;
    if (RelocationSearch%found) then
      /*LibMQSDebug.print('LibSkureldae.CheckRelocationSuitable - l_TagID ' || l_TagID || ' - relocation already exists', 4);*/
      close RelocationSearch;
      return false;
    end if;
    close RelocationSearch;
    /*Check inventory not on a mixed pallet*/
    if (p_PalletID is not null) then
      if (dcsdba.LibSession.GlobalOptionEnabled ( 'SKU_RELOC_SPLIT'
                                                , p_SiteID
                                                , p_ClientID
                                                ) = 0) then
        open  PalletSearch;
        fetch PalletSearch into l_PalletID;
        if (PalletSearch%found) then
          /*LibMQSDebug.print('LibSkureldae.CheckRelocationSuitable - l_PalletID ' || l_PalletID || ' - inventory on a mixed pallet', 4);*/
          close PalletSearch;
          return false;
        end if;
        close PalletSearch;
      end if;
    end if;
    /*LibMQSDebug.print('LibSkureldae.CheckRelocationSuitable - return true', 4);*/
    return true;
  end jda_CheckRelocationSuitable;
  /*
  function GetBuildInformation
				return varchar2
				is
				begin
					return 'Machine - Linux dehze01-lsv301 2.6.32-431.el6.x86_64 #1 SMP Sun Nov 10 22 19 54 EST 2013 x86_64 x86_64 x86_64 GNU/Linux, System - dehze01-lsv301 DEVNLRP2, Number - 4, Increment - 2, Version - WhsMgmt 8.3.1, Datestamp - 05/06/15 15:37:47';
				end GetBuildInformation;
  */
/****************************************************************************************************************/
/*  From this point forward all code is created by Rhenus.                                                      */
/*  The purpose of this code is to replace the modification build in WMS2009 SKU relocation                     */
/*  The goal is to create relocates as a sort of replenishment based on any allocation done on the pallet.      */
/*  This is needed because the standard replenish functionality can't handle batch and/or expiry                */
/*  controlled inventory.                                                                                       */
/****************************************************************************************************************/
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : transform wildcards into valid SQL wildcards
-- Description: changes WMS wildcards into SQL wildcards.
------------------------------------------------------------------------------------------------
  function TransformWildCards(p_String in varchar2)
    return varchar2 is
      l_Len     number;
      l_Count   number := 0;
      l_Char    character;
      l_String  varchar2(200) := '';
  begin
    l_Len := Length(p_String);
    /*This prevents an exception from being thrown*/
    if (l_Len = 0 or l_Len is null) then
      return l_String;
    end if;
    for l_Count in 1..l_Len loop
      l_Char := substr(p_String, l_Count, 1);
      if (l_Char = '*') then
      l_Char := '%';
      end if;
      if (l_Char = '?') then
      l_Char := '_';
      end if;
      l_String := l_String || l_Char;
    end loop;
    return l_String;
  end TransformWildCards;
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : transform wildcards 
-- Description: returns valid SQL wildcards
------------------------------------------------------------------------------------------------
  procedure TransformWildCardsProc(p_String in out varchar2)
  is
  begin
    p_String := TransformWildCards(p_String);
  end TransformWildCardsProc;
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : validation of inventory
-- Description: check if inventory is ok to relocate.
------------------------------------------------------------------------------------------------
 function checkinvsuitable_f  ( p_tagid           dcsdba.move_task.tag_id%type
                              , p_clientid        dcsdba.move_task.client_id%type
                              , p_ownerid         dcsdba.move_task.owner_id%type
                              , p_siteid          dcsdba.move_task.site_id%type
                              , p_locationid      dcsdba.move_task.from_loc_id%type
                              , p_zone1           dcsdba.sku_relocation.from_zone%type
                              , p_skuid           dcsdba.move_task.sku_id%type
                              , p_configid        dcsdba.move_task.config_id%type
                              , p_originid        dcsdba.move_task.origin_id%type
                              , p_conditionid     dcsdba.move_task.condition_id%type
                              , p_allowed_status  varchar2
                              , p_lock_code       varchar2
                              , p_qty_to_move     number
                              , p_tozone          dcsdba.sku_relocation.to_zone%type
                              )
    return integer
  is

    cursor c_invrec
    is
      select  i.*
      from    dcsdba.inventory i
      where   i.tag_id        = p_tagid
      and     i.client_id     = p_clientid
      and     i.owner_id      = p_ownerid
      and     i.site_id       = p_siteid
      and     i.location_id   = p_locationid
      and     (i.zone_1       = p_zone1 or i.zone_1 like p_zone1)
      and     i.sku_id        = p_skuid
      and     (i.config_id     = p_configid or p_configid is null)
      and     (nvl(i.origin_id,'N') = nvl(p_originid,'N') or p_originid is null)
      and     (nvl(i.condition_id,'N') = nvl(p_conditionid,'N') or p_originid is null)
      and     rownum = 1;
  --          

    cursor c_checkothertasks
    is
      select  count(*)
      from    dcsdba.move_task m
      where   m.task_type     = 'M'
      and     m.tag_id        = p_tagid
      and     m.from_loc_id   = p_locationid
      and     m.sku_id        = p_skuid
      and     m.site_id       = p_siteid
      and     m.client_id     = p_clientid
      and     m.owner_id      = p_ownerid
      and     (nvl(m.origin_id,'N') = nvl(p_originid,'N') or p_originid is null)
      and     (nvl(m.condition_id,'N') = nvl(p_conditionid,'N') or p_originid is null)
    ;
    --
    cursor c_checkforpickface 
    is
      select  count(*)
      from    dcsdba.pick_face p
      where   p.client_id     = p_clientid
      and     p.owner_id      = p_ownerid
      and     p.site_id       = p_siteid
      and     p.sku_id        = p_skuid
      and     p.zone_1        = p_tozone;
    --

    l_suitable  number;
    l_invrec    c_invrec%rowtype;
    l_tasks     number;
    l_pface     number;

  begin
    open  c_invrec;
    fetch c_invrec into l_invrec;
    if c_invrec%notfound then 
      l_suitable := 0;
      --dbms_output.put_line('Inventory check could not find inventory');
    else
      l_suitable := l_invrec.key;
      --dbms_output.put_line('Inventory check found inventory ' || l_suitable);
    end if;
    close c_invrec;

    /* check inventory matches the allowed lock statusses*/
    if l_suitable != 0 then  
      if p_allowed_status = 'Locked' then
        --dbms_output.put_line('Allowed status Locked check');
        if l_invrec.Lock_status = 'Locked' and l_invrec.Lock_code = p_lock_code then
          --dbms_output.put_line('Inventory locked with correct lock code');
          null;
        else
          --dbms_output.put_line('Inventory is not locked or has wrong lock code check failed');
          l_suitable := 0;
        end if;
      end if;
    end if;

    if l_suitable != 0 then
      if p_allowed_status = 'UnLocked' then
        --dbms_output.put_line('Inventory unlocked check');
        if l_invrec.Lock_status = 'UnLocked' then
          --dbms_output.put_line('Inventory is Unlocked');
          null;
        else
          --dbms_output.put_line('Inventory is not locked check failed');
          l_suitable := 0;
        end if;
      end if;
    end if;

    /* check if inventory is part of a pick face*/
    if l_suitable != 0 then  
      if l_invrec.pick_face = 'F' or l_invrec.pick_face = 'D' then
        --dbms_output.put_line('Inventory is part of a pick face');
        l_suitable := 0;
      end if;
    end if;

    /*check if whole inventory is required*/
    if l_suitable != 0 then  
      --dbms_output.put_line('Inventory is not part of a pick face');
      if l_invrec.qty_on_hand <= p_qty_to_move then
        --dbms_output.put_line('Existing pick task requires the whole tag');
        l_suitable := 0;
      end if;
    end if;

    /*check if relocates or replenishments already exist for inventory record*/
    if l_suitable != 0 then  
      --dbms_output.put_line('Existing pick task does not require the whole tag');
      open  c_checkothertasks;
      fetch c_checkothertasks into l_tasks;
      close c_checkothertasks;
      if l_tasks != 0 then
        --dbms_output.put_line('Another relocate for inventory exist');
        l_suitable := 0;
      end if;
    end if;

    if l_suitable != 0 then
      --dbms_output.put_line('No other relocate for inventory exist');
      open    c_checkforpickface;
      fetch   c_checkforpickface into l_pface;
      close   c_checkforpickface;
      if l_pface != 0 then
        --dbms_output.put_line('The to zone of sku relocation record is the same as zone from pick face of that SKU');
        l_suitable := 0;
      end if;
    end if;

    if l_suitable != 0 then
      dbms_output.put_line('inventory record passed all tests');
    end if;

    return l_suitable;

  end checkinvsuitable_f;                              
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Create sku relocation tasks
-- Description: Create relocation tasks as if it where replenishments.
------------------------------------------------------------------------------------------------
  procedure cnl_ProcessSkuRelocation_p (p_siteid in dcsdba.sku_relocation.site_id%type)
  is
    cursor c_skurelocsearch ( p_siteid in dcsdba.sku_relocation.site_id%type)
    is
      select    client_id
      ,         site_id
      ,         sku_id
      ,         condition_id
      ,         origin_id
      ,         owner_id
      ,         to_zone
      ,         to_subzone_1
      ,         to_subzone_2
      ,         from_zone
      ,         from_subzone_1
      ,         from_subzone_2
      ,         nvl(disallow_tag_swap, 'N') disallow_tag_Swap
      ,         task_id
      ,         allowed_inv_status
      ,         lock_code
      ,         priority
      from      dcsdba.sku_relocation
      where     site_id = p_siteid
      and       task_id = 'SKURELOCATE'
      order by  site_id
      ,         sku_id
      ,         priority
      ,         to_zone;
    --

    cursor c_skumpcktasksearch( p_skuid         dcsdba.sku_relocation.sku_id%type
                              , p_clientid      dcsdba.sku_relocation.client_id%type
                              , p_siteid        dcsdba.sku_relocation.site_id%type
                              , p_conditionid   dcsdba.sku_relocation.condition_id%type
                              , p_originid      dcsdba.sku_relocation.origin_id%type
                              , p_ownerid       dcsdba.sku_relocation.owner_id%type
                              , p_fromzone      dcsdba.sku_relocation.from_zone%type
                              , p_from_subzone1 dcsdba.sku_relocation.from_subzone_1%type
                              , p_from_subzone2 dcsdba.sku_relocation.from_subzone_2%type
                              )
    is
      select    m.*
      from      dcsdba.move_task m
      where     m.client_id             = p_clientid
      and       m.sku_id                = p_skuid
      and       m.site_id               = p_siteid
      and       ( m.condition_id    = p_conditionid or p_conditionid is null)
      and       ( m.origin_id       = p_originid or p_originid is null)
      and       m.owner_id = p_ownerid
      and       m.from_loc_id           = ( select  l.location_id
                                            from    dcsdba.location l
                                            where   l.site_id               = p_siteid
                                            and     l.location_id           = m.from_loc_id
                                            and     ( l.zone_1                = p_fromzone or 
                                                      l.zone_1 like p_fromzone)
                                            and     (nvl(l.subzone_1,'N')    = nvl(p_from_subzone1,'N') or 
                                                     nvl(l.subzone_1,'N')    like p_from_subzone1)
                                            and     (nvl(l.subzone_2,'N')    = nvl(p_from_subzone2,'N') or 
                                                     nvl(l.subzone_2,'N')    like p_from_subzone2)
                                          )
      and       m.task_type             = 'O';
    --
    cursor c_getinvrec (p_key number)
    is
      select  *
      from    dcsdba.inventory
      where   key = p_key;
    --

    l_inv_key     integer;
    l_invrec      c_getinvrec%rowtype;
    l_putsearchok integer;
    l_toloc       varchar2(20);
    l_facekey     number;
    l_createtask  integer;

  begin
    for skurelocrow in c_skurelocsearch(p_siteid) loop
      --dbms_output.put_line('Sku relocation records have been found'||skurelocrow.sku_id);
      transformwildcardsproc(skurelocrow.from_zone);
      transformwildcardsproc(skurelocrow.from_subzone_1);
      transformwildcardsproc(skurelocrow.from_subzone_2);
--      dbms_output.put_line('Any wild cards in the from zones have been transformened');
      /* search for any move task for the sku in the from zone*/
      for skumpcktaskrow in c_skumpcktasksearch ( skurelocrow.sku_id
                                                , skurelocrow.client_id
                                                , skurelocrow.site_id
                                                , skurelocrow.condition_id
                                                , skurelocrow.origin_id
                                                , skurelocrow.owner_id
                                                , skurelocrow.from_zone
                                                , skurelocrow.from_subzone_1
                                                , skurelocrow.from_subzone_2
                                                ) loop
--        dbms_output.put_line('Pick tasks have been found');
 --       dbms_output.put_line(skumpcktaskrow.sku_id);
        l_inv_key := checkinvsuitable_f  ( skumpcktaskrow.tag_id
                                         , skumpcktaskrow.client_id
                                         , skumpcktaskrow.owner_id
                                         , skumpcktaskrow.site_id
                                         , skumpcktaskrow.from_loc_id
                                         , skurelocrow.from_zone
                                         , skumpcktaskrow.sku_id
                                         , skumpcktaskrow.config_id
                                         , skurelocrow.origin_id
                                         , skurelocrow.condition_id
                                         , skurelocrow.allowed_inv_status
                                         , skurelocrow.lock_code
                                         , skumpcktaskrow.qty_to_move
                                         , skurelocrow.to_zone
                                         );
        if l_inv_key != 0 then -- Inventory is ok to process.
          --dbms_output.put_line('inv record has been identified with key ' || l_inv_key);
          open  c_getinvrec (l_inv_key);
          fetch c_getinvrec into l_invrec;
          dcsdba.LibPutSearch.FindPutawayLocation  ( l_putsearchok
                                                   , l_toloc
                                                   , l_facekey
                                                   , l_invrec.client_id
                                                   , l_invrec.sku_id
                                                   , l_invrec.tag_id
                                                   , l_invrec.pallet_id
                                                   , l_invrec.config_id
                                                   , l_invrec.qty_on_hand -- whole pallet/inventory relocation
                                                   , l_invrec.batch_id
                                                   , l_invrec.condition_id
                                                   , l_invrec.site_id
                                                   , l_invrec.location_id
                                                   , 0.0
                                                   , 'N'
                                                   , l_invrec.pallet_volume
                                                   , l_invrec.pallet_height
                                                   , l_invrec.receipt_dstamp
                                                   , l_invrec.owner_id
                                                   , l_invrec.origin_id
                                                   , l_invrec.pallet_depth
                                                   , l_invrec.pallet_width
                                                   , l_invrec.pallet_weight
                                                   , l_invrec.pallet_config
                                                   , l_invrec.user_def_type_1
                                                   , l_invrec.user_def_type_2
                                                   , l_invrec.user_def_type_3
                                                   , l_invrec.user_def_type_4
                                                   , l_invrec.user_def_type_5
                                                   , l_invrec.user_def_type_6
                                                   , l_invrec.user_def_type_7
                                                   , l_invrec.user_def_type_8
                                                   , l_invrec.user_def_chk_1
                                                   , l_invrec.user_def_chk_2
                                                   , l_invrec.user_def_chk_3
                                                   , l_invrec.user_def_chk_4
                                                   , 'Z'
                                                   , l_invrec.lock_code
                                                   , null /* Hazmat	*/
                                                   , null /* InventoryKey	*/
                                                   , l_invrec.ce_receipt_type
                                                   , l_invrec.ce_under_bond
                                                   , skurelocrow.to_zone
                                                   , skurelocrow.to_subzone_1
                                                   , skurelocrow.to_subzone_2
                                                   );
          if l_putsearchok = 1 then -- Location has been found.
  --           dbms_output.put_line('Location has been found to relocate to ' || l_toloc);
             l_createtask := dcsdba.LibMoveTask.CreateMoveTask  ( 'SKURELOCATE'
                                                                , null
                                                                , l_invrec.client_id
                                                                , l_invrec.sku_id
                                                                , l_invrec.tag_id
                                                                , l_invrec.location_id
                                                                , l_toloc
                                                                , 'Released'
                                                                , l_invrec.qty_on_hand
                                                                , l_invrec.config_id
                                                                , l_invrec.description
                                                                , null -- work group
                                                                , null -- consignment
                                                                , 'M'  -- M = relocate
                                                                , null -- container id
                                                                , null -- pallet id
                                                                , null -- Consol Link
                                                                , skurelocrow.priority
                                                                , null -- pick face key
                                                                , 'N'  -- Allocation
                                                                , l_invrec.site_id
                                                                , l_invrec.condition_id
                                                                , null -- FirstKey
                                                                , null -- PalletGrouped
                                                                , null -- Pallet grouped
                                                                , l_invrec.owner_id
                                                                , null -- KitSKUID
                                                                , null -- KitLineID
                                                                , null -- KitLink
                                                                , null -- KitRatio
                                                                , l_invrec.origin_id
                                                                , null -- OriginalDStamp
                                                                , null -- PalletConfig
                                                                , null -- CustomerID
                                                                , null -- DueTaskID
                                                                , null -- DueLineID
                                                                , null -- DueType
                                                                , null -- Repack
                                                                , null -- BOL ID
                                                                , 'N'  -- PickFaceAutoEx
                                                                , null -- PrintLabel
                                                                , null -- OldTaskID
                                                                , 'N'  -- RepackQCDone
                                                                , null -- CatchWeight
                                                                , 'N'  -- UsePickToGrid
                                                                , null -- PickReAllocFlag
                                                                , null -- StageRouteID
                                                                , null -- StageRouteSeq
                                                                , 'N'  -- Labelling
                                                                , null -- PFConsolLink
                                                                , null -- SerialNumber
                                                                , 'N'  -- IgnoreStage
                                                                , null -- ShipmentGroup
                                                                , l_invrec.ce_under_bond
                                                                , null -- KitPlanId
                                                                , null -- PlanSequence
                                                                , null -- ContainerConfig
                                                                , skurelocrow.disallow_tag_swap
                                                                , cerotationid => l_invrec.ce_rotation_id
                                                                , ceavailstatus => l_invrec.ce_avail_status
                                                                );
          end if;
    --      dbms_output.put_line('move task key for relocate create is ' || l_createtask);
          close c_getinvrec;
        end if;
      end loop;
    end loop;
    commit;  
  end cnl_ProcessSkuRelocation_p;

end   cnl_wms_Skureldae_pck;
--show errors;