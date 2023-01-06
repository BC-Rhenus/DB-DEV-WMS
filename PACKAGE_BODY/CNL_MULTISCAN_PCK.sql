CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_MULTISCAN_PCK" is
/********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: back end functionality for multiscan device from Logistore.
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
-- Private constant declarations
--
-- Private variable declarations
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : check if a value is a number
-- Description: 
------------------------------------------------------------------------------------------------
  function is_number (p_string in varchar2)
   return int
  is
   v_new_num number;
  begin
   v_new_num := to_number(p_string);
   return 1;
  exception
    when value_error then
   return 0;
  end is_number;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : validation of client
-- Description: check if client exist. 0 = error, 1 = ok.
------------------------------------------------------------------------------------------------
  function  chk_client_f  (p_client_id in varchar2)
    return integer
  is
    cursor  c_client (a_client_id varchar2)
    is
      ( select  count(client_id) client -- Always 0 or 1.
        from    dcsdba.client 
        where   client_id = upper(a_client_id)
      );
    clt_chk integer;  
  begin
    open    c_client (p_client_id);
    fetch   c_client 
    into    clt_chk;
    close   c_client;

    return  clt_chk;

  end chk_client_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : validation SKU returns 0 or more. 1 = ok, 0 = not ok, more then 1 = not ok.
-- Description: check if inserted ID exists as SKU, EAN, UPC, TUC or supplier SKU for the client.
------------------------------------------------------------------------------------------------
  Function chk_sku_f  ( p_id          in varchar2
                      , p_client_id   in varchar2
                      )
    return integer
  is

  --  count unique number of sku's where p_id exists as ean, upc or sku_id.
    cursor c_sku (  a_id        varchar2
                 ,  a_client_id varchar2
                 )
        is
          select  count(sku_id) sku                                            
          from    dcsdba.sku
          where   client_id = upper(a_client_id)
          and     (   sku_id    = upper(a_id) or  
                      EAN       = upper(a_id) or 
                      UPC       = upper(a_id)
                  );

  -- If c_sku returns 1 then fetch the sku_id.
    cursor c_get_sku_from_sku ( a_id        varchar2
                              , a_client_id varchar2
                              )
        is
          select    sku_id
          from      dcsdba.sku
          where     client_id = upper(a_client_id)
          and       ( sku_id  = upper(a_id) or 
                      ean     = upper(a_id) or 
                      upc     = upper(a_id)
                    );

  -- if c_sku returns 1 then count unique sku from sku_tuc where sku_id is different then fetched sku_id.
    cursor  c_tuc_d (  a_id        varchar2
                    ,  a_client_id varchar2
                    ,  a_sku_id    varchar2 
                    )
        is
            select  count(distinct sku_id) sku 
            from    dcsdba.sku_tuc
            where   client_id =   upper(a_client_id)
            and     tuc       =   upper(a_id)
            and     sku_id    <>  upper(a_sku_id);

    -- If c_tuc_d returns 1 then fetch the sku_id.
    cursor c_get_sku_from_tuc ( a_id        varchar2
                              , a_client_id varchar2
                              )
        is
            select    distinct sku_id
            from      dcsdba.sku_tuc
            where     client_id = upper(a_client_id)
            and       tuc       = upper(a_id);

  -- if c_sku returns 0 count number of unique sku_id that exist with this id.
    cursor  c_tuc (  a_id        varchar2
                  ,  a_client_id varchar2
                  )
        is
            select  count(distinct sku_id) sku 
            from    dcsdba.sku_tuc
            where   client_id = upper(a_client_id)
            and     tuc       = upper(a_id);

  --  if 1 sku has been found so far count all other unique sku_id from supplier sku.
    cursor  c_sup_sku_d (  a_id         varchar2
                        ,  a_client_id  varchar2
                        ,  a_sku_id     varchar2
                        )
        is
            select  count(sku_id) sku
            from    dcsdba.supplier_sku
            where   client_id       =   upper(a_client_id)
            and     supplier_sku_id =   upper(a_id)
            and     sku_id          <>  upper(a_sku_id);

  --
    cursor  c_sup_sku (  a_id        varchar2
                      ,  a_client_id varchar2
                      )
        is
            select  count(sku_id) sku -- always 0 or 1
            from    dcsdba.supplier_sku
            where   client_id       = upper(a_client_id)
            and     supplier_sku_id = upper(a_id);

  --
    l_sku_count integer;
    l_tuc_count integer;
    l_sup_count integer;
    l_sku_sku   varchar2(50);
    l_tuc_sku   varchar2(50);

    l_result  integer;
    l_total   integer;
  --
  begin
    l_sku_count := 0;
    l_tuc_count := 0;
    l_sup_count := 0;
    l_sku_sku   := 'N';
    l_tuc_sku   := 'N';
    l_result    := 0;
    l_total     := 0;

    open  c_sku (p_id, p_client_id);                                            -- Count number of sku's with sku_id or ean or upc equal to p_id
    fetch c_sku                                                                 -- 0 found is ok. 1 found is ok. more then one is not ok.
    into  l_result;
    close c_sku;
    l_sku_count := l_result;
    if l_sku_count = 1 then
      open  c_get_sku_from_sku (p_id, p_client_id);                             -- Gets the SKU id that belongs to the ID entered when just one has been found
      fetch c_get_sku_from_sku
      into  l_sku_sku;
      close c_get_sku_from_sku;
    end if;

    if l_sku_sku = 'N' and l_sku_count = 0 then                                 -- No sku was found in SKU table using ID so now we search for a TUC.
      open  c_tuc (p_id, p_client_id);                                          -- 0 found is ok. 1 found is ok. more then one found is not ok.
      fetch c_tuc 
      into  l_result;
      close c_tuc;
      l_tuc_count := l_result;
    elsif   l_sku_sku != 'N' and l_sku_count = 1 then                           -- If a SKU was found in SKU table we look for any other SKU that uses the same ID as TUC.
      open  c_tuc_d (p_id, p_client_id, l_sku_sku);                             -- 0 found is ok. 1 found is not ok.
      fetch c_tuc_d 
      into  l_result;
      close c_tuc_d;
      l_tuc_count := l_result;
    end if;

    if l_tuc_count = 1 then                                                     -- If one TUC was found we fetch the SKU id from tuc_sku.
      open  c_get_sku_from_tuc (p_id, p_client_id);
      fetch c_get_sku_from_tuc
      into  l_tuc_sku;
      close c_get_sku_from_tuc;
    end if;

    if l_sku_sku = 'N' and l_tuc_sku = 'N' and l_tuc_count = 0 and l_sku_count = 0 then -- nothing found so far so look for any supplier SKU.
      open  c_sup_sku (p_id, p_client_id);
      fetch c_sup_sku 
      into  l_result;
      close c_sup_sku;
      l_sup_count := l_result;
    elsif l_sku_sku = 'N' and l_tuc_sku != 'N' and l_tuc_count = 1 and l_sku_count = 0 then -- found a TUC SKU and no SKU with the id.
      open  c_sup_sku_d (p_id, p_client_id, l_tuc_sku);                                     -- Check if a supplier sku exist with the same id but then for another SKU id.
      fetch c_sup_sku_d
      into  l_result;
      close c_sup_sku_d;
      l_sup_count := l_result;
    elsif l_sku_sku != 'N' and l_tuc_sku = 'N' and l_tuc_count = 0 and l_sku_count = 1 then -- found a SKU but no tuc SKU. in sku so see if also an supplier sku exists for a different SKU.
      open  c_sup_sku_d (p_id, p_client_id, l_sku_sku);                                     -- Check if a supplier sku exist with the same id but then for another SKU id.
      fetch c_sup_sku_d
      into  l_result;
      close c_sup_sku_d;
      l_sup_count := l_result;
    end if;

    l_total := l_sku_count + l_tuc_count + l_sup_count;

    -- 0 means no match found operator can't continue
    -- 1 means match is found operator can continue.
    -- >1 means multiple matches found operator must enter the exact SKU id before he can continue.

    return l_total;

  end chk_sku_f;
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Process data 
-- Description: processes the data received from the multiscan device or WMS
------------------------------------------------------------------------------------------------
  Function get_clnt_trck_lvl_f (p_client_id varchar2)
    return varchar2
  is
    cursor c_trck (a_client_id varchar2)
    is
      select  track_level
      from    dcsdba.sku_config_tracking_level
      where   client_id = upper(a_client_id)
      and     track_level != 'HALFPALL';
    --
    l_result varchar2(1000);
  begin
      for r in c_trck(p_client_id) loop
        if l_result is null then
          l_result := r.track_level;
        else
          l_result := l_result||','||r.track_level;
        end if;
      end loop;
/*      if l_result is null then
        l_result := 'NO-TRACK-LEVEL';
      else
        null;
      end if;*/
    return l_result;
  end get_clnt_trck_lvl_f;
------------------------------------------------------------------------------------------------
-- Author      : M. Swinkels, 10-01-2017
-- Purpose     : To capture the actual sku id.
-- Description : When the id entered is unique fetch the actual SKU id.
--               required for updating the SKU and creating and linking the pack configuration.
------------------------------------------------------------------------------------------------
  function get_sku_f   ( p_id          in varchar2
                       , p_client_id   in varchar2
                       )
    return varchar2
  is

    cursor c_sku (  a_id        varchar2
                 ,  a_client_id varchar2
                 )
    is
      select  sku_id
      from    dcsdba.sku
      where   client_id = upper(a_client_id)
      and     (sku_id    = upper(a_id) or ean = upper(a_id) or upc = upper(a_id));
    --
    cursor  c_tuc (  a_id        varchar2
                  ,  a_client_id varchar2
                  )
    is
      select  sku_id
      from    dcsdba.sku_tuc
      where   client_id = upper(a_client_id)
      and     tuc       = upper(a_id);
    --
    cursor  c_sup_sku (  a_id        varchar2
                      ,  a_client_id varchar2
                      )
    is
      select  sku_id
      from    dcsdba.supplier_sku
      where   client_id       = upper(a_client_id)
      and     supplier_sku_id = upper(a_id);
    --
    l_sku   varchar2(50);

  begin
    l_sku := null;

    open  c_sku (p_id, p_client_id);
    fetch c_sku into  l_sku;
    if c_sku%notfound then      
      l_sku := null;
    end if;
    close c_sku;

    if l_sku is null then
      open  c_tuc (p_id, p_client_id);
      fetch c_tuc into  l_sku;
      if c_tuc%notfound then
        l_sku := null;
      end if;
      close c_tuc;
    end if;

    if l_sku is null then
      open  c_sup_sku (p_id, p_client_id);
      fetch c_sup_sku into  l_sku;
      if c_sup_sku%notfound then
        l_sku := p_id;
      end if;
      close c_sup_sku;
    end if;

    return l_sku;

  end get_sku_f;
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : check weight
-- Description: Run a check to see if the weight of each tracking level matches with the previous.
--              The margin is determined by the weight of the lowest tracking level.
--              <= 0.005  Kg then no check.
--              <= 0.010  Kg 20% margin
--              <= 0.020  Kg 10% margin
--              >  0.020  Kg  5% margin
------------------------------------------------------------------------------------------------
  function check_weight_f  (  p_weight_1      number
                           ,  p_weight_2      number
                           ,  p_weight_3      number
                           ,  p_weight_4      number
                           ,  p_weight_5      number
                           ,  p_weight_6      number
                           ,  p_weight_7      number
                           ,  p_weight_8      number
                           ,  p_ratio_1_to_2  number
                           ,  p_ratio_2_to_3  number
                           ,  p_ratio_3_to_4  number
                           ,  p_ratio_4_to_5  number
                           ,  p_ratio_5_to_6  number
                           ,  p_ratio_6_to_7  number
                           ,  p_ratio_7_to_8  number
                           )
    return integer
    is
      l_result integer;
  begin
    l_result := 1;
    if nvl(p_weight_1,0.00001) <= 0.005 then --5g no check.
      null;
    elsif nvl(p_weight_1,0.00001) <= 0.01 then --Between 5 and 10g 20% margin
            --Check weight between track level 1 and 2
            if nvl(p_weight_2,0) <> 0 and nvl(p_ratio_1_to_2,0) <> 0 then
              if p_weight_1 * p_ratio_1_to_2 > ((p_weight_2/100)*120) then --20% margin
                l_result := 0;
              else  
                l_result := 1;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 2 and 3
              if nvl(p_weight_3,0) <> 0 and nvl(p_ratio_2_to_3,0) <> 0 then
                if p_weight_2 * p_ratio_2_to_3 > ((p_weight_3/100)*120) then --20% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 3 and 4
              if nvl(p_weight_4,0) <> 0 and nvl(p_ratio_3_to_4,0) <> 0 then
                if p_weight_3 * p_ratio_3_to_4 > ((p_weight_4/100)*120) then --20% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 4 and 5
              if nvl(p_weight_5,0) <> 0 and nvl(p_ratio_4_to_5,0) <> 0 then
                if p_weight_4 * p_ratio_4_to_5 > ((p_weight_5/100)*120) then --20% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 5 and 6
              if nvl(p_weight_6,0) <> 0 and nvl(p_ratio_5_to_6,0) <> 0 then
                if p_weight_5 * p_ratio_5_to_6 > ((p_weight_6/100)*120) then --20% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 6 and 7
              if nvl(p_weight_7,0) <> 0 and nvl(p_ratio_6_to_7,0) <> 0 then
                if p_weight_6 * p_ratio_6_to_7 > ((p_weight_7/100)*120) then --20% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 7 and 8
              if nvl(p_weight_8,0) <> 0 and nvl(p_ratio_7_to_8,0) <> 0 then
                if p_weight_7 * p_ratio_7_to_8 > ((p_weight_8/100)*120) then --20% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;
    elsif nvl(p_weight_1,0.00001) <= 0.02 then --Between 10 and 20g 10% margin
            --Check weight between track level 1 and 2
            if nvl(p_weight_2,0) <> 0 and nvl(p_ratio_1_to_2,0) <> 0 then
              if p_weight_1 * p_ratio_1_to_2 > ((p_weight_2/100)*110) then --10% margin
                l_result := 0;
              else  
                l_result := 1;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 2 and 3
              if nvl(p_weight_3,0) <> 0 and nvl(p_ratio_2_to_3,0) <> 0 then
                if p_weight_2 * p_ratio_2_to_3 > ((p_weight_3/100)*110) then --10% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 3 and 4
              if nvl(p_weight_4,0) <> 0 and nvl(p_ratio_3_to_4,0) <> 0 then
                if p_weight_3 * p_ratio_3_to_4 > ((p_weight_4/100)*110) then --10% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 4 and 5
              if nvl(p_weight_5,0) <> 0 and nvl(p_ratio_4_to_5,0) <> 0 then
                if p_weight_4 * p_ratio_4_to_5 > ((p_weight_5/100)*110) then --10% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 5 and 6
              if nvl(p_weight_6,0) <> 0 and nvl(p_ratio_5_to_6,0) <> 0 then
                if p_weight_5 * p_ratio_5_to_6 > ((p_weight_6/100)*110) then --10% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 6 and 7
              if nvl(p_weight_7,0) <> 0 and nvl(p_ratio_6_to_7,0) <> 0 then
                if p_weight_6 * p_ratio_6_to_7 > ((p_weight_7/100)*110) then --10% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 7 and 8
              if nvl(p_weight_8,0) <> 0 and nvl(p_ratio_7_to_8,0) <> 0 then
                if p_weight_7 * p_ratio_7_to_8 > ((p_weight_8/100)*110) then --10% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;
    elsif nvl(p_weight_1,0.00001) > 0.02 then --more then 20g 5% margin
            --Check weight between track level 1 and 2
            if nvl(p_weight_2,0) <> 0 and nvl(p_ratio_1_to_2,0) <> 0 then
              if p_weight_1 * p_ratio_1_to_2 > ((p_weight_2/100)*105) then --5% margin
                l_result := 0;
              else  
                l_result := 1;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 2 and 3
              if nvl(p_weight_3,0) <> 0 and nvl(p_ratio_2_to_3,0) <> 0 then
                if p_weight_2 * p_ratio_2_to_3 > ((p_weight_3/100)*105) then --5% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 3 and 4
              if nvl(p_weight_4,0) <> 0 and nvl(p_ratio_3_to_4,0) <> 0 then
                if p_weight_3 * p_ratio_3_to_4 > ((p_weight_4/100)*105) then --5% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 4 and 5
              if nvl(p_weight_5,0) <> 0 and nvl(p_ratio_4_to_5,0) <> 0 then
                if p_weight_4 * p_ratio_4_to_5 > ((p_weight_5/100)*105) then --5% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 5 and 6
              if nvl(p_weight_6,0) <> 0 and nvl(p_ratio_5_to_6,0) <> 0 then
                if p_weight_5 * p_ratio_5_to_6 > ((p_weight_6/100)*105) then --5% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 6 and 7
              if nvl(p_weight_7,0) <> 0 and nvl(p_ratio_6_to_7,0) <> 0 then
                if p_weight_6 * p_ratio_6_to_7 > ((p_weight_7/100)*105) then --5% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if;

            if l_result = 1 then
              --Check weight between track level 7 and 8
              if nvl(p_weight_8,0) <> 0 and nvl(p_ratio_7_to_8,0) <> 0 then
                if p_weight_7 * p_ratio_7_to_8 > ((p_weight_8/100)*105) then --5% margin
                  l_result := 0;
                else  
                  l_result := 1;
                end if;
              end if;
            end if; 
    end if;
    -- 1 = OK
    -- 0 = Failed

    return l_result;

  end check_weight_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : check if tracking levels are not used twice
-- Description: checks all tracking levels for any duplicates
------------------------------------------------------------------------------------------------
  function check_track_level_f  ( p_trck_lvl_1  varchar2
                                , p_trck_lvl_2  varchar2
                                , p_trck_lvl_3  varchar2
                                , p_trck_lvl_4  varchar2
                                , p_trck_lvl_5  varchar2
                                , p_trck_lvl_6  varchar2
                                , p_trck_lvl_7  varchar2
                                , p_trck_lvl_8  varchar2
                                )
  return integer
  is
    l_result integer;
  begin
    if  p_trck_lvl_1 = nvl(p_trck_lvl_2,0) or 
        p_trck_lvl_1 = nvl(p_trck_lvl_3,0) or 
        p_trck_lvl_1 = nvl(p_trck_lvl_4,0) or 
        p_trck_lvl_1 = nvl(p_trck_lvl_5,0) or
        p_trck_lvl_1 = nvl(p_trck_lvl_6,0) or
        p_trck_lvl_1 = nvl(p_trck_lvl_7,0) or
        p_trck_lvl_1 = nvl(p_trck_lvl_8,0) or
        p_trck_lvl_2 = nvl(p_trck_lvl_3,0) or
        p_trck_lvl_2 = nvl(p_trck_lvl_4,0) or
        p_trck_lvl_2 = nvl(p_trck_lvl_5,0) or
        p_trck_lvl_2 = nvl(p_trck_lvl_6,0) or
        p_trck_lvl_2 = nvl(p_trck_lvl_7,0) or
        p_trck_lvl_2 = nvl(p_trck_lvl_8,0) or
        p_trck_lvl_3 = nvl(p_trck_lvl_4,0) or
        p_trck_lvl_3 = nvl(p_trck_lvl_5,0) or
        p_trck_lvl_3 = nvl(p_trck_lvl_6,0) or
        p_trck_lvl_3 = nvl(p_trck_lvl_7,0) or
        p_trck_lvl_3 = nvl(p_trck_lvl_8,0) or
        p_trck_lvl_4 = nvl(p_trck_lvl_5,0) or
        p_trck_lvl_4 = nvl(p_trck_lvl_6,0) or
        p_trck_lvl_4 = nvl(p_trck_lvl_7,0) or
        p_trck_lvl_4 = nvl(p_trck_lvl_8,0) or
        p_trck_lvl_5 = nvl(p_trck_lvl_6,0) or
        p_trck_lvl_5 = nvl(p_trck_lvl_7,0) or
        p_trck_lvl_5 = nvl(p_trck_lvl_8,0) or
        p_trck_lvl_6 = nvl(p_trck_lvl_7,0) or
        p_trck_lvl_6 = nvl(p_trck_lvl_8,0) or
        p_trck_lvl_7 = nvl(p_trck_lvl_8,0) then
      l_result := 0;
    else
      l_result := 1;
    end if;
  return l_result;
  end check_track_level_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Checks the first tracking level.
-- Description: check if first tracking level is the same in already existing conifgs if any
--              The WMS interface checks if the first tracking level is the same as the first 
--              tracking level of an already linked pack configuration.
--              To prevent the error ending up in the interface we notify the operator and stop 
--              the process.
------------------------------------------------------------------------------------------------
  function check_first_track_level_f  ( p_client_id   varchar2
                                      , p_id          varchar2
                                      , p_trck_lvl_1  varchar2
                                      )
  return integer
  is
    -- check if any config already exists for SKU
    cursor chk_c (  a_client_id varchar2
                 ,  a_sku       varchar2
                 )
     is
     select count(*)
     from   dcsdba.sku_sku_config ssc
     where  ssc.client_id = a_client_id
     and    ssc.sku_id = a_sku;
    -- get the first tracking level from the already existing config(s)
    cursor get_tl_c ( a_client_id   varchar2
                    , a_sku         varchar2
                    )
      is
      select  sc.track_level_1
      from    dcsdba.sku_config sc
      where   sc.client_id = a_client_id
      and     sc.config_id = (select  ssc.config_id
                              from    dcsdba.sku_sku_config ssc
                              where   ssc.client_id = a_client_id
                              and     ssc.sku_id = a_sku
                              and     rownum = 1
                             );
    --
    l_count   integer;
    l_first   varchar2(8);
    l_sku     varchar2(30);
    l_result  integer; -- 1 is OK 0 is not OK.
  begin
    l_result := 1;
    l_sku := get_sku_f(upper(p_id), upper(p_client_id));
    open   chk_c (p_client_id, l_sku);
    fetch  chk_c into l_count;
    close  chk_c;
    if l_count = 0 then
      null;
    else 
      open  get_tl_c (p_client_id, l_sku);
      fetch get_tl_c into l_first;
      close get_tl_c;
      if l_first != p_trck_lvl_1 then
        l_result := 0;
      else 
        null;
      end if;
    end if;
  return l_result;
  end check_first_track_level_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Double check the tracking levels used. 
-- Description: The multiscan software can have problems and so a double check is done to ensure
--              only tracking levels are used that exist for the client.
------------------------------------------------------------------------------------------------
Function dbl_chk_trck_lvl_f ( p_client_id   varchar2
                              , p_trck_lvl_1  varchar2
                              , p_trck_lvl_2  varchar2
                              , p_trck_lvl_3  varchar2
                              , p_trck_lvl_4  varchar2
                              , p_trck_lvl_5  varchar2
                              , p_trck_lvl_6  varchar2
                              , p_trck_lvl_7  varchar2
                              , p_trck_lvl_8  varchar2) 
    return integer
  is
    cursor c_trck ( a_client_id varchar2
                  , a_trck_lvl  varchar2
                  )
    is
      select  count(*)
      from    dcsdba.sku_config_tracking_level
      where   client_id = upper(a_client_id)
      and     track_level = upper(a_trck_lvl);
    --
    l_tmp     integer;
    l_result  integer;

  begin
    l_tmp := 1;
    --dbms_output.put_line('0' || l_tmp);
    if p_trck_lvl_8 is not null then
      open  c_trck (p_client_id, p_trck_lvl_8);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(8 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_7 is not null then
      open  c_trck (p_client_id, p_trck_lvl_7);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(7 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_6 is not null then
      open  c_trck (p_client_id, p_trck_lvl_6);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(6 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_5 is not null then
      open  c_trck (p_client_id, p_trck_lvl_5);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(5 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_4 is not null then
      open  c_trck (p_client_id, p_trck_lvl_4);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(4 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_3 is not null then
      open  c_trck (p_client_id, p_trck_lvl_3);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(3 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_2 is not null then
      open  c_trck (p_client_id, p_trck_lvl_2);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(2 || l_tmp);
    end if;

    if l_tmp = 1 and p_trck_lvl_1 is not null then
      open  c_trck (p_client_id, p_trck_lvl_1);
      fetch c_trck into l_tmp;
      close c_trck;
      --dbms_output.put_line(1 || l_tmp);
    end if;

    l_result := l_tmp;
    return l_result;
  end dbl_chk_trck_lvl_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Sanity check
-- Description: combine all checks into one result.
------------------------------------------------------------------------------------------------
  procedure sanity_check_p (  p_weight_1      in number
                           ,  p_weight_2      in number
                           ,  p_weight_3      in number
                           ,  p_weight_4      in number
                           ,  p_weight_5      in number
                           ,  p_weight_6      in number
                           ,  p_weight_7      in number
                           ,  p_weight_8      in number
                           ,  p_ratio_1_to_2  in number
                           ,  p_ratio_2_to_3  in number
                           ,  p_ratio_3_to_4  in number
                           ,  p_ratio_4_to_5  in number
                           ,  p_ratio_5_to_6  in number
                           ,  p_ratio_6_to_7  in number
                           ,  p_ratio_7_to_8  in number
                           ,  p_trck_lvl_1    in varchar2
                           ,  p_trck_lvl_2    in varchar2
                           ,  p_trck_lvl_3    in varchar2
                           ,  p_trck_lvl_4    in varchar2
                           ,  p_trck_lvl_5    in varchar2
                           ,  p_trck_lvl_6    in varchar2
                           ,  p_trck_lvl_7    in varchar2
                           ,  p_trck_lvl_8    in varchar2
                           ,  p_client_id     in varchar2
                           ,  p_id            in varchar2
                           ,  p_check         out integer
                           ,  p_message       out varchar
                           )
    is
      v_check             integer;
      v_message           varchar2(1000);
      l_ratio_1_check     integer;                                               
      l_weight_check      integer;                                              -- result of weight check
      l_trck_lvl_check    integer;                                              -- result of tracking levels check
      l_check_first_tl    integer;                                              -- result of first tracking level check

    begin
      v_check   := 1;
      -- v_message := 'Data processed successfully';

      -- check if ratio is filled.
      if      nvl(p_ratio_1_to_2,0) = 0 and nvl(p_trck_lvl_2,'X') != 'X'
      then    v_check   := 0;
              v_message := 'First ratio is missing';
      else    null;
      end if;

      -- Check if tracking level used are allowed for customer.
      if      v_check = 1 
      then    v_check := dbl_chk_trck_lvl_f(p_client_id, p_trck_lvl_1, p_trck_lvl_2, p_trck_lvl_3, p_trck_lvl_4, p_trck_lvl_5, p_trck_lvl_6, p_trck_lvl_7, p_trck_lvl_8);
              if      v_check = 0 
              then    v_message := 'A tracking level is used that is not allowed for the selected client';
              else    null;
              end if;
      else    null;
      end if;

      -- Check if weights are accoring ratio and within margin. 
      if      v_check = 1
      then    v_check := check_weight_f( p_weight_1, p_weight_2, p_weight_3, p_weight_4, p_weight_5, p_weight_6, p_weight_7, p_weight_8, p_ratio_1_to_2, p_ratio_2_to_3, p_ratio_3_to_4, p_ratio_4_to_5, p_ratio_5_to_6, p_ratio_6_to_7, p_ratio_7_to_8);
              if      v_check = 0
              then    v_message := 'Weights are not according ratio';
              else    null;
              end if;
      else    null;
      end if;

      if      v_check = 1
      then    v_check := check_track_level_f ( p_trck_lvl_1, p_trck_lvl_2, p_trck_lvl_3, p_trck_lvl_4, p_trck_lvl_5, p_trck_lvl_6, p_trck_lvl_7, p_trck_lvl_8);
              if      v_check = 0
              then    v_message := 'a tracking level is used twice';
              else    null;
              end if;
      else    null;
      end if;

      -- Check if lowest tracking level is the same as the lowest tracking level from already linked pack configurations
      if      v_check = 1
      then    v_check := check_first_track_level_f(  upper(p_client_id), upper(p_id), p_trck_lvl_1);
              if      v_check = 0
              then    v_message := 'first tracking level used is different from existing config';
              else    null;
              end if;
      else    null;
      end if;

      p_message := v_message;
      p_check   := v_check;
  end sanity_check_p;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : What is the name of the highest tracking level used.
-- Description: get the name of the highest tracking level used for creating a good pack config note.
------------------------------------------------------------------------------------------------
  function get_h_trcklvl_f    ( p_trck_lvl_1 varchar2
                              , p_trck_lvl_2 varchar2
                              , p_trck_lvl_3 varchar2
                              , p_trck_lvl_4 varchar2
                              , p_trck_lvl_5 varchar2
                              , p_trck_lvl_6 varchar2
                              , p_trck_lvl_7 varchar2
                              , p_trck_lvl_8 varchar2
                              )
    return varchar2
  is
    l_h_trck_lvl  varchar2(8);
  begin
    case
      when p_trck_lvl_8 is not null then l_h_trck_lvl := p_trck_lvl_8;
      when p_trck_lvl_7 is not null then l_h_trck_lvl := p_trck_lvl_7;
      when p_trck_lvl_6 is not null then l_h_trck_lvl := p_trck_lvl_6;
      when p_trck_lvl_5 is not null then l_h_trck_lvl := p_trck_lvl_5;
      when p_trck_lvl_4 is not null then l_h_trck_lvl := p_trck_lvl_4;
      when p_trck_lvl_3 is not null then l_h_trck_lvl := p_trck_lvl_3;
      when p_trck_lvl_2 is not null then l_h_trck_lvl := p_trck_lvl_2;
      when p_trck_lvl_1 is not null then l_h_trck_lvl := p_trck_lvl_1;
    end case;
    return l_h_trck_lvl;
  end get_h_trcklvl_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : create notes description
-- Description: Create a notes description for the pack configuration.
------------------------------------------------------------------------------------------------
  function get_scf_not_f ( p_pallet_type  varchar2
                         , p_sku_id       varchar2
                         , p_trck_lvl     varchar2
                         )
    return varchar2
  is
    l_notes varchar2(80);
  begin
    if p_pallet_type = 'EURO' or p_pallet_type = 'BLOK' then
      l_notes := 'A pack configuration for a '||p_pallet_type||' pallet with SKU '||upper(p_sku_id);
    elsif p_pallet_type = 'OTHER' then
      l_notes := 'A pack configuration for a pallet with SKU '||upper(p_sku_id);
    else
      l_notes := 'A pack configuration for a '||p_trck_lvl||' with SKU '||upper(p_sku_id);
    end if;
    return l_notes;
  end get_scf_not_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Gets the default pallet height of a client.
-- Description: Every customer has a different default pallet height. eg. Infinitas 120 vs Peli 185
------------------------------------------------------------------------------------------------
  function get_def_hgt_f (p_client_id varchar2)
    return varchar2
  is
    cursor c_def_pal_height (a_client_id varchar2)
    is
      select  nvl(division_contact_fax,1.8)
      from    dcsdba.client
      where   client_id = upper(p_client_id);

      l_clt_def_pal_hgt   varchar2(50); -- Value from client can be different then a number and needs to be checked first.
      l_result number;
  begin
    open  c_def_pal_height (p_client_id);
    fetch c_def_pal_height into l_clt_def_pal_hgt;
    close c_def_pal_height;

    if is_number(l_clt_def_pal_hgt) = 1 then 
      l_result := to_number(l_clt_def_pal_hgt);
    else
      l_result := 1.8;
    end if;

    return l_result;
  end get_def_hgt_f;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : get layer height, each per layer and number of layers
-- Description: calculates layer height, each per layer and corrects pallet height based on calculation result.
------------------------------------------------------------------------------------------------
  procedure get_epl_lh_ph_p   ( p_tot_each_i            in  number  -- total each on the pallet
                              , p_min_layer_height_i    in  number  -- Usually box height
                              , p_est_pallet_height_i   in  number  -- Calculated pallet height or inserted pallet height.
                              , p_each_per_layer_o      out number  -- Total each per layer 
                              , p_pallet_height_o       out number  -- Corrected and checked pallet height
                              , p_layer_height_o        out number  -- Calulated or checked layer height
                              , p_num_layers_o          out number  -- number of layers on a pallet 
                              )
  is
    l_each_per_layer    number := 1;                                --  We always start with 1 each per layer
    l_chk				        number;
    l_est_pallet_height number;
    l_pallet_height     number;
    l_layer_height      number;
  begin
    if p_est_pallet_height_i < p_min_layer_height_i then
      l_est_pallet_height := p_min_layer_height_i;
    else
      l_est_pallet_height := p_est_pallet_height_i;
    end if;
    p_layer_height_o := 0;
    while p_layer_height_o < p_min_layer_height_i                   -- Recalculate as long as the layer height is not equal or lower then the minimum layer height
    loop
      while mod(p_tot_each_i,l_each_per_layer) <> 0                 -- As long as total each on pallet can't be devided by each per layer continue searching for a new each per layer.
      loop  
        l_each_per_layer := l_each_per_layer + 1;
      end loop;
      p_each_per_layer_o    := l_each_per_layer;
      l_layer_height        := round(l_est_pallet_height/(p_tot_each_i/l_each_per_layer),6);
      p_layer_height_o      := l_layer_height;
      l_pallet_height       := round(round(l_est_pallet_height/(p_tot_each_i/l_each_per_layer),6)*(p_tot_each_i/l_each_per_layer),6);
      p_pallet_height_o     := l_pallet_height;
      p_num_layers_o        := ceil(l_pallet_height/l_layer_height);
      l_each_per_layer      := l_each_per_layer + 1;
    end loop;
  end get_epl_lh_ph_p;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Create config id
-- Description: create a pack configuration id based on logic.
------------------------------------------------------------------------------------------------
  function create_config_id_f ( p_trck_lvl_1  varchar2
                              , p_trck_lvl_2  varchar2
                              , p_trck_lvl_3  varchar2
                              , p_trck_lvl_4  varchar2
                              , p_trck_lvl_5  varchar2
                              , p_trck_lvl_6  varchar2
                              , p_trck_lvl_7  varchar2
                              , p_trck_lvl_8  varchar2
                              , p_pal_type    varchar2
                              , p_num_layers  number
                              )
    return varchar2
  is
    l_max_trck_lvl  varchar2(8);
    l_config        varchar2(15);
    l_num_layers    number;
  begin
    if p_num_layers > 99 then
      l_num_layers := 99;
    else 
      l_num_layers := p_num_layers;
    end if;
    case
      when p_trck_lvl_8 is not null then l_max_trck_lvl := p_trck_lvl_8;
      when p_trck_lvl_7 is not null then l_max_trck_lvl := p_trck_lvl_7;
      when p_trck_lvl_6 is not null then l_max_trck_lvl := p_trck_lvl_6;
      when p_trck_lvl_5 is not null then l_max_trck_lvl := p_trck_lvl_5;
      when p_trck_lvl_4 is not null then l_max_trck_lvl := p_trck_lvl_4;
      when p_trck_lvl_3 is not null then l_max_trck_lvl := p_trck_lvl_3;
      when p_trck_lvl_2 is not null then l_max_trck_lvl := p_trck_lvl_2;
      when p_trck_lvl_1 is not null then l_max_trck_lvl := p_trck_lvl_1;
    end case;

    if l_max_trck_lvl = 'PALLET' then

      l_config := 'L'||l_num_layers||'PAL'||substr(p_pal_type,1,2)||cnl_multiscan_seq1.nextval;
    else
      l_config := substr(l_max_trck_lvl,1,5)||cnl_multiscan_seq1.nextval;
    end if;

   return l_config;

  end create_config_id_f;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 28-Aug-2017
-- Purpose : Get error text
------------------------------------------------------------------------------------------------ 
  function get_errortext ( p_mergeerror  varchar2
                         )
    return varchar2
  is
    cursor c_err ( a_label       varchar2
                 )
    is
      select  text
      from    dcsdba.language_text
      where   label = p_mergeerror
      and     language = 'EN_GB'
      and     rownum = 1;
    --

    l_retval varchar2(270);
  begin
      open  c_err(p_mergeerror);
      fetch c_err into l_retval;
      if c_err%notfound then
        l_retval := 'No Error text found';
      end if;
      close c_err;
      l_retval := p_mergeerror || '-' || l_retval;
      return l_retval;
  end get_errortext;
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : update SKU
-- Description: processes the data to update the SKU in WMS only when the new product flag is set.
------------------------------------------------------------------------------------------------
  procedure upd_sku_p ( p_result        in out integer
                      , p_message       in out varchar2
                      , p_sku_id        in varchar2
                      , p_client_id     in varchar2
                      , p_each_depth    in varchar2
                      , p_each_width    in varchar2
                      , p_each_height   in varchar2
                      , p_each_weight   in varchar2
                      , p_each_volume   in number
                      , p_packed_weight  in number
                      , p_packed_volume  in number
                      , p_user          in varchar2
                      )
  is
    cursor c (a_sku varchar2, a_client varchar2)
      is
      select  *
      from    dcsdba.sku s
      where   s.sku_id = a_sku
      and     s.client_id = a_client;
    --
    l_sku     c%rowtype;
    l_retval  integer;
    --
  begin
    open c (p_sku_id, p_client_id);
    fetch c into l_sku;
    l_retval:= dcsdba.libmergesku.directsku( p_mergeerror             => p_message
                                           , p_toupdatecols           => null
                                           , p_mergeaction            => 'U'
                                           , p_clientid               => p_client_id
                                           , p_skuid                  => p_sku_id
                                           , p_ean                    => l_sku.ean
                                           , p_upc                    => l_sku.upc
                                           , p_description            => l_sku.description
                                           , p_productgroup           => l_sku.product_group
                                           , p_eachheight             => p_each_height
                                           , p_eachweight             => p_each_weight
                                           , p_eachvolume             => p_each_volume
                                           , p_eachvalue              => l_sku.each_value
                                           , p_qcstatus               => l_sku.qc_status
                                           , p_shelflife              => l_sku.shelf_life
                                           , p_qcfrequency            => l_sku.qc_frequency
                                           , p_splitlowest            => l_sku.split_lowest
                                           , p_conditionreqd          => l_sku.condition_reqd
                                           , p_expiryreqd             => l_sku.expiry_reqd
                                           , p_originreqd             => l_sku.origin_reqd
                                           , p_serialatpack           => l_sku.serial_at_pack
                                           , p_serialatpick           => l_sku.serial_at_pick
                                           , p_serialatreceipt        => l_sku.serial_at_receipt
                                           , p_serialrange            => l_sku.serial_range
                                           , p_serialformat           => l_sku.serial_format
                                           , p_serialvalidmerge       => l_sku.serial_valid_merge
                                           , p_serialnoreuse          => l_sku.serial_no_reuse
                                           , p_picksequence           => l_sku.pick_sequence
                                           , p_pickcountqty           => l_sku.pick_count_qty
                                           , p_countfrequency         => l_sku.count_frequency
                                           , p_oapwipenabled          => l_sku.oap_wip_enabled
                                           , p_kitsku                 => l_sku.kit_sku
                                           , p_kitsplit               => l_sku.kit_split
                                           , p_kittriggerqty          => l_sku.kit_trigger_qty
                                           , p_kitqtydue              => l_sku.kit_qty_due
                                           , p_kittinglocid           => l_sku.kitting_loc_id
                                           , p_allocationgroup        => l_sku.allocation_group
                                           , p_putawaygroup           => l_sku.putaway_group
                                           , p_abcdisable             => l_sku.abc_disable
                                           , p_handlingclass          => l_sku.handling_class
                                           , p_obsoleteproduct        => l_sku.obsolete_product
                                           , p_newproduct             => 'N'
                                           , p_disallowupload         => l_sku.disallow_upload
                                           , p_disallowcrossdock      => l_sku.disallow_cross_dock
                                           , p_manufdstampreqd        => l_sku.manuf_dstamp_reqd
                                           , p_manufdstampdflt        => l_sku.manuf_dstamp_dflt
                                           , p_minshelflife           => l_sku.min_shelf_life
                                           , p_colour                 => l_sku.colour
                                           , p_skusize                => l_sku.sku_size
                                           , p_hazmat                 => l_sku.hazmat
                                           , p_hazmatid               => l_sku.hazmat_id
                                           , p_shipshelflife          => l_sku.ship_shelf_life
                                           , p_nmfcnumber             => l_sku.nmfc_number
                                           , p_incubrule              => l_sku.incub_rule
                                           , p_incubhours             => l_sku.incub_hours
                                           , p_eachwidth              => p_each_width
                                           , p_eachdepth              => p_each_depth
                                           , p_reordertriggerqty      => l_sku.reorder_trigger_qty
                                           , p_lowtriggerqty          => l_sku.low_trigger_qty
                                           , p_disallowmergerules     => l_sku.disallow_merge_rules
                                           , p_packdespatchrepack     => l_sku.pack_despatch_repack
                                           , p_specid                 => l_sku.spec_id
                                           , p_userdeftype1           => l_sku.user_def_type_1
                                           , p_userdeftype2           => l_sku.user_def_type_2
                                           , p_userdeftype3           => l_sku.user_def_type_3
                                           , p_userdeftype4           => l_sku.user_def_type_4
                                           , p_userdeftype5           => l_sku.user_def_type_5
                                           , p_userdeftype6           => l_sku.user_def_type_6
                                           , p_userdeftype7           => l_sku.user_def_type_7
                                           , p_userdeftype8           => 'MULTISCAN-' || p_user
                                           , p_userdefchk1            => l_sku.user_def_chk_1
                                           , p_userdefchk2            => l_sku.user_def_chk_2
                                           , p_userdefchk3            => l_sku.user_def_chk_3
                                           , p_userdefchk4            => l_sku.user_def_chk_4
                                           , p_userdefdate1           => l_sku.user_def_date_1
                                           , p_userdefdate2           => l_sku.user_def_date_2
                                           , p_userdefdate3           => l_sku.user_def_date_3
                                           , p_userdefdate4           => l_sku.user_def_date_4
                                           , p_userdefnum1            => l_sku.user_def_num_1
                                           , p_userdefnum2            => l_sku.user_def_num_2
                                           , p_userdefnum3            => l_sku.user_def_num_3
                                           , p_userdefnum4            => l_sku.user_def_num_4
                                           , p_userdefnote1           => l_sku.user_def_note_1
                                           , p_userdefnote2           => l_sku.user_def_note_2
                                           , p_timezonename           => null
                                           , p_beamunits              => l_sku.beam_units
                                           , p_cewarehousetype        => l_sku.ce_warehouse_type
                                           , p_cecustomsexcise        => l_sku.ce_customs_excise
                                           , p_cestandardcost         => l_sku.ce_standard_cost
                                           , p_cestandardcurrency     => l_sku.ce_standard_currency
                                           , p_disallowclustering     => l_sku.disallow_clustering
                                           , p_clientgroup            => null
                                           , p_maxstack               => l_sku.max_stack
                                           , p_stackdescription       => l_sku.stack_description
                                           , p_stacklimitation        => l_sku.stack_limitation
                                           , p_cedutystamp            => l_sku.ce_duty_stamp
                                           , p_captureweight          => l_sku.capture_weight
                                           , p_weighatreceipt         => l_sku.weigh_at_receipt
                                           , p_upperweighttolerance   => l_sku.upper_weight_tolerance
                                           , p_lowerweighttolerance   => l_sku.lower_weight_tolerance
                                           , p_serialatloading        => l_sku.serial_at_loading
                                           , p_serialatkitting        => l_sku.serial_at_kitting
                                           , p_serialatunkitting      => l_sku.serial_at_unkitting
                                           , p_cecommoditycode        => l_sku.ce_commodity_code
                                           , p_cecoo                  => l_sku.ce_coo
                                           , p_cecwc                  => l_sku.ce_cwc
                                           , p_cevatcode              => l_sku.ce_vat_code
                                           , p_ceproducttype          => l_sku.ce_product_type
                                           , p_commoditycode          => l_sku.commodity_code
                                           , p_commoditydesc          => l_sku.commodity_desc
                                           , p_familygroup            => l_sku.family_group
                                           , p_breakpack              => l_sku.breakpack
                                           , p_clearable              => l_sku.clearable
                                           , p_stagerouteid           => l_sku.stage_route_id
                                           , p_serialmaxrange         => l_sku.serial_max_range
                                           , p_serialdynamicrange     => l_sku.serial_dynamic_range
                                           , p_expiryatrepack         => l_sku.expiry_at_repack
                                           , p_udfatrepack            => l_sku.udf_at_repack
                                           , p_manufactureatrepack    => l_sku.manufacture_at_repack
                                           , p_repackbypiece          => l_sku.repack_by_piece
                                           , p_eachquantity           => l_sku.each_quantity
                                           , p_packedheight           => l_sku.packed_height
                                           , p_packedwidth            => l_sku.packed_width
                                           , p_packeddepth            => l_sku.packed_depth
                                           , p_packedvolume           => p_packed_volume
                                           , p_packedweight           => p_packed_weight
                                           , p_awkward                => l_sku.awkward
                                           , p_twomanlift             => l_sku.two_man_lift
                                           , p_decatalogued           => l_sku.decatalogued
                                           , p_stockcheckruleid       => l_sku.stock_check_rule_id
                                           , p_unkittinginherit       => l_sku.unkitting_inherit
                                           , p_serialatstockcheck     => l_sku.serial_at_stock_check
                                           , p_serialatstockadjust    => l_sku.serial_at_stock_adjust
                                           , p_kitshipcomponents      => l_sku.kit_ship_components
                                           , p_unallocatable          => l_sku.unallocatable
                                           , p_batchatkitting         => l_sku.batch_at_kitting
                                           , p_serialpereach          => l_sku.serial_per_each
                                           , p_vmiallowallocation     => l_sku.vmi_allow_allocation
                                           , p_vmiallowinterfaced     => l_sku.vmi_allow_interfaced
                                           , p_vmiallowmanual         => l_sku.vmi_allow_manual
                                           , p_vmiallowreplenish      => l_sku.vmi_allow_replenish
                                           , p_vmiagingdays           => l_sku.vmi_aging_days
                                           , p_vmioverstockqty        => l_sku.vmi_overstock_qty
                                           , p_scraponreturn          => l_sku.scrap_on_return
                                           , p_harmonisedproductcode  => l_sku.harmonised_product_code
                                           , p_hanginggarment         => l_sku.hanging_garment
                                           , p_conveyable             => l_sku.conveyable
                                           , p_fragile                => l_sku.fragile
                                           , p_gender                 => l_sku.gender
                                           , p_highsecurity           => l_sku.high_security
                                           , p_ugly                   => l_sku.ugly
                                           , p_collatable             => l_sku.collatable
                                           , p_ecommerce              => l_sku.ecommerce
                                           , p_promotion              => l_sku.promotion
                                           , p_foldable               => l_sku.foldable
                                           , p_style                  => l_sku.style
                                           , p_businessunitcode       => l_sku.business_unit_code
                                           , p_tagmerge               => l_sku.tag_merge
                                           , p_carrierpalletmixing    => l_sku.carrier_pallet_mixing
                                           , p_specialcontainertype   => l_sku.special_container_type
                                           , p_disallowrdtoverpicking => l_sku.disallow_rdt_over_picking
                                           , p_noallocbackorder       => l_sku.no_alloc_back_order
                                           , p_returnminshelflife     => l_sku.return_min_shelf_life
                                           , p_weighatgridpick        => l_sku.weigh_at_grid_pick
                                           , p_ceexciseproductcode    => l_sku.ce_excise_product_code
                                           , p_cedegreeplato          => l_sku.ce_degree_plato
                                           , p_cedesignationorigin    => l_sku.ce_designation_origin
                                           , p_cedensity              => l_sku.ce_density
                                           , p_cebrandname            => l_sku.ce_brand_name
                                           , p_cealcoholicstrength    => l_sku.ce_alcoholic_strength
                                           , p_cefiscalmark           => l_sku.ce_fiscal_mark
                                           , p_cesizeofproducer       => l_sku.ce_size_of_producer
                                           , p_cecommercialdesc       => l_sku.ce_commercial_desc
                                           , p_serialnooutbound       => l_sku.serial_no_outbound
                                           , p_fullpalletatreceipt    => l_sku.full_pallet_at_receipt
                                           , p_alwaysfullpallet       => l_sku.always_full_pallet
                                           , p_subwithinproductgrp    => l_sku.sub_within_product_grp
                                           , p_serialcheckstring      => l_sku.serial_check_string
                                           , p_carrierproducttype     => l_sku.carrier_product_type
                                           , p_maxpackconfigs         => l_sku.max_pack_configs
                                           , p_parcelpackingbypiece   => l_sku.parcel_packing_by_piece);
    if l_retval = 0 then
      p_result := 0;
      p_message := get_errortext(p_message);
    end if;
    close c;

  end upd_sku_p;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : insert pack config
-- Description: inserts a record into the interface sku config table.
------------------------------------------------------------------------------------------------
  procedure ins_skc_p  (  p_result          in out integer
                       ,  p_message         in out varchar2
                       ,  p_config_id	      in varchar2
                       ,  p_client_id	      in varchar2
                       ,  p_sku_id	        in varchar2
                       ,  p_track_level_1	  in varchar2
                       ,  p_ratio_1_to_2	  in number
                       ,  p_track_level_2	  in varchar2
                       ,  p_ratio_2_to_3	  in number
                       ,  p_track_level_3	  in varchar2
                       ,  p_ratio_3_to_4	  in number
                       ,  p_track_level_4	  in varchar2
                       ,  p_ratio_4_to_5	  in number
                       ,  p_track_level_5	  in varchar2
                       ,  p_ratio_5_to_6	  in number
                       ,  p_track_level_6	  in varchar2
                       ,  p_ratio_6_to_7	  in number
                       ,  p_track_level_7	  in varchar2
                       ,  p_ratio_7_to_8	  in number
                       ,  p_track_level_8	  in varchar2
                       ,  p_each_per_layer	in number
                       ,  p_layer_height	  in number
                       ,  p_weight_2	      in number
                       ,  p_height_2	      in number
                       ,  p_width_2	        in number
                       ,  p_depth_2	        in number
                       ,  p_weight_3	      in number
                       ,  p_height_3	      in number
                       ,  p_width_3	        in number
                       ,  p_depth_3	        in number
                       ,  p_weight_4	      in number
                       ,  p_height_4	      in number
                       ,  p_width_4	        in number
                       ,  p_depth_4	        in number
                       ,  p_weight_5	      in number
                       ,  p_height_5	      in number
                       ,  p_width_5	        in number
                       ,  p_depth_5	        in number
                       ,  p_weight_6	      in number
                       ,  p_height_6	      in number
                       ,  p_width_6	        in number
                       ,  p_depth_6	        in number
                       ,  p_weight_7	      in number
                       ,  p_height_7	      in number
                       ,  p_width_7	        in number
                       ,  p_depth_7	        in number
                       ,  p_notes	          in varchar2
                       ,  p_weight_8	      in number
                       ,  p_height_8	      in number
                       ,  p_width_8	        in number
                       ,  p_depth_8	        in number
                       ,  p_tag_volume      in number
                       ,  p_volume_2        in number
                       ,  p_volume_3        in number
                       ,  p_volume_4        in number
                       ,  p_volume_5        in number
                       ,  p_volume_6        in number
                       ,  p_volume_7        in number
                       ,  p_volume_8        in number
                       ,  p_user            in varchar2
                       ,  p_comment         in varchar2
                       )
  is
    l_retval integer;
  begin
    p_result := 1;
    l_retval := dcsdba.libmergeconfig.directskuconfig ( p_mergeerror          => p_message
                                                      , p_toupdatecols        => null
                                                      , p_mergeaction         => 'A'
                                                      , p_clientid            => p_client_id
                                                      , p_configid            => p_config_id
                                                      , p_tagvolume           => p_tag_volume
                                                      , p_volumeateach        => 'Y'
                                                      , p_tracklevel1         => p_track_level_1
                                                      , p_ratio1to2           => p_ratio_1_to_2
                                                      , p_tracklevel2         => p_track_level_2
                                                      , p_ratio2to3           => p_ratio_2_to_3
                                                      , p_tracklevel3         => p_track_level_3
                                                      , p_ratio3to4           => p_ratio_3_to_4
                                                      , p_tracklevel4         => p_track_level_4
                                                      , p_ratio4to5           => p_ratio_4_to_5
                                                      , p_tracklevel5         => p_track_level_5
                                                      , p_ratio5to6           => p_ratio_5_to_6
                                                      , p_tracklevel6         => p_track_level_6
                                                      , p_ratio6to7           => p_ratio_6_to_7
                                                      , p_tracklevel7         => p_track_level_7
                                                      , p_ratio7to8           => p_ratio_7_to_8
                                                      , p_tracklevel8         => p_track_level_8
                                                      , p_splitlowest         => null
                                                      , p_eachperlayer        => p_each_per_layer
                                                      , p_layerheight         => p_layer_height
                                                      , p_notes               => p_notes
                                                      , p_volume2             => p_volume_2
                                                      , p_weight2             => p_weight_2
                                                      , p_height2             => p_height_2
                                                      , p_width2              => p_width_2
                                                      , p_depth2              => p_depth_2
                                                      , p_volume3             => p_volume_3
                                                      , p_weight3             => p_weight_3
                                                      , p_height3             => p_height_3
                                                      , p_width3              => p_width_3
                                                      , p_depth3              => p_depth_3
                                                      , p_volume4             => p_volume_4
                                                      , p_weight4             => p_weight_4
                                                      , p_height4             => p_height_4
                                                      , p_width4              => p_width_4
                                                      , p_depth4              => p_depth_4
                                                      , p_volume5             => p_volume_5
                                                      , p_weight5             => p_weight_5
                                                      , p_height5             => p_height_5
                                                      , p_width5              => p_width_5
                                                      , p_depth5              => p_depth_5
                                                      , p_volume6             => p_volume_6
                                                      , p_weight6             => p_weight_6
                                                      , p_height6             => p_height_6
                                                      , p_width6              => p_width_6
                                                      , p_depth6              => p_depth_6
                                                      , p_volume7             => p_volume_7
                                                      , p_weight7             => p_weight_7
                                                      , p_height7             => p_height_7
                                                      , p_width7              => p_width_7
                                                      , p_depth7              => p_depth_7
                                                      , p_performancefactor   => null
                                                      , p_timezonename        => null
                                                      , p_volume8             => p_volume_8
                                                      , p_weight8             => p_weight_8
                                                      , p_height8             => p_height_8
                                                      , p_width8              => p_width_8
                                                      , p_depth8              => p_depth_8
                                                      , p_stagerouteid1       => null
                                                      , p_stagerouteid2       => null
                                                      , p_stagerouteid3       => null
                                                      , p_stagerouteid4       => null
                                                      , p_stagerouteid5       => null
                                                      , p_stagerouteid6       => null
                                                      , p_stagerouteid7       => null
                                                      , p_stagerouteid8       => null
                                                      , p_laborc1tracklevel   => null
                                                      , p_laborc2tracklevel   => null
                                                      , p_laborh1tracklevel   => null
                                                      , p_laborh2tracklevel   => null
                                                      , p_laborh3tracklevel   => null
                                                      , p_shippingunitlev1    => null
                                                      , p_shippingunitlev2    => null
                                                      , p_shippingunitlev3    => null
                                                      , p_shippingunitlev4    => null
                                                      , p_shippingunitlev5    => null
                                                      , p_shippingunitlev6    => null
                                                      , p_shippingunitlev7    => null
                                                      , p_shippingunitlev8    => null
                                                      , p_rdtdisplaylev1      => null
                                                      , p_rdtdisplaylev2      => null
                                                      , p_rdtdisplaylev3      => null
                                                      , p_rdtdisplaylev4      => null
                                                      , p_rdtdisplaylev5      => null
                                                      , p_rdtdisplaylev6      => null
                                                      , p_rdtdisplaylev7      => null
                                                      , p_rdtdisplaylev8      => null
                                                      , p_disallowmergerules  => null
                                                      , p_userdeftype1        => null
                                                      , p_userdeftype2        => null
                                                      , p_userdeftype3        => null
                                                      , p_userdeftype4        => null
                                                      , p_userdeftype5        => null
                                                      , p_userdeftype6        => null
                                                      , p_userdeftype7        => null
                                                      , p_userdeftype8        => 'MULTISCAN-'||p_user
                                                      , p_userdefchk1         => null
                                                      , p_userdefchk2         => null
                                                      , p_userdefchk3         => null
                                                      , p_userdefchk4         => null
                                                      , p_userdefdate1        => null
                                                      , p_userdefdate2        => null
                                                      , p_userdefdate3        => null
                                                      , p_userdefdate4        => null
                                                      , p_userdefnum1         => null
                                                      , p_userdefnum2         => null
                                                      , p_userdefnum3         => null
                                                      , p_userdefnum4         => null
                                                      , p_userdefnote1        => null
                                                      , p_userdefnote2        => p_comment
                                                      , p_clientgroup         => null
                                                      , p_rdttracklevel1      => null
                                                      , p_rdttracklevel2      => null
                                                      , p_rdttracklevel3      => null
                                                      , p_rdttracklevel4      => null
                                                      , p_rdttracklevel5      => null
                                                      , p_rdttracklevel6      => null
                                                      , p_rdttracklevel7      => null
                                                      , p_rdttracklevel8      => null
                                                      );
      if l_retval = 0 then
        p_result := 0;
        p_message := get_errortext(p_message);
      end if;
  end ins_skc_p;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : inserts sku - config link
-- Description: inserts a record into the interface_sku_sku_config for linking config to sku.
------------------------------------------------------------------------------------------------
  procedure ins_ssc_p ( p_result      in out integer
                      , p_message     in out varchar2
                      , p_client_id   in varchar2
                      , p_sku_id      in varchar2
                      , p_config_id   in varchar2
                      )
  is
    l_retval integer;
  begin
    l_retval := dcsdba.libmergeskuskuconfig.directskuskuconfig( p_mergeerror        => p_message
                                                              , p_toupdatecols      => null
                                                              , p_mergeaction       => 'A'
                                                              , p_clientid          => p_client_id
                                                              , p_skuid             => p_sku_id
                                                              , p_configid          => p_config_id
                                                              , p_mainconfigid      => null
                                                              , p_minfullpalletperc => null
                                                              , p_maxfullpalletperc => null
                                                              , p_disabled          => null
                                                              , p_timezonename      => null
                                                              );
    if l_retval = 0 then
      p_result := 0;
      p_message := get_errortext(p_message);
    end if;
  end ins_ssc_p;

------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 10-01-2017
-- Purpose    : Process data 
-- Description: processes the data received from the multiscan device or WMS
------------------------------------------------------------------------------------------------
 procedure proc_data_p   (  p_user          in varchar2                         -- Multiscan operator
                          , p_client_id     in varchar2                         -- WMS client id
                          , p_id            in varchar2                         -- SKU, EAN, UPC, TUC or Supplier SKU
                          , p_num_trck_lvl  in number                           -- Number of tracking levels
                          , p_pallet_type   in varchar2                         -- Pallet type used (EURO, BLOK, NOPALLET, OTHER)
                          , p_trck_lvl_1    in varchar2                         -- Lowest tracking level
                          , p_depth_1       in number                           -- Each depth
                          , p_width_1       in number                           -- Each width
                          , p_height_1      in number                           -- Each height
                          , p_weight_1      in number                           -- Each weight
                          , p_trck_lvl_2    in varchar2   default null          -- Second tracking level
                          , p_ratio_1_to_2  in number     default null          -- Ratio num lowest tracking level in second tracking level
                          , p_depth_2       in number     default null          -- Second tracking level depth
                          , p_width_2       in number     default null          -- Second tracking level width
                          , p_height_2      in number     default null          -- Second tracking level height
                          , p_weight_2      in number     default null          -- Second tracking level weight
                          , p_trck_lvl_3    in varchar2   default null          -- Third tracking level
                          , p_ratio_2_to_3  in number     default null          -- Ratio num second tracking level in third tracking level
                          , p_depth_3       in number     default null          -- Third tracking level depth
                          , p_width_3       in number     default null          -- Third tracking level width
                          , p_height_3      in number     default null          -- Third tracking level height
                          , p_weight_3      in number     default null          -- Third tracking level weight                          
                          , p_trck_lvl_4    in varchar2   default null          -- Fourth tracking level
                          , p_ratio_3_to_4  in number     default null          -- Ratio num third tracking level in fourth tracking level
                          , p_depth_4       in number     default null          -- Fourth tracking level depth
                          , p_width_4       in number     default null          -- Fourth tracking level width
                          , p_height_4      in number     default null          -- Fourth tracking level height
                          , p_weight_4      in number     default null          -- Fourth tracking level weight                          
                          , p_trck_lvl_5    in varchar2   default null          -- Fifth tracking level
                          , p_ratio_4_to_5  in number     default null          -- Ratio num fourth tracking level in fifth tracking level
                          , p_depth_5       in number     default null          -- Fifth tracking level depth
                          , p_width_5       in number     default null          -- Fifth tracking level width
                          , p_height_5      in number     default null          -- Fifth tracking level height
                          , p_weight_5      in number     default null          -- Fifth tracking level weight                          
                          , p_trck_lvl_6    in varchar2   default null          -- Sixth tracking level
                          , p_ratio_5_to_6  in number     default null          -- Ratio num fifth tracking level in sixth tracking level
                          , p_depth_6       in number     default null          -- Sixth tracking level depth
                          , p_width_6       in number     default null          -- Sixth tracking level width
                          , p_height_6      in number     default null          -- Sixth tracking level height
                          , p_weight_6      in number     default null          -- Sixth tracking level weight                          
                          , p_trck_lvl_7    in varchar2   default null          -- Seventh tracking level
                          , p_ratio_6_to_7  in number     default null          -- Ratio num sixth tracking level in seventh tracking level
                          , p_depth_7       in number     default null          -- Seventh tracking level depth
                          , p_width_7       in number     default null          -- Seventh tracking level width
                          , p_height_7      in number     default null          -- Seventh tracking level height
                          , p_weight_7      in number     default null          -- Seventh tracking level weight                          
                          , p_trck_lvl_8    in varchar2   default null          -- Eighth tracking level
                          , p_ratio_7_to_8  in number     default null          -- Ratio num seventh tracking level in eighth tracking level
                          , p_depth_8       in number     default null          -- Eighth tracking level depth
                          , p_width_8       in number     default null          -- Eighth tracking level width
                          , p_height_8      in number     default null          -- Eighth tracking level height
                          , p_weight_8      in number     default null          -- Eighth tracking level weight                          
                          , p_layer_height  in number     default null          -- Hieght of a single layer on a pallet
                          , p_each_per_layer in number    default null          -- number of lowest tracking level in a single layer
                          , p_num_layers    in number     default null          -- number of layers
                          , p_ok            out integer                         -- 1 OK , 0 not OK
                          , p_message       out varchar2                        -- Message that describes the result.
                          )
  is
    -- Generic variables
    v_config_id	        varchar2(15);
    v_client_id	        varchar2(10)  := upper(p_client_id);
    v_sku_id            varchar2(50)  := get_sku_f(upper(p_id), upper(p_client_id));
    v_user              varchar2(20)  := p_user;
    v_comment           varchar2(200);

    -- sku_fields   
    v_each_height       number(15,6);
    v_each_weight       number(13,6);
    v_each_width        number(7,3);
    v_each_depth        number(7,3);
    v_each_volume       number(13,6);
    v_packed_weight     number(13,6);
    v_packed_volume     number(13,6);

    -- sku_config fields
    v_track_level_1	    varchar2(8);
    v_ratio_1_to_2	    number(12,6);
    v_track_level_2	    varchar2(8);
    v_ratio_2_to_3	    number(6,0);
    v_track_level_3	    varchar2(8);
    v_ratio_3_to_4	    number(6,0);
    v_track_level_4	    varchar2(8);
    v_ratio_4_to_5	    number(6,0);
    v_track_level_5	    varchar2(8);
    v_ratio_5_to_6	    number(6,0);
    v_track_level_6	    varchar2(8);
    v_ratio_6_to_7	    number(6,0);
    v_track_level_7	    varchar2(8);
    v_ratio_7_to_8	    number(6,0);
    v_track_level_8	    varchar2(8);
    v_each_per_layer	  number(6,0);
    v_layer_height	    number(15,6);
    v_weight_2	        number(13,6);
    v_height_2	        number(15,6);
    v_width_2	          number(7,3);
    v_depth_2	          number(7,3);
    v_volume_2          number(13,6);
    v_weight_3	        number(13,6);
    v_height_3	        number(15,6);
    v_width_3	          number(7,3);
    v_depth_3 	        number(7,3);
    v_volume_3          number(13,6);
    v_weight_4	        number(13,6);
    v_height_4	        number(15,6);
    v_width_4	          number(7,3);
    v_depth_4	          number(7,3);
    v_volume_4          number(13,6);
    v_weight_5	        number(13,6);
    v_height_5	        number(15,6);
    v_width_5	          number(7,3);
    v_depth_5	          number(7,3);
    v_volume_5          number(13,6);
    v_weight_6	        number(13,6);
    v_height_6	        number(15,6);
    v_width_6	          number(7,3);
    v_depth_6	          number(7,3);
    v_volume_6          number(13,6);
    v_weight_7	        number(13,6);
    v_height_7	        number(15,6);
    v_width_7	          number(7,3);
    v_depth_7	          number(7,3);
    v_volume_7          number(13,6);
    v_notes	            varchar2(80);
    v_weight_8	        number(13,6);
    v_height_8	        number(15,6);
    v_width_8	          number(7,3);
    v_depth_8	          number(7,3);
    v_volume_8          number(13,6);
    v_tag_volume        number;                                                 

    l_client_def_hgt    number;                                                 -- default pallet height to use when no height is available or can be calculated.
    l_is_pal            varchar2(1);                                            -- Pack config is for a pallet Y/N
    l_pal_depth         number;                                                 -- Depth of highest tracking level
    l_pal_width         number;                                                 -- Width of highest tracking level
    l_pal_height        number;                                                 -- Height of highest tracking level
    l_pal_weight        number;                                                 -- Weight of highest tracking level     
    l_pal_ratio         number;                                                 -- Last ratio Can be null!!!!
    l_total_each        number;                                                 -- Total each on a pallet
    l_min_layer_height  number;                                                 -- minimum layer height to use for calculation when it is not set by operator
    l_prev_depth        number;                                                 -- Depth of second highest tracking level
    l_prev_width        number;                                                 -- width of second highest tracking level
    l_prev_height       number;                                                 -- height of second highest tracking level
    l_prev_weight       number;                                                 -- weight of second highest tracking level
    l_prev_ratio        number;                                                 -- second highest ratio
    l_each_per_layer    number;                                                 -- Total each in  a single layer
    l_est_pal_hgt       number;                                                 -- Estimate pallet height based on ratio
    l_layer_height      number;                                                 -- Height of a single layer    
    l_highest_trk_lvl   varchar2(8);                                            -- name of highest tracking level
    l_calculate         varchar2(1) := 'N';                                     -- Calculation required yes / no
    l_num_layers        number;                                                 -- calculated number of layers
    l_comment           varchar2(200);                                          -- Comment for when data is missing and must be calculated you can see why the results are as they are in WMS.

    l_half_pallet_vol   number;                                                 -- volue of half a euro pallet. Used for virtual tracking level.
    t_layer_height      number;                                                 -- Temp layer height used for check.

  begin
    -- first run sanity check before continue the processing of the data.
    sanity_check_p (p_weight_1, p_weight_2, p_weight_3, p_weight_4, p_weight_5, p_weight_6, p_weight_7, p_weight_8, p_ratio_1_to_2, p_ratio_2_to_3, p_ratio_3_to_4, p_ratio_4_to_5, p_ratio_5_to_6, p_ratio_6_to_7, p_ratio_7_to_8, p_trck_lvl_1, p_trck_lvl_2, p_trck_lvl_3, p_trck_lvl_4, p_trck_lvl_5, p_trck_lvl_6, p_trck_lvl_7, p_trck_lvl_8, p_client_id, p_id, p_ok, p_message);

    -- When sanity check is succesfull continu processing.
    if      p_ok = 1 
    -- Capture and save the data from the multiscan for analysing.
    then    insert into cnl_multiscan_data  values  (p_user, upper(p_client_id), upper(p_id), p_num_trck_lvl, p_pallet_type, p_layer_height, p_each_per_layer, p_num_layers, p_trck_lvl_1, p_depth_1, p_width_1, p_height_1, p_weight_1, p_trck_lvl_2, p_ratio_1_to_2, p_depth_2, p_width_2, p_height_2, p_weight_2, p_trck_lvl_3, p_ratio_2_to_3, p_depth_3, p_width_3, p_height_3, p_weight_3, p_trck_lvl_4, p_ratio_3_to_4, p_depth_4, p_width_4, p_height_4, p_weight_4, p_trck_lvl_5, p_ratio_4_to_5, p_depth_5, p_width_5, p_height_5, p_weight_5, p_trck_lvl_6, p_ratio_5_to_6, p_depth_6, p_width_6, p_height_6, p_weight_6, p_trck_lvl_7, p_ratio_6_to_7, p_depth_7, p_width_7, p_height_7, p_weight_7, p_trck_lvl_8, p_ratio_7_to_8, p_depth_8, p_width_8, p_height_8, p_weight_8, to_char(sysdate,'yyyymm'), to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'));

            -- Remove old records from cnl_multiscan_data
            delete  cnl_multiscan_data m
            where   m.month < to_char(sysdate,'yyyymm')-1;

            -- Figure out if it is a pallet and capture pallet details and details of one tracking level lower.
            case
            when    p_trck_lvl_2 = 'PALLET' 
            then    l_pal_depth         := p_depth_2/100;
                    l_pal_weight        := p_weight_2;
                    l_pal_width         := p_width_2/100;
                    l_pal_height        := p_height_2/100;
                    l_prev_depth        := p_depth_1/100;
                    l_prev_weight       := p_weight_1;
                    l_prev_width        := p_width_1/100;
                    l_prev_height       := p_height_1/100;
                    l_pal_ratio         := p_ratio_1_to_2;  -- Only one ratio available and this is required!
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_1/100;
            when    p_trck_lvl_3 = 'PALLET' 
            then    l_pal_depth         := p_depth_3/100;
                    l_pal_weight        := p_weight_3;
                    l_pal_width         := p_width_3/100;
                    l_pal_height        := p_height_3/100;
                    l_prev_depth        := p_depth_2/100;
                    l_prev_weight       := p_weight_2;
                    l_prev_width        := p_width_2/100;
                    l_prev_height       := p_height_2/100;
                    l_pal_ratio         := p_ratio_2_to_3;
                    l_prev_ratio        := p_ratio_1_to_2;
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_2/100;
            when    p_trck_lvl_4 = 'PALLET' 
            then    l_pal_depth         := p_depth_4/100;
                    l_pal_weight        := p_weight_4;
                    l_pal_width         := p_width_4/100;
                    l_pal_height        := p_height_4/100;
                    l_prev_depth        := p_depth_3/100;
                    l_prev_weight       := p_weight_3;
                    l_prev_width        := p_width_3/100;
                    l_prev_height       := p_height_3/100;
                    l_pal_ratio         := p_ratio_3_to_4;
                    l_prev_ratio        := p_ratio_2_to_3;
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_2/100;
            when    p_trck_lvl_5 = 'PALLET' 
            then    l_pal_depth         := p_depth_5/100;
                    l_pal_weight        := p_weight_5;
                    l_pal_width         := p_width_5/100;
                    l_pal_height        := p_height_5/100;
                    l_prev_depth        := p_depth_4/100;
                    l_prev_weight       := p_weight_4;
                    l_prev_width        := p_width_4/100;
                    l_prev_height       := p_height_4/100;
                    l_pal_ratio         := p_ratio_4_to_5;
                    l_prev_ratio        := p_ratio_3_to_4;
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_2/100;
            when    p_trck_lvl_6 = 'PALLET' 
            then    l_pal_depth         := p_depth_6/100;
                    l_pal_weight        := p_weight_6;
                    l_pal_width         := p_width_6/100;
                    l_pal_height        := p_height_6/100;
                    l_prev_depth        := p_depth_5/100;
                    l_prev_weight       := p_weight_5;
                    l_prev_width        := p_width_5/100;
                    l_prev_height       := p_height_5/100;
                    l_pal_ratio         := p_ratio_5_to_6;
                    l_prev_ratio        := p_ratio_4_to_5;
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_2/100;
            when    p_trck_lvl_7 = 'PALLET' 
            then    l_pal_depth         := p_depth_7/100;
                    l_pal_weight        := p_weight_7;
                    l_pal_width         := p_width_7/100;
                    l_pal_height        := p_height_7/100;
                    l_prev_depth        := p_depth_6/100;
                    l_prev_weight       := p_weight_6;
                    l_prev_width        := p_width_6/100;
                    l_prev_height       := p_height_6/100;
                    l_pal_ratio         := p_ratio_6_to_7;
                    l_prev_ratio        := p_ratio_5_to_6;
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_2/100;
            when    p_trck_lvl_8 = 'PALLET' 
            then    l_pal_depth         := p_depth_8/100;
                    l_pal_weight        := p_weight_8;
                    l_pal_width         := p_width_8/100;
                    l_pal_height        := p_height_8/100;
                    l_prev_depth        := p_depth_7/100;
                    l_prev_weight       := p_weight_7;
                    l_prev_width        := p_width_7/100;
                    l_prev_height       := p_height_7/100;
                    l_pal_ratio         := p_ratio_7_to_8;
                    l_prev_ratio        := p_ratio_6_to_7;
                    l_is_pal            := 'Y';
                    l_min_layer_height  := p_height_2/100;
            else    l_is_pal      := 'N';
            end case;  

            -- Get the highest tracking level used.
            l_highest_trk_lvl := get_h_trcklvl_f (p_trck_lvl_1, p_trck_lvl_2, p_trck_lvl_3, p_trck_lvl_4, p_trck_lvl_5, p_trck_lvl_6, p_trck_lvl_7, p_trck_lvl_8);

            -- Set variables   
            v_each_height     := p_height_1/100; 
            v_each_weight     := p_weight_1;
            v_each_width      := p_width_1/100;
            v_each_depth      := p_depth_1/100;
            v_notes	          := get_scf_not_f ( p_pallet_type, v_sku_id, l_highest_trk_lvl);
            v_track_level_1   := p_trck_lvl_1;	   
            v_track_level_2	  := p_trck_lvl_2;	   
            v_track_level_3	  := p_trck_lvl_3;
            v_track_level_4	  := p_trck_lvl_4;
            v_track_level_5	  := p_trck_lvl_5;
            v_track_level_6	  := p_trck_lvl_6;
            v_track_level_7	  := p_trck_lvl_7;
            v_track_level_8	  := p_trck_lvl_8;
            v_ratio_1_to_2	  := p_ratio_1_to_2;
            v_width_2	        := p_width_2/100;
            v_width_3	        := p_width_3/100;
            v_width_4	        := p_width_4/100;
            v_width_5	        := p_width_5/100;
            v_width_6	        := p_width_6/100;
            v_width_7	        := p_width_7/100;
            v_width_8	        := p_width_8/100;
            v_depth_2	        := p_depth_2/100;
            v_depth_3	        := p_depth_3/100;
            v_depth_4	        := p_depth_4/100;
            v_depth_5	        := p_depth_5/100;
            v_depth_6	        := p_depth_6/100;
            v_depth_7	        := p_depth_7/100;
            v_depth_8	        := p_depth_8/100;
            if    l_is_pal = 'N' -- When the pack config is not for a pallet.
            then  v_each_per_layer	:= null;
                  v_layer_height	  := null;
                  v_ratio_2_to_3    := p_ratio_2_to_3;
                  v_ratio_3_to_4	  := p_ratio_3_to_4;
                  v_ratio_4_to_5	  := p_ratio_4_to_5;
                  v_ratio_5_to_6	  := p_ratio_5_to_6;
                  v_ratio_6_to_7	  := p_ratio_6_to_7;
                  v_ratio_7_to_8	  := p_ratio_7_to_8;
                  v_weight_2	      := p_weight_2;
                  v_weight_3	      := p_weight_3;
                  v_weight_4	      := p_weight_4;
                  v_weight_5	      := p_weight_5;
                  v_weight_6	      := p_weight_6;
                  v_weight_7	      := p_weight_7;
                  v_weight_8	      := p_weight_8;
                  v_height_2	      := p_height_2/100;
                  v_height_3	      := p_height_3/100;
                  v_height_4	      := p_height_4/100;
                  v_height_5	      := p_height_5/100;
                  v_height_6	      := p_height_6/100;
                  v_height_7	      := p_height_7/100;
                  v_height_8	      := p_height_8/100;
                  l_total_each      := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1);
            else  case --When pack configuration is for a pallet
                  -- Pallet height, layer height and the last ratio are not filled in by operator.
                  when  l_pal_height is null and p_layer_height is null and l_pal_ratio is null
                  then  l_client_def_hgt      := get_def_hgt_f(p_client_id);
                        l_pal_ratio           := floor((l_pal_depth*l_pal_width*l_client_def_hgt)/(l_prev_depth*l_prev_width*l_prev_height));
                        l_total_each          := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1)*l_pal_ratio;
                        l_calculate           := 'Y';
                        l_comment             := 'Client default height is used to calculate last ratio, layer height, each per layer and actual pallet height';
                  -- Pallet height, layer height are not filled in by operator.
                  when  l_pal_height is null and p_layer_height is null and l_pal_ratio is not null 
                  then  l_client_def_hgt      := ceil(l_prev_depth*l_prev_width*l_prev_height*l_pal_ratio/l_pal_width/l_pal_depth);
                        l_total_each          := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1);     
                        l_calculate           := 'Y';
                        l_comment             := 'Last ratio is used to calculate pallet height, layer height and each per layer';
                  -- Layer height is not filled in by operator
                  when  l_pal_height is not null and p_layer_height is null and l_pal_ratio is not null
                  then  l_total_each          := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1);
                        l_client_def_hgt      := l_pal_height;
                        l_calculate           := 'Y';
                        l_comment             := 'layer height and each per layer and actual pallet height are calculated based on input';
                  -- Pallet height, last ratio are not filled in by operator
                  when  l_pal_height is null and p_layer_height is not null and l_pal_ratio is null 
                  then  if      p_layer_height/100 < l_min_layer_height 
                        then    t_layer_height      := l_min_layer_height;
                        else    t_layer_height      := p_layer_height/100;
                        end if;
                        if      p_num_layers is null 
                        then    l_client_def_hgt    := get_def_hgt_f(p_client_id);
                        else    l_client_def_hgt    := t_layer_height*p_num_layers;
                        end if;  
                        l_pal_height          := floor(l_client_def_hgt/t_layer_height)*t_layer_height;
                        l_pal_ratio           := ((l_pal_height/t_layer_height)*p_each_per_layer)/nvl(p_ratio_1_to_2,1)/nvl(p_ratio_2_to_3,1)/nvl(p_ratio_3_to_4,1)/nvl(p_ratio_4_to_5,1)/nvl(p_ratio_5_to_6,1)/nvl(p_ratio_6_to_7,1)/nvl(p_ratio_7_to_8,1);
                        l_total_each          := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1)*l_pal_ratio;
                        l_layer_height        := t_layer_height;
                        l_each_per_layer      := p_each_per_layer;
						if 		p_num_layers is null
						then	l_num_layers		:= l_pal_height/l_layer_height;
                        else	l_num_layers        := p_num_layers;
						end if;
                        l_calculate           := 'N';
                        l_comment             := 'Client default pallet height plus layer height is used to calculate actual pallet height and ratio.';
                  -- Pallet height, layer height and last ratio are all filled in by operator.
                  when  l_pal_height is not null and p_layer_height is not null and l_pal_ratio is not null
                  then  l_layer_height        := p_layer_height/100;
                        l_each_per_layer      := p_each_per_layer;
                        l_total_each          := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1);
                        l_calculate           := 'N';
                        if      p_num_layers is null 
                        then    l_num_layers        := ceil(l_pal_height/l_layer_height);
                        else    l_num_layers        := p_num_layers;
                        end if; 
                  when  l_pal_height is null and p_layer_height is not null and l_pal_ratio is not null
                  then  l_layer_height        := p_layer_height/100;
                        l_each_per_layer      := p_each_per_layer;
                        l_total_each          := nvl(p_ratio_1_to_2,1)*nvl(p_ratio_2_to_3,1)*nvl(p_ratio_3_to_4,1)*nvl(p_ratio_4_to_5,1)*nvl(p_ratio_5_to_6,1)*nvl(p_ratio_6_to_7,1)*nvl(p_ratio_7_to_8,1);
                        l_calculate           := 'N';
                        if      p_num_layers is null 
                        then    l_num_layers        := ceil(l_total_each/l_each_per_layer);
                        else    l_num_layers        := p_num_layers;
                        end if;
                        l_pal_height := l_num_layers*l_layer_height;
                  end case;

                  if        l_calculate = 'Y' 
                  then      get_epl_lh_ph_p ( p_tot_each_i          => l_total_each
                                            , p_min_layer_height_i  => l_min_layer_height
                                            , p_est_pallet_height_i => l_client_def_hgt
                                            , p_each_per_layer_o    => l_each_per_layer
                                            , p_pallet_height_o     => l_pal_height
                                            , p_layer_height_o      => l_layer_height
                                            , p_num_layers_o        => l_num_layers
                                            );
                  else      null;
                  end if;

                  if        l_pal_weight is null 
                  then      l_pal_weight        := l_pal_ratio*l_prev_weight;
                  end if;

                  v_each_per_layer          := l_each_per_layer;
                  v_layer_height	          := l_layer_height;

                  -- set ratio, height and weight
                  case
                  when  p_trck_lvl_8 = 'PALLET' 
                  then  if      p_ratio_7_to_8 is null 
                        then    v_ratio_7_to_8    := l_pal_ratio;
                        else    v_ratio_7_to_8    := p_ratio_7_to_8;
                        end if;
                        v_ratio_6_to_7      := p_ratio_6_to_7;
                        v_ratio_5_to_6      := p_ratio_5_to_6;
                        v_ratio_4_to_5      := p_ratio_4_to_5;
                        v_ratio_3_to_4      := p_ratio_3_to_4;
                        v_ratio_2_to_3      := p_ratio_2_to_3;
                        v_weight_2	        := p_weight_2;
                        v_weight_3	        := p_weight_3;
                        v_weight_4	        := p_weight_4;
                        v_weight_5	        := p_weight_5;
                        v_weight_6	        := p_weight_6;
                        v_weight_7	        := p_weight_7;
                        if      p_weight_8 is null 
                        then    v_weight_8	      := l_pal_weight;
                        else    v_weight_8        := p_weight_8;
                        end if;
                        v_height_2	        := p_height_2/100;
                        v_height_3	        := p_height_3/100;
                        v_height_4	        := p_height_4/100;
                        v_height_5	        := p_height_5/100;
                        v_height_6	        := p_height_6/100;
                        v_height_7	        := p_height_7/100;
                        if      p_height_8 is null 
                        then    v_height_8	      := l_pal_height;
                        else    v_height_8	      := p_height_8/100;
                        end if;
                  when  p_trck_lvl_7 = 'PALLET' 
                  then  v_ratio_7_to_8      := p_ratio_7_to_8;
                        if      p_ratio_6_to_7 is null 
                        then    v_ratio_6_to_7    := l_pal_ratio;
                        else    v_ratio_6_to_7    := p_ratio_6_to_7;
                        end if;
                        v_ratio_5_to_6      := p_ratio_5_to_6;
                        v_ratio_4_to_5      := p_ratio_4_to_5;
                        v_ratio_3_to_4      := p_ratio_3_to_4;
                        v_ratio_2_to_3      := p_ratio_2_to_3;
                        v_weight_2	        := p_weight_2;
                        v_weight_3	        := p_weight_3;
                        v_weight_4	        := p_weight_4;
                        v_weight_5	        := p_weight_5;
                        v_weight_6	        := p_weight_6;
                        if      p_weight_7 is null 
                        then    v_weight_7	      := l_pal_weight;
                        else    v_weight_7        := p_weight_7;
                        end if;
                        v_weight_8	        := p_weight_8;
                        v_height_2	        := p_height_2/100;
                        v_height_3	        := p_height_3/100;
                        v_height_4	        := p_height_4/100;
                        v_height_5	        := p_height_5/100;
                        v_height_6	        := p_height_6/100;
                        if      p_height_7 is null 
                        then    v_height_7	      := l_pal_height;
                        else    v_height_7	      := p_height_7/100;
                        end if;
                        v_height_8	        := p_height_8/100;
                  when  p_trck_lvl_6 = 'PALLET'
                  then  v_ratio_7_to_8      := p_ratio_7_to_8;
                        v_ratio_6_to_7      := p_ratio_6_to_7;
                        if      p_ratio_5_to_6 is null 
                        then    v_ratio_5_to_6    := l_pal_ratio;
                        else    v_ratio_5_to_6    := p_ratio_5_to_6;
                        end if;
                        v_ratio_4_to_5      := p_ratio_4_to_5;
                        v_ratio_3_to_4      := p_ratio_3_to_4;
                        v_ratio_2_to_3      := p_ratio_2_to_3;
                        v_weight_2	        := p_weight_2;
                        v_weight_3	        := p_weight_3;
                        v_weight_4	        := p_weight_4;
                        v_weight_5	        := p_weight_5;
                        if      p_weight_6 is null 
                        then    v_weight_6	      := l_pal_weight;
                        else    v_weight_6        := p_weight_6;
                        end if;
                        v_weight_7	        := p_weight_7;
                        v_weight_8	        := p_weight_8;
                        v_height_2	        := p_height_2/100;
                        v_height_3	        := p_height_3/100;
                        v_height_4	        := p_height_4/100;
                        v_height_5	        := p_height_5/100;
                        if      p_height_6 is null 
                        then    v_height_6	      := l_pal_height;
                        else    v_height_6	      := p_height_6/100;
                        end if;
                        v_height_7	        := p_height_7/100;
                        v_height_8	        := p_height_8/100;
                  when  p_trck_lvl_5 = 'PALLET'
                  then  v_ratio_7_to_8      := p_ratio_7_to_8;
                        v_ratio_6_to_7      := p_ratio_6_to_7;
                        v_ratio_5_to_6      := p_ratio_5_to_6;
                        if      p_ratio_4_to_5 is null 
                        then    v_ratio_4_to_5    := l_pal_ratio;
                        else    v_ratio_4_to_5    := p_ratio_4_to_5;
                        end if;
                        v_ratio_3_to_4      := p_ratio_3_to_4;
                        v_ratio_2_to_3      := p_ratio_2_to_3;
                        v_weight_2	        := p_weight_2;
                        v_weight_3	        := p_weight_3;
                        v_weight_4	        := p_weight_4;
                        if      p_weight_5 is null 
                        then    v_weight_5	      := l_pal_weight;
                        else    v_weight_5        := p_weight_5;
                        end if;
                        v_weight_6	        := p_weight_6;
                        v_weight_7	        := p_weight_7;
                        v_weight_8	        := p_weight_8;
                        v_height_2	        := p_height_2/100;
                        v_height_3	        := p_height_3/100;
                        v_height_4	        := p_height_4/100;
                        if      p_height_5 is null 
                        then    v_height_5	      := l_pal_height;
                        else    v_height_5	      := p_height_5/100;
                        end if;
                        v_height_6	        := p_height_6/100;
                        v_height_7	        := p_height_7/100;
                        v_height_8	        := p_height_8/100;
                  when  p_trck_lvl_4 = 'PALLET'
                  then  v_ratio_7_to_8      := p_ratio_7_to_8;
                        v_ratio_6_to_7      := p_ratio_6_to_7;
                        v_ratio_5_to_6      := p_ratio_5_to_6;
                        v_ratio_4_to_5      := p_ratio_4_to_5;
                        if      p_ratio_3_to_4 is null 
                        then    v_ratio_3_to_4    := l_pal_ratio;
                        else    v_ratio_3_to_4    := p_ratio_3_to_4;
                        end if;
                        v_ratio_2_to_3      := p_ratio_2_to_3;
                        v_weight_2	        := p_weight_2;
                        v_weight_3	        := p_weight_3;
                        if      p_weight_4 is null 
                        then    v_weight_4	      := l_pal_weight;
                        else    v_weight_4        := p_weight_4;
                        end if;
                        v_weight_5	        := p_weight_5;
                        v_weight_6	        := p_weight_6;
                        v_weight_7	        := p_weight_7;
                        v_weight_8	        := p_weight_8;
                        v_height_2	        := p_height_2/100;
                        v_height_3	        := p_height_3/100;
                        if      p_height_4 is null 
                        then    v_height_4	      := l_pal_height;
                        else    v_height_4	      := p_height_4/100;
                        end if;
                        v_height_5	        := p_height_5/100;
                        v_height_6	        := p_height_6/100;
                        v_height_7	        := p_height_7/100;
                        v_height_8	        := p_height_8/100;
                  when  p_trck_lvl_3 = 'PALLET'
                  then  v_ratio_7_to_8      := p_ratio_7_to_8;
                        v_ratio_6_to_7      := p_ratio_6_to_7;
                        v_ratio_5_to_6      := p_ratio_5_to_6;
                        v_ratio_4_to_5      := p_ratio_4_to_5;
                        v_ratio_3_to_4      := p_ratio_3_to_4;
                        if      p_ratio_2_to_3 is null 
                        then    v_ratio_2_to_3    := l_pal_ratio;
                        else    v_ratio_2_to_3    := p_ratio_2_to_3;
                        end if;
                        v_weight_2	        := p_weight_2;
                        if      p_weight_3 is null 
                        then    v_weight_3	      := l_pal_weight;
                        else    v_weight_3        := p_weight_3;
                        end if;
                        v_weight_4	        := p_weight_4;
                        v_weight_5	        := p_weight_5;
                        v_weight_6	        := p_weight_6;
                        v_weight_7	        := p_weight_7;
                        v_weight_8	        := p_weight_8;
                        v_height_2	        := p_height_2/100;
                        if      p_height_3 is null 
                        then    v_height_3	      := l_pal_height;
                        else    v_height_3	      := p_height_3/100;
                        end if;
                        v_height_4	        := p_height_4/100;
                        v_height_5	        := p_height_5/100;
                        v_height_6	        := p_height_6/100;
                        v_height_7	        := p_height_7/100;
                        v_height_8	        := p_height_8/100;
                  when  p_trck_lvl_2 = 'PALLET'
                  then  v_ratio_7_to_8      := p_ratio_7_to_8;
                        v_ratio_6_to_7      := p_ratio_6_to_7;
                        v_ratio_5_to_6      := p_ratio_5_to_6;
                        v_ratio_4_to_5      := p_ratio_4_to_5;
                        v_ratio_3_to_4      := p_ratio_3_to_4;
                        v_ratio_2_to_3      := p_ratio_2_to_3;
                        if      p_height_2 is null 
                        then    v_height_2	      := l_pal_height;
                        else    v_height_2        := p_height_2/100;
                        end if;
                        v_height_3	        := p_height_3/100;
                        v_height_4	        := p_height_4/100;
                        v_height_5	        := p_height_5/100;
                        v_height_6	        := p_height_6/100;
                        v_height_7	        := p_height_7/100;
                        v_height_8	        := p_height_8/100;
                        if      v_weight_2	is null 
                        then    v_weight_2        := l_pal_weight;
                        else    v_weight_2        := p_weight_2;
                        end if;
                        v_weight_3	        := p_weight_3;
                        v_weight_4	        := p_weight_4;
                        v_weight_5	        := p_weight_5;
                        v_weight_6	        := p_weight_6;
                        v_weight_7	        := p_weight_7;
                        v_weight_8	        := p_weight_8;
                  end case;
            end if;

            -- Calculate volumes
            -- because of the rounding the calculation some times result in 0 and WMS does not except 0 as volume. Therefore we make it 0.000001

            v_each_volume       := round(v_each_depth*v_each_width*v_each_height,6);
            if   v_each_volume = 0 
            then v_each_volume := 0.000001; 
            end if;

            v_volume_2          := round(v_depth_2*v_width_2*v_height_2,6);
            if   v_volume_2 = 0 
            then v_volume_2 := 0.000001; 
            end if;            

            v_volume_3          := round(v_depth_3*v_width_3*v_height_3,6);
            if   v_volume_3 = 0 
            then v_volume_3 := 0.000001; 
            end if;

            v_volume_4          := round(v_depth_4*v_width_4*v_height_4,6);
            if   v_volume_4 = 0 
            then v_volume_4 := 0.000001; 
            end if;

            v_volume_5          := round(v_depth_5*v_width_5*v_height_5,6);
            if   v_volume_5 = 0 
            then v_volume_5 := 0.000001; 
            end if;

            v_volume_6          := round(v_depth_6*v_width_6*v_height_6,6);
            if   v_volume_6 = 0 
            then v_volume_6 := 0.000001; 
            end if;

            v_volume_7          := round(v_depth_7*v_width_7*v_height_7,6);
            if   v_volume_7 = 0 
            then v_volume_7 := 0.000001; 
            end if;

            v_volume_8          := round(v_depth_8*v_width_8*v_height_8,6);
            if   v_volume_8 = 0 
            then v_volume_8 := 0.000001; 
            end if;

            -- set tag volume as max volume + 2% 
            if v_volume_8 is null then
              if v_volume_7 is null then
                if v_volume_6 is null then
                  if v_volume_5 is null then
                    if v_volume_4 is null then
                      if v_volume_3 is null then
                        if v_volume_2 is null then
                          v_tag_volume := v_each_volume + (v_each_volume/100)*2;
                        else
                          v_tag_volume := v_volume_2 + (v_volume_2/100)*2;
                        end if;
                      else
                        v_tag_volume := v_volume_3 + (v_volume_3/100)*2;
                      end if;
                    else
                      v_tag_volume := v_volume_4 + (v_volume_4/100)*2;
                    end if;
                  else
                    v_tag_volume := v_volume_5 + (v_volume_5/100)*2;
                  end if;
                else
                  v_tag_volume := v_volume_6 + (v_volume_6/100)*2;
                end if;
              else
                v_tag_volume := v_volume_7 + (v_volume_7/100)*2;
              end if;
            else
              v_tag_volume := v_volume_8 + (v_volume_8/100)*2;
            end if;

            if   v_tag_volume = 0 
            then v_tag_volume := 0.000001; 
            end if;

            -- create unique config id 
            v_config_id         := create_config_id_f ( p_trck_lvl_1, p_trck_lvl_2, p_trck_lvl_3, p_trck_lvl_4, p_trck_lvl_5, p_trck_lvl_6, p_trck_lvl_7, p_trck_lvl_8, p_pallet_type, l_num_layers);
            v_comment           := l_comment;
--------------------------------****************************************************************************            
            --Update for when only 1 tracking level is used to prevent that WMS thinks 1 each is a full pallet.
            if      p_num_trck_lvl = 1 and p_trck_lvl_1 != 'PALLET'
            then    v_ratio_1_to_2 := floor(0.072/v_tag_volume);
                    if v_ratio_1_to_2 = 0
                    then v_ratio_1_to_2 := 1;
                    end if;
                    v_tag_volume := v_tag_volume*v_ratio_1_to_2;
            else    null;
            end if;

            -- Make pack config for half a pallet when only two tracking levels exist and it's not a pallet
            if      p_num_trck_lvl = 2 and p_trck_lvl_2 != 'PALLET'
            then    
                    l_half_pallet_vol := (get_def_hgt_f(p_client_id)/2) * 0.8 * 1.2;

                    v_ratio_2_to_3 := floor(l_half_pallet_vol/v_volume_2);
                    if  v_ratio_2_to_3 = 0
                    then v_ratio_2_to_3 := 1;
                    end if;
                    v_tag_volume := v_tag_volume * v_ratio_2_to_3;
                    v_volume_3 := l_half_pallet_vol;
                    v_width_3 := 0.8;
                    v_depth_3 := 1.2;
                    v_height_3 := get_def_hgt_f(p_client_id)/2;
                    v_weight_3 := v_weight_2 * v_ratio_2_to_3;
                    v_track_level_3 := 'HALFPALL';
            end if;
------------------------------********************************************************            
            --Set packed weight and volume in sku calculated backwards including package material
            if v_track_level_2 is null then
              v_packed_weight := v_each_weight;
              v_packed_volume := round(v_tag_volume/(nvl(v_ratio_1_to_2,1)*nvl(v_ratio_2_to_3,1)*nvl(v_ratio_3_to_4,1)*nvl(v_ratio_4_to_5,1)*nvl(v_ratio_5_to_6,1)*nvl(v_ratio_6_to_7,1)*nvl(v_ratio_7_to_8,1)),6);
            else
              v_packed_weight := round(v_weight_2/v_ratio_1_to_2,3);
              v_packed_volume := round(v_tag_volume/(nvl(v_ratio_1_to_2,1)*nvl(v_ratio_2_to_3,1)*nvl(v_ratio_3_to_4,1)*nvl(v_ratio_4_to_5,1)*nvl(v_ratio_5_to_6,1)*nvl(v_ratio_6_to_7,1)*nvl(v_ratio_7_to_8,1)),6);
            end if;

            -- Insert new pack configuration in interface table
            p_message := null;
            ins_skc_p (p_result => p_ok, p_message => p_message, p_config_id => v_config_id, p_client_id => v_client_id, p_sku_id => v_sku_id, p_track_level_1 => v_track_level_1 , p_ratio_1_to_2 => v_ratio_1_to_2, p_track_level_2  => v_track_level_2, p_ratio_2_to_3  => v_ratio_2_to_3, p_track_level_3  => v_track_level_3, p_ratio_3_to_4  => v_ratio_3_to_4, p_track_level_4  => v_track_level_4, p_ratio_4_to_5  => v_ratio_4_to_5, p_track_level_5  => v_track_level_5, p_ratio_5_to_6  => v_ratio_5_to_6, p_track_level_6  => v_track_level_6, p_ratio_6_to_7  => v_ratio_6_to_7, p_track_level_7  => v_track_level_7, p_ratio_7_to_8  => v_ratio_7_to_8, p_track_level_8  => v_track_level_8, p_each_per_layer => v_each_per_layer, p_layer_height  => v_layer_height, p_weight_2  => v_weight_2, p_height_2  => v_height_2, p_width_2  => v_width_2, p_depth_2  => v_depth_2, p_weight_3  => v_weight_3, p_height_3  => v_height_3, p_width_3  => v_width_3, p_depth_3  => v_depth_3, p_weight_4  => v_weight_4, p_height_4  => v_height_4, p_width_4  => v_width_4, p_depth_4  => v_depth_4, p_weight_5  => v_weight_5, p_height_5  => v_height_5, p_width_5  => v_width_5, p_depth_5  => v_depth_5, p_weight_6  => v_weight_6, p_height_6  => v_height_6, p_width_6  => v_width_6, p_depth_6  => v_depth_6, p_weight_7  => v_weight_7, p_height_7  => v_height_7, p_width_7  => v_width_7, p_depth_7  => v_depth_7, p_notes  => v_notes, p_weight_8  => v_weight_8, p_height_8  => v_height_8, p_width_8  => v_width_8, p_depth_8  => v_depth_8, p_tag_volume => v_tag_volume , p_volume_2 => v_volume_2, p_volume_3 => v_volume_3, p_volume_4 => v_volume_4, p_volume_5 => v_volume_5, p_volume_6 => v_volume_6, p_volume_7 => v_volume_7, p_volume_8 => v_volume_8, p_user => v_user, p_comment => v_comment);
            if p_ok = 1 then 
              -- Inert pack configuration linking record in interface table
              ins_ssc_p (p_result => p_ok, p_message => p_message, p_client_id => v_client_id, p_sku_id => v_sku_id, p_config_id => v_config_id);
              if p_ok = 1 then 
                -- Update sku
                upd_sku_p (p_result => p_ok, p_message => p_message, p_sku_id => v_sku_id, p_client_id => v_client_id, p_each_depth => v_each_depth, p_each_width => v_each_width, p_each_height => v_each_height, p_each_weight => v_each_weight, p_each_volume => v_each_volume, p_packed_weight => v_packed_weight, p_packed_volume => v_packed_volume, p_user => v_user);
                if p_ok = 1 then
                  -- p_message := 'Data processed successfully';
                  commit;
                else
                  rollback;
                end if;
              else
                rollback;
              end if;
            else
              rollback;
            end if;

    else    null;
    end if;
 end  proc_data_p;

end cnl_multiscan_pck;
-- show errors;