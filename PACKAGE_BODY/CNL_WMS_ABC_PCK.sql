CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_ABC_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: WMS functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/
--
-- Private type declarations
--
--
-- Private constant declarations
--
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 20-Dec-2016
-- Purpose    : Function to get the client where we use the ranking numbers from.
-- Description: Dispatcher lacks functionality to analyse ABC data over multiple clients/owners.
--              To be able to do this we agreed that all clients in a client group for ABC 
--              will have the same ABC ranking criteria set.
--              This way this piece of code can simply select the first record from the
--              ABC ranking table for any client in the client group used.
------------------------------------------------------------------------------------------------
  function client_rank (p_client_group varchar2)
    return varchar2
  is
    cursor c_client (a_client_group varchar2)
    is
      select  client_id 
      from    dcsdba.client_group_clients 
      where   client_group = a_client_group 
      and rownum = 1;
    l_client  varchar2(20);
  begin
    open    c_client(p_client_group);
    fetch   c_client into l_client;
    if      c_client%notfound 
    then    l_client := 'NO CLIENT';
    end if;
    close   c_client;
    return  l_client;
  end client_rank;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Function to get the owner where we use the ranking numbers from.
------------------------------------------------------------------------------------------------
  function owner_rank (p_client varchar2)
    return varchar2
  is
    cursor c_owner (a_client varchar2)
    is
      select  owner_id 
      from    dcsdba.abc_ranking 
      where   client_id = a_client 
      and rownum = 1;
    l_owner  varchar2(20);
  begin
    open    c_owner(p_client);
    fetch   c_owner into l_owner;
    if      c_owner%notfound 
    then    l_owner := 'NO OWNER';
    end if;
    close   c_owner;
    return  l_owner;
  end owner_rank;

-----------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Function to get the rankings that are used A, B and/or C etc
------------------------------------------------------------------------------------------------

  function  get_abc_ranking ( p_client_id  in varchar2
                            , p_owner_id      in varchar2
                            , p_site_id       in varchar2
                            )
    return integer
  is
  --
    cursor c_abc_ranking  ( a_client_id varchar2
                          , a_owner_id  varchar2
                          , a_site_id   varchar2
                          )
    is
      select  count(*)
      from    dcsdba.abc_ranking
      where   client_id = a_client_id
      and     owner_id  = a_owner_id
      and     site_id   = a_site_id;
   --         
    r_rankings     integer;
  --
  begin

    open  c_abc_ranking ( p_client_id 
                        , p_owner_id 
                        , p_site_id  
                        );
    fetch c_abc_ranking
      into  r_rankings;
    close c_abc_ranking;

    return r_rankings;

  end get_abc_ranking;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Figure out if we need to rank by X amount of sku's or by a percentage of all sku's.
------------------------------------------------------------------------------------------------    

  function get_rank_type    ( p_client_id in  varchar2
                            , p_owner_id  in  varchar2
                            , p_site_id   in  varchar2
                            , p_frequency in  varchar2
                            )
    return varchar2
  is
  --
    cursor c_rank_type ( a_client_id varchar2
                       , a_owner_id  varchar2
                       , a_site_id   varchar2
                       , a_frequency varchar2
                       )
    is
      select  *
      from    dcsdba.abc_ranking
      where   client_id     = a_client_id
      and     owner_id      = a_owner_id
      and     site_id       = a_site_id
      and     abc_frequency = a_frequency;
    --  
    r_rank_type       c_rank_type%rowtype;
    l_type            varchar2(20);
    --
  begin

    open c_rank_type ( p_client_id
                     , p_owner_id
                     , p_site_id
                     , p_frequency
                     );
    fetch c_rank_type
      into r_rank_type;
    close c_rank_type;

    if r_rank_type.abc_percentage is not null then 
      l_type := 'percentage';
    else
      l_type := 'number';
    end if;

    return l_type;

  end get_rank_type;

--------------------------------------------------------------------------------------------------
-- Author     : M. Swinkels, 20-Dec-2016
-- Purpose    : Function to get the number or percentage per rank
-- Description: For each rank we need to capture the amount of sku's to rank or the percentage to rank.
--------------------------------------------------------------------------------------------------

  Function get_rank_num ( p_client_id in  varchar2
                        , p_owner_id  in  varchar2
                        , p_site_id   in  varchar2
                        , p_frequency in  varchar2
                        , p_rank_type in  varchar2
                        )
    return number
  is
  --
    cursor c_rank_num  ( a_client_id varchar2
                       , a_owner_id  varchar2
                       , a_site_id   varchar2
                       , a_frequency varchar2
                       )
    is
      select  abc_percentage
      ,       abc_number
      from    dcsdba.abc_ranking
      where   client_id     = a_client_id
      and     owner_id      = a_owner_id
      and     site_id       = a_site_id
      and     abc_frequency = a_frequency;  
  --      
    r_rank_num      c_rank_num%rowtype;
    l_num           number(10,0);
  --  
  begin

    open   c_rank_num ( p_client_id
                      , p_owner_id
                      , p_site_id
                      , p_frequency
                      );
    fetch  c_rank_num
      into  r_rank_num;
    close  c_rank_num;

    if p_rank_type = 'percentage' then
      l_num := r_rank_num.abc_percentage;
    else
      l_num := r_rank_num.abc_number;
    end if;

    return l_num;

  end get_rank_num;

  --------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Update old data cnl_wms_abc_ranking.
--------------------------------------------------------------------------------------------------  
  procedure prep_tables  ( p_client_group  varchar2
                         , p_site_id       varchar2
                         )
    is
  begin
    delete  cnl_wms_abc_ranking
    where   vers = 'OLD'
    and     client_group = p_client_group;
    commit;

    update  cnl_wms_abc_ranking
    set     vers = 'OLD'
    where   vers = 'NEW'
    and     client_group = p_client_group;
    commit;

    delete  dcsdba.sku_ranking 
    where   client_id in (  select client_id 
                            from dcsdba.client_group_clients 
                            where client_group = p_client_group
                          )
    and     site_id = p_site_id;
    commit;

  end prep_tables;

--------------------------------------------------------------------------------------------------
-- Author       : M. Swinkels, 20-Dec-2016
-- Purpose      : Procedure to capture the data to abc rank
-- Description  : All data required for ranking the sku's from all clients in the client group used
--                is captured from the inventory transaction table. 
--                It is then ranked by percentage and by numbering.
--                the list is completed by adding all sku's from the sku table that had no transactions.
--                Only sku's that have the flag disable ABC set will not be ranked at all.
--------------------------------------------------------------------------------------------------
  procedure my_abc_ranking_data   ( p_num_months    number
                                  , p_site_id       varchar2
                                  , p_client_group  varchar2
                                  ) 
  is
   cursor c_abc_data ( a_client_group varchar2
                     , a_num_months   number
                     , a_site_id      varchar2
                     )
   is   
    select      itl.sku_id                                                                      as sku_id
    ,           itl.client_id                                                                   as client_id
    ,           sum(itl.update_qty)                                                             as tot_each_picked
    ,           count(itl.sku_id||itl.client_id||itl.site_id)                                   as Times_picked 
    from        dcsdba.inventory_transaction itl
    where       itl.client_id in (select  client_id
                                  from    dcsdba.client_group_clients
                                  where   client_group = a_client_group
                                 )
    and         itl.site_id   = a_site_id
    and         itl.code      = 'Pick'
    and         itl.from_loc_id in ( select  loc.location_id 
                                     from    dcsdba.location loc 
                                     where   loc_type in ( 'Bin'
                                                         , 'Bulk'
                                                         , 'Tag-FIFO'
                                                         , 'Tag-LIFO'
                                                         , 'Tag-Operator'
                                                         , 'Receive Dock'
                                                         ) 
                                     and loc.site_id = a_site_id
                                   )   
    and         trunc(itl.dstamp) > add_months(sysdate, - a_num_months) 
    group by    itl.sku_id
    ,           itl.client_id
    order by    times_picked desc;

  begin

  -- Insert all captured records into the table cnl_wms_abc_ranking. 
    for r in c_abc_data ( p_client_group
                        , p_num_months
                        , p_site_id
                        ) 
    loop
      insert into cnl_wms_abc_ranking
      values      ( r.sku_id
                  , r.client_id
                  , p_client_group
                  , p_site_id
                  , r.times_picked
                  , r.tot_each_picked
                  , null--r.rank
                  , null--r.row_num
                  , current_timestamp(6)
                  , 'NEW'
                  , 'NO'
                  , null
                  );
    end loop;

    commit; 

  end my_abc_ranking_data;
--------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Procedure to capture sku's with no transaction
--------------------------------------------------------------------------------------------------
  procedure get_remainder_sku ( p_client_group varchar2
                               ,p_site_id varchar2
                              )
  is
    cursor c_get_sku (a_client_group varchar2)
    is
      select  sku_id
      ,       client_id
      from    dcsdba.sku
      where   client_id||sku_id not in (  select  client_id||sku_id
                                          from    cnl_wms_abc_ranking
                                          where   client_group = a_client_group
                                          and     vers = 'NEW'
                                        )
      and     (abc_disable is null or abc_disable = 'N')
      and     client_id in (  select  client_id
                              from    dcsdba.client_group_clients
                              where   client_group = p_client_group
                            );

    l_max_row_num     number;  
    l_max_rank        number;

    begin
      for r in c_get_sku (p_client_group)
      loop
        insert into cnl_wms_abc_ranking
        values  (  r.sku_id
                ,  r.client_id
                ,  p_client_group
                ,  p_site_id
                ,  0
                ,  0
                ,  null--l_max_rank
                ,  null--l_max_row_num
                , current_timestamp(6)
                , 'NEW'
                , 'NO'
                , null
                );
        commit;
      end loop;
    end get_remainder_sku;  
--------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : set ranks and row numbers
--------------------------------------------------------------------------------------------------
  procedure rank  (p_client_group varchar2
                  ,p_site_id      varchar2
                  )
  is
    cursor  c_rank  (a_client_group varchar2
                    ,a_site_id      varchar2
                    )
      is
      select    sku_id
      ,         client_id
      ,         site_id
      ,         times_picked
      ,         tot_each_picked
      ,         ntile(100)   over (order by times_picked desc) as rank              -- Creates 100 buckets with data where bucket 1 has the record with the highest QTY in "Highest_picked"
      ,         row_number() over (order by times_picked desc) as row_num           -- The sku with the highest QTY in times_picked gets the first row_num
      from      cnl_wms_abc_ranking
      where     client_group = a_client_group
      and       site_id = a_site_id
      and       vers = 'NEW';
  begin
    for r in c_rank(p_client_group, p_site_id) loop
      update  cnl_wms_abc_ranking
      set     rank            = r.rank
      ,       row_num         = r.row_num
      where   client_group    = p_client_group
      and     client_id       = r.client_id
      and     sku_id          = r.sku_id
      and     site_id         = r.site_id
      and     vers            = 'NEW';
  commit;
  end loop;
  end rank;

--------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Update WMS sku ranking
--------------------------------------------------------------------------------------------------
  procedure upd_sku_rank  ( p_client_group in varchar2
                          , p_site_id      in varchar2
                          )
    is

    cursor c_client_id is
      select  distinct client_id 
      from    cnl_wms_abc_ranking
      where   client_group = p_client_group;

    cursor c_owner_id (a_client_id varchar2) is
      select  owner_id
      from    dcsdba.owner
      where   owner.client_id = a_client_id;

  begin
    for rc in c_client_id loop
      for ro in c_owner_id (rc.client_id) loop
        insert into dcsdba.sku_ranking
        select      client_id
        ,           sku_id
        ,           site_id
        ,           ro.owner_id
        ,           abc_frequency
        ,           times_picked
        from        cnl_wms_abc_ranking
        where       client_group = p_client_group
        and         client_id = rc.client_id
        and         vers = 'NEW';
        commit;
      end loop;
    end loop;
    update          cnl_wms_abc_ranking
    set             processed = 'YES'
    where           client_group = p_client_group;
    commit;

  end upd_sku_rank;

--------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Update WMS pick_face (if any exist)
--------------------------------------------------------------------------------------------------
  procedure upd_pick_face ( p_client_group in varchar2
                          , p_site_id      in varchar2
                          )
  is
    cursor c_sku
    is
      select  sku_id
      ,       client_id
      ,       abc_frequency
      from    cnl_wms_abc_ranking
      where   client_group = p_client_group
      and     vers = 'NEW';

  begin
    for r in c_sku loop
      update  dcsdba.pick_face
      set     abc_frequency = r.abc_frequency
      where   sku_id = r.sku_id
      and     client_id = r.client_id;
    end loop;
    commit;  
  end upd_pick_face;

--------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 20-Dec-2016
-- Purpose : Procedure to call from WMS.
--------------------------------------------------------------------------------------------------

  procedure abc_ranking ( p_client_group  varchar2  			      -- client group defined in WMS starting with ABC_.... that holds all clients that require a single abc rank over all sku's
                        , p_num_months    integer   			      -- The number of months to analyse.
                        , p_site_id       varchar2  			      -- The site in which to search for transactions.
                        )
  is

    l_client          varchar2(20);
    l_owner           varchar2(20);
    l_number_of_ranks integer;
    l_rank_type       varchar2(20);
    l_a_num           number; -- number of sku's that must get an A rank
    l_b_num           number;
    l_c_num           number;
    l_d_num           number;
    l_a_perc          number; -- Percentage of the sku' that require an A rank.
    l_b_perc          number;
    l_c_perc          number;
    l_d_perc          number;

  begin
    l_client          := client_rank(p_client_group);
    l_owner           := owner_rank(l_client);
    l_number_of_ranks := get_abc_ranking(l_client,l_owner,p_site_id);
    -- Delete oldest records and update the last records
    prep_tables   ( p_client_group
                  , p_site_id
                  );

    -- Get new ranking data
    my_abc_ranking_data             ( p_num_months
                                    , p_site_id
                                    , p_client_group
                                    );
    get_remainder_sku               ( p_client_group
                                    , p_site_id
                                    );
    rank                            ( p_client_group
                                    , p_site_id
                                    );
    --
    l_rank_type := get_rank_type(l_client, l_owner, p_site_id, 'A');-- get the rank details for A frequency
    if l_rank_type = 'number' then
      l_a_num := get_rank_num(l_client, l_owner, p_site_id, 'A', l_rank_type);-- Automatically all others will be ranked B.
      update  cnl_wms_abc_ranking
      set     abc_frequency = 'A'
      where   row_num <= l_a_num
      and     client_group = p_client_group
      and     vers = 'NEW';
      update  cnl_wms_abc_ranking
      set     abc_frequency = 'B'
      where   row_num > l_a_num
      and     client_group = p_client_group
      and     vers = 'NEW';
      commit;
      if l_number_of_ranks > 2 then -- We add rank C
        l_b_num := get_rank_num(l_client, l_owner, p_site_id, 'B', l_rank_type);
        update  cnl_wms_abc_ranking
        set     abc_frequency = 'C'
        where   row_num > (l_b_num+l_a_num)
        and     client_group = p_client_group
        and     vers = 'NEW';
        commit;
        if l_number_of_ranks > 3 then -- we add rank D
          l_c_num := get_rank_num(l_client, l_owner, p_site_id, 'C', l_rank_type);
          update  cnl_wms_abc_ranking
          set     abc_frequency = 'D'
          where   row_num > (l_c_num+l_b_num+l_a_num)
          and     client_group = p_client_group
          and     vers = 'NEW';
          commit;
          if l_number_of_ranks > 4 then -- we add rank E
            l_d_num := get_rank_num(l_client, l_owner, p_site_id, 'D', l_rank_type);
            update  cnl_wms_abc_ranking
            set     abc_frequency = 'E'
            where   row_num > (l_d_num+l_c_num+l_b_num+l_a_num)
            and     client_group = p_client_group
            and     vers = 'NEW';
            commit;
          end if;
        end if;
      end if;
    else
      l_a_perc := get_rank_num(l_client, l_owner, p_site_id, 'A', l_rank_type);
      update  cnl_wms_abc_ranking
      set     abc_frequency = 'A'
      where   rank <= l_a_perc
      and     client_group = p_client_group
      and     vers = 'NEW';
      commit;
      update  cnl_wms_abc_ranking
      set     abc_frequency = 'B'
      where   rank > l_a_perc
      and     client_group = p_client_group
      and     vers = 'NEW';
      commit;      
      if l_number_of_ranks > 2 then -- we add rank C
        l_b_perc := get_rank_num(l_client, l_owner, p_site_id, 'B', l_rank_type);
        update  cnl_wms_abc_ranking
        set     abc_frequency = 'C'
        where   rank > (l_b_perc+l_a_perc)
        and     client_group = p_client_group
        and     vers = 'NEW';
        commit;      
        if l_number_of_ranks > 3 then -- we add rank D
          l_c_perc := get_rank_num(l_client, l_owner, p_site_id, 'C', l_rank_type);
          update  cnl_wms_abc_ranking
          set     abc_frequency = 'D'
          where   rank > (l_c_perc+l_b_perc+l_a_perc)
          and     client_group = p_client_group
          and     vers = 'NEW';
          commit;      
          if l_number_of_ranks > 4 then -- we add rank E
            l_d_perc := get_rank_num(l_client, l_owner, p_site_id, 'D', l_rank_type);
            update  cnl_wms_abc_ranking
            set     abc_frequency = 'E'
            where   rank > (l_d_perc+l_c_perc+l_b_perc+l_a_perc)
            and     client_group = p_client_group
            and     vers = 'NEW';
            commit;      
          end if;
        end if;
      end if;
    end if;
  --
  upd_sku_rank  ( p_client_group
                , p_site_id
                );
  --
  upd_pick_face ( p_client_group
                , p_site_id
                );

  end abc_ranking;

end cnl_wms_abc_pck;

--show errors;