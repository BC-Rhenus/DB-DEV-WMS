CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_WMS_MERGERULE_PCK" is
/***********************************************************************************************
* NOTE: ANY CHANGE IN THIS PCK BODY REQUIRES A RESTART OF THE APPLICATION.
*       MERGE RULES AND OR RDT DATA RULES WILL START FAILING IF YOU DON'T.
************************************************************************************************/
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 27-Sep-2016
-- Purpose : Add unique id to consignment
------------------------------------------------------------------------------------------------
  function new_cons_id (p_string in varchar2)
    return varchar2
  is
    v_new_consignment varchar2(20);
  begin
    v_new_consignment := substr(p_string,1,13) || cnl_consignment_seq1.nextval;

    return v_new_consignment;

  end new_cons_id;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 27-Sep-2016
-- Purpose : Calculate a ship_by_date for an order.
------------------------------------------------------------------------------------------------
  function is_number (p_string in varchar2)
    return int
  is
    v_new_num number;
  begin
    v_new_num := to_number(p_string);

  return 1;

  exception
    when value_error 
    then
      return 0;

  end is_number;
---------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 27-Sep-2016
-- Pupose  : The function that is called to fetch a ship by date in WMS
---------------------------------------------------------------------------------------------------
  function call_shipbydate ( p_client_id       in varchar2
                           , p_creation_date   in timestamp with local Time zone
                           , p_code            in varchar2
                           , p_order_id        in varchar2
                           , p_ship_by_date    in timestamp with local Time zone default null
                           , p_carrier_id      in varchar2 default null
                           , p_service_level   in varchar2 default null
                           , p_calendar_id     in varchar2 default null
                           )
    return timestamp with local Time zone
  is 
    -- This cursor should always return 2 records. 1 for cut off time and 1 for nbr of days to add.
    cursor c_get_cutoff(    a_client varchar2
                        ,   a_code varchar2
                        )
    is
        select  description
        ,       text_data                
        from    dcsdba.system_profile
        where   parent_profile_id = '-ROOT-_USER_SHIP-BY-DATE-CALCULATION_' || a_client || '_' || a_code;
    --
    cursor c_holiday(   a_date      varchar2
                    ,   a_calendar  varchar2
                    )
    is
        select  count(*)
        from    dcsdba.calendar_holidays
        where   calendar_id = a_calendar
        and     a_date between to_char(start_dstamp,'DDMMYYYY') and to_char(stop_dstamp,'DDMMYYYY');
    --

    l_time          varchar2(6);
    l_temptime      varchar2(100);
    l_tempdays      number;
    l_cutofftime    varchar2(100);
    l_add_days      number;
    l_ship_by_date  timestamp(6) with local Time zone;
    l_calc_date     timestamp(6) with local Time zone;
    l_holiday       integer;
    l_check_ok      integer;
  begin
    l_time := to_char(p_creation_date,'hh24miss');

    --Get cut of times and days to add.
    for r_c in c_get_cutoff(p_client_id, p_code) loop
        l_temptime    := substr(r_c.text_data,1,6);
        l_tempdays    := to_number(substr(r_c.text_data,8,6));
        if (l_time <= l_temptime and (l_temptime < l_cutofftime or l_cutofftime is null)) or l_cutofftime is null then
            l_cutofftime  := substr(r_c.text_data,1,6);
            l_add_days    := to_number(substr(r_c.text_data,8,6));
        end if;
    end loop;

    --Calculate date        
    if l_cutofftime is null then
       l_calc_date := p_creation_date;
    else        
        if     l_time <= l_cutofftime then
            l_calc_date := p_creation_date + l_add_days;
        else
            l_calc_date := p_creation_date + l_add_days+1;
        end if;
    end if;

    --Ship by date or calcualted date?
    if p_ship_by_date is null then
        l_ship_by_date := l_calc_date;
    else
        if p_ship_by_date < sysdate then
            l_ship_by_date := l_calc_date;
        else
            l_ship_by_date := p_ship_by_date;
        end if;
    end if;

    -- weekend and/or holiday check
    l_check_ok := 0;
    while l_check_ok = 0 loop
	--Weekend check
	if      to_char(l_ship_by_date,'D') = '7' 
	then 	
		l_ship_by_date := l_ship_by_date + 2; -- When saturday add 2 days.
	elsif   to_char(l_ship_by_date,'D') = '1' 
	then 	
		l_ship_by_date := l_ship_by_date + 2; -- When Sunday add 2 days.
	end if;
	-- date in the past
	if	trunc(l_ship_by_date) < trunc(sysdate)
	then
		l_ship_by_date := l_ship_by_date + 1;
	end if;
	-- Holiday Check
	open 	c_holiday( to_char(l_ship_by_date,'DDMMYYYY'), p_calendar_id);
	fetch 	c_holiday 
	into 	l_holiday;
	close 	c_holiday;
	if 	l_holiday = 0 
	then 
		l_check_ok := 1; -- No holidays found
	else
		l_check_ok := 0; -- holidays found
		l_ship_by_date := l_ship_by_date + 1;
	end if;
    end loop;

    return l_ship_by_date;

  end call_shipbydate;     
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 27-Sep-2016
-- Purpose : Get all orders and calculate the estimate number of pallets.
--           The minimum value returned is always 1
--           A default pallet volume of 1.6 is used when no good pack config can be retrieved.
------------------------------------------------------------------------------------------------
function est_nbr_pal_f( p_order_id  in varchar2
		      , p_client_id in varchar2
		      ) 
	return number
is
	cursor	c_lines
	is	
		select	l.sku_id          		sku_id
		,       l.tracking_level  		tracking_level
		,      	l.config_id       		config_id
		,      	sum(nvl(l.qty_ordered,0))     	qty_ordered
		,      	sum(nvl(l.expected_volume,0))	expected_volume
		from   	dcsdba.order_line l
		where  	l.order_id 	= p_order_id
		and    	l.client_id	= p_client_id
		group
		by	l.sku_id
		,	l.tracking_level
		,	l.config_id
	;
	-- Fetch the first pack config found containing PAL in the name found linked to the SKU
	cursor c_config( b_sku_id    	dcsdba.sku.sku_id%type
		       , b_config_id	dcsdba.sku_config.config_id%type
		       ) 
	is
		select	p.config_id
		,	nvl(p.ratio_1_to_2,1) 	ratio_1_to_2
		,       nvl(p.ratio_2_to_3,1) 	ratio_2_to_3
		,       nvl(p.ratio_3_to_4,1) 	ratio_3_to_4
		,       nvl(p.ratio_4_to_5,1) 	ratio_4_to_5
		,       nvl(p.ratio_5_to_6,1) 	ratio_5_to_6
		,       nvl(p.ratio_6_to_7,1) 	ratio_6_to_7
		,       nvl(p.ratio_7_to_8,1) 	ratio_7_to_8
		,       p.track_level_1
		,       p.track_level_2
		,       p.track_level_3
		,       p.track_level_4
		,       p.track_level_5
		,       p.track_level_6
		,       p.track_level_7
		,       p.track_level_8
		,	nvl(s.each_volume,0) 	volume_1
		,	nvl(p.volume_2,0)	volume_2
		,	nvl(p.volume_3,0)	volume_3
		,	nvl(p.volume_4,0)	volume_4
		,	nvl(p.volume_5,0)	volume_5
		,	nvl(p.volume_6,0)	volume_6
		,	nvl(p.volume_7,0)	volume_7
		,	nvl(p.volume_8,0)	volume_8
		from	dcsdba.sku_config p
		inner
		join	dcsdba.sku_sku_config c
		on	c.config_id	= p.config_id
		and	c.client_id	= p.client_id
		and	c.sku_id 	= b_sku_id
		inner
		join	dcsdba.sku s
		on	s.sku_id	= b_sku_id
		and	s.client_id	= p.client_id
		where   p.client_id 	= p_client_id
		and	(	p.config_id	= b_config_id
			or	b_config_id	is null
			)
		order
		by	p.track_level_8 desc nulls last
		,	p.track_level_7 desc nulls last
		,	p.track_level_6 desc nulls last
		,	p.track_level_5 desc nulls last
		,	p.track_level_4 desc nulls last
		,	p.track_level_3 desc nulls last
		,	p.track_level_2 desc nulls last
		,	p.track_level_1 desc nulls last
		,	p.ratio_1_to_2	desc
		,	p.ratio_2_to_3	desc
		,	p.ratio_3_to_4	desc
		,	p.ratio_4_to_5	desc
		,	p.ratio_5_to_6	desc
		,	p.ratio_6_to_7	desc
		,	p.ratio_7_to_8	desc
	;
	--

	v_default_volume	number 	:= 1.6;
	v_temp_pallets		number := 0;
	v_total_pallets		number := 0;
	v_config		c_config%rowtype;
begin
	<<lines_loop>>
	for	l in c_lines
	loop
		-- Try and find a pack cofiguration for a pallet linked to the SKU.
		open	c_config( l.sku_id
				, l.config_id
				);
		fetch	c_config 
		into	v_config;
		if	c_config%notfound
		then
			-- No config found. number of pallets based on expectred volume in order line and a pallet volume of max 1.6.
			close	c_config;
			v_temp_pallets	:= v_temp_pallets + l.expected_volume / v_default_volume;
			continue lines_loop;
		else
			close	c_config;	
			-- Search for tracking level pallet.
			if      v_config.track_level_8 = 'PALLET'
			or 	v_config.track_level_7 = 'PALLET'
			or 	v_config.track_level_6 = 'PALLET'
			or 	v_config.track_level_5 = 'PALLET'
			or 	v_config.track_level_4 = 'PALLET'
			or 	v_config.track_level_3 = 'PALLET'
			or 	v_config.track_level_2 = 'PALLET'
			or 	v_config.track_level_1 = 'PALLET'
			then 
				if	l.tracking_level = 'PALLET'
				then
					v_temp_pallets := v_temp_pallets +
							   l.qty_ordered;
				elsif	l.tracking_level = v_config.track_level_7
				then
					v_temp_pallets	:= v_temp_pallets + 
							   l.qty_ordered /
							   v_config.ratio_7_to_8;
				elsif	l.tracking_level = v_config.track_level_6
				then
					v_temp_pallets	:= v_temp_pallets +
							   l.qty_ordered /
						           v_config.ratio_7_to_8 *
							   v_config.ratio_6_to_7;
				elsif	l.tracking_level = v_config.track_level_5
				then
					v_temp_pallets	:= v_temp_pallets +
							   l.qty_ordered /
						           v_config.ratio_7_to_8 *
							   v_config.ratio_6_to_7 *
							   v_config.ratio_5_to_6;
				elsif	l.tracking_level = v_config.track_level_4
				then
					v_temp_pallets	:= v_temp_pallets +
							   l.qty_ordered /
						           v_config.ratio_7_to_8 *
							   v_config.ratio_6_to_7 *
							   v_config.ratio_5_to_6 *
							   v_config.ratio_4_to_5;
				elsif	l.tracking_level = v_config.track_level_3
				then
					v_temp_pallets	:= v_temp_pallets +
							   l.qty_ordered /
						           v_config.ratio_7_to_8 *
							   v_config.ratio_6_to_7 *
							   v_config.ratio_5_to_6 *
							   v_config.ratio_4_to_5 *
							   v_config.ratio_3_to_4;
				elsif	l.tracking_level = v_config.track_level_2
				then
					v_temp_pallets	:= v_temp_pallets +
							   l.qty_ordered /
						           v_config.ratio_7_to_8 *
							   v_config.ratio_6_to_7 *
							   v_config.ratio_5_to_6 *
							   v_config.ratio_4_to_5 *
							   v_config.ratio_3_to_4 *
							   v_config.ratio_2_to_3;
				elsif	l.tracking_level = v_config.track_level_1
				or	l.tracking_level = null
				then
					v_temp_pallets	:= v_temp_pallets +
							   l.qty_ordered /(
						           v_config.ratio_7_to_8 *
							   v_config.ratio_6_to_7 *
							   v_config.ratio_5_to_6 *
							   v_config.ratio_4_to_5 *
							   v_config.ratio_3_to_4 *
							   v_config.ratio_2_to_3 *
							   v_config.ratio_1_to_2);
				end if;
			else	-- no pallet config
				if	l.tracking_level = v_config.track_level_8
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_8 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_7
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_7 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_6
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_6 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_5
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_5 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_4
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_4 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_3
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_3 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_2
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_2 * l.qty_ordered) / v_default_volume);
				elsif	l.tracking_level = v_config.track_level_1
				or	l.tracking_level = null
				then
					v_temp_pallets	:= v_temp_pallets + ((v_config.volume_1 * l.qty_ordered) / v_default_volume);
				end if;
			end if;
		end if;
	end loop;--lines_loop
	--
	return ceil(v_temp_pallets);
	--
exception
	when others
	then
		return ceil(nvl(v_temp_pallets,0));
end est_nbr_pal_f;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 27-Sep-2016
-- Purpose : Get all orders to calculate the estimate number of boxes.
------------------------------------------------------------------------------------------------
    function  est_nbr_box_f(  p_order_id      in varchar2
                         ,  p_client_id     in varchar2
                         ) 
        return number
    is

        cursor  c_get_lines( a_order_id  varchar2
                           , a_client_id varchar2
                           ) 
        is
                select  l.client_id       client_id
                ,       l.line_id         line_id
                ,       l.order_id        order_id
                ,       l.sku_id          sku_id
                ,       l.qty_ordered     qty_ordered
                ,       l.tracking_level  tracking_level
                ,       l.config_id       config_id
                ,       l.expected_volume expected_volume
                from    dcsdba.order_line l
                where   l.order_id = a_order_id
                and     l.client_id = a_client_id
        ;
        --
        cursor  c_get_config_id( a_client_id varchar2
                               , a_sku_id    varchar2
                               ) 
        is
                select  scc.config_id       config_id
                from    dcsdba.sku_sku_config scc
                where   scc.client_id   = a_client_id
                and     scc.sku_id      = a_sku_id
                and     scc.config_id   = (  select  sc.config_id 
                                             from    dcsdba.sku_config sc
                                             where ( sc.track_level_1 = 'PALLET' or
                                                     sc.track_level_2 = 'PALLET' or
                                                     sc.track_level_3 = 'PALLET' or
                                                     sc.track_level_4 = 'PALLET' or
                                                     sc.track_level_5 = 'PALLET' or
                                                     sc.track_level_6 = 'PALLET' or
                                                     sc.track_level_7 = 'PALLET' or
                                                     sc.track_level_8 = 'PALLET'
                                                    )
                                            and     sc.config_id = scc.config_id
                                            and     rownum = 1
                                          )
        ;
        --
        cursor  c_get_config_id_2( a_client_id varchar2
                                 , a_sku_id    varchar2
                                 ) 
        is
                select  scc.config_id      config_id
                from    dcsdba.sku_sku_config scc
                where   scc.client_id = a_client_id
                and     scc.sku_id    = a_sku_id
                and     rownum = 1
        ;
        --
        cursor c_config_id  ( a_config_id varchar2
                            , a_client_id varchar2
                            ) 
        is
                select  p.config_id               config_id
                ,       p.client_id               client_id
                ,       nvl(p.ratio_1_to_2,1)     r_1_2
                ,       p.track_level_1           tl_1
                ,       nvl(p.ratio_2_to_3,1)     r_2_3
                ,       p.track_level_2           tl_2
                ,       nvl(p.ratio_3_to_4,1)     r_3_4
                ,       p.track_level_3           tl_3
                ,       nvl(p.ratio_4_to_5,1)     r_4_5
                ,       p.track_level_4           tl_4
                ,       nvl(p.ratio_5_to_6,1)     r_5_6
                ,       p.track_level_5           tl_5
                ,       nvl(p.ratio_6_to_7,1)     r_6_7
                ,       p.track_level_6           tl_6
                ,       nvl(p.ratio_7_to_8,1)     r_7_8
                ,       p.track_level_7           tl_7
                ,       p.track_level_8           tl_8
                from    dcsdba.sku_config p
                where   p.config_id = a_config_id
                and     p.client_id = a_client_id
        ;
        --
        l_config_id                 c_config_id%rowtype;
        l_get_config_id             c_get_config_id%rowtype;
        l_get_config_id_2           c_get_config_id_2%rowtype;
        l_my_config_id              varchar2(15);
        l_nbr_of_box_per_line       number(12,3);
        l_tot_nbr_of_box            number(12,3) :=0;
        --
    begin
        for r_get_lines in c_get_lines(  p_order_id
                                      , p_client_id
                                      )
        loop
            -- Set number of boxes to 0 as default.
            l_nbr_of_box_per_line := 0;

            -- Get the pack configuration to calculate with
            if      r_get_lines.config_id is not null -- no pack configuration in line
            then 
                    l_my_config_id := r_get_lines.config_id;
            else
                    -- Get pallet pack configuration from WMS
                    open    c_get_config_id (r_get_lines.client_id, r_get_lines.sku_id);
                    fetch   c_get_config_id 
                    into    l_get_config_id;
                    if      c_get_config_id%notfound 
                    then    
                            -- get any pack configuration from WMS
                            open    c_get_config_id_2 (r_get_lines.client_id, r_get_lines.sku_id);
                            fetch   c_get_config_id_2 
                            into    l_get_config_id_2;
                            if      c_get_config_id_2%notfound 
                            then    
                                    -- no configuration found
                                    l_my_config_id := 'NOCONFIG';
                            else  
                                    l_my_config_id := l_get_config_id_2.config_id;
                            end if;
                            close   c_get_config_id_2;
                    else
                            l_my_config_id := l_get_config_id.config_id;
                    end if;
                    close   c_get_config_id;
            end if;

            -- Get config details.
            if      l_my_config_id <> 'NOCONFIG' 
            then        
                    open    c_config_id ( l_my_config_id, r_get_lines.client_id);
                    fetch   c_config_id 
                    into    l_config_id;
                    if      (l_config_id.tl_8 not in ('BOX','OBX') or l_config_id.tl_8 is null ) and
                            (l_config_id.tl_7 not in ('BOX','OBX') or l_config_id.tl_7 is null ) and
                            (l_config_id.tl_6 not in ('BOX','OBX') or l_config_id.tl_6 is null ) and
                            (l_config_id.tl_5 not in ('BOX','OBX') or l_config_id.tl_5 is null ) and
                            (l_config_id.tl_4 not in ('BOX','OBX') or l_config_id.tl_4 is null ) and
                            (l_config_id.tl_3 not in ('BOX','OBX') or l_config_id.tl_3 is null ) and
                            (l_config_id.tl_2 not in ('BOX','OBX') or l_config_id.tl_2 is null ) and
                            (l_config_id.tl_1 not in ('BOX','OBX') or l_config_id.tl_1 is null )
                    then    -- No boxes found in pack configuration
                            l_nbr_of_box_per_line := 0;
                    else 
                            -- Calculate number of boxes when no tracking level in line.
                            if      nvl(r_get_lines.tracking_level,'N') = 'N' 
                            then    -- No specific tracking level in line
                                    if      l_config_id.tl_8 in ('BOX','OBX') 
                                    then    -- Tracking level 8 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7 / l_config_id.r_7_8;
                                    elsif   l_config_id.tl_7 in ('BOX','OBX')  
                                    then    -- Tracking level 7 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7;
                                    elsif   l_config_id.tl_6 in ('BOX','OBX') 
                                    then    -- Tracking level 6 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6;  
                                    elsif   l_config_id.tl_5 in ('BOX','OBX')
                                    then    -- Tracking level 5 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5;
                                    elsif   l_config_id.tl_4 in ('BOX','OBX')
                                    then    -- Tracking level 4 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4;
                                    elsif   l_config_id.tl_3 in ('BOX','OBX')
                                    then    -- Tracking level 3 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3;
                                    elsif   l_config_id.tl_2 in ('BOX','OBX')
                                    then    -- Tracking level 2 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2;
                                    elsif   l_config_id.tl_1 in ('BOX','OBX')
                                    then    -- Tracking level 1 is a box.
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered;
                                    end if;
                            else    -- a specific tracking level is used in the line
                                    if      r_get_lines.tracking_level in ('BOX','OBX')
                                    then
                                            l_nbr_of_box_per_line := r_get_lines.qty_ordered;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_8 
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX') 
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2 * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7 * l_config_id.r_7_8;
                                            elsif   l_config_id.tl_2 in ('BOX','OBX') 
                                            then
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7 * l_config_id.r_7_8;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX') 
                                            then
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7 * l_config_id.r_7_8;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX') 
                                            then
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7 * l_config_id.r_7_8;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX') 
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_5_6 * l_config_id.r_6_7 * l_config_id.r_7_8;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX') 
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_6_7 * l_config_id.r_7_8;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX') 
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_7 
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2 * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7;
                                            elsif   l_config_id.tl_2 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_4_5 * l_config_id.r_5_6 * l_config_id.r_6_7;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_5_6 * l_config_id.r_6_7;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_6 
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2 * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6;
                                            elsif   l_config_id.tl_2 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_3_4 * l_config_id.r_4_5 * l_config_id.r_5_6;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_4_5 * l_config_id.r_5_6;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_5_6;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX')
                                            then
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX')
                                            then
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_6_7 / l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_5 
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2 * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5;
                                            elsif   l_config_id.tl_2 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_2_3 * l_config_id.r_3_4 * l_config_id.r_4_5;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_3_4 * l_config_id.r_4_5;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_4_5;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_5_6;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_5_6 / l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_5_6 / l_config_id.r_6_7 / l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_4 
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2 * l_config_id.r_2_3 * l_config_id.r_3_4;
                                            elsif   l_config_id.tl_2 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_2_3 * l_config_id.r_3_4;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_3_4;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_4_5;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_4_5 / l_config_id.r_5_6;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX') 
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7 / l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_3 
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2 * l_config_id.r_2_3;
                                            elsif   l_config_id.tl_2 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_2_3;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_3_4;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_3_4 / l_config_id.r_4_5;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7 / l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_2
                                    then 
                                            if      l_config_id.tl_1 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered * l_config_id.r_1_2;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_2_3;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_2_3 / l_config_id.r_3_4;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7 / l_config_id.r_7_8;
                                            end if;
                                    elsif   r_get_lines.tracking_level = l_config_id.tl_1 
                                    then 
                                            if      l_config_id.tl_2 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2;
                                            elsif   l_config_id.tl_3 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3;
                                            elsif   l_config_id.tl_4 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4;
                                            elsif   l_config_id.tl_5 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5;
                                            elsif   l_config_id.tl_6 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6;
                                            elsif   l_config_id.tl_7 in ('BOX','OBX')
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7;
                                            elsif   l_config_id.tl_8 in ('BOX','OBX') 
                                            then 
                                                    l_nbr_of_box_per_line := r_get_lines.qty_ordered / l_config_id.r_1_2 / l_config_id.r_2_3 / l_config_id.r_3_4 / l_config_id.r_4_5 / l_config_id.r_5_6 / l_config_id.r_6_7 / l_config_id.r_7_8;
                                            end if;
                                    end if;
                            end if;
                    end if;      
                    close c_config_id; 
            else
                    l_nbr_of_box_per_line := r_get_lines.qty_ordered;
            end if;
            l_tot_nbr_of_box := l_tot_nbr_of_box + l_nbr_of_box_per_line;
    end loop;

    return ceil(l_tot_nbr_of_box);

  end est_nbr_box_f;  
---------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 27-Sep-2016
-- Pupose  : The function that is called to fetch a ship by date in WMS
---------------------------------------------------------------------------------------------------
    function add_vas_activity ( p_container_id_i          in varchar2 default null
                              , p_client_id_i             in varchar2 
                              , p_order_id_i              in varchar2 
                              , p_sku_id_i                in varchar2 default null
                              , p_activity_name_i         in varchar2
                              , p_activity_sequence_i     in number   default null
                              , p_activity_instruction_i  in varchar2 default null
                              )
        return integer
    is 
        l_retval integer := 1;
    begin
        cnl_sys.cnl_as_pck.add_vas_activity( p_container_id_i          => p_container_id_i
                                           , p_client_id_i             => p_client_id_i
                                           , p_order_id_i              => p_order_id_i
                                           , p_sku_id_i                => p_sku_id_i
                                           , p_activity_name_i         => p_activity_name_i
                                           , p_activity_sequence_i     => p_activity_sequence_i
                                           , p_activity_instruction_i  => p_activity_instruction_i
                                           );
        return l_retval;
    end add_vas_activity;
---------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 07-Jul-2019
-- Pupose  : Gets the ship by date based on address 
---------------------------------------------------------------------------------------------------
function call_cust_ship_by_date_f(	p_creation_date_i 	timestamp with local time zone
				 ,	p_client_id_i 		varchar2
			         , 	p_customer_id_i 	varchar2
			         )
	return timestamp with local time zone
is
	cursor c_dates
	is
		select	next_day( to_timestamp( trunc(p_creation_date_i)||
						     (	select	to_char(delivery_close_time,'HHMISS') 
						     	from 	dcsdba.address 
						     	where	address_id = p_customer_id_i
						     	and 	credit_days is not null 
						     	and	client_id = p_client_id_i
						     ),'DD-MON-RR HH:MI:SS.FF'),(	select	decode(credit_days,2,'MON'
														  ,3,'TUE'
														  ,4,'WED'
														  ,5,'THU'
														  ,6,'FRI')  
											from  	dcsdba.address 
											where	address_id = p_customer_id_i
											and 	credit_days is not null 
											and	client_id = p_client_id_i)) curr_cut_of_date,
			next_day( to_timestamp( trunc(p_creation_date_i+7)||
						     (	select	to_char(delivery_close_time,'HHMISS') 
						     	from 	dcsdba.address 
						     	where	address_id = p_customer_id_i
						     	and 	credit_days is not null 
						     	and	client_id = p_client_id_i
						     ),'DD-MON-RR HH:MI:SS.FF'),(	select	decode(credit_days,2,'MON'
														  ,3,'TUE'
														  ,4,'WED'
														  ,5,'THU'
														  ,6,'FRI')  
											from  	dcsdba.address 
											where	address_id = p_customer_id_i
											and 	credit_days is not null 
											and	client_id = p_client_id_i)) next_cut_of_date,
			next_day(p_creation_date_i,(select decode(nvl(delivery_open_mon,'N')||
								  nvl(delivery_open_tue,'N')||
							          nvl(delivery_open_wed,'N')||
							          nvl(delivery_open_thur,'N')||
							          nvl(delivery_open_fri,'N')
							          ,'YNNNN',2
							          ,'NYNNN',3
							          ,'NNYNN',4
							          ,'NNNYN',5
							          ,'NNNNY',6)
						   from  	dcsdba.address 
						   where	address_id = p_customer_id_i
						   and 		credit_days is not null 
						   and		client_id = p_client_id_i)) curr_ship_day	,					
			next_day(p_creation_date_i+7,(select decode(nvl(delivery_open_mon,'N')||
							            nvl(delivery_open_tue,'N')||
							            nvl(delivery_open_wed,'N')||
							            nvl(delivery_open_thur,'N')||
							            nvl(delivery_open_fri,'N')
							            ,'YNNNN',2
							            ,'NYNNN',3
							            ,'NNYNN',4
							            ,'NNNYN',5
							            ,'NNNNY',6)
						     from  	dcsdba.address 
						     where	address_id = p_customer_id_i
						     and 	credit_days is not null 
						     and	client_id = p_client_id_i)) next_ship_day						
		from dual
		;
	r_curr_cut_of_date		timestamp with local time zone;
	r_next_cut_of_date		timestamp with local time zone;
	r_curr_ship_day			timestamp with local time zone;
	r_next_ship_day			timestamp with local time zone;	
	l_retdate			timestamp with local time zone;	
begin
	open 	c_dates;
	fetch	c_dates into  	r_curr_cut_of_date
			,	r_next_cut_of_date
			,	r_curr_ship_day
			,	r_next_ship_day;
	close	c_dates;
	if 	p_creation_date_i <= r_curr_cut_of_date
	then
		l_retdate := r_curr_ship_day;
	else
		l_retdate := r_next_ship_day;
	end if;
	return l_retdate;
end call_cust_ship_by_date_f;
---------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 30-Aug-2019
-- Pupose  : Gets the first working day ahead or before check date 
---------------------------------------------------------------------------------------------------
function get_work_day_f( p_date_i 	in timestamp with local Time zone
      		       , p_days_i	in number
		       , p_plusminus_i	in varchar2 -- - or +
		       , p_calendar_id  in varchar2 default null
		       )
	return timestamp with local Time zone
	is
	--l_calendar varchar2(10) := 'RHENUS'; -- Holiday calender ID in WMS

	cursor c_holiday( b_date varchar2, b_calendar_id varchar2)
	is
	        select  count(*)
		from    dcsdba.calendar_holidays
		where   calendar_id = b_calendar_id
		and     b_date between to_char(start_dstamp,'DDMMYYYY') and to_char(stop_dstamp,'DDMMYYYY')
	;

	--
	l_holiday	integer :=0;
	l_check		integer :=0; -- 0 is not OK and 1 is OK.
	l_retval 	timestamp with local Time zone;
	l_date		timestamp with local Time zone := p_date_i;
	l_loop_times	number := p_days_i;
	--
begin
	execute immediate 'alter session set time_zone = ''Europe/Amsterdam''';
	--
	if 	l_loop_times is null or 
		l_loop_times = 0
	then
		l_loop_times := 1;
	end if;
	-- Expected is that the date inserted as parameter is a working day so not a holiday or a weekend day
	for i in 1 .. l_loop_times
	loop
		if	p_days_i > 0
		then
			if	p_plusminus_i = '-'
			then
				l_date := l_date - 1;
			else
				l_date := l_date + 1;
			end if;
		end if;
		--
		l_check := 0;
		while 	l_check = 0
		loop		
			-- check if it is saturday
			if      to_char(l_date,'D') = '7' -- saturday
			then
				if	p_plusminus_i = '-'
				then
					l_date := l_date - 1; -- Make it Friday
				else
					l_date := l_date + 2; -- Make it monday
				end if;
			end if;
			-- Check if it is sunday 
			if 	to_char(l_date,'D') = '1' -- Sunday
			then
				if	p_plusminus_i = '-'
				then
					l_date := l_date - 2; -- Make it Friday
				else
					l_date := l_date + 1; -- Make it monday
				end if;
			end if;
			-- check if it is a holiday
			open 	c_holiday( to_char(l_date,'DDMMYYYY'),p_calendar_id);
			fetch 	c_holiday 
			into 	l_holiday;
			close 	c_holiday;
			--
			if 	l_holiday > 0
			then
				if	p_plusminus_i = '-'
				then
					l_date := l_date - 1; -- set previous day
				else
					l_date := l_date + 1; -- set next day
				end if;
			else
				l_check := 1; -- Finished
			end if;
		end loop;
	end loop;
	--
	l_retval 	:= l_date;
	return l_retval;
end get_work_day_f;
---------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 03-Sep-2019
-- Pupose  : sets parameters for the Rhenus inhouse build QC functionlity
---------------------------------------------------------------------------------------------------
	function set_qc_parameters_f ( p_order_id_i		in dcsdba.order_header.order_id%type
				     , p_client_id_i		in dcsdba.order_header.client_id%type
				     , p_site_id_i		in dcsdba.order_header.from_site_id%type
				     , p_qc_req_yn_i		in cnl_sys.cnl_wms_qc_order.qc_req_yn%type -- QC is required 
				     , p_qc_batch_yn_i		in cnl_sys.cnl_wms_qc_order.qc_batch_yn%type -- QC batch id is required
				     , p_qc_qty_def_yn_i 	in cnl_sys.cnl_wms_qc_order.qc_qty_def_yn%type -- QTY is default 1 during QC
				     , p_qc_sku_select_yn_i	in cnl_sys.cnl_wms_qc_order.qc_sku_select_yn%type -- SKU can be selected from overview during QC
				     , p_qc_qty_upd_yn_i	in cnl_sys.cnl_wms_qc_order.qc_qty_upd_yn%type -- Default QTY can be changed
				     , p_qc_serial_yn_i		in cnl_sys.cnl_wms_qc_order.qc_serial_yn%type default null-- If serial checking is required yn
				     )
	return integer
	is
		l_retval integer := 1;
	begin
		cnl_sys.cnl_warehouse_handling_pck.set_qc_parameters_p( p_order_id_i
			   					      , p_client_id_i
			   					      , p_site_id_i
								      , p_qc_req_yn_i
								      , p_qc_batch_yn_i
								      , p_qc_qty_def_yn_i
								      , p_qc_sku_select_yn_i
								      , p_qc_qty_upd_yn_i
								      , p_qc_serial_yn_i
								      );
		return l_retval;
	end set_qc_parameters_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 25-Sep-2020
-- Purpose : A function to fetch the pallet type linked to the SKU.
------------------------------------------------------------------------------------------------
	function sku_default_pallet_type_f( p_client_id_i	in dcsdba.client.client_id%type
					  , p_sku_id_i		in dcsdba.sku.sku_id%type
					  )
	return 	dcsdba.pallet_config.config_id%type
	is
		l_retval	dcsdba.pallet_config.config_id%type;
	begin
		select	upper(pallet_type)
		into	l_retval
		from	cnl_sys.cnl_wms_sku_pallet_link
		where	client_id 	= p_client_id_i
		and	sku_id		= p_sku_id_i
		and	rownum  	= 1
		;
		return 	l_retval;
	exception
		when 	others
		then
			return null;
	end sku_default_pallet_type_f;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-Feb-2021
-- Purpose : A function update the order_header_extend table
------------------------------------------------------------------------------------------------
function update_order_header_extend_f( p_order_id_i	in cnl_sys.cnl_wms_order_header_extend.order_id%type
				     , p_client_id_i	in cnl_sys.cnl_wms_order_header_extend.client_id%type
				     , p_string_i	in varchar2
				     )
	return integer
is
	l_retval	integer;
begin
	l_retval := cnl_sys.cnl_wms_table_extend_pck.update_order_header_extend_f( p_order_id_i		=> p_order_id_i
										 , p_client_id_i	=> p_client_id_i
									         , p_string_i		=> p_string_i
										 );
	return l_retval;
end update_order_header_extend_f;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 16-Feb-2021
-- Purpose : A function update the order_header_extend table
------------------------------------------------------------------------------------------------
function update_order_line_extend_f( p_order_id_i	in cnl_sys.cnl_wms_order_line_extend.order_id%type
				   , p_client_id_i	in cnl_sys.cnl_wms_order_line_extend.client_id%type
				   , p_line_id_i	in cnl_sys.cnl_wms_order_line_extend.line_id%type
				   , p_string_i		in varchar2
				   )
	return integer
is
	l_retval	integer;
begin
	l_retval := cnl_sys.cnl_wms_table_extend_pck.update_order_line_extend_f( p_order_id_i	=> p_order_id_i
									       , p_client_id_i	=> p_client_id_i
									       , p_line_id_i	=> p_line_id_i
									       , p_string_i	=> p_string_i
									       );
	return l_retval;
end update_order_line_extend_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 21-Jul-2021
-- Purpose : A procedure to add a special instruction to an order using merge rules
-- CR 	   : BDS-5460
------------------------------------------------------------------------------------------------	
procedure insert_special_ins_p( p_code_i		in  dcsdba.special_ins.code%type
			      ,	p_client_id_i		in  dcsdba.special_ins.client_id%type
			      ,	p_reference_id_i	in  dcsdba.special_ins.reference_id%type
			      ,	p_line_id_i		in  dcsdba.special_ins.line_id%type default null
			      ,	p_type_i		in  dcsdba.special_ins.type%type
			      ,	p_text_i		in  dcsdba.special_ins.text%type default null
			      , p_retval_o		out integer
			      )
is
	l_check	integer;
	pragma 	autonomous_transaction;
begin
	select	count(*)
	into 	l_check
	from	dcsdba.special_ins
	where	code		= p_code_i
	and	client_id	= p_client_id_i
	and	reference_id	= p_reference_id_i
	and	nvl(line_id,999)=nvl(p_line_id_i,999)
	;
	if	l_check = 0
	then
		insert
		into	dcsdba.special_ins
		(	code
		,	client_id
		,	reference_id
		,	line_id
		,	sequence
		,	type
		,	text
		,	use_rdt
		,	use_dstream
		,	use_interface
		,	host_reference_id
		,	host_line_id
		,	archived
		)
		values
		(	p_code_i
		,	p_client_id_i
		,	p_reference_id_i
		,	p_line_id_i
		,	null -- sequence
		,	p_type_i -- O/P
		,	p_text_i
		,	'Y' -- use_rdt
		,	'N' -- use_dstream
		,	'N' -- use_interface
		,	null -- host_reference_id
		,	null -- host_line_id
		,	'N' -- archived
		);
	end if;
	p_retval_o := 1;
	commit;
exception
	when others
	then
		cnl_sys.cnl_util_pck.add_cnl_error( p_sql_code_i		=> sqlcode				-- Oracle SQL code or user defined error code
						  , p_sql_error_message_i	=> sqlerrm				-- SQL error message
						  , p_line_number_i		=> dbms_utility.format_error_backtrace	-- Procedure or function line number the error occured
						  , p_package_name_i		=> 'cnl_wms_mergerule_pck'		-- Package name the error occured
						  , p_routine_name_i		=> 'insert_special_ins_p'		-- Procedure or function generarting the error
						  , p_routine_parameters_i	=> 'code => '||p_code_i
										|| ' client_id => '||p_client_id_i
										|| ' reference_id =>'||p_reference_id_i
										|| ' line_id => '||to_char(p_line_id_i)
										|| ' type => '||p_type_i
										|| ' text => '||p_text_i
						  , p_comments_i		=> null					-- Additional comments describing the issue
						  );
		p_retval_o := 0;
end insert_special_ins_p;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 21-Jul-2021
-- Purpose : A function to add a special instruction to an order using merge rules
-- CR 	   : BDS-5460
------------------------------------------------------------------------------------------------
function insert_special_ins_f( p_code_i		in  dcsdba.special_ins.code%type
			     , p_client_id_i	in  dcsdba.special_ins.client_id%type
			     , p_reference_id_i	in  dcsdba.special_ins.reference_id%type
			     , p_line_id_i	in  dcsdba.special_ins.line_id%type default null
			     , p_type_i		in  dcsdba.special_ins.type%type
			     , p_text_i		in  dcsdba.special_ins.text%type default null
			     )
	return integer
is
	l_retval integer;
begin
	insert_special_ins_p( p_code_i		=> p_code_i
			    , p_client_id_i	=> p_client_id_i
			    , p_reference_id_i	=> p_reference_id_i
			    , p_line_id_i	=> p_line_id_i
			    , p_type_i		=> p_type_i
			    , p_text_i		=> p_text_i
			    , p_retval_o	=> l_retval
			    );
	return l_retval;
end insert_special_ins_f;

------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 21-Jul-2021
-- Purpose : A function update serial numbers with the receipt id created by Streamliner
-- CR 	   : BDS-5469
------------------------------------------------------------------------------------------------
function update_serial_receipt_id_f( p_pre_advice_id_i	dcsdba.pre_advice_header.pre_advice_id%type
				   , p_client_id_i	dcsdba.client.client_id%type
				   )
	return integer
is
	cursor c_lines
	is
		select 	distinct 
			l.host_pre_advice_id
		,	l.host_line_id
		,	l.sku_id
		from	dcsdba.pre_advice_line l
		where	l.client_id 	= p_client_id_i
		and	l.pre_advice_id	= p_pre_advice_id_i 
		;
begin
	for i in c_lines
	loop
		update	dcsdba.serial_number
		set	receipt_id	= p_pre_advice_id_i
		where	sku_id		= i.sku_id
		and	user_def_type_2	= i.host_line_id
		and	client_id	= p_client_id_i
		and	status 		= 'D' -- due in
		and	receipt_id 	= i.host_pre_advice_id
		;
	end loop;
	commit;
	return 1;
exception
	when others
	then
		return 0;
end update_serial_receipt_id_f;
------------------------------------------------------------------------------------------------
-- Author  : M. Swinkels, 3-Okt-2019
-- Purpose : Initialize package to load it faster.
------------------------------------------------------------------------------------------------
begin
  -- initialization
  null;
end cnl_wms_mergerule_pck;