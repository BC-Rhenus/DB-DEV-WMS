CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_EDI_PCK" is
/********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: Martijn Swinkels
* $Date: 07-03-2019
**********************************************************************************
*
* Description: 
* Package used by integrators like XIB, Seeburger BIS
* 
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
/***************************************************************************************************************
* function to get track and trace details from Centiro
***************************************************************************************************************/                   
    function get_tracking_nbr_f( p_client_id_i          varchar2
                               , p_site_id_i            varchar2
                               , p_order_id_i           varchar2
                               , p_container_id_i       varchar2 default null
                               , p_pallet_id_i          varchar2 default null
                               , p_con_labelled_i       varchar2 default null
                               , p_pal_labelled_i       varchar2 default null
                               )
        return varchar2
    is
	cursor c_pcl( b_parcel_id varchar2
		    , b_site_id   varchar2
		    , b_client_id varchar2
                    , b_order_id  varchar2
                    )
	is
		select	max("idPRC") pcl_id
		from    Parcels@centiro.rhenus.de 
		where   "ParcelID" = b_parcel_id
		and     "CodeSEN"  = b_client_id || '@' || b_site_id
		and     "OrderNo"  = b_order_id
	;
	--
	cursor c_shp( b_site_id   varchar2
		    , b_client_id varchar2
                    , b_order_id  varchar2
                    )
	is
		select	max("idSHP") shp_id
		from    Shipments@centiro 
		where   "CodeSEN"  = b_client_id || '@' || b_site_id
		and     "OrderNo"  = b_order_id
	;
	--
        cursor c_dat( b_shp integer)
        is
                select  s."SequenceNo"    tracking_number
                from    Shipments@centiro s
                where   s."idSHP" = b_shp
        ;
        --
        cursor c_pat( b_pcl integer)
        is
                select  p."SequenceNo"		tracking_number
                from	Parcels@centiro.rhenus.de p
                where	p."idPRC"		= b_pcl
        ;

        cursor  c_client(b_client_id varchar2)
        is
                select  client_id
                from    dcsdba.client
                where   (client_id = b_client_id or user_def_type_1 = b_client_id)
                and     rownum = 1
        ;

	cursor 	c_saas(b_client_id varchar2)
	is
		select count(*)
		from	dcsdba.client_group_clients
		where	client_group = 'CTOSAAS'
		and	client_id = b_client_id
	;
        --
	r_shp		c_shp%rowtype;
	r_pcl		c_pcl%rowtype;
        r_client        c_client%rowtype;
        l_client        varchar2(50);  
	l_cnt		integer;
        l_site          varchar2(50);
        r_dat           c_dat%rowtype;
        r_pat           c_pat%rowtype;
        l_retval        varchar2(50);
        l_parcel_id     varchar2(50);
        l_track_nbr     varchar2(50);
    begin
	open c_saas(p_client_id_i);
	fetch c_saas 
	into l_cnt;
	close c_saas;

	if	l_cnt > 0
	then
		-- set parcel id
		if      p_container_id_i is not null and
			nvl(p_con_labelled_i,'N') = 'Y'
		then
			l_parcel_id := p_container_id_i;
		else
			l_parcel_id := p_pallet_id_i;
		end if;

		-- get Centiro client id
		open    c_client(p_client_id_i);
		fetch   c_client into r_client;
		if      c_client%notfound
		then
			close       c_client;
			l_client    := p_client_id_i;
		else
			close c_client;
			l_client    := r_client.client_id;
		end if;

		-- Set centiro site id
		if      p_site_id_i = 'RCLTLB'
		then
			l_site      := 'NLTLG01';
		elsif   p_site_id_i = 'RCLEHV'
		then
			l_site  := 'NLSBR01';
		else
			l_site  := p_site_id_i;
		end if;

		-- Get tracking number from parcel
		if      l_parcel_id is not null
		then
			open 	c_pcl( p_container_id_i
				     , l_site
				     , l_client
				     , p_order_id_i
				     );
			fetch 	c_pcl
			into	r_pcl;
			if 	c_pcl%notfound
			then
				close   c_pcl;
				l_track_nbr := null;
			else
				close	c_pcl;
				open    c_pat( r_pcl.pcl_id);
				fetch   c_pat 
				into 	r_pat;
				if      c_pat%notfound
				then
					close   c_pat;
					l_track_nbr := null;
				else
					close   c_pat;
					l_track_nbr := r_pat.tracking_number;
				end if;
			end if;
		end if;
		-- Get tracking number from shipment
		if      l_track_nbr is null
		then
			open 	c_shp( l_site
				     , l_client
				     , p_order_id_i
				     );
			fetch 	c_shp
			into	r_shp;
			if 	c_shp%notfound
			then
				close       c_shp;
				l_track_nbr := null;
			else
				close 	c_shp;
				open    c_dat(r_shp.shp_id );
				fetch   c_dat 
				into 	r_dat;
				if      c_dat%notfound 
				then    
					close       c_dat;
					l_track_nbr := null;
				else
					close       c_dat;
					l_track_nbr := r_dat.tracking_number;
				end if;
			end if;
		end if;
		l_retval := l_track_nbr;
		return l_retval;
	else
		return null;
	end if;
    exception
        when others
        then
                l_retval    := null;  
    end get_tracking_nbr_f;
/***************************************************************************************************************
* procedure to get track and trace details from Centiro
***************************************************************************************************************/                   
    procedure get_tracking_nbr_p( p_client_id_i     in  varchar2
                                , p_site_id_i       in  varchar2
                                , p_order_id_i      in  varchar2
                                , p_container_id_i  in  varchar2 default null
                                , p_pallet_id_i     in  varchar2 default null
                                , p_con_labelled_i  in  varchar2 default null
                                , p_pal_labelled_i  in  varchar2 default null
                                , p_bol_o           out varchar2
                                )
    is
        l_bol varchar2(50);
    begin
        l_bol   := get_tracking_nbr_f(p_client_id_i,p_site_id_i,p_order_id_i, p_container_id_i, p_pallet_id_i, p_con_labelled_i, p_pal_labelled_i);
        p_bol_o := l_bol;
    end get_tracking_nbr_p;
/***************************************************************************************************************
* procedure to get track and trace details from Centiro
***************************************************************************************************************/                   
	procedure check_csl_dossier( p_vat_sales_order_nr 	in  varchar2
				   , p_ok_yn			out integer
				   )
	is
		cursor	c_check
		is
			select 	1
			from 	customs_basic.dossier@csl_rcl
			where	dossiertype = 'O'
			and	ordernumber = p_vat_sales_order_nr
			and	status = 'OK'
		;
		--
		l_check integer;
		l_ok_yn integer;
	begin
		open 	c_check;
		fetch 	c_check into l_check;
		if	c_check%found
		then
			close c_check;
			p_ok_yn := 1;
		else
			close c_check;
			p_ok_yn := 0;
		end if;
	end check_csl_dossier;
--
end cnl_edi_pck;