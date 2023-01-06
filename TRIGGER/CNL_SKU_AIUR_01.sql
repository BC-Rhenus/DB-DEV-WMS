CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_SKU_AIUR_01" 
	after   insert or update on dcsdba.sku
	for each row
	  WHEN (	nvl(new.serial_at_pick,'N') 	= 'Y' or 
		nvl(new.serial_at_pack,'N') 	= 'Y' or 
		nvl(new.serial_at_receipt,'N') 	= 'Y' or
		nvl(old.serial_at_pick,'N') 	= 'Y' or 
		nvl(old.serial_at_pack,'N') 	= 'Y' or 
		nvl(old.serial_at_receipt,'N') 	= 'Y') declare
		cursor c_ser( a_serial_number	varchar2
			    , a_client_id	varchar2
			    , a_sku_id		varchar2
			    )
		is
			select	1
			from	dcsdba.serial_number s
			where	s.serial_number = a_serial_number
			and	s.client_id 	= a_client_id
			and	s.sku_id 	= a_sku_id
		;
		--
		r_ser number;
		--
		pragma autonomous_transaction;
	begin
		-- A sku update
		if      updating 
		then
			-- Delete all serial numbers from serial number table if any exist and sku is no longer serial controlled.
			if 	nvl(:new.serial_at_pick,'N') 	= 'N' 
			and	nvl(:new.serial_at_pack,'N') 	= 'N' 
			and	nvl(:new.serial_at_receipt,'N') = 'N' 
			then
				delete 	dcsdba.serial_number s
				where	s.client_id 	= :new.client_id
				and	s.sku_id 	= :new.sku_id
				and	s.status 	= 'P'
				and	s.site_id 	is null
				and	s.order_id 	is null
				and	s.serial_number in (:old.ean, :new.ean, :old.upc, :new.upc, :new.sku_id);
				commit;
			else	
				-- SKU is serial controlled now check if SKU id already exist as a serial number
				r_ser := 0;
				open 	c_ser(:new.sku_id, :new.client_id, :new.sku_id);
				fetch	c_ser into r_ser;
				if	c_ser%notfound then r_ser := 0;	end if;
				close 	c_ser;
				if 	nvl(r_ser,0) = 0
				then	-- SKU is not yet created as serial number
					insert into dcsdba.serial_number( serial_number
									, client_id
									, sku_id
									, status --P
									, uploaded --Y
									, repacked --N
									, in_transit --N
									, user_def_chk_1 --N
									, user_def_chk_2 --N
									, user_def_chk_3 --N
									, user_def_chk_4 --N
									) values( :new.sku_id
										, :new.client_id
										, :new.sku_id
										, 'P','Y','N','N','N','N','N','N');
				end if;
				commit;
				-- check ean number and add new ean and remove old ean
				if	nvl(:old.ean,'N') != 'N' -- Old EAN number existed
				then
					delete 	dcsdba.serial_number s
					where	s.serial_number = :old.ean
					and	s.sku_id 	= :new.sku_id
					and	s.client_id	= :new.client_id
					and	s.status 	= 'P';
				end if;
				commit;
				--				
				if	nvl(:new.ean,'N') != 'N' -- sku has an EAN
				then
					r_ser := 0;
					open 	c_ser(:new.ean, :new.client_id, :new.sku_id);
					fetch	c_ser into r_ser;
					if	c_ser%notfound then r_ser := 0;	end if;
					close 	c_ser;
					if	nvl(r_ser,0) = 0
					then	-- insert new EAN number as serial
						insert into dcsdba.serial_number( serial_number
										, client_id
										, sku_id
										, status --P
										, uploaded --Y
										, repacked --N
										, in_transit --N
										, user_def_chk_1 --N
										, user_def_chk_2 --N
										, user_def_chk_3 --N
										, user_def_chk_4 --N
										) values( :new.ean
											, :new.client_id
											, :new.sku_id
											, 'P','Y','N','N','N','N','N','N');
					end if;
				end if;
				commit;
				-- check UPC number and add new UPC and remove old UPC
				if	nvl(:old.upc,'N') != 'N' -- Old EAN number existed
				then
					delete 	dcsdba.serial_number s
					where	s.serial_number = :old.upc
					and	s.sku_id 	= :new.sku_id
					and	s.client_id	= :new.client_id
					and	s.status 	= 'P';
				end if;
				commit;
				--
				if	nvl(:new.upc,'N') != 'N'-- SKU has an UPC number
				then
					r_ser := 0;
					open 	c_ser(:new.upc, :new.client_id, :new.sku_id);
					fetch	c_ser into r_ser;
					if	c_ser%notfound then r_ser := 0;	end if;
					close 	c_ser;
					if	nvl(r_ser,0) = 0
					then	-- insert new UPC number as serial
						insert into dcsdba.serial_number( serial_number
										, client_id
										, sku_id
										, status --P
										, uploaded --Y
										, repacked --N
										, in_transit --N
										, user_def_chk_1 --N
										, user_def_chk_2 --N
										, user_def_chk_3 --N
										, user_def_chk_4 --N
										) values( :new.upc
											, :new.client_id
											, :new.sku_id
											, 'P','Y','N','N','N','N','N','N');
					end if;
				end if;
				commit;
			end if;
		end if; -- end update


		-- Inserting a new SKU.
		if 	inserting
		then
			r_ser := 0;
			open 	c_ser(:new.sku_id, :new.client_id, :new.sku_id);
			fetch 	c_ser into r_ser;
			if	c_ser%notfound then r_ser := 0;	end if;
			close 	c_ser;
			if	nvl(r_ser,0) = 1
			then
				null;
			else
				insert into dcsdba.serial_number( serial_number
								, client_id
								, sku_id
								, status --P
								, uploaded --Y
								, repacked --N
								, in_transit --N
								, user_def_chk_1 --N
								, user_def_chk_2 --N
								, user_def_chk_3 --N
								, user_def_chk_4 --N
								) values( :new.sku_id
									, :new.client_id
									, :new.sku_id
									, 'P','Y','N','N','N','N','N','N');
			end if; -- end insert new sku
			commit;
			--
			r_ser := 0;
			-- Insert of SKU with EAN number
			if	nvl(:new.ean,'N') != 'N'
			then
				r_ser := 0;
				open 	c_ser(:new.ean, :new.client_id, :new.sku_id);
				fetch 	c_ser into r_ser;
				if	c_ser%notfound then r_ser := 0;	end if;
				close 	c_ser;
				if	nvl(r_ser,0) = 1
				then
					null;
				else
					insert into dcsdba.serial_number( serial_number
									, client_id
									, sku_id
									, status --P
									, uploaded --Y
									, repacked --N
									, in_transit --N
									, user_def_chk_1 --N
									, user_def_chk_2 --N
									, user_def_chk_3 --N
									, user_def_chk_4 --N
									) values( :new.ean
										, :new.client_id
										, :new.sku_id
										, 'P','Y','N','N','N','N','N','N')
					;
				end if; -- end insert ean
			end if; -- end insert new sku ean
			commit;
			-- Insert of SKU with UPC number
			if	nvl(:new.upc,'N') != 'N'
			then
				r_ser := 0;
				open 	c_ser(:new.upc, :new.client_id, :new.sku_id);
				fetch 	c_ser into r_ser;
				if	c_ser%notfound then r_ser := 0;	end if;
				close 	c_ser;
				if	nvl(r_ser,0) = 1
				then
					null;
				else
					insert into dcsdba.serial_number( serial_number
									, client_id
									, sku_id
									, status --P
									, uploaded --Y
									, repacked --N
									, in_transit --N
									, user_def_chk_1 --N
									, user_def_chk_2 --N
									, user_def_chk_3 --N
									, user_def_chk_4 --N
									) values( :new.upc
										, :new.client_id
										, :new.sku_id
										, 'P','Y','N','N','N','N','N','N')
					;
				end if; -- End insert UPC
			end if; -- end insert new SKU UPC
			commit;
		end if; -- inserting
		commit;
exception 
	when others
	then
		null;
end cnl_as_sku_aiur_01;