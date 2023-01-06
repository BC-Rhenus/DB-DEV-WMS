CREATE OR REPLACE FORCE VIEW "CNL_SYS"."CNL_AS_TPB_STORAGE_DATA" ("CLIENT_ID", "NBR_OF_BINS", "BIN_TYPE", "NBR_OF_COMPARTMENTS", "BINS_PER_CLIENT") AS 
  with my_data as
(
select      count(distinct l.tu_key)        as nbr_of_bins
,           count(l.lu_key)                 as nbr_of_compartments
,           t.tu_type_id                    as bin_type
,           o.owner_id                      as client_id
from        rhenus_synq.load_unit@as_synq l
,           rhenus_synq.product@as_synq p
,           rhenus_synq.owner@as_synq o
,           rhenus_synq.transport_unit@as_synq t
where       l.product_key   = p.product_key
and         p.owner_key     = o.owner_key
and         t.tu_key        = l.tu_key
and         t.class_type    = 'AUTOSTORE_BIN'
group by    t.tu_type_id
,           o.owner_id
order by    o.owner_id
)
select      m1.client_id
,           m1.nbr_of_bins
,           m1.bin_type
,           m1.nbr_of_compartments
,	    (	select 	sum(m2.nbr_of_bins)
		from 	my_data m2
		where	m2.client_id = m1.client_id
		group by client_id
	    ) bins_per_client
from        my_data m1
order by    m1.client_id, m1.bin_type