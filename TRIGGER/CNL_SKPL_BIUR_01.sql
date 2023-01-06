CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_SKPL_BIUR_01" 
  BEFORE INSERT OR UPDATE ON "CNL_SYS"."CNL_WMS_SKU_PALLET_LINK"
  REFERENCING FOR EACH ROW
  DECLARE BEGIN
   IF inserting THEN
      IF ( :new.id IS NULL ) THEN
         SELECT
            cnl_wms_sku_pallet_link_seq1.NEXTVAL
         INTO :new.id
         FROM
            dual;

      END IF;
   :new.concurrency_key := sys_guid();
   END IF;

   IF updating THEN
      :new.concurrency_key := sys_guid();
   END IF;

END cnl_skpl_biur_01;