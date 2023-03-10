CREATE TABLE "CNL_SYS"."CNL_AS_MASTERDATA" 
   (	"CNL_KEY" NUMBER, 
	"WMS_DATA_TBL" VARCHAR2(3), 
	"WMS_ACTION" VARCHAR2(1), 
	"CNL_IF_STATUS" VARCHAR2(50), 
	"WMS_CLIENT_ID" VARCHAR2(50), 
	"WMS_SKU_ID" VARCHAR2(50), 
	"WMS_CONFIG_ID" VARCHAR2(50), 
	"WMS_TUC" VARCHAR2(50), 
	"WMS_SUPPLIER_SKU_ID" VARCHAR2(50), 
	"SYNQ_KEY" NUMBER, 
	"SYNQ_ACTION" VARCHAR2(3), 
	"DSTAMP" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"AS_SITE_ID" VARCHAR2(20)
   ) ;

CREATE INDEX "CNL_SYS"."CNL_AS_MASTERDATA_IDX" ON "CNL_SYS"."CNL_AS_MASTERDATA" ("WMS_CLIENT_ID", "WMS_SKU_ID") 
  ;