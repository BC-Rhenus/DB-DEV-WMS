CREATE TABLE "CNL_SYS"."CNL_WMS_SKU_PALLET_LINK" 
   (	"CLIENT_ID" VARCHAR2(50 CHAR), 
	"SKU_ID" VARCHAR2(50 CHAR), 
	"PALLET_TYPE" VARCHAR2(50 CHAR) NOT NULL ENABLE, 
	"CONCURRENCY_KEY" VARCHAR2(32 CHAR) DEFAULT sys_guid() NOT NULL ENABLE, 
	"ID" NUMBER NOT NULL ENABLE, 
	"CREATED_BY" VARCHAR2(255 CHAR) NOT NULL ENABLE, 
	"CREATION_DATE" DATE NOT NULL ENABLE, 
	"LAST_UPDATED_BY" VARCHAR2(255 CHAR) NOT NULL ENABLE, 
	"LAST_UPDATE_DATE" DATE NOT NULL ENABLE, 
	 CONSTRAINT "SKU_PAL_PK" PRIMARY KEY ("CLIENT_ID", "SKU_ID")
  USING INDEX  ENABLE
   ) ;

CREATE INDEX "CNL_SYS"."SKU_PAL_PAL_IDX" ON "CNL_SYS"."CNL_WMS_SKU_PALLET_LINK" ("PALLET_TYPE") 
  ;

CREATE INDEX "CNL_SYS"."SKU_PAL_SKU_IDX" ON "CNL_SYS"."CNL_WMS_SKU_PALLET_LINK" ("SKU_ID") 
  ;