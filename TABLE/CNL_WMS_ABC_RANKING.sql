CREATE TABLE "CNL_SYS"."CNL_WMS_ABC_RANKING" 
   (	"SKU_ID" VARCHAR2(50), 
	"CLIENT_ID" VARCHAR2(10), 
	"CLIENT_GROUP" VARCHAR2(10), 
	"SITE_ID" VARCHAR2(10), 
	"TIMES_PICKED" NUMBER(8,0), 
	"TOT_EACH_PICKED" NUMBER(15,0), 
	"RANK" NUMBER(3,0), 
	"ROW_NUM" NUMBER, 
	"RANKING_DATE" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"VERS" VARCHAR2(3), 
	"PROCESSED" VARCHAR2(3), 
	"ABC_FREQUENCY" VARCHAR2(1)
   ) ;

CREATE INDEX "CNL_SYS"."CNL_WMS_ABC_CS_IDX" ON "CNL_SYS"."CNL_WMS_ABC_RANKING" ("CLIENT_ID", "SKU_ID") 
  ;

CREATE INDEX "CNL_SYS"."CNL_WMS_ABC_GR_IDX" ON "CNL_SYS"."CNL_WMS_ABC_RANKING" ("CLIENT_GROUP") 
  ;