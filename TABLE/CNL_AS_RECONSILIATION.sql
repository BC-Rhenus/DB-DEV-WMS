CREATE TABLE "CNL_SYS"."CNL_AS_RECONSILIATION" 
   (	"DSTAMP" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"COMMENTS" VARCHAR2(4000 CHAR), 
	"ACTION" VARCHAR2(4000 CHAR), 
	"CLIENT_ID" VARCHAR2(30 CHAR), 
	"SKU_ID" VARCHAR2(50 CHAR), 
	"TAG_ID" VARCHAR2(50 CHAR), 
	"WMS_QTY_ON_HAND" NUMBER, 
	"WMS_LOC_ASMISSING" NUMBER, 
	"WMS_LOC_ASFOUND" NUMBER, 
	"AS_QTY_ON_HAND" NUMBER, 
	"AS_SUSPECT_QTY" NUMBER, 
	"DIFFERENCE" NUMBER, 
	"RECONSILE_KEY" NUMBER
   ) ;

CREATE INDEX "CNL_SYS"."CNL_AS_RECONSILIATION_IDX" ON "CNL_SYS"."CNL_AS_RECONSILIATION" ("RECONSILE_KEY") 
  ;

CREATE INDEX "CNL_SYS"."CNL_AS_RECONSILIATION_IDX2" ON "CNL_SYS"."CNL_AS_RECONSILIATION" ("SKU_ID", "CLIENT_ID", "TAG_ID") 
  ;