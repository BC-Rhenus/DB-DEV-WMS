CREATE TABLE "CNL_SYS"."CNL_CONTAINER_VAS_ACTIVITY" 
   (	"CONTAINER_ID" VARCHAR2(30 CHAR), 
	"CLIENT_ID" VARCHAR2(30 CHAR), 
	"ORDER_ID" VARCHAR2(50 CHAR), 
	"SKU_ID" VARCHAR2(50 CHAR), 
	"ACTIVITY_NAME" VARCHAR2(50 CHAR) NOT NULL ENABLE, 
	"ACTIVITY_SEQUENCE" NUMBER, 
	"ACTIVITY_DESCRIPTION" VARCHAR2(200 CHAR), 
	"ACTIVITY_INSTRUCTION" VARCHAR2(4000 CHAR), 
	 CONSTRAINT "FK_ACTIVITY_NAME" FOREIGN KEY ("ACTIVITY_NAME")
	  REFERENCES "CNL_SYS"."CNL_VAS_ACTIVITY" ("ACTIVITY_NAME") ENABLE
   ) ;

CREATE INDEX "CNL_SYS"."CNL_CON_CON_VAS_ACTIVITY_IDX" ON "CNL_SYS"."CNL_CONTAINER_VAS_ACTIVITY" ("CONTAINER_ID") 
  ;

CREATE INDEX "CNL_SYS"."CNL_CON_ORD_VAS_ACTIVITY_IDX" ON "CNL_SYS"."CNL_CONTAINER_VAS_ACTIVITY" ("ORDER_ID") 
  ;

CREATE INDEX "CNL_SYS"."CNL_CON_SKU_VAS_ACTIVITY_IDX" ON "CNL_SYS"."CNL_CONTAINER_VAS_ACTIVITY" ("SKU_ID") 
  ;