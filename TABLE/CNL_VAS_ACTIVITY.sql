CREATE TABLE "CNL_SYS"."CNL_VAS_ACTIVITY" 
   (	"ID" NUMBER, 
	"ACTIVITY_NAME" VARCHAR2(50 CHAR), 
	"ACTIVITY_DESCRIPTION" VARCHAR2(200 CHAR), 
	 CONSTRAINT "PK_ID" PRIMARY KEY ("ID")
  USING INDEX  ENABLE, 
	 CONSTRAINT "UN_ACTIVITY_NAME" UNIQUE ("ACTIVITY_NAME")
  USING INDEX  ENABLE
   ) ;