CREATE TABLE "CNL_SYS"."CNL_ERROR" 
   (	"ERROR_DATE" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"SQL_ERROR_CODE" VARCHAR2(10 CHAR), 
	"SQL_ERROR_MESSAGE" VARCHAR2(4000 CHAR), 
	"LINE_NUMBER" VARCHAR2(4000 CHAR), 
	"PACKAGE_NAME" VARCHAR2(30 CHAR), 
	"ROUTINE_NAME" VARCHAR2(30 CHAR), 
	"ROUTINE_PARAMETERS" VARCHAR2(4000 CHAR), 
	"COMMENTS" VARCHAR2(4000 CHAR)
   ) ;