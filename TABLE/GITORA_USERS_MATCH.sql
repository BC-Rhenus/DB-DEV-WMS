CREATE TABLE "CNL_SYS"."GITORA_USERS_MATCH" 
   (	"OS_USER" VARCHAR2(64 CHAR) NOT NULL ENABLE, 
	"GITORA_USER" VARCHAR2(64 CHAR) NOT NULL ENABLE, 
	"GITORA_PW" VARCHAR2(64 CHAR) NOT NULL ENABLE, 
	 CONSTRAINT "GUH_PK" PRIMARY KEY ("OS_USER")
  USING INDEX  ENABLE
   ) ;