CREATE TABLE "CNL_SYS"."CNL_WMS_ORDER_LINE_EXTEND" 
   (	"ORDER_ID" VARCHAR2(20 CHAR), 
	"CLIENT_ID" VARCHAR2(10 CHAR), 
	"LINE_ID" NUMBER(6,0), 
	"SKU_ID" VARCHAR2(50 CHAR) NOT NULL ENABLE, 
	"QTY_ORDERED" NUMBER(15,6) NOT NULL ENABLE, 
	"CONTAINS_HAZMAT" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_UGLY_SKU" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_AWKWARD_SKU" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_DUAL_USE_SKU" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_CONFIG_KIT" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_TWO_MAN_LIFT" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_CONVEYABLE_SKU" VARCHAR2(1 CHAR) DEFAULT 'N', 
	"CONTAINS_KIT" VARCHAR2(1 CHAR) DEFAULT 'N', 
	 CONSTRAINT "ORDER_LINE_EXTEND_PK" PRIMARY KEY ("ORDER_ID", "CLIENT_ID", "LINE_ID")
  USING INDEX  ENABLE
   ) ;