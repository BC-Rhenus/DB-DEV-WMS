CREATE TABLE "CNL_SYS"."CNL_TEST_IF_ORDER_LINE" 
   (	"TEST_KEY" NUMBER(*,0), 
	"TEST_COMMENT" VARCHAR2(4000 CHAR), 
	"CLIENT_ID" VARCHAR2(10 CHAR), 
	"ORDER_ID" VARCHAR2(16 CHAR), 
	"LINE_ID" NUMBER(6,0), 
	"HOST_ORDER_ID" VARCHAR2(20 CHAR), 
	"HOST_LINE_ID" VARCHAR2(20 CHAR), 
	"SKU_ID" VARCHAR2(50 CHAR), 
	"CUSTOMER_SKU_ID" VARCHAR2(50 CHAR), 
	"CONFIG_ID" VARCHAR2(15 CHAR), 
	"TRACKING_LEVEL" VARCHAR2(8 CHAR), 
	"BATCH_ID" VARCHAR2(15 CHAR), 
	"BATCH_MIXING" VARCHAR2(1 CHAR), 
	"SHELF_LIFE_DAYS" NUMBER(5,0), 
	"SHELF_LIFE_PERCENT" NUMBER(3,0), 
	"ORIGIN_ID" VARCHAR2(10 CHAR), 
	"CONDITION_ID" VARCHAR2(10 CHAR), 
	"LOCK_CODE" VARCHAR2(10 CHAR), 
	"SPEC_CODE" VARCHAR2(99 CHAR), 
	"QTY_ORDERED" NUMBER(15,6), 
	"ALLOCATE" VARCHAR2(1 CHAR), 
	"BACK_ORDERED" VARCHAR2(1 CHAR), 
	"KIT_SPLIT" VARCHAR2(1 CHAR), 
	"DEALLOCATE" VARCHAR2(1 CHAR), 
	"NOTES" VARCHAR2(80 CHAR), 
	"PSFT_INT_LINE" NUMBER(5,0), 
	"PSFT_SCHD_LINE" NUMBER(7,2), 
	"PSFT_DMND_LINE" NUMBER(4,0), 
	"SAP_PICK_REQ" VARCHAR2(10 CHAR), 
	"DISALLOW_MERGE_RULES" VARCHAR2(1 CHAR), 
	"LINE_VALUE" NUMBER(12,3), 
	"RULE_ID" VARCHAR2(10 CHAR), 
	"SOH_ID" VARCHAR2(4 CHAR), 
	"USER_DEF_TYPE_1" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_2" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_3" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_4" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_5" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_6" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_7" VARCHAR2(30 CHAR), 
	"USER_DEF_TYPE_8" VARCHAR2(30 CHAR), 
	"USER_DEF_CHK_1" VARCHAR2(1 CHAR), 
	"USER_DEF_CHK_2" VARCHAR2(1 CHAR), 
	"USER_DEF_CHK_3" VARCHAR2(1 CHAR), 
	"USER_DEF_CHK_4" VARCHAR2(1 CHAR), 
	"USER_DEF_DATE_1" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"USER_DEF_DATE_2" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"USER_DEF_DATE_3" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"USER_DEF_DATE_4" TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	"USER_DEF_NUM_1" NUMBER(15,6), 
	"USER_DEF_NUM_2" NUMBER(15,6), 
	"USER_DEF_NUM_3" NUMBER(15,6), 
	"USER_DEF_NUM_4" NUMBER(15,6), 
	"USER_DEF_NOTE_1" VARCHAR2(200 CHAR), 
	"USER_DEF_NOTE_2" VARCHAR2(200 CHAR), 
	"TASK_PER_EACH" VARCHAR2(1 CHAR), 
	"USE_PICK_TO_GRID" VARCHAR2(1 CHAR), 
	"IGNORE_WEIGHT_CAPTURE" VARCHAR2(1 CHAR), 
	"STAGE_ROUTE_ID" VARCHAR2(20 CHAR), 
	"MIN_QTY_ORDERED" NUMBER(15,6), 
	"MAX_QTY_ORDERED" NUMBER(15,6), 
	"EXPECTED_VOLUME" NUMBER(15,6), 
	"EXPECTED_WEIGHT" NUMBER(15,6), 
	"EXPECTED_VALUE" NUMBER(12,3), 
	"CUSTOMER_SKU_DESC1" VARCHAR2(80 CHAR), 
	"CUSTOMER_SKU_DESC2" VARCHAR2(80 CHAR), 
	"PURCHASE_ORDER" VARCHAR2(25 CHAR), 
	"PRODUCT_PRICE" NUMBER(12,3), 
	"PRODUCT_CURRENCY" VARCHAR2(3 CHAR), 
	"DOCUMENTATION_UNIT" VARCHAR2(8 CHAR), 
	"EXTENDED_PRICE" NUMBER(12,3), 
	"TAX_1" NUMBER(12,3), 
	"TAX_2" NUMBER(12,3), 
	"DOCUMENTATION_TEXT_1" VARCHAR2(180 CHAR), 
	"SERIAL_NUMBER" VARCHAR2(30 CHAR), 
	"OWNER_ID" VARCHAR2(10 CHAR), 
	"COLLECTIVE_MODE" VARCHAR2(1 CHAR) DEFAULT '', 
	"COLLECTIVE_SEQUENCE" NUMBER(10,0), 
	"CE_RECEIPT_TYPE" VARCHAR2(2 CHAR), 
	"CE_COO" VARCHAR2(3 CHAR), 
	"KIT_PLAN_ID" VARCHAR2(30 CHAR), 
	"LOCATION_ID" VARCHAR2(20 CHAR), 
	"UNALLOCATABLE" VARCHAR2(1 CHAR), 
	"MIN_FULL_PALLET_PERC" NUMBER(3,0), 
	"MAX_FULL_PALLET_PERC" NUMBER(3,0), 
	"FULL_TRACKING_LEVEL_ONLY" VARCHAR2(1 CHAR), 
	"SUBSTITUTE_GRADE" VARCHAR2(10 CHAR), 
	"DISALLOW_SUBSTITUTION" VARCHAR2(1 CHAR), 
	"SESSION_TIME_ZONE_NAME" VARCHAR2(64 CHAR) DEFAULT sessiontimezone, 
	"TIME_ZONE_NAME" VARCHAR2(64 CHAR), 
	"NLS_CALENDAR" VARCHAR2(30 CHAR), 
	"CLIENT_GROUP" VARCHAR2(10 CHAR)
   ) ;