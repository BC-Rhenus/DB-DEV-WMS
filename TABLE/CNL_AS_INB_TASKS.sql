CREATE TABLE "CNL_SYS"."CNL_AS_INB_TASKS" 
   (	"CNL_BATCH_KEY" NUMBER, 
	"WMS_MT_KEY" NUMBER, 
	"WMS_MT_NEW_KEY" NUMBER, 
	"WMS_MT_QTY_TO_MOVE" NUMBER, 
	"WMS_MT_TAG_ID" VARCHAR2(50), 
	"WMS_MT_TASK_TYPE" VARCHAR2(1), 
	"CNL_IF_STATUS" VARCHAR2(50), 
	"CNL_SPLIT_TASKS" NUMBER DEFAULT 0, 
	"SYNQ_KEY" NUMBER, 
	"AS_SITE_ID" VARCHAR2(20), 
	"AS_QTY_PUTAWAYED" NUMBER, 
	"DSTAMP" TIMESTAMP (6) WITH LOCAL TIME ZONE
   ) ;

CREATE INDEX "CNL_SYS"."CNL_AS_INB_TASKS_IDX" ON "CNL_SYS"."CNL_AS_INB_TASKS" ("WMS_MT_TAG_ID", "WMS_MT_KEY") 
  ;