CREATE TABLE "CNL_SYS"."CNL_MULTISCAN_DATA" 
   (	"USERS" VARCHAR2(30), 
	"CLIENT_ID" VARCHAR2(30), 
	"SKU_ID" VARCHAR2(50), 
	"NBR_TRCK_LVLS" NUMBER, 
	"PALLET_TYPE" VARCHAR2(50), 
	"LAYER_HEIGHT" NUMBER, 
	"EACH_PER_LAYER" NUMBER, 
	"NUM_LAYERS" NUMBER, 
	"TRACK_LEVEL_1" VARCHAR2(30), 
	"EACH_DEPTH" NUMBER, 
	"EACH_WIDTH" NUMBER, 
	"EACH_HEIGHT" NUMBER, 
	"EACH_WEIGHT" NUMBER, 
	"TRACK_LEVEL_2" VARCHAR2(30), 
	"RATIO_1_TO_2" NUMBER, 
	"DEPTH_2" NUMBER, 
	"WIDTH_2" NUMBER, 
	"HEIGHT_2" NUMBER, 
	"WEIGHT_2" NUMBER, 
	"TRACK_LEVEL_3" VARCHAR2(30), 
	"RATIO_2_TO_3" NUMBER, 
	"DEPTH_3" NUMBER, 
	"WIDTH_3" NUMBER, 
	"HEIGHT_3" NUMBER, 
	"WEIGHT_3" NUMBER, 
	"TRACK_LEVEL_4" VARCHAR2(30), 
	"RATIO_3_TO_4" NUMBER, 
	"DEPTH_4" NUMBER, 
	"WIDTH_4" NUMBER, 
	"HEIGHT_4" NUMBER, 
	"WEIGHT_4" NUMBER, 
	"TRACK_LEVEL_5" VARCHAR2(30), 
	"RATIO_4_TO_5" NUMBER, 
	"DEPTH_5" NUMBER, 
	"WIDTH_5" NUMBER, 
	"HEIGHT_5" NUMBER, 
	"WEIGHT_5" NUMBER, 
	"TRACK_LEVEL_6" VARCHAR2(30), 
	"RATIO_5_TO_6" NUMBER, 
	"DEPTH_6" NUMBER, 
	"WIDTH_6" NUMBER, 
	"HEIGHT_6" NUMBER, 
	"WEIGHT_6" NUMBER, 
	"TRACK_LEVEL_7" VARCHAR2(30), 
	"RATIO_6_TO_7" NUMBER, 
	"DEPTH_7" NUMBER, 
	"WIDTH_7" NUMBER, 
	"HEIGHT_7" NUMBER, 
	"WEIGHT_7" NUMBER, 
	"TRACK_LEVEL_8" VARCHAR2(30), 
	"RATIO_7_TO_8" NUMBER, 
	"DEPTH_8" NUMBER, 
	"WIDTH_8" NUMBER, 
	"HEIGHT_8" NUMBER, 
	"WEIGHT_8" NUMBER, 
	"MONTH" VARCHAR2(6), 
	"PROCESS_DSTAMP" VARCHAR2(40)
   ) ;

CREATE INDEX "CNL_SYS"."CNL_MULTISAN_DATA_IDX" ON "CNL_SYS"."CNL_MULTISCAN_DATA" ("CLIENT_ID", "SKU_ID") 
  ;