CREATE OR REPLACE FORCE VIEW "CNL_SYS"."CNL_CONTAINER_DATA_ARCHIVE" ("CONTAINER_ID", "CONTAINER_TYPE", "PALLET_ID", "PALLET_TYPE", "CONTAINER_N_OF_N", "SITE_ID", "CLIENT_ID", "OWNER_ID", "ORDER_ID", "CUSTOMER_ID", "CARRIER_ID", "SERVICE_LEVEL", "WMS_WEIGHT", "WMS_HEIGHT", "WMS_WIDTH", "WMS_DEPTH", "WMS_DATABASE", "DWS_UNIT_ID", "DWS_STATION_ID", "DWS_LFT_STATUS", "DWS_LFT_DESCRIPTION", "DWS_PACKAGE_TYPE", "DWS_WEIGHT", "DWS_HEIGHT", "DWS_WIDTH", "DWS_DEPTH", "DWS_DSTAMP", "CTO_ENABLED_YN", "CTO_PP_FILENAME", "CTO_PP_DSTAMP", "CTO_CP_FILENAME", "CTO_CP_DSTAMP", "CTO_CARRIER", "CTO_SERVICE", "CTO_SEQUENCE_NR", "CTO_TRACKING_NR", "CTO_TRACKING_URL", "CTO_ERROR_CODE", "CTO_ERROR_MESSAGE", "CTO_PPR_DSTAMP", "CREATED_BY", "CREATION_DATE", "LAST_UPDATED_BY", "LAST_UPDATE_DATE", "ARCHIVED", "ARCHIVED_DSTAMP", "ARCHIVE_PENDING") AS 
  select "CONTAINER_ID","CONTAINER_TYPE","PALLET_ID","PALLET_TYPE","CONTAINER_N_OF_N","SITE_ID","CLIENT_ID","OWNER_ID","ORDER_ID","CUSTOMER_ID","CARRIER_ID","SERVICE_LEVEL","WMS_WEIGHT","WMS_HEIGHT","WMS_WIDTH","WMS_DEPTH","WMS_DATABASE","DWS_UNIT_ID","DWS_STATION_ID","DWS_LFT_STATUS","DWS_LFT_DESCRIPTION","DWS_PACKAGE_TYPE","DWS_WEIGHT","DWS_HEIGHT","DWS_WIDTH","DWS_DEPTH","DWS_DSTAMP","CTO_ENABLED_YN","CTO_PP_FILENAME","CTO_PP_DSTAMP","CTO_CP_FILENAME","CTO_CP_DSTAMP","CTO_CARRIER","CTO_SERVICE","CTO_SEQUENCE_NR","CTO_TRACKING_NR","CTO_TRACKING_URL","CTO_ERROR_CODE","CTO_ERROR_MESSAGE","CTO_PPR_DSTAMP","CREATED_BY","CREATION_DATE","LAST_UPDATED_BY","LAST_UPDATE_DATE","ARCHIVED","ARCHIVED_DSTAMP","ARCHIVE_PENDING" from cnl_container_data_archives