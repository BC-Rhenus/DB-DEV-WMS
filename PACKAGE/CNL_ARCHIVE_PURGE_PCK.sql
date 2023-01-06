CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_ARCHIVE_PURGE_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: Functions and procedures related to warehouse handlilng application
**********************************************************************************
* $Log: $
**********************************************************************************/
	procedure p_cnl_process_housekeeping;
	--
	procedure archive_purge_order_extend_p;
	--
end cnl_archive_purge_pck;