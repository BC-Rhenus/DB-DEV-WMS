CREATE OR REPLACE PACKAGE "CNL_SYS"."CNL_DB_JOB_PCK" is
/**********************************************************************************
* $Archive: $
* $Revision: $   
* $Author: $
* $Date: $
**********************************************************************************
* Description: DB Job functionality within CNL_SYS schema
**********************************************************************************
* $Log: $
**********************************************************************************/

   procedure submit_once (p_procedure_i  in varchar2
                         ,p_code_i       in varchar2
                         ,p_delay_i      in number    -- in Seconds
                         );
   --
   procedure run_job (p_name_i in varchar2);
   --
   procedure drop_job (p_name_i in varchar2);
   --
   procedure enable_job (p_name_i in varchar2);
   --
   procedure disable_job (p_name_i in varchar2);
   --
end cnl_db_job_pck;