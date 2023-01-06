CREATE OR REPLACE PACKAGE BODY "CNL_SYS"."CNL_DB_JOB_PCK" is
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
--
-- Private type declarations
--
--
-- Private constant declarations
--
   g_yes         constant varchar2(1)     := 'Y';
   g_no          constant varchar2(1)     := 'N';
--
-- Private variable declarations
--
--
-- Private routines
--
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-06-2016
-- Purpose : Create Submit Once Job
------------------------------------------------------------------------------------------------
procedure submit_once (p_procedure_i in varchar2
                      ,p_code_i      in varchar2
                      ,p_delay_i     in number     -- in Seconds
                      )
is
   l_time        varchar2(100);
   l_startdate   timestamp with time zone;
   l_err_txt     varchar2(350);
   --
begin
   select to_char (sysdate, 'hh24miss')
   into   l_time
   from   dual;
   --
   select dbms_scheduler.stime + numtodsinterval (p_delay_i, 'second') 
   into   l_startdate
   from   dual;
   --
   dbms_scheduler.create_job (job_name   => 'CNLJOB_'
                                            || p_code_i
                                            || '_'
                                            || l_time
                             ,job_type   => 'PLSQL_BLOCK'
                             ,job_action => p_procedure_i
                             ,start_date => l_startdate
--                             ,repeat_interval => 'FREQ=DAILY'
                             ,enabled    => true
                             ,auto_drop  => true
                             );
   dbms_scheduler.set_attribute (name      => 'CNLJOB_'
                                           || p_code_i
                                           || '_'
                                           || l_time
                                ,attribute => 'JOB_PRIORITY'
                                ,value     => 3
                                ); 
   commit work;
exception
   when others
   then
      --
      l_err_txt := substr (sqlerrm, 1, 350);
      dbms_output.put_line ('Err. txt: ' || l_err_txt);
      --
      raise;
end submit_once;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-06-2016
-- Purpose : Run the Job once
------------------------------------------------------------------------------------------------
procedure run_job (p_name_i in varchar2)
is
   l_err_txt     varchar2(350);
begin
   dbms_scheduler.run_job (job_name => p_name_i);

exception
   when others
   then
      --
      l_err_txt := substr (sqlerrm, 1, 350);
      dbms_output.put_line ('Err. txt: ' || l_err_txt);
      --
      raise;
end run_job;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-06-2016
-- Purpose : Drop the Job
------------------------------------------------------------------------------------------------
procedure drop_job (p_name_i in varchar2)
is
   l_err_txt     varchar2(350);
begin
   dbms_scheduler.drop_job (job_name => p_name_i);

exception
   when others
   then
      --
      l_err_txt := substr (sqlerrm, 1, 350);
      dbms_output.put_line ('Err. txt: ' || l_err_txt);
      --
      raise;
end drop_job;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-06-2016
-- Purpose : Enable the Job
------------------------------------------------------------------------------------------------
procedure enable_job (p_name_i in varchar2)
is 
   l_err_txt     varchar2(350);
begin
   dbms_scheduler.enable (name => p_name_i);
exception
   when others
   then
      --
      l_err_txt := substr (sqlerrm, 1, 350);
      dbms_output.put_line ('Err. txt: ' || l_err_txt);
      --
      raise;
end enable_job;
------------------------------------------------------------------------------------------------
-- Author  : B. Bitter, 12-06-2016
-- Purpose : Disable the Job
------------------------------------------------------------------------------------------------
procedure disable_job (p_name_i in varchar2)
is 
   l_err_txt     varchar2(350);
begin
   dbms_scheduler.disable (name => p_name_i);

exception
   when others
   then
      --
      l_err_txt := substr (sqlerrm, 1, 350);
      dbms_output.put_line ('Err. txt: ' || l_err_txt);
      --
      raise;
end disable_job;
--
--
begin
  -- Initialization
  null;
end cnl_db_job_pck;