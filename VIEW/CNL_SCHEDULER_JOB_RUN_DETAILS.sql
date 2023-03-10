CREATE OR REPLACE FORCE VIEW "CNL_SYS"."CNL_SCHEDULER_JOB_RUN_DETAILS" ("LOG_ID", "LOG_DATE", "OWNER", "JOB_NAME", "JOB_SUBNAME", "STATUS", "ERROR#", "REQ_START_DATE", "ACTUAL_START_DATE", "RUN_DURATION", "INSTANCE_ID", "SESSION_ID", "SLAVE_PID", "CPU_USED", "CREDENTIAL_OWNER", "CREDENTIAL_NAME", "DESTINATION_OWNER", "DESTINATION", "ADDITIONAL_INFO") AS 
  select "LOG_ID","LOG_DATE","OWNER","JOB_NAME","JOB_SUBNAME","STATUS","ERROR#","REQ_START_DATE","ACTUAL_START_DATE","RUN_DURATION","INSTANCE_ID","SESSION_ID","SLAVE_PID","CPU_USED","CREDENTIAL_OWNER","CREDENTIAL_NAME","DESTINATION_OWNER","DESTINATION","ADDITIONAL_INFO"
from Dba_Scheduler_Job_Run_Details
where Owner = 'CNL_SYS'
order by Log_ID desc