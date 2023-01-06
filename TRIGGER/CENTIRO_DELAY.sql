CREATE OR REPLACE TRIGGER "CNL_SYS"."CENTIRO_DELAY" 
BEFORE INSERT ON  CNL_SYS.CENTIRO_MONITOR 
for each row 
BEGIN 
:new.delay := :new.pickup - :new.creation;
END;