CREATE OR REPLACE TRIGGER "CNL_SYS"."CNL_CNT_VAS_ACTIVITY_BIR_01" 
    before insert on cnl_sys.cnl_container_vas_activity
    for each row
    declare
    cursor c_act(b_name varchar2)
    is
        select  activity_description
        from    cnl_sys.cnl_vas_activity
        where   activity_name = b_name
    ;
    r_act   c_act%rowtype;
begin
    open c_act(:new.activity_name);
    fetch c_act into r_act;
    if c_act%found
    then
        close c_act;
        :new.activity_description := r_act.activity_description;
    else
        close c_act;
    end if;
    if :new.activity_sequence is null
    then
        :new.activity_sequence := 0;
    end if;
end;