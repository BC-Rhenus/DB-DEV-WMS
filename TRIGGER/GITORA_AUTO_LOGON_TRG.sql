CREATE OR REPLACE TRIGGER "CNL_SYS"."GITORA_AUTO_LOGON_TRG" 
   after logon on cnl_sys.schema
declare
   l_program     varchar2(100);
   l_gitora_user cnl_sys.gitora_users_match.gitora_user%type;
   l_gitora_pw   cnl_sys.gitora_users_match.gitora_pw%type;

   cursor c_gitora_user
   is
      select guh.gitora_user
      ,      guh.gitora_pw
      from cnl_sys.gitora_users_match guh
      where upper(guh.os_user) = upper(sys_context('userenv','OS_USER'))
      ;

begin
   /* Get the program that is connecting.
      For example:
     'dwfde61.exe'      -- Oracle Designer Generator
     'sqlplusw.exe'     -- Oracle sqlplus
     'sqlplus.exe'      -- Oracle sqlplus
     'SQL Developer'    -- Oracle SQL developer
     'Toad.exe'         -- Toad
     'plsqldev.exe'     -- PL/SQL Developer
     'JDBC Thin Client'
   */
   select program
   into   l_program
   from   v$session
   where  audsid=sys_context('USERENV','SESSIONID');

	if sys_context('userenv','OS_USER') != 'ora11'
	then
		insert into cnl_sys.gitora_logon_trg_log
			(logon_date
			,logon_message
			)
		values
			(sysdate
			,'logon: os_user: '''||sys_context('userenv','OS_USER')||''' program: '||l_program
			);

		open  c_gitora_user;
		fetch c_gitora_user
		into  l_gitora_user
		,     l_gitora_pw;
		close c_gitora_user;

		if l_gitora_user is null
		then
			-- log failure to match os_usr to Gitora user
			insert into cnl_sys.gitora_logon_trg_log
				(logon_date
				,logon_message
				)
			values
				(sysdate
				,'os_user name '''||sys_context('userenv','OS_USER')||''' cannot be matched to a Gitora user'
				);
		else
			-- try to log on to Gitora.
			begin
				GITORA.api_gitora.login(l_gitora_user,l_gitora_pw);
			exception
				when others
				then
					-- just log, no need to raise
					insert into cnl_sys.gitora_logon_trg_log
					(logon_date
					,logon_message
					)
					values
					(sysdate
					,'Login to Gitora failed for username '||l_gitora_user
					);
				end;
		end if;
	end if;
exception
  when others
  then
     -- this trigger should never cause a user to not be able to log in to the database.
     -- It only needs to check if an os_name can be linked to a Gitora user.
     null;
end;