CREATE OR REPLACE PROCEDURE "CNL_SYS"."GITORA_LOGIN" ( p_user_name varchar2
					, p_password  varchar2
					)
is
begin
	gitora.api_gitora.login(p_user_name,p_password);
end;