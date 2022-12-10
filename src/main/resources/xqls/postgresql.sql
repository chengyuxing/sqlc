/*[user_tables]*/
select tablename
from pg_tables
where tableowner = :username
  and schemaname not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[user_procedures]*/
select concat(nm.nspname, '.', c.proname, '(', oidvectortypes(c.proargtypes), ')') procedure_name
from pg_catalog.pg_proc c
         inner join pg_catalog.pg_namespace nm on nm.oid = c.pronamespace
where pg_get_userbyid(c.proowner) = :username
  and c.probin is null
  and nm.nspname not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[procedure_def]*/
select pg_get_functiondef(:procedure_name::regprocedure);