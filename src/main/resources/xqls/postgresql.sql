/*[user_tables]*/
select concat(schemaname, '.', tablename), '' as type
from pg_catalog.pg_tables
where tableowner = :username
  and schemaname not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[user_procedures]*/
select concat(nm.nspname, '.', c.proname, '(', oidvectortypes(c.proargtypes), ')') procedure_name,
       'proc' as                                                              type
from pg_catalog.pg_proc c
         inner join pg_catalog.pg_namespace nm on nm.oid = c.pronamespace
where pg_get_userbyid(c.proowner) = :username
  and c.probin is null
  and nm.nspname not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[procedure_def]*/
select pg_get_functiondef(:procedure_name::regprocedure);


/*[user_views]*/
select concat(schemaname, '.', viewname) view_name, 'view' as type
from pg_catalog.pg_views
where viewowner = :username
  and schemaname not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[view_def]*/
select pg_get_viewdef(:view_name);


/*[user_triggers]*/
select distinct concat(event_object_schema, '.', event_object_table, '.', trigger_name),
                'tg' as type
from information_schema.triggers
where trigger_schema not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[trigger_def]*/
select distinct pg_get_triggerdef(b.oid)
from information_schema.triggers a
         inner join pg_catalog.pg_trigger b on a.trigger_name = b.tgname
where concat(event_object_schema, '.', event_object_table) = :table_name
  and trigger_name = :trigger_name
limit 1;

/*[table_triggers]*/
select distinct pg_get_triggerdef(b.oid)
from information_schema.triggers a
         inner join pg_catalog.pg_trigger b on a.trigger_name = b.tgname
where concat(event_object_schema, '.', event_object_table) = :table_name;