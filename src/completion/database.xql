/*
  Configure database object auto complete suggestions.
  Please follow the format below to add SQL for your database.

  Runtime variables: username, schema

  /*[database name from jdbc metadata]*/
  <1 column query SQL content>;
*/

/*[postgresql]*/
select concat(schemaname, '.', tablename)
from pg_catalog.pg_tables
where tableowner = :username
  and schemaname not in ('pg_toast', 'pg_catalog', 'information_schema');

/*[oracle]*/
SELECT TABLE_NAME FROM USER_TABLES;

/*[mysql]*/
select concat(TABLE_SCHEMA, '.', TABLE_NAME)
from information_schema.TABLES
where TABLE_SCHEMA != 'information_schema';