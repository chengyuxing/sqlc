/*[user_tables]*/
select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA = :schema;