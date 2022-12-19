/*[user_tables]*/
select concat(TABLE_SCHEMA, '.', TABLE_NAME) as table_name, '' as type
from information_schema.TABLES
where TABLE_SCHEMA != 'information_schema'
-- #if :schema <> blank
  and TABLE_SCHEMA = :schema
-- #fi
;;

/*[table_desc]*/
select c.COLUMN_NAME    as name,
       c.COLUMN_TYPE    as type,
       c.COLUMN_DEFAULT as "default",
       c.IS_NULLABLE    as "notNull",
       c.COLUMN_COMMENT as "comment"
from information_schema.COLUMNS c
where concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) = :table_name;;