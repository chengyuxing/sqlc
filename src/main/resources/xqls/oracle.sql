/*[user_tables]*/
SELECT TABLE_NAME
FROM USER_TABLES;;

/*[table_desc]*/
select col.COLUMN_NAME                                                           as name,
       DECODE(CHAR_LENGTH, 0, DATA_TYPE, DATA_TYPE || '(' || CHAR_LENGTH || ')') as type,
       DATA_DEFAULT                                                              as "default",
       DECODE(NULLABLE, 'N', 'yes', 'no')                                        as "notNull",
       COMMENTS                                                                  as "comment"
from ALL_TAB_COLUMNS col
         inner join ALL_COL_COMMENTS cmt on col.COLUMN_NAME = cmt.COLUMN_NAME
where col.TABLE_NAME = cmt.TABLE_NAME
  and col.TABLE_NAME = :table_name
  --#if :schema <> blank
  and col.OWNER = :schema
  --#fi
;;

/*[table_def]*/
begin
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', true);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', false);
end;
select to_char(DBMS_METADATA.GET_DDL('TABLE', :table_name
    --#if :schema <> blank
    , :schema
    --#fi
    ))
from dual
union all
select 'COMMENT ON TABLE "' || OWNER || '"."' || TABLE_NAME || '" IS' || '''' || COMMENTS || ''''
from ALL_TAB_COMMENTS
where TABLE_NAME = :table_name
  --#if :schema <> blank
  and OWNER = :schema
  --#fi
union all
select 'COMMENT ON COLUMN "' || OWNER || '"."' || TABLE_NAME || '"."' || COLUMN_NAME || '" IS ' || '''' || COMMENTS ||
       ''''
from ALL_COL_COMMENTS
where TABLE_NAME = :table_name
  --#if :schema <> blank
  and OWNER = :schema
  --#fi
union all
select to_char(DBMS_METADATA.GET_DDL('INDEX', INDEX_NAME, OWNER))
from ALL_INDEXES
where TABLE_NAME = :table_name
  --#if :schema <> blank
    and TABLE_OWNER = :schema
  --#fi
;;