/*[user_tables]*/
SELECT TABLE_NAME as table_name, '' as type
FROM USER_TABLES;;

/*[table_desc]*/
select col.COLUMN_NAME                                                           as name,
       DECODE(CHAR_LENGTH, 0, DATA_TYPE, DATA_TYPE || '(' || CHAR_LENGTH || ')') as type,
       DATA_DEFAULT                                                              as "default",
       DECODE(NULLABLE, 'N', 'yes', 'no')                                        as "notNull",
       REPLACE(COMMENTS, '''', '''''')                                           as "comment"
from ALL_TAB_COLUMNS col
         inner join ALL_COL_COMMENTS cmt on col.COLUMN_NAME = cmt.COLUMN_NAME
where col.TABLE_NAME = cmt.TABLE_NAME
  and col.TABLE_NAME = :table_name
  --#if :schema <> blank
  and col.OWNER = :schema
--#fi
;;

/*[table_def_init]*/
begin
DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true);
DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', true);
DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false);
DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false);
DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', false);
end;

/*[table_def]*/
select replace(replace(rtrim(xmlagg(xmlcdata(col || chr(10)).extract('//text()') order by idx).getClobVal(), ','),
                       '<![CDATA[',
                       ''), ']]>', '') table_def
from (select to_char(DBMS_METADATA.GET_DDL('TABLE', :table_name
    --#if :schema <> blank
    , :schema
    --#fi
    ))            col,
             1 as idx
      from dual
      union all
      select chr(10) || 'COMMENT ON TABLE "' || OWNER || '"."' || TABLE_NAME || '" IS ' || '''' || REPLACE(COMMENTS, '''', '''''') || ''';' col,
             2 as                                                                                                   idx
      from ALL_TAB_COMMENTS
      where TABLE_NAME = :table_name
        --#if :schema <> blank
        and OWNER = :schema
        --#fi
      and comments is not null
      union all
      select chr(10) || 'COMMENT ON COLUMN "' || OWNER || '"."' || TABLE_NAME || '"."' || COLUMN_NAME || '" IS ' ||
             '''' ||
             REPLACE(COMMENTS, '''', '''''') ||
             ''';' col,
             3 as  idx
      from ALL_COL_COMMENTS
      where TABLE_NAME = :table_name
        --#if :schema <> blank
        and OWNER = :schema
        --#fi
      and comments is not null
      union all
      select to_char(DBMS_METADATA.GET_DDL('INDEX', INDEX_NAME, OWNER)) col, 4 as idx
      from ALL_INDEXES
      where UNIQUENESS != 'UNIQUE'
        and TABLE_NAME = :table_name
        --#if :schema <> blank
            and TABLE_OWNER = :schema
         --#fi
     ) t;;
