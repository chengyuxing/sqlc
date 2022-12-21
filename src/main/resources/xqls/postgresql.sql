/*[user_tables]*/
select concat(schemaname, '.', tablename), '' as type
from pg_catalog.pg_tables
where tableowner = :username
  and schemaname not in ('pg_toast', 'pg_catalog', 'information_schema');;

/*[user_procedures]*/
select concat(nm.nspname, '.', c.proname, '(', oidvectortypes(c.proargtypes), ')') procedure_name,
       'proc' as                                                                   type
from pg_catalog.pg_proc c
         inner join pg_catalog.pg_namespace nm on nm.oid = c.pronamespace
where pg_get_userbyid(c.proowner) = :username
  and c.probin is null
  and nm.nspname not in ('pg_toast', 'pg_catalog', 'information_schema');;

/*[procedure_def]*/
select pg_get_functiondef(:procedure_name::regprocedure);;


/*[user_views]*/
select concat(schemaname, '.', viewname) view_name, 'view' as type
from pg_catalog.pg_views
where viewowner = :username
  and schemaname not in ('pg_toast', 'pg_catalog', 'information_schema');;

/*[view_def]*/
select pg_get_viewdef(:view_name);;


/*[user_triggers]*/
select distinct concat(event_object_schema, '.', event_object_table, '.', trigger_name),
                'tg' as type
from information_schema.triggers
where trigger_schema not in ('pg_toast', 'pg_catalog', 'information_schema');;

/*[trigger_def]*/
select distinct pg_get_triggerdef(b.oid)
from information_schema.triggers a
         inner join pg_catalog.pg_trigger b on a.trigger_name = b.tgname
where concat(event_object_schema, '.', event_object_table) = :table_name
  and trigger_name = :trigger_name
limit 1;;

/*[table_def]*/
WITH attrdef AS (SELECT n.nspname,
                        c.relname,
                        c.oid,
                        pg_catalog.array_to_string(
                                    c.reloptions || array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x),
                                    ', ')                               as relopts,
                        c.relpersistence,
                        a.attnum,
                        a.attname,
                        a.attrelid,
                        pg_catalog.format_type(a.atttypid, a.atttypmod) as atttype,
                        (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) for 128)
                         FROM pg_catalog.pg_attrdef d
                         WHERE d.adrelid = a.attrelid
                           AND d.adnum = a.attnum
                           AND a.atthasdef)                             as attdefault,
                        a.attnotnull,
                        (SELECT c.collname
                         FROM pg_catalog.pg_collation c,
                              pg_catalog.pg_type t
                         WHERE c.oid = a.attcollation
                           AND t.oid = a.atttypid
                           AND a.attcollation <> t.typcollation)        as attcollation,
                        a.attidentity
                        -- #choose
                              -- #when :version < 12
                                    ,'' as attgenerated
                              -- #break
                              -- #default
                                    ,a.attgenerated
                              -- #break
                        -- #end
                 FROM pg_catalog.pg_attribute a
                          JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                          JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                          LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
                 WHERE concat(n.nspname, '.', c.relname) = :table_name
                   AND a.attnum > 0
                   AND NOT a.attisdropped
                 ORDER BY a.attnum),
     coldef AS (SELECT attrdef.nspname,
                       attrdef.relname,
                       attrdef.oid,
                       attrdef.relopts,
                       attrdef.relpersistence,
                       case
                           when attrdef.attdefault ~ '^nextval\('
                               then pg_catalog.format('%I %s', attrdef.attname, 'serial')
                           else
                               pg_catalog.format('%I %s%s%s%s%s', attrdef.attname, attrdef.atttype,
                                                 case
                                                     when attrdef.attcollation is null then ''
                                                     else pg_catalog.format(' COLLATE %I', attrdef.attcollation) end,
                                                 case when attrdef.attnotnull then ' NOT NULL' else '' end,
                                                 case
                                                     when attrdef.attdefault is null then ''
                                                     else case
                                                              when attrdef.attgenerated = 's' then pg_catalog.format(
                                                                      ' GENERATED ALWAYS AS (%s) STORED',
                                                                      attrdef.attdefault)
                                                              when attrdef.attgenerated <> ''
                                                                  then ' GENERATED AS NOT_IMPLEMENTED'
                                                              else pg_catalog.format(' DEFAULT %s', attrdef.attdefault) end end,
                                                 case
                                                     when attrdef.attidentity <> '' then pg_catalog.format(
                                                             ' GENERATED %s AS IDENTITY', case attrdef.attidentity
                                                                                              when 'd' then 'BY DEFAULT'
                                                                                              when 'a' then 'ALWAYS'
                                                                                              else 'NOT_IMPLEMENTED' end)
                                                     else '' end) end as col_create_sql
                FROM attrdef
                ORDER BY attrdef.attnum),
     tabdef AS (SELECT coldef.nspname,
                       coldef.relname,
                       coldef.oid,
                       coldef.relopts,
                       coldef.relpersistence,
                       concat(string_agg(coldef.col_create_sql, E',\n    '),
                              (select concat(E',\n    ', pg_get_constraintdef(oid))
                               from pg_catalog.pg_constraint
                               where contype = 'p'
                                 and conrelid = coldef.oid)) as cols_create_sql
                FROM coldef
                GROUP BY coldef.nspname, coldef.relname, coldef.oid, coldef.relopts, coldef.relpersistence)
SELECT pg_catalog.format('CREATE%s TABLE %I.%I%s%s%s;%s',
                         case tabdef.relpersistence when 't' then ' TEMP' when 'u' then ' UNLOGGED' else '' end,
                         tabdef.nspname,
                         tabdef.relname,
                         coalesce((SELECT pg_catalog.format(E'\n    PARTITION OF %I.%I %s\n', pn.nspname, pc.relname,
                                                            pg_get_expr(c.relpartbound, c.oid))
                                   FROM pg_catalog.pg_class c
                                            JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid
                                            JOIN pg_catalog.pg_class pc ON pc.oid = i.inhparent
                                            JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
                                   WHERE c.oid = tabdef.oid),
                                  pg_catalog.format(E' (\n    %s\n)', tabdef.cols_create_sql)
                             ),
                         case
                             when tabdef.relopts <> '' then pg_catalog.format(' WITH (%s)', tabdef.relopts)
                             else '' end,
                         coalesce(E'\nPARTITION BY ' || pg_get_partkeydef(tabdef.oid), ''),
                         coalesce((select E'\n\n' || string_agg(case d.objsubid
                                                                    when 0 then
                                                                        pg_catalog.format(
                                                                                'COMMENT ON TABLE %I.%I IS %L;',
                                                                                tabdef.nspname,
                                                                                tabdef.relname, d.description)
                                                                    else pg_catalog.format(
                                                                            'COMMENT ON COLUMN %I.%I.%I IS %L;',
                                                                            tabdef.nspname,
                                                                            tabdef.relname, attrdef.attname,
                                                                            d.description) end,
                                                                E'\n\n')
                                   from pg_catalog.pg_description d
                                            left join attrdef on d.objoid = attrdef.attrelid
                                       AND d.objsubid = attrdef.attnum
                                   where objoid = :table_name::regclass
                                     and d.description is not null), '')
           ) as table_create_sql
FROM tabdef;;

/*[table_triggers]*/
select distinct pg_get_triggerdef(b.oid) || ';'
from information_schema.triggers a
         inner join pg_catalog.pg_trigger b on a.trigger_name = b.tgname
where concat(event_object_schema, '.', event_object_table) = :table_name;;

/*[table_indexes]*/
select indexdef || ';'
from pg_catalog.pg_indexes
where concat(schemaname, '.', tablename) = :table_name;;

/*[table_desc]*/
SELECT a.attname                                       as name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
       (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) for 128)
        FROM pg_catalog.pg_attrdef d
        WHERE d.adrelid = a.attrelid
          AND d.adnum = a.attnum
          AND a.atthasdef)                             as "default",
       a.attnotnull                                    as "notNull",
       d.description                                   as "comment"
FROM pg_catalog.pg_attribute a
         JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
         JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
         left join pg_catalog.pg_description d on d.objoid = a.attrelid and d.objsubid = a.attnum
         LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
WHERE concat(n.nspname, '.', c.relname) = :table_name
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;;