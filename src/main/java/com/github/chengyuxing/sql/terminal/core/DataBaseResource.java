package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.GET_MYSQL_SCHEMA;

public class DataBaseResource {
    private static final Logger log = LoggerFactory.getLogger(DataBaseResource.class);
    private final Pattern TRIGGER_PREFIX_REGEX = Pattern.compile("^create\\s+or\\s+replace\\s[\\s\\S]+", Pattern.CASE_INSENSITIVE);

    private final String dbName;
    private final DataSourceLoader dataSourceLoader;
    private final Baki baki;
    private final XQLFileManager xqlFileManager;
    private Supplier<Pair<String, Map<String, Object>>> queryTablesFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryProceduresFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryProcedureDefFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryViewsFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryViewDefFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryTriggersFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryTriggerDefFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryTableDef;
    private Function<String, Pair<String, Map<String, Object>>> queryTableIndexesFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryTableTriggersFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryTableDescFunc;

    public DataBaseResource(DataSourceLoader dataSourceLoader) {
        this.dbName = dataSourceLoader.getDbName();
        this.dataSourceLoader = dataSourceLoader;
        this.baki = this.dataSourceLoader.getSysBaki();
        this.xqlFileManager = new XQLFileManager();
        this.xqlFileManager.setDelimiter(";;");
        init();
    }

    void init() {
        switch (dbName) {
            case "postgresql":
                xqlFileManager.add("pg", "xqls/postgresql.sql");
                queryTablesFunc = () -> Pair.of("pg.user_tables", Args.of("username", dataSourceLoader.getUsername()));
                queryProceduresFunc = () -> Pair.of("pg.user_procedures", Args.of("username", dataSourceLoader.getUsername()));
                queryProcedureDefFunc = name -> Pair.of("pg.procedure_def", Args.of("procedure_name", name));
                queryViewsFunc = () -> Pair.of("pg.user_views", Args.of("username", dataSourceLoader.getUsername()));
                queryViewDefFunc = name -> Pair.of("pg.view_def", Args.of("view_name", name));
                queryTriggersFunc = () -> Pair.of("pg.user_triggers", Collections.emptyMap());
                queryTriggerDefFunc = name -> {
                    // e.g: test.big.notice_big
                    int dotIdx = name.lastIndexOf(".");
                    String tableName = name.substring(0, dotIdx);
                    String triggerName = name.substring(dotIdx + 1);
                    return Pair.of("pg.trigger_def", Args.create("table_name", tableName, "trigger_name", triggerName));
                };
                queryTableDef = name -> {
                    Args<Object> args = Args.of("table_name", name);
                    try {
                        int version = baki.metaData().getDatabaseMajorVersion();
                        args.add("version", version);
                    } catch (SQLException e) {
                        PrintHelper.printlnError(e);
                    }
                    return Pair.of("pg.table_def", args);
                };
                queryTableIndexesFunc = name -> Pair.of("pg.table_indexes", Args.of("table_name", name));
                queryTableTriggersFunc = name -> Pair.of("pg.table_triggers", Args.of("table_name", name));
                queryTableDescFunc = name -> Pair.of("pg.table_desc", Args.of("table_name", name));
                break;
            case "oracle":
                xqlFileManager.add("oracle", "xqls/oracle.sql");
                queryTablesFunc = () -> Pair.of("oracle.user_tables", Collections.emptyMap());
                queryTableDescFunc = name -> Pair.of("oracle.table_desc", getSchemaAndTable(name));
                queryTableDef = name -> Pair.of("oracle.table_def", getSchemaAndTable(name));
                break;
            case "mysql":
                xqlFileManager.add("mysql", "xqls/mysql.sql");
                queryTablesFunc = () -> {
                    Matcher m = GET_MYSQL_SCHEMA.matcher(dataSourceLoader.getJdbcUrl());
                    if (m.find()) {
                        return Pair.of("mysql.user_tables", Args.of("schema", m.group("schema")));
                    }
                    return Pair.of("mysql.user_tables", Collections.emptyMap());
                };
                queryTableDescFunc = name -> Pair.of("mysql.table_desc", Args.of("table_name", name));
                break;
        }
        xqlFileManager.init();
    }

    List<String> getNames(Supplier<Pair<String, Map<String, Object>>> supplier) {
        if (supplier != null) {
            Pair<String, Map<String, Object>> pair = supplier.get();
            String sql = xqlFileManager.get(pair.getItem1(), pair.getItem2(), false);
            if (!sql.equals("")) {
                try (Stream<DataRow> s = baki.query(sql).args(pair.getItem2()).stream()) {
                    return s.map(d -> {
                        if (!StringUtil.hasLength(d.getString(1))) {
                            return d.getString(0);
                        }
                        return d.getString(1) + ":" + d.getString(0);
                    }).collect(Collectors.toList());
                } catch (Exception e) {
                    PrintHelper.printlnError(e);
                    return Collections.emptyList();
                }
            }
        }
        log.debug("not implement now.");
        return Collections.emptyList();
    }

    public String getDefinition(Function<String, Pair<String, Map<String, Object>>> func, String name) {
        if (func != null) {
            Pair<String, Map<String, Object>> pair = func.apply(name);
            String sql = xqlFileManager.get(pair.getItem1(), pair.getItem2(), false);
            if (!sql.equals("")) {
                return baki.query(sql).args(pair.getItem2())
                        .findFirst()
                        .map(d -> {
                            String def = d.getString(0);
                            if (def != null) {
                                return def;
                            }
                            return "";
                        })
                        .orElse("");
            }
        }
        throw new UnsupportedOperationException(dbName + ": operation not implement currently.");
    }

    public List<String> getDefinitions(Function<String, Pair<String, Map<String, Object>>> func, String name) {
        if (func != null) {
            Pair<String, Map<String, Object>> pair = func.apply(name);
            String sql = xqlFileManager.get(pair.getItem1(), pair.getItem2(), false);
            if (!sql.equals("")) {
                try (Stream<DataRow> s = baki.query(sql).args(pair.getItem2()).stream()) {
                    return s.map(d -> {
                        String def = d.getString(0);
                        if (def != null) {
                            return def;
                        }
                        return "";
                    }).collect(Collectors.toList());
                }
            }
        }
        log.debug("not implement now.");
        return Collections.emptyList();
    }

    public String getProcedureDefinition(String name) {
        return getDefinition(queryProcedureDefFunc, name);
    }

    public String getViewDefinition(String name) {
        String view = getDefinition(queryViewDefFunc, name).trim();
        if (!StringUtil.startsWiths(view, "create")) {
            return "CREATE OR REPLACE VIEW " + name + " AS " + view;
        }
        return view;
    }

    public String getTableDefinition(String name) {
        if (dbName.equals("oracle")) {
            baki.execute(xqlFileManager.get("oracle.table_def_init"));
        }
        String table = getDefinition(queryTableDef, name).trim();
        String indexes = getDefinitions(queryTableIndexesFunc, name).stream().map(this::formatIndex).collect(Collectors.joining("\n\n"));
        String triggers = getDefinitions(queryTableTriggersFunc, name).stream().map(s -> formatTrigger(s, false)).collect(Collectors.joining("\n\n"));
        return table + "\n\n" + indexes + "\n\n" + triggers;
    }

    public String getTriggerDefinition(String name) {
        String trigger = getDefinition(queryTriggerDefFunc, name).trim();
        return formatTrigger(trigger, true);
    }

    public String getDefinition(String object) {
        if (object.contains(":")) {
            int colonIdx = object.indexOf(":");
            String type = object.substring(0, colonIdx);
            String name = object.substring(colonIdx + 1);
            switch (type) {
                case "proc":
                    return getProcedureDefinition(name);
                case "tg":
                    return getTriggerDefinition(name);
                case "view":
                    return getViewDefinition(name);
                default:
                    throw new UnsupportedOperationException("un know type: " + type + " e.g. tg(trigger), view, proc(procedure)");
            }
        }
        //without ':' then default get table definition
        return getTableDefinition(object);
    }

    public List<List<String>> getTableDesc(String name) {
        if (queryTableDescFunc != null) {
            Pair<String, Map<String, Object>> pair = queryTableDescFunc.apply(name);
            try (Stream<DataRow> s = baki.query(xqlFileManager.get(pair.getItem1(), pair.getItem2(), false)).args(pair.getItem2()).stream()) {
                AtomicBoolean first = new AtomicBoolean(true);
                List<List<String>> rows = new ArrayList<>();
                s.forEach(d -> {
                    if (first.get()) {
                        rows.add(d.names());
                        first.set(false);
                    }
                    List<String> cols = d.values().stream().map(col -> Optional.ofNullable(col).map(Object::toString).orElse("<null>")).collect(Collectors.toList());
                    rows.add(cols);
                });
                return rows;
            }
        }
        throw new UnsupportedOperationException(dbName + ": operation not implement currently.");
    }

    public List<String> getUserProcedures() {
        return getNames(queryProceduresFunc);
    }

    public List<String> getUserViews() {
        return getNames(queryViewsFunc);
    }

    public List<String> getUserTriggers() {
        return getNames(queryTriggersFunc);
    }

    public List<String> getUserTableNames() {
        return getNames(queryTablesFunc);
    }

    public Set<String> getSqlKeyWordsWithDefault() {
        Set<String> keywords = getSqlKeywords("default");
        keywords.addAll(getSqlKeywords(dbName));
        return keywords;
    }

    public Set<String> getSqlKeywords(String dbName) {
        Path cnf = Paths.get(Constants.APP_DIR.getParent().toString(), "completion", dbName + ".cnf");
        if (!Files.exists(cnf)) {
            PrintHelper.printlnDarkWarning("cannot find " + dbName + " keywords completion cnf file: " + cnf);
            return Collections.emptySet();
        }
        try (Stream<String> lines = Files.lines(cnf, StandardCharsets.UTF_8)) {
            return lines.map(line -> Arrays.asList(line.split("\\s+")))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            PrintHelper.printlnError(e);
            return Collections.emptySet();
        }
    }

    String formatIndex(String index) {
        StringJoiner sb = new StringJoiner(" ");
        String[] words = index.split("\\s+");
        for (String w : words) {
            if (w.equalsIgnoreCase("on")) {
                sb.add("\n\t").add(w);
            } else sb.add(w);
        }
        return sb.toString();
    }

    String formatTrigger(String trigger, boolean appendReplace) {
        if (appendReplace) {
            Matcher m = TRIGGER_PREFIX_REGEX.matcher(trigger);
            if (!m.find()) {
                trigger = StringUtil.replaceFirstIgnoreCase(trigger, "create", "CREATE OR REPLACE");
            }
        }
        StringJoiner sb = new StringJoiner(" ");
        String[] words = trigger.split("\\s+");
        for (String w : words) {
            if (StringUtil.equalsAnyIgnoreCase(w, "before", "after", "on", "for")) {
                sb.add("\n\t").add(w);
            } else if (w.equalsIgnoreCase("execute")) {
                sb.add("\n").add(w);
            } else sb.add(w);
        }
        return sb.toString();
    }

    public String getDbName() {
        return dbName;
    }

    public DataSourceLoader getDataSourceLoader() {
        return dataSourceLoader;
    }

    public static Args<Object> getSchemaAndTable(String s) {
        int dotIdx = s.indexOf(".");
        if (dotIdx != -1) {
            String[] arr = s.split("\\.");
            String schema = arr[0].trim();
            String table = arr[1].trim();
            return Args.create("schema", schema, "table_name", table);
        }
        return Args.of("table_name", s);
    }
}
