package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.vars.Constants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.GET_MYSQL_SCHEMA;

public class DataBaseResource {
    private final Pattern TRIGGER_PREFIX_REGEX = Pattern.compile("^create\\s+or\\s+replace\\s[\\s\\S]+", Pattern.CASE_INSENSITIVE);
    private final String dbName;
    private final DataSourceLoader dataSourceLoader;
    private final XQLFileManager xqlFileManager;
    private Supplier<Pair<String, Map<String, Object>>> queryTablesFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryProceduresFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryProcedureDefFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryViewsFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryViewDefFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryTriggersFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryTriggerDefFunc;

    public DataBaseResource(String dbName, DataSourceLoader dataSourceLoader) {
        this.dbName = dbName;
        this.dataSourceLoader = dataSourceLoader;
        this.xqlFileManager = new XQLFileManager();
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
                break;
            case "oracle":
                xqlFileManager.add("oracle", "xqls/oracle.sql");
                queryTablesFunc = () -> Pair.of("oracle.user_tables", Collections.emptyMap());
                break;
            case "mysql":
                xqlFileManager.add("mysql", "xqls/mysql.sql");
                queryTablesFunc = () -> {
                    Matcher m = GET_MYSQL_SCHEMA.matcher(dataSourceLoader.getJdbcUrl());
                    if (m.find()) {
                        return Pair.of("mysql.user_tables", Args.of("schema", m.group("schema")));
                    }
                    return Pair.of("", Collections.emptyMap());
                };
                break;
        }
        xqlFileManager.init();
    }

    List<String> getNames(Supplier<Pair<String, Map<String, Object>>> supplier) {
        if (supplier != null) {
            Pair<String, Map<String, Object>> pair = supplier.get();
            String sql = xqlFileManager.get(pair.getItem1());
            if (!sql.equals("")) {
                try (Stream<DataRow> s = dataSourceLoader.getBaki().query(sql).args(pair.getItem2()).stream()) {
                    return s.map(d -> {
                        if (d.getString(1).equals("")) {
                            return d.getString(0);
                        }
                        return d.getString(1) + ":" + d.getString(0);
                    }).collect(Collectors.toList());
                }
            }
        }
        return Collections.emptyList();
    }

    public String getDefinition(Function<String, Pair<String, Map<String, Object>>> func, String name) {
        if (func != null) {
            Pair<String, Map<String, Object>> pair = func.apply(name);
            String sql = xqlFileManager.get(pair.getItem1());
            if (!sql.equals("")) {
                return dataSourceLoader.getBaki().query(sql).args(pair.getItem2())
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
        return "";
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

    public String getTriggerDefinition(String name) {
        String trigger = getDefinition(queryTriggerDefFunc, name).trim();
        Matcher m = TRIGGER_PREFIX_REGEX.matcher(trigger);
        if (!m.find()) {
            trigger = StringUtil.replaceFirstIgnoreCase(trigger, "create", "CREATE OR REPLACE");
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

    public String getDefinition(String type, String name) {
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
        try (Stream<String> lines = Files.lines(cnf)) {
            return lines.map(line -> Arrays.asList(line.split("\\s+")))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            PrintHelper.printlnError(e);
            return Collections.emptySet();
        }
    }
}
