package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.GET_MYSQL_SCHEMA;

public class DataBaseResource {
    private final String dbName;
    private final DataSourceLoader dataSourceLoader;
    private final XQLFileManager xqlFileManager;
    private Supplier<Pair<String, Map<String, Object>>> queryTablesFunc;
    private Supplier<Pair<String, Map<String, Object>>> queryProceduresFunc;
    private Function<String, Pair<String, Map<String, Object>>> queryProcedureDefFunc;

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

    List<String> queryStrings(Supplier<Pair<String, Map<String, Object>>> supplier) {
        if (supplier != null) {
            Pair<String, Map<String, Object>> pair = supplier.get();
            String sql = xqlFileManager.get(pair.getItem1());
            if (!sql.equals("")) {
                try (Stream<DataRow> s = dataSourceLoader.getBaki().query(sql).args(pair.getItem2()).stream()) {
                    return s.map(d -> d.getString(0)).collect(Collectors.toList());
                }
            }
        }
        return Collections.emptyList();
    }

    public String getProcedureDefinition(String procedureName) {
        if (queryProcedureDefFunc != null) {
            Pair<String, Map<String, Object>> pair = queryProcedureDefFunc.apply(procedureName);
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

    public List<String> getUserProcedures() {
        return queryStrings(queryProceduresFunc);
    }

    public List<String> getUserTableNames() {
        return queryStrings(queryTablesFunc);
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
