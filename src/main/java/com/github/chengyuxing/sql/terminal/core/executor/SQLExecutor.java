package com.github.chengyuxing.sql.terminal.core.executor;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.terminal.cli.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public abstract class SQLExecutor implements IExecutor {
    private static final Logger log = LoggerFactory.getLogger(SQLExecutor.class);

    private final Baki baki;

    public SQLExecutor(Baki baki) {
        this.baki = baki;
    }

    public void execute(String sql) throws IOException {
        String mySql = getSql(sql);
        String output = Context.outputPath.get();
        if (output.isEmpty()) {
            printResult(mySql);
            return;
        }
        outputResult(mySql, output);
    }

    private void printResult(String sql) throws IOException {
        Stdout.printlnHighlightSql(sql);
        Pair<String, Map<String, Object>> pair = SqlUtil.prepareSqlWithArgs(sql, paramsReader(sql));
        String fullSql = pair.getItem1();
        Map<String, Object> args = pair.getItem2();
        PrintHelper.printExecuteResultByType(baki, fullSql, SqlUtil.detectSQLType(fullSql), args);
    }

    private void outputResult(String sql, String output) throws IOException {
        Stdout.printlnHighlightSql(sql);
        Pair<String, Map<String, Object>> sqlAndArgs = SqlUtil.prepareSqlWithArgs(sql, paramsReader(sql));
        SqlType sqlType = SqlUtil.detectSQLType(sql);
        if (sqlType != SqlType.QUERY) {
            Stdout.printlnWarning("Only query can redirect to file.");
            return;
        }
        try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...",
                () -> baki.query(sqlAndArgs.getItem1()).args(sqlAndArgs.getItem2()).stream())) {
            Stdout.printlnNotice("redirect query to file...");
            Path path = Paths.get(output);
            if (Files.isDirectory(path)) {
                path = path.resolve("sqlc_query_result_" + System.currentTimeMillis());
            }
            FileHelper.writeFile(s, path.toString());
        } catch (Exception e) {
            log.error("exec error", e);
            throw new RuntimeException("an error when waiting execute: " + sql, e);
        }
    }

    private String getSql(String sqlOrPath) throws IOException {
        Path path = Paths.get(sqlOrPath);
        if (Files.exists(path) && Files.isRegularFile(path)) {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        }
        return sqlOrPath;
    }
}
