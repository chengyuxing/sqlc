package com.github.chengyuxing.sql.terminal.core.executor;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.Stdout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class SQLExecutor extends IExecutor {
    private final BakiDao baki;

    public SQLExecutor(BakiDao baki) {
        this.baki = baki;
    }

    @Override
    protected void printResult(String sql) throws IOException {
        prepareSQL(sql, (mysql, args) ->
                PrintHelper.printExecuteResultByType(baki, mysql, SqlUtil.detectSQLType(mysql), args));
    }

    @Override
    protected void outputResult(String sql, String output) throws IOException {
        prepareSQL(sql, (mysql, args) -> {
            SqlType sqlType = SqlUtil.detectSQLType(mysql);
            if (sqlType != SqlType.QUERY) {
                Stdout.printlnWarning("only query can output to file");
                return;
            }
            try {
                FileHelper.writeFile(baki, mysql, args, output);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    protected String parseSQL(String sqlOrPath) {
        String sql = sqlOrPath;
        Path path = Paths.get(sqlOrPath);
        if (!sqlOrPath.contains("\n") && Files.exists(path) && Files.isRegularFile(path)) {
            try {
                sql = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return sql;
    }
}
