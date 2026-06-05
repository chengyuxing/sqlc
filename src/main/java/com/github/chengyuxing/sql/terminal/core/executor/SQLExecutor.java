package com.github.chengyuxing.sql.terminal.core.executor;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.SqlUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class SQLExecutor extends AbstractExecutor {
    private final BakiDao baki;

    public SQLExecutor(BakiDao baki) {
        this.baki = baki;
    }

    @Override
    protected void printResult(String sql) throws IOException {
        prepareSQL(sql, (mysql, args) ->
                PrintHelper.printExecuteResultByType(baki, mysql, SqlUtils.detectSQLType(mysql), args));
    }

    @Override
    protected void outputResult(String sql, String output) throws IOException {
        prepareSQL(sql, (mysql, args) -> {
            try {
                if (SqlUtils.allowOutput2file(mysql)) {
                    FileHelper.writeFile(baki, mysql, args, output);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    protected String parseSQL(String sqlOrPath) {
        String sql = sqlOrPath;
        Path path = PathUtils.resolve(sqlOrPath);
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
