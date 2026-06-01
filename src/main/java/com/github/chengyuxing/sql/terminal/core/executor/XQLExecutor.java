package com.github.chengyuxing.sql.terminal.core.executor;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.cli.Context;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class XQLExecutor extends AbstractExecutor {
    private final BakiDao baki;

    public XQLExecutor(BakiDao baki) {
        this.baki = baki;
    }

    @Override
    protected void printResult(String sqlName) throws IOException {
        prepareSQL(sqlName, (sql, args) ->
                PrintHelper.printExecuteResultByType(baki, ref(sqlName), SqlUtil.detectSQLType(sql), args));
    }

    @Override
    protected void outputResult(String sqlName, String output) throws IOException {
        prepareSQL(sqlName, (mysql, args) -> {
            SqlType sqlType = SqlUtil.detectSQLType(mysql);
            if (sqlType != SqlType.QUERY) {
                Stdout.printlnWarning("only query can output to file");
                return;
            }
            try {
                FileHelper.writeFile(baki, ref(sqlName), args, output);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    protected String parseSQL(String sqlName) {
        return Context.xqlFileManager.get(sqlName);
    }

    private String ref(String sqlName) {
        return "&" + sqlName;
    }
}
