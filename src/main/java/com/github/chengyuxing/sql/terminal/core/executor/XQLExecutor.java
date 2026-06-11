package com.github.chengyuxing.sql.terminal.core.executor;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.util.SqlUtils;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class XQLExecutor extends AbstractExecutor {
    private final BakiDao baki;

    public XQLExecutor(BakiDao baki) {
        super(baki.getSqlGenerator());
        this.baki = baki;
    }

    @Override
    protected void printResult(String sqlName) throws IOException {
        prepareSQL(sqlName, (sql, args) ->
                PrintHelper.printExecuteResultByType(baki, sqlName, SqlUtils.detectSQLType(sql), args));
    }

    @Override
    protected void outputResult(String sqlName, String output) throws IOException {
        prepareSQL(sqlName, (mysql, args) -> {
            try {
                if (SqlUtils.allowOutput2file(mysql)) {
                    FileHelper.writeFile(baki, sqlName, args, output);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    protected String parseSQL(String sqlName) {
        return baki.getXqlFileManager().get(sqlName.substring(1));
    }
}
