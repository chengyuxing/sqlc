package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import com.github.chengyuxing.sql.terminal.core.executor.SQLExecutor;
import com.github.chengyuxing.sql.transaction.Tx;
import org.jline.reader.LineReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public class StdInputMode implements Callable<Integer> {
    private final App app;
    private final SQLExecutor sqlExecutor;
    private final String sql;

    public StdInputMode(BakiLoader bakiLoader, App app, String sql) {
        this.app = app;
        this.sqlExecutor = new SQLExecutor(bakiLoader.getUserBaki()) {
            @Override
            protected LineReader paramsReader(String sql) {
                return null;
            }

            @Override
            protected void prepareSQL(String sql, BiConsumer<String, Map<String, Object>> consumer) {
                String newSQL = parseSQL(sql);
                consumer.accept(newSQL, Collections.emptyMap());
            }
        };
        this.sql = sql;
    }

    @Override
    public Integer call() throws Exception {
        if (app.ioOptions != null) {
            App.ExecuteOptions execute = app.ioOptions.executeOptions;
            if (execute != null) {
                Context.viewMode.set(execute.format);
                Context.outputPath.set(execute.output);
            }
        }
        if (app.enableTransaction) {
            Tx.using(() -> {
                try {
                    this.sqlExecutor.execute(sql);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            this.sqlExecutor.execute(sql);
        }
        return 0;
    }
}
