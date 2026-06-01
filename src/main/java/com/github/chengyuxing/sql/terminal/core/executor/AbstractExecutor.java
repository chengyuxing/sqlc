package com.github.chengyuxing.sql.terminal.core.executor;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import org.jline.reader.LineReader;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

public abstract class AbstractExecutor {
    protected abstract LineReader paramsReader(String sql);

    protected abstract String parseSQL(String sql);

    protected abstract void printResult(String sql) throws IOException;

    protected abstract void outputResult(String sql, String output) throws IOException;

    public void execute(String target) throws IOException {
        String output = Context.outputPath.get();
        if (output.isEmpty()) {
            printResult(target);
            return;
        }
        outputResult(target, output);
    }

    protected void prepareSQL(String sql, BiConsumer<String, Map<String, Object>> consumer) throws IOException {
        String newSQL = parseSQL(sql);
        Stdout.printlnHighlightSql(newSQL);
        Pair<String, Map<String, Object>> data = SqlUtil.prepareSqlWithArgs(newSQL, paramsReader(newSQL));
        consumer.accept(data.getItem1(), data.getItem2());
    }
}
