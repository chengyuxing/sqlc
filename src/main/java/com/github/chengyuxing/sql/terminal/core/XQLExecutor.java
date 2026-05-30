package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.terminal.common.Context;
import org.jline.reader.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public class XQLExecutor {
    private static final Logger log = LoggerFactory.getLogger(XQLExecutor.class);

    private final BakiDao baki;
    private final LineReader lineReader;

    public XQLExecutor(BakiDao baki, LineReader lineReader) {
        this.baki = baki;
        this.lineReader = lineReader;
    }

    public void execute(String sqlName) throws IOException {
        String output = Context.outputPath.get();
        if (output.isEmpty()) {
            printResult(sqlName);
            return;
        }
        outputResult(sqlName, output);
    }

    private void printResult(String sqlName) {
        String sql = Context.xqlFileManager.get(sqlName);
        Stdout.printlnHighlightSql(sql);
        Pair<String, Map<String, Object>> pair = SqlUtil.prepareSqlWithArgs(sql, lineReader);
        PrintHelper.printOneSqlResultByType(baki, "&" + sqlName, pair.getItem1(), pair.getItem2());
    }

    private void outputResult(String sqlName, String output) {
        String sql = Context.xqlFileManager.get(sqlName);
        Stdout.printlnHighlightSql(sql);
        Pair<String, Map<String, Object>> sqlAndArgs = SqlUtil.prepareSqlWithArgs(sql, lineReader);
        try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...", () -> baki.query("&" + sqlName).args(sqlAndArgs.getItem2()).stream())) {
            Stdout.printlnNotice("redirect query to file...");
            Path path = Paths.get(output);
            if (Files.isDirectory(path)) {
                path = path.resolve("sqlc_query_result_" + System.currentTimeMillis());
            }
            FileHelper.writeFile(s, path.toString());
        } catch (Exception e) {
            log.error("xql exec error", e);
            throw new RuntimeException("an error when waiting execute: " + sql, e);
        }
    }
}
