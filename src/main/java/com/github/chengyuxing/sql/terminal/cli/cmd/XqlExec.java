package com.github.chengyuxing.sql.terminal.cli.cmd;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.core.UserBaki;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.vars.Data;
import org.jline.reader.LineReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.REDIRECT_SYMBOL;

public class XqlExec {
    private final UserBaki baki;
    private final LineReader lineReader;

    public XqlExec(UserBaki baki, LineReader lineReader) {
        this.baki = baki;
        this.lineReader = lineReader;
    }

    public void exec(String sqlName) {
        String sql;
        if (sqlName.contains(REDIRECT_SYMBOL)) {
            // me.query $> /my_path.csv
            Pair<String, String> pair = SqlUtil.getSqlAndRedirect(sqlName);
            sqlName = pair.getItem1();
            sql = Data.xqlFileManager.get(sqlName);
            if (SqlUtil.getType(sql) == SqlType.QUERY) {
                PrintHelper.printlnHighlightSql(sql);
                Pair<String, Map<String, Object>> sqlAndArgs = SqlUtil.prepareSqlWithArgs(sql, lineReader);
                final String sqlAddress = sqlName;
                try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...", () -> baki.query("&" + sqlAddress).args(sqlAndArgs.getItem2()).stream())) {
                    PrintHelper.printlnNotice("redirect query to file...");
                    Path output = Paths.get(pair.getItem2());
                    if (Files.isDirectory(output)) {
                        output = output.resolve("sqlc_query_result_" + System.currentTimeMillis());
                    }
                    FileHelper.writeFile(s, output.toString());
                    return;
                } catch (Exception e) {
                    throw new RuntimeException("an error when waiting execute: " + sql, e);
                }
            }
            PrintHelper.printlnWarning("only query support redirect operation!");
            return;
        }
        sql = Data.xqlFileManager.get(sqlName);
        PrintHelper.printlnHighlightSql(sql);
        Pair<String, Map<String, Object>> pair = SqlUtil.prepareSqlWithArgs(sql, lineReader);
        PrintHelper.printOneSqlResultByType(baki, "&" + sqlName, pair.getItem1(), pair.getItem2());
    }
}
