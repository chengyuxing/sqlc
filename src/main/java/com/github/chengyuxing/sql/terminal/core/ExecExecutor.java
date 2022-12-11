package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import org.jline.reader.LineReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.REDIRECT_SYMBOL;

/**
 * exec指令执行器
 */
public class ExecExecutor {
    private final Baki baki;
    private final String execContent;

    public ExecExecutor(Baki baki, String execContent) {
        this.baki = baki;
        this.execContent = execContent;
    }

    /**
     * 执行
     *
     * @param reader readline
     * @throws IOException                             如果输入文件不存在
     * @throws org.jline.reader.UserInterruptException ctrl+c
     * @throws org.jline.reader.EndOfFileException     ctrl+d
     */
    public void exec(LineReader reader) throws Exception {
        if (execContent.contains(REDIRECT_SYMBOL)) {
            Pair<String, String> pair = SqlUtil.getSqlAndRedirect(execContent);
            List<String> sqls = SqlUtil.multiSqlList(pair.getItem1());
            if (!sqls.isEmpty()) {
                if (sqls.size() == 1) {
                    if (SqlUtil.getType(sqls.get(0)) == SqlType.QUERY) {
                        PrintHelper.printlnHighlightSql(sqls.get(0));
                        String sql = sqls.get(0);
                        Pair<String, Map<String, Object>> sqlAndArgs = SqlUtil.prepareSqlWithArgs(sql, reader);
                        try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...", () -> baki.query(sqlAndArgs.getItem1()).args(sqlAndArgs.getItem2()).stream())) {
                            PrintHelper.printlnNotice("redirect query to file...");
                            FileHelper.writeFile(s, pair.getItem2());
                        } catch (Exception e) {
                            throw new RuntimeException("an error when waiting execute: " + sql, e);
                        }
                    } else {
                        PrintHelper.printlnWarning("only query support redirect operation!");
                    }
                } else {
                    PrintHelper.printlnWarning("only single query support redirect operation!");
                }
            }
        } else {
            List<String> sqls = SqlUtil.multiSqlList(execContent);
            if (!sqls.isEmpty()) {
                if (sqls.size() == 1) {
                    String sql = sqls.get(0);
                    PrintHelper.printlnHighlightSql(sql);
                    Pair<String, Map<String, Object>> pair = SqlUtil.prepareSqlWithArgs(sql, reader);
                    String fullSql = pair.getItem1();
                    Map<String, Object> args = pair.getItem2();
                    PrintHelper.printOneSqlResultByType(baki, fullSql, fullSql, args);
                } else {
                    PrintHelper.printMultiSqlResult(baki, sqls, reader);
                }
            }
        }
    }
}
