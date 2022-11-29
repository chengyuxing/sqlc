package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import org.jline.reader.LineReader;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.REDIRECT_SYMBOL;

public class Executor {
    private final Baki baki;
    private final String execContent;

    public Executor(Baki baki, String execContent) {
        this.baki = baki;
        this.execContent = execContent;
    }

    public void exec(LineReader reader) {
        try {
            if (execContent.contains(REDIRECT_SYMBOL)) {
                Pair<String, String> pair = SqlUtil.getSqlAndRedirect(execContent);
                List<String> sqls = SqlUtil.multiSqlList(pair.getItem1());
                if (sqls.size() > 1) {
                    PrintHelper.printlnWarning("only single query support redirect operation!");
                } else if (SqlUtil.getType(sqls.get(0)) == SqlType.QUERY) {
                    PrintHelper.printlnHighlightSql(sqls.get(0));
                    String sql = sqls.get(0);
                    Map<String, Object> argx = reader == null ? Collections.emptyMap() : SqlUtil.prepareSqlArgIf(sql, reader);
                    try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...", () -> baki.query(sql).args(argx).stream())) {
                        FileHelper.writeFile(s, pair.getItem2());
                    }
                } else {
                    PrintHelper.printlnWarning("only query support redirect operation!");
                }
            } else {
                List<String> sqls = SqlUtil.multiSqlList(execContent);
                if (sqls.size() > 0) {
                    if (sqls.size() == 1) {
                        String sql = sqls.get(0);
                        Map<String, Object> argx = Collections.emptyMap();
                        if (reader != null) {
                            argx = SqlUtil.prepareSqlArgIf(sql, reader);
                        }
                        PrintHelper.printlnHighlightSql(sql);
                        PrintHelper.printOneSqlResultByType(baki, sql, sql, argx);
                    } else {
                        PrintHelper.printMultiSqlResult(baki, sqls);
                    }
                } else {
                    PrintHelper.printlnDanger("no sql script to execute.");
                }
            }
        } catch (Exception e) {
            PrintHelper.printlnError(e);
        }
    }

    public void exec() {
        exec(null);
    }
}
