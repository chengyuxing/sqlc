package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.terminal.cli.completer.ProcParamCompleter;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import org.jline.builtins.Completers;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;

import static com.github.chengyuxing.sql.terminal.common.Constants.*;

public abstract class AbstractMode {
    protected final App app;
    protected final BakiLoader bakiLoader;
    private final String loginId;
    private final LineReader sqlParamReader;
    private final LineReader procParamReader;

    protected AbstractMode(App app, BakiLoader bakiLoader, Terminal terminal) {
        this.app = app;
        this.bakiLoader = bakiLoader;
        this.loginId = StringUtils.hash(app.username + "@" + app.jdbcUrl, "md5");

        this.sqlParamReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new Completers.FilesCompleter(CURRENT_DIR))
                .variable(LineReader.HISTORY_FILE, SQLC_HISTORY_PATH.resolve("history_sql_param_" + this.loginId))
                .history(new DefaultHistory())
                .build();

        this.procParamReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(ProcParamCompleter.INSTANCE)
                .variable(LineReader.HISTORY_FILE, SQLC_HISTORY_PATH.resolve("history_proc_param_" + this.loginId))
                .history(new DefaultHistory())
                .build();
    }

    public String getLoginId() {
        return loginId;
    }

    public LineReader getSqlParamReader() {
        return sqlParamReader;
    }

    public LineReader getProcParamReader() {
        return procParamReader;
    }
}
