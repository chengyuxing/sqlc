package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.terminal.cli.completer.ProcParamCompleter;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import org.jline.builtins.Completers;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;

import static com.github.chengyuxing.sql.terminal.common.Constants.SQLC_TEMP_PATH;
import static com.github.chengyuxing.sql.terminal.common.Constants.USER_HOME;

public abstract class AbstractMode {
    protected final StartupShell shell;
    protected final BakiLoader bakiLoader;
    private final String loginId;
    private final LineReader sqlParamReader;
    private final LineReader procParamReader;

    protected AbstractMode(StartupShell shell, BakiLoader bakiLoader, Terminal terminal) {
        this.shell = shell;
        this.bakiLoader = bakiLoader;
        this.loginId = StringUtils.hash(shell.username + "@" + shell.jdbcUrl, "md5");

        this.sqlParamReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new AggregateCompleter(new Completers.FilesCompleter(USER_HOME)))
                .variable(LineReader.HISTORY_FILE, SQLC_TEMP_PATH.resolve("history_sql_param_" + this.loginId))
                .history(new DefaultHistory())
                .build();

        this.procParamReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(
                        new AggregateCompleter(ProcParamCompleter.in,
                                ProcParamCompleter.out,
                                ProcParamCompleter.inout,
                                new ArgumentCompleter(new Completers.FilesCompleter(USER_HOME))
                        )
                )
                .variable(LineReader.HISTORY_FILE, SQLC_TEMP_PATH.resolve("history_proc_param_" + this.loginId))
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
