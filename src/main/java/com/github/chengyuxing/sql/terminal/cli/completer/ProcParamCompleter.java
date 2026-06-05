package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.sql.terminal.common.Constants;
import com.github.chengyuxing.sql.terminal.util.SqlUtils;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import static com.github.chengyuxing.sql.terminal.common.Constants.CURRENT_DIR;

public class ProcParamCompleter {
    private static final Completer outParamCompleter = new StringsCompleter(SqlUtils.getProcedureOutParamTypes().keySet());

    private static final Completer out = new ArgumentCompleter(
            new StringsCompleter("out"),
            outParamCompleter,
            NullCompleter.INSTANCE
    );

    private static final Completer inout = new ArgumentCompleter(
            new StringsCompleter("inout"),
            outParamCompleter,
            new Completers.FilesCompleter(Constants.CURRENT_DIR),
            NullCompleter.INSTANCE
    );

    public static final Completer INSTANCE = new AggregateCompleter(
            out,
            inout,
            new ArgumentCompleter(new Completers.FilesCompleter(CURRENT_DIR))
    );
}
