package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.sql.terminal.common.Constants;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

public class ProcParamCompleter {
    private static final Completer outParamCompleter = new StringsCompleter(SqlUtil.getProcedureOutParamTypes().keySet());

    public static final Completer in = new ArgumentCompleter(
            new StringsCompleter("in"),
            new Completers.FilesCompleter(Constants.USER_HOME),
            NullCompleter.INSTANCE
    );

    public static final Completer out = new ArgumentCompleter(
            new StringsCompleter("out"),
            outParamCompleter,
            NullCompleter.INSTANCE
    );

    public static final Completer inout = new ArgumentCompleter(
            new StringsCompleter("inout"),
            outParamCompleter,
            new Completers.FilesCompleter(Constants.USER_HOME),
            NullCompleter.INSTANCE
    );
}
