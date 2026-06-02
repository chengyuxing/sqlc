package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.sql.terminal.common.Constants;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.*;

public class ExecCompleter {
    private final static Completer[] group = new Completer[]{
            new StringsCompleter(),
            new Completers.FilesCompleter(Constants.CURRENT_DIR)
    };

    public static final AggregateCompleter INSTANCE = new AggregateCompleter(group);

    public static void setXQLNames(Collection<String> names) {
        Set<String> refs = new HashSet<>();
        for (String var : names) {
            refs.add("&" + var);
        }
        group[0] = new StringsCompleter(refs);
    }
}
