package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;

import static com.github.chengyuxing.sql.terminal.vars.Data.*;

public class CompleterBuilder {
    private final List<Completer> completers = new ArrayList<>();
    private final Map<String, Pair<List<String>, List<String>>> cmdDesc = new LinkedHashMap<>();

    public void add(Pair<String, Completer> completer, String mainDesc, String... argsDesc) {
        add(completer, mainDesc, Collections.emptyList(), argsDesc);
    }

    public void add(Pair<String, Completer> completer, String mainDesc, Collection<String> moreMainDesc, String... argsDesc) {
        completers.add(completer.getItem2());
        List<String> mainDesces = new ArrayList<>();
        mainDesces.add(mainDesc);
        mainDesces.addAll(moreMainDesc);
        List<String> argsDescList = Arrays.asList(argsDesc);
        cmdDesc.put(completer.getItem1(), Pair.of(mainDesces, argsDescList));
    }

    public List<Completer> getCompleters() {
        return completers;
    }

    public Map<String, Pair<List<String>, List<String>>> getCmdDesc() {
        return cmdDesc;
    }

    public static CompleterBuilder builtins = new CompleterBuilder() {
        {
            add(CliCompleters.readQuery(":exec"), "read a sql file for execute query, and redirect query result to ",
                    Arrays.asList("file( depends on '-f') optional.", "e.g:", ":exec /query.sql", ":exec /query.sql " + Constants.REDIRECT_SYMBOL + " /path/result.[sql|tsv|csv|xlsx|json]"),
                    "[sql-file]", "[" + Constants.REDIRECT_SYMBOL + " output]");
            add(CliCompleters.read4Batch(":exec@"), "read a multi-line file for execute batch insert, file type:", Arrays.asList(
                    ".sql: delimiter default ';'",
                    ".csv|.tsv|.xls(x): default header-index is 0, it means first line ",
                    "is fields, -1 means no fields, 5 means fields at 5th line and ",
                    "start read from 5th line.",
                    "xls(x) sheet index default is 0."
            ), "[input-file] [sheet-index] [header-index]");
            add(CliCompleters.cmdBuilder(":exec&",
                            xqlNameCompleter,
                            NullCompleter.INSTANCE),
                    "get sql by name(after ':load .../your.sql as me') to execute.",
                    Arrays.asList("e.g: ", ":exec& me.query"),
                    "[sql-name]");
            add(CliCompleters.readXqlFile(":load"), "read load alia a xql file for execute by name.",
                    Arrays.asList("e.g:", ":load /my.xql as my"),
                    "[xql-file]", "as", "[alias]");
            add(CliCompleters.singleCmd(":c"), "enable cache query results.");
            add(CliCompleters.singleCmd(":C"), "disable cache query results.");
            add(CliCompleters.singleCmd(":C!"), "disable and clean cache query results.");
            add(CliCompleters.singleCmd(":paste"), "paste block of sql to execute(Ctrl+o, Enter, Ctrl+x).", Collections.singletonList("or Ctrl+g to get some help!"));
            add(CliCompleters.singleCmd(":ls"), "list all of cache.");
            add(CliCompleters.singleCmd(":status"), "show current status.");
            add(CliCompleters.cmdBuilder(":edit", editCmdCompleter, NullCompleter.INSTANCE),
                    "open editor for update procedure/view/trigger definition.", Arrays.asList(
                            "save: Ctrl+o, Enter",
                            "submit change: Ctrl+x",
                            "or Ctrl+g to get some help!"
                    ), "[procedure-name]");
            add(CliCompleters.cmdBuilder(":get",
                            cacheNameCompleter,
                            new StringsCompleter(Constants.REDIRECT_SYMBOL),
                            new Completers.DirectoriesCompleter(Paths.get(File.separator)),
                            NullCompleter.INSTANCE),
                    "get cache by key [redirect to file].",
                    Arrays.asList("e.g: ", ":get res0", ":get res0 " + Constants.REDIRECT_SYMBOL + " /result.[sql|tsv|csv|xlsx|json]"),
                    "[key]", "[" + Constants.REDIRECT_SYMBOL + " output]");
            add(CliCompleters.cmdBuilder(":rm", cacheNameCompleter, NullCompleter.INSTANCE), "remove the cache by key.", "[key]");
            add(CliCompleters.transaction(":tx"), "use transaction.", "[begin|commit|rollback]");
            add(CliCompleters.view(":view"), "set result view format(display and redirected file format).", "[csv|tsv|json|excel]");
            add(CliCompleters.singleCmd(":d"), "delimiter for multi-sql to batch execute.", Collections.singletonList("default ';'(single semicolon)"), "[delimiter]");
            add(CliCompleters.singleCmd(":q"), "quit.");
            add(CliCompleters.singleCmd(":help"), "get some help.");
            add(CliCompleters.directoriesCmd(Constants.CURRENT_DIR), "redirect to file symbol.");
            add(CliCompleters.keywordsCmd(), "sql keywords.");
        }
    };
}
