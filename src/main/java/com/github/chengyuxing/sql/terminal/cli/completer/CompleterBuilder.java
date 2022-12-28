package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

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
                            new StringsCompleter(Constants.REDIRECT_SYMBOL),
                            new Completers.DirectoriesCompleter(Constants.CURRENT_DIR),
                            NullCompleter.INSTANCE),
                    "get sql by name(after ':load .../your.sql as me') to execute or",
                    Arrays.asList("redirect query to file.", "e.g: ", ":exec& me.query"),
                    "[sql-name] [" + Constants.REDIRECT_SYMBOL + " output]");
            add(CliCompleters.readXqlFile(":load"), "read load alia a xql file for execute by name.",
                    Arrays.asList("e.g:", ":load /my.xql as my"),
                    "[xql-file]", "as", "[alias]");
            add(CliCompleters.singleCmd(":paste"), "paste block of sql to execute(Ctrl+o, Enter, Ctrl+x).", Collections.singletonList("or Ctrl+g to get some help!"));
            add(CliCompleters.cmdBuilder(":edit",
                            new DynamicCompleter() {{
                                listening(word -> {
                                    if (word.startsWith("proc:")) {
                                        return dbObjects.getProcedures();
                                    }
                                    if (word.startsWith("tg:")) {
                                        return dbObjects.getTriggers();
                                    }
                                    if (word.startsWith("view:")) {
                                        return dbObjects.getViews();
                                    }
                                    return dbObjects.getProcedures();
                                });
                            }},
                            NullCompleter.INSTANCE),
                    "open editor for update procedure/view/trigger definition.", Arrays.asList(
                            "save: Ctrl+o, Enter",
                            "submit change: Ctrl+x",
                            "e.g:",
                            "proc:test.my_func()"
                    ), "[[proc|tg|view]:object]");
            add(CliCompleters.cmdBuilder(":desc",
                            new DynamicCompleter() {{
                                listening(word -> dbObjects.getTables());
                            }},
                            new StringsCompleter(Constants.REDIRECT_SYMBOL),
                            new Completers.DirectoriesCompleter(Constants.CURRENT_DIR),
                            NullCompleter.INSTANCE), "show table description or redirect to tsv file.",
                    Arrays.asList("e.g.", ":desc test.my_table " + Constants.REDIRECT_SYMBOL + " /root/my.tsv"),
                    "[table] [" + Constants.REDIRECT_SYMBOL + " output]"
            );
            add(CliCompleters.cmdBuilder(":ddl",
                            new DynamicCompleter() {{
                                listening(word -> {
                                    if (word.startsWith("proc:")) {
                                        return dbObjects.getProcedures();
                                    }
                                    if (word.startsWith("tg:")) {
                                        return dbObjects.getTriggers();
                                    }
                                    if (word.startsWith("view:")) {
                                        return dbObjects.getViews();
                                    }
                                    return dbObjects.getTables();
                                });
                            }},
                            new StringsCompleter(Constants.REDIRECT_SYMBOL),
                            new Completers.DirectoriesCompleter(Constants.CURRENT_DIR),
                            NullCompleter.INSTANCE), "show object(table, procedure/function, view, trigger) ddl ",
                    Arrays.asList("or redirect to sql file.",
                            "e.g:",
                            ":ddl test.my_table " + Constants.REDIRECT_SYMBOL + " /root/table.sql",
                            ":ddl proc:test.my_func()",
                            ":ddl tg:test.my_trigger()"),
                    "[[proc|tg|view:]object] [" + Constants.REDIRECT_SYMBOL + " output]"
            );
            add(CliCompleters.transaction(":tx"), "use transaction.", "[begin|commit|rollback]");
            add(CliCompleters.view(":view"), "set result view format(display and redirected file format).", "[csv|tsv|json|excel]");
            add(CliCompleters.singleCmd(":d"), "delimiter for multi-sql to batch execute.", Collections.singletonList("default ';'(single semicolon)"), "[delimiter]");
            add(CliCompleters.singleCmd(":status"), "show current status.");
            add(CliCompleters.singleCmd(":q"), "quit.");
            add(CliCompleters.singleCmd(":help"), "get some help.");
            add(CliCompleters.directoriesCmd(Constants.CURRENT_DIR), "redirect to file symbol.");
            add(CliCompleters.keywordsCmd(), "sql keywords.");
        }
    };
}
