package com.github.chengyuxing.sql.terminal.cli.interactive;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.terminal.cli.completer.ExecCompleter;
import com.github.chengyuxing.sql.terminal.cli.completer.KeywordsCompleter;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import static com.github.chengyuxing.sql.terminal.common.Constants.CURRENT_DIR;

public class Commands {
    public static final Command exec = new Command(":exec",
            "Execute sql file or xql name(&sql_name).",
            "sqlFile|&xqlName",
            new ExecCompleter(),
            NullCompleter.INSTANCE);

    public static final Command import_ = new Command(":import",
            "Read a file for execute batch insert,\nsupport: .sql(insert file)|.csv|.tsv|.json|.xls(x).",
            "file [sheetIndex [headerIndex]] | [headerIndex]",
            new Completers.FilesCompleter(CURRENT_DIR, "*.sql|*.json|*.csv|*.tsv|*.xlsx|*.xls"),
            NullCompleter.INSTANCE);

    public static final Command load = new Command(":load",
            "Read load xql file for execute by name,\ne.g. :load /my.xql",
            "file",
            new Completers.FilesCompleter(CURRENT_DIR, "*.xql"),
            NullCompleter.INSTANCE);

    public static Command paste = new Command(":paste",
            "Paste block of sql to execute(Ctrl+o, Enter, Ctrl+x)\nor Ctrl+g to get some help.");

    public static final Command transaction = new Command(":tx",
            "Use transaction.",
            "[begin|commit|rollback]",
            new StringsCompleter("begin", "commit", "rollback"),
            NullCompleter.INSTANCE);

    public static final Command toggleView = new Command(":view",
            "Set result format(display and output file type).",
            "csv|tsv|json|excel",
            new StringsCompleter("csv", "tsv", "json", "excel"),
            NullCompleter.INSTANCE);

    public static final Command output = new Command(":output",
            "Enable(:output /xxx) and Disable(:output) query\nresult output redirect to file.",
            "[file]",
            new Completers.DirectoriesCompleter(CURRENT_DIR),
            NullCompleter.INSTANCE);

    public static final Command status = new Command(":status", "Show current status.");

    public static final Command quit = new Command(":q", "Quit.");

    public static final Command help = new Command(":help", "Show this help message.");

    public static final Command sqlKeywords = new Command("", "Sql input interactive mode", "",
            new KeywordsCompleter());

    public static Command[] builtin = new Command[]{
            exec,
            import_,
            load,
            paste,
            transaction,
            toggleView,
            output,
            status,
            quit,
            help,
            sqlKeywords
    };

    public static Completer[] getCompleters() {
        Completer[] completers = new Completer[builtin.length];
        for (int i = 0; i < completers.length; i++) {
            ArgumentCompleter completer = builtin[i].getCompleter();
            // 1 argument completer just take the actually completer.
            // e.g. sqlKeywords
            // Fixed typing 'select * fr' should suggest 'form' word but it doesn't
            if (completer.getCompleters().size() == 1) {
                completers[i] = completer.getCompleters().get(0);
            } else {
                completers[i] = builtin[i].getCompleter();
            }
        }
        return completers;
    }

    public static void printHelp() {
        for (Command command : builtin) {
            if (command.getName().isEmpty()) {
                continue;
            }

            String argsHolder = command.getArgsDescription().isEmpty() ? "" : "<" + command.getArgsDescription() + ">";
            String cmd = "  " + command.getName() + " " + argsHolder;

            Stdout.printf("%-26s", Style.DARK_YELLOW, cmd);

            boolean cmdGt18 = cmd.length() > 22;

            if (cmdGt18) {
                Stdout.println();
            }

            String[] lines = command.getDescription().split("\n");
            for (int i = 0; i < lines.length; i++) {
                if (i == 0) {
                    if (cmdGt18) {
                        Stdout.printf("%-26s%s%n", "", lines[i]);
                    } else {
                        Stdout.printf("%s%n", lines[i]);
                    }
                } else {
                    Stdout.printf("%-26s%s%n", "", lines[i]);
                }
            }
        }
    }
}
