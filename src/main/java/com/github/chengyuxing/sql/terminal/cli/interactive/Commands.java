package com.github.chengyuxing.sql.terminal.cli.interactive;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.terminal.cli.completer.XQLNameCompleter;
import com.github.chengyuxing.sql.terminal.common.Constants;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.chengyuxing.sql.terminal.common.Constants.CURRENT_DIR;

public class Commands {
    public static final Command exec = new Command(":exec",
            "Execute sql file or xql name(&sql_name).",
            "sqlFile|&xqlName",
            new AggregateCompleter(
                    XQLNameCompleter.INSTANCE,
                    new Completers.FilesCompleter(Constants.CURRENT_DIR)
            ),
            NullCompleter.INSTANCE);

    public static final Command import_ = new Command(":import",
            "Read a file for execute batch insert,\nsupport: .sql(insert file)|.csv|.tsv|.json|.xls(x).",
            "file [sheetIndex [headerIndex]] | [headerIndex]",
            new Completers.FilesCompleter(CURRENT_DIR, "*.sql|*.json|*.csv|*.tsv|*.xlsx|*.xls"),
            NullCompleter.INSTANCE);

    public static final Command load = new Command(":xql",
            "Read load xql file for execute by name,\ne.g. :xql file or folder",
            "file|folder",
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

    public static final Command status = new Command(":status", "Show current config status and extra item status.",
            "[&[<alias>[.<name>]]]",
            XQLNameCompleter.INSTANCE,
            NullCompleter.INSTANCE);

    public static final Command quit = new Command(":q", "Quit.");

    public static final Command help = new Command(":help", "Show this help message.");

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
    };

    public static Completer[] getCompleters(Completer... mergeMore) {
        List<Completer> completers = new ArrayList<>();
        for (Command command : builtin) {
            completers.add(command.getCompleter());
        }
        completers.addAll(Arrays.asList(mergeMore));
        return completers.toArray(new Completer[0]);
    }

    public static void printHelp() {
        for (Command command : builtin) {
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
