package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.lalyos.jfiglet.FigletFont;
import org.jetbrains.annotations.Nullable;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(name = Help.appSimpleName,
        mixinStandardHelpOptions = true,
        version = Help.version,
        sortOptions = false,
        headerHeading = "%n" + Help.appName + "%n%n",
        description = {
                "%nWhen no '-e' and '--import' command is specified, Interactive Mode starts.",
                "Type ':help' in Interactive Mode."
        },
        optionListHeading = "%nCommand Mode Options:%n",
        header = {
                "A Command Line SQL tool, supporting Query, DDL, DML, Procedure/Function, Transaction, Batch Execute, Export File!",
                "Home page: " + Help.url,
                ""
        }
)
public class App implements Callable<Integer> {
    private static final Logger log = LoggerFactory.getLogger("SQLC");

    @CommandLine.Option(names = {"-u", "--url"}, required = true, description = "Jdbc url, e.g: -ujdbc:postgresql://...")
    String jdbcUrl = "";

    @CommandLine.Option(names = {"-n", "--name"}, description = "Database username.")
    String username = "";

    @CommandLine.Option(names = {"-p", "--password"}, description = "Database password.")
    String password = "";

    @CommandLine.Option(names = "--driver", description = "Specify the JDBC Driver class name if not found.")
    String jdbcDriver = "";

    @Nullable
    @CommandLine.ArgGroup
    App.IoOptions ioOptions;

    @CommandLine.Option(names = "--with-tx", description = "Using transaction.")
    boolean enableTransaction;

    @CommandLine.Option(names = "--ping", description = "Check whether the database connection is successful then 'pong' respond.")
    boolean ping;

    @CommandLine.Option(names = "--batch-size", paramLabel = "<n>", description = "The size for the batch insert.")
    int batchSize = 0;

    @CommandLine.Option(names = "--named-param-prefix", paramLabel = "<char>", description = "Prepare SQL named parameter prefix symbol.")
    char namedParamPrefix = ':';

    static class IoOptions {
        @Nullable
        @CommandLine.ArgGroup(exclusive = false)
        ExecuteOptions executeOptions;

        @Nullable
        @CommandLine.ArgGroup(exclusive = false)
        ImportOptions importOptions;

        @CommandLine.Option(names = "--xql", paramLabel = "<file|folder>", description = "Load xql file or folder's all xql file.")
        String[] xql = new String[0];
    }

    static class ImportOptions {
        @CommandLine.Option(names = "--import", required = true, description = "Read a file for execute batch insert.%nsupport .sql(insert file)|.csv|.tsv|.json|.xls(x)")
        String file = "";

        @CommandLine.Option(names = "--sheet-index", paramLabel = "<n>", description = "Specify the xls(x) sheet index for '--import' command.")
        int sheetIndex = 0;

        @CommandLine.Option(names = "--header-index", paramLabel = "<n>", description = "Specify the xls(x),tsv,csv header field mapper row index for '--import' command.")
        int headerIndex = 0;
    }

    static class ExecuteOptions {
        @CommandLine.Option(names = {"-e", "--execute"}, required = true, description = "Execute sql content or sql file.")
        String[] sql = new String[0];

        @CommandLine.Option(names = {"-o", "--output"}, description = "Output query result as file.")
        String output = "";

        @CommandLine.Option(names = {"-f", "--format"}, paramLabel = "tsv|csv|json|excel", description = "Format of query result for display and output file.")
        View format = View.tsv;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            final Terminal terminal = TerminalBuilder.builder()
                    .name(Help.appName)
                    .encoding(StandardCharsets.UTF_8)
                    .system(true)
                    .build();

            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();

            // FIXME 正式环境取消注释
            BakiLoader.loadDrivers("drivers");
            BakiLoader bakiLoader = BakiLoader.of(jdbcUrl);

            if (!doLogin(bakiLoader, lineReader)) {
                return 0;
            }

            if (ping) {
                Stdout.printlnData("pong");
                return 0;
            }

            Stdout.print(FigletFont.convertOneLine(Help.coreName), Style.CYAN);
            Stdout.printf("Welcome to %s %s (%s, %s)%n", Help.appName, Help.version, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
            Stdout.printf("Go to %s get more information about this.%n", Stdout.colorful(Help.url, Style.UNDERLINE));
            Stdout.printf("DataBase: %s %s%n", bakiLoader.dbName(), bakiLoader.dbVersion());

            loadConfig(bakiLoader);

            if (ioOptions == null) {
                return new InteractiveMode(this, bakiLoader, terminal).call();
            }
            return new CommandMode(this, bakiLoader, terminal).call();
        } catch (Exception e) {
            Stdout.printlnError(e);
            return 0;
        }
    }

    private boolean doLogin(BakiLoader bakiLoader, LineReader lineReader) {
        if (!jdbcDriver.isEmpty()) {
            bakiLoader.setDriver(jdbcDriver);
        }
        if (username.isEmpty()) {
            try {
                bakiLoader.setUsername(lineReader.readLine("username: "));
            } catch (UserInterruptException | EndOfFileException e) {
                Stdout.println("cancel login.");
                return false;
            }
        } else {
            bakiLoader.setUsername(username);
        }

        if (password.isEmpty()) {
            for (int i = 5; i >= 0; i--) {
                try {
                    if (i == 0) {
                        Stdout.println("login denied.");
                        return false;
                    }
                    bakiLoader.setPassword(lineReader.readLine("password: ", '*'));
                    bakiLoader.init();
                    break;
                } catch (UserInterruptException | EndOfFileException e) {
                    Stdout.println("cancel login.");
                    return false;
                } catch (Exception e) {
                    log.error("password login", e);
                    Stdout.printlnError(e);
                    Stdout.printlnDanger("please try again.");
                }
            }
        } else {
            bakiLoader.setPassword(password);
            bakiLoader.init();
        }
        return true;
    }

    private void loadConfig(BakiLoader bakiLoader) throws IOException {
        if (batchSize > 0) {
            bakiLoader.getUserBaki().setBatchSize(batchSize);
        }
        XQLFileManager xqlFileManager = bakiLoader.getUserBaki().getXqlFileManager();
        xqlFileManager.setNamedParamPrefix(namedParamPrefix);
        bakiLoader.getUserBaki().setXqlFileManager(xqlFileManager);

        if (ioOptions != null) {
            String[] xqls = ioOptions.xql;
            // If 1 xql, detect is folder or file
            if (xqls.length == 1) {
                Path first = PathUtils.resolve(xqls[0]);
                String[] files = Files.isDirectory(first)
                        ? FileHelper.getFiles(first, ".xql")
                        : xqls;
                FileHelper.loadXqlFiles(xqlFileManager, files);
            } else {
                FileHelper.loadXqlFiles(xqlFileManager, xqls);
            }
        }
    }
}
