package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.terminal.Help;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.Stdout;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "sqlc",
        mixinStandardHelpOptions = true,
        version = Help.version,
        sortOptions = false,
        headerHeading = "%nRabbit SQL CLI%n%n",
        description = {
                "%nWhen no '-e' and '--import' command is specified, Interactive Mode starts.",
                "Type ':help' in Interactive Mode."
        },
        optionListHeading = "%nCommand Mode Options:%n",
        header = {
                "A Command Line SQL tool, supporting Query, DDL, DML, Procedure/Function, Transaction, Batch Execute, Export File!",
                "Home page: https://github.com/chengyuxing/sqlc/tree/3.x",
                ""
        }
)
public class StartupShell implements Callable<Integer> {
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
    StartupShell.IoOptions ioOptions;

    @CommandLine.Option(names = "--with-tx", description = "Using transaction.")
    boolean enableTransaction;

    @CommandLine.Option(names = "--ping", description = "Check whether the database connection is successful then 'pong' respond.")
    boolean ping;

    static class IoOptions {
        @Nullable
        @CommandLine.ArgGroup(exclusive = false)
        ExecuteOptions executeOptions;

        @Nullable
        @CommandLine.ArgGroup(exclusive = false)
        ImportOptions importOptions;
    }

    static class ImportOptions {
        @CommandLine.Option(names = "--import", required = true, description = "Read a file for execute batch insert.%nsupport .sql(insert file)|.csv|.tsv|.json|.xls(x)")
        String file = "";

        @CommandLine.Option(names = "--sheet-index", description = "Specify the xls(x) sheet index for '--import' command.")
        int sheetIndex = 0;

        @CommandLine.Option(names = "--header-index", description = "Specify the xls(x),tsv,csv header field mapper row index for '--import' command.")
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

    @Override
    public Integer call() {
        try {
            final Terminal terminal = TerminalBuilder.builder()
                    .name("Rabbit SQL CLI terminal")
                    .encoding(StandardCharsets.UTF_8)
                    .system(true)
                    .build();

            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();

            // FIXME 正式环境取消注释
            BakiLoader.loadDrivers("drivers");
            BakiLoader datasource = BakiLoader.of(jdbcUrl);

            if (!jdbcDriver.isEmpty()) {
                datasource.setDriver(jdbcDriver);
            }

            if (username.isEmpty()) {
                try {
                    datasource.setUsername(lineReader.readLine("username: "));
                } catch (UserInterruptException | EndOfFileException e) {
                    System.err.println("cancel login.");
                    return 0;
                }
            } else {
                datasource.setUsername(username);
            }

            if (password.isEmpty()) {
                for (int i = 5; i >= 0; i--) {
                    try {
                        if (i == 0) {
                            System.err.println("login denied.");
                            return 0;
                        }
                        datasource.setPassword(lineReader.readLine("password: ", '*'));
                        datasource.init();
                        break;
                    } catch (UserInterruptException | EndOfFileException e) {
                        System.err.println("cancel login.");
                        return 0;
                    } catch (Exception e) {
                        log.error("password login", e);
                        Stdout.printlnError(e);
                        Stdout.printlnDanger("please try again.");
                    }
                }
            } else {
                datasource.setPassword(password);
                datasource.init();
            }

            Stdout.print(FigletFont.convertOneLine("Rabbit SQL"), Style.CYAN);
            Stdout.printf("Welcome to Rabbit SQL CLI %s (%s, %s)%n", Help.version, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
            Stdout.printf("Go to %s get more information about this.%n", Help.url);
            Stdout.printf("DataBase: %s%n", datasource.dbName() + " " + datasource.dbVersion());

            if (ping) {
                Stdout.printlnData("pong");
                return 0;
            }

            if (ioOptions == null) {
                return new InteractiveMode(this, datasource, terminal).call();
            }
            return new CommandMode(this, datasource, terminal).call();
        } catch (Exception e) {
            Stdout.printlnError(e);
            return 0;
        }
    }
}
