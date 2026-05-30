package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.core.SQLExecutor;
import com.github.chengyuxing.sql.terminal.core.XQLExecutor;
import com.github.chengyuxing.sql.terminal.cli.completer.ExecCompleter;
import com.github.chengyuxing.sql.terminal.cli.completer.KeywordsCompleter;
import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.cli.component.SqlHistory;
import com.github.chengyuxing.sql.terminal.cli.interactive.Commands;
import com.github.chengyuxing.sql.terminal.core.*;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.terminal.common.Context;
import com.github.chengyuxing.sql.transaction.Tx;
import org.jline.builtins.ConfigurationPath;
import org.jline.console.CommandRegistry;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.JlineCommandRegistry;
import org.jline.keymap.KeyMap;
import org.jline.reader.*;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.widget.AutosuggestionWidgets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import static com.github.chengyuxing.sql.terminal.common.Constants.*;
import static com.github.chengyuxing.sql.terminal.common.Constants.APP_DIR;
import static com.github.chengyuxing.sql.terminal.common.Constants.USER_HOME;

public class InteractiveMode extends AbstractMode implements Callable<Integer> {
    private final AsyncCat cat = new AsyncCat();
    private final CommandRegistry.CommandSession session;
    private final List<String> sqlBuilder;
    private final LineReader lineReader;
    private final BakiDao baki;
    private final JlineCommandRegistry commandRegistry;
    private final Prompt prompt;
    private final SQLExecutor exec;
    private final XQLExecutor xqlExec;

    protected InteractiveMode(StartupShell shell, BakiLoader bakiLoader, Terminal terminal) {
        super(shell, bakiLoader);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (Context.txActive.get()) {
                Tx.rollback();
                Stdout.printlnWarning("Transaction rollback!");
            }
            bakiLoader.close();
            cat.close();
            Context.tempFiles.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception e) {
                    Stdout.printlnError(e);
                }
            });
            System.out.println("Bye bye :(");
        }));

        this.session = new CommandRegistry.CommandSession(terminal);
        this.sqlBuilder = new ArrayList<>();
        this.lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .parser(cliParser)
                .completer(new AggregateCompleter(Commands.getCompleters()))
                .variable(LineReader.HISTORY_FILE, SQLC_USER_PATH.resolve("history_" + StringUtils.hash(shell.username + "@" + shell.jdbcUrl, "md5")))
                .history(new SqlHistory(sqlBuilder))
                .build();
        this.baki = bakiLoader.getUserBaki();
        this.commandRegistry = new Builtins(CURRENT_DIR, new ConfigurationPath(APP_DIR, USER_HOME), s -> lineReader.getBuiltinWidgets().get(s));
        this.prompt = new Prompt(shell.jdbcUrl);
        this.exec = new SQLExecutor(this.baki, this.lineReader);
        this.xqlExec = new XQLExecutor(this.baki, this.lineReader);
        init();
    }

    private void init() {
        AutosuggestionWidgets suggest = new AutosuggestionWidgets(lineReader);
        // press ctrl+o jump to next word.
        suggest.getKeyMap().bind(new Reference(LineReader.FORWARD_WORD), KeyMap.ctrl('o'));
        suggest.enable();

        DataBaseResource dataBaseResource = new DataBaseResource(bakiLoader);

        KeywordsCompleter keywordsCompleter = (KeywordsCompleter) Commands.sqlKeywords.getCompleter().getCompleters().get(0);

        keywordsCompleter.addVarsNames(dataBaseResource.getSqlKeyWordsWithDefault());
        cat.supplyAsync(dataBaseResource::getUserTableNames)
                .whenCompleteAsync((tables, e) ->
                        keywordsCompleter.addVarsNames(tables));

        Context.promptReference.set(prompt);

        Stdout.println("Type in command or sql script to execute query, ddl, dml..., or try :help.");
    }

    @Override
    public Integer call() {
        try {
            exit:
            while (true) {
                try {
                    String line = lineReader.readLine(Context.getPromptState(shell.jdbcUrl)).trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                    // execute command
                    if (line.startsWith(":")) {
                        sqlBuilder.clear();
                        switch (line) {
                            case ":q":
                                if (Context.txActive.get()) {
                                    Stdout.printlnWarning("Warning: Transaction is active now, please :tx commit or :tx rollback before quit, Control c, server shutdown or kill command will be rollback transaction!");
                                    break;
                                } else {
                                    break exit;
                                }
                            case ":help":
                                Stdout.printf("Typing the statement and execute it with a ';' at the end.%n");
                                Stdout.printf("Interactive Mode Commands:%n");
                                Commands.printHelp();
                                break;
                            case ":status":
                                Stdout.printlnInfo("View Mode: " + Context.viewMode.get());
                                Stdout.printlnInfo("Transaction: " + (Context.txActive.get() ? "enabled" : "disabled"));
                                break;
                            case ":tx":
                                if (Context.txActive.get()) {
                                    Stdout.printlnWarning("transaction is active now!");
                                } else {
                                    Tx.begin();
                                    Context.txActive.set(true);
                                }
                                break;
                            case ":paste":
                                pasteAction();
                                break;
                            default:
                                if (line.startsWith(":import")) {
                                    String[] args = line.substring(7).trim().split("\\s+");
                                    importAction(args);
                                    break;
                                }

                                // :exec [xql name | file | sql]
                                if (line.startsWith(":exec")) {
                                    String target = line.substring(5).trim();
                                    executeAction(target);
                                    break;
                                }

                                // :load my.xql
                                if (line.startsWith(":load")) {
                                    String xql = line.substring(5).trim();
                                    loadXqlFileAction(xql);
                                    break;
                                }

                                if (line.startsWith(":tx")) {
                                    String action = line.substring(3).trim();
                                    action = action.isEmpty() ? "begin" : action.toLowerCase();
                                    int code = transactionToggleAction(action);
                                    if (code == 0) {
                                        break;
                                    }
                                }

                                if (line.startsWith(":view")) {
                                    String view = line.substring(5).trim();
                                    toggleViewAction(view);
                                    break;
                                }

                                if (line.startsWith(":output")) {
                                    String path = line.substring(7).trim();
                                    Context.outputPath.set(path);
                                    break;
                                }

                                Stdout.printlnWarning("command invalid, command :help to get some help!");
                                break;
                        }
                        continue;
                    }
                    // merge sql to execute
                    if (line.endsWith(";")) {
                        line = line.substring(0, line.length() - 1);
                        sqlBuilder.add(line);
                        String sql = String.join("\n", sqlBuilder);
                        // execute sql
                        if (!sql.isEmpty()) {
                            exec.execute(sql);
                            sqlBuilder.clear();
                            Context.appending.set(false);
                        }
                    } else {
                        sqlBuilder.add(line);
                        Context.appending.set(true);
                    }
                } catch (UserInterruptException e) {
                    // ctrl+c
                    Stdout.printlnNotice(":q");
                    break;
                } catch (EndOfFileException e) {
                    Stdout.printlnNotice(":q");
                    // ctrl+d
                    break;
                } catch (Exception e) {
                    Stdout.printlnError(e);
                    sqlBuilder.clear();
                }
            }
        } catch (Exception e) {
            Stdout.printlnError(e);
        }
        return 0;
    }

    // actions
    private void toggleViewAction(String view) {
        try {
            Context.viewMode.set(View.valueOf(view));
            Stdout.printlnNotice("use " + view + " view!");
        } catch (IllegalArgumentException e) {
            Stdout.printlnWarning("Unsupported view type.");
        }
    }

    private void pasteAction() throws Exception {
        String temp = "~sqlc_paste_temp_" + System.currentTimeMillis();
        Path path = SQLC_USER_PATH.resolve(temp);
        Context.tempFiles.add(path);
        try {
            commandRegistry.invoke(session, "nano", "-$", path);
            if (Files.exists(path)) {
                String sqlContent = String.join("\n", Files.readAllLines(path, StandardCharsets.UTF_8)).trim();
                if (!sqlContent.isEmpty()) {
                    exec.execute(sqlContent);
                }
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private void importAction(String[] args) throws Exception {
        if (args.length == 0) {
            Stdout.printlnWarning("command invalid, :import file [sheetIndex [headerIndex]] | [headerIndex]");
            return;
        }
        String file = args[0];
        int sheetIndex = args.length == 2 ? Integer.parseInt(args[1]) : 0;
        int headerIndex = args.length == 3 ? Integer.parseInt(args[2]) : 0;

        // change the index for other file type use the first index for the header row index
        if (!StringUtils.endsWithsIgnoreCase(file, "xls", "xlsx")) {
            headerIndex = sheetIndex;
            sheetIndex = 0;
        }
        BatchInsertHelper.readFile4batch(baki, file, sheetIndex, headerIndex);
    }

    private void executeAction(String target) throws IOException {
        if (target.isEmpty()) {
            Stdout.printlnWarning("file or xql name is required!");
            return;
        }
        if (target.startsWith("&")) {
            xqlExec.execute(target.substring(1));
        } else {
            exec.execute(target);
        }
    }

    private void loadXqlFileAction(String xql) {
        if (!xql.endsWith(".xql")) {
            Stdout.printlnWarning("xql file is required!");
            return;
        }
        String alias = FileResource.getFileName(xql, false);
        String path = "file:" + xql;
        Context.xqlFileManager.add(alias, path);
        Context.xqlFileManager.init();
        Context.xqlFileManager.foreach((a, r) -> {
            Stdout.printlnNotice("----------" + a + "----------");
            r.getEntry().forEach((name, sql) -> {
                String info = XQLFileManager.encodeSqlReference(a, name) + (sql.getDescription().isEmpty() ? "" : " -> " + sql.getDescription());
                Stdout.printlnNotice(info);
            });
        });
        Stdout.printlnPrimary(Objects.requireNonNull(Context.xqlFileManager.getResource(alias)).getEntry().size() + " SQL objects loaded!");
        if (baki.getXqlFileManager() == null) {
            baki.setXqlFileManager(Context.xqlFileManager);
        }
        ExecCompleter execCompleter = (ExecCompleter) Commands.exec.getCompleter().getCompleters().get(1);
        execCompleter.setXqlNames(Context.xqlFileManager.names());
        Stdout.printlnPrimary("Type ':exec &sql_name' to execute!");
    }

    private int transactionToggleAction(String action) {
        switch (action) {
            case "begin":
                if (Context.txActive.get()) {
                    Stdout.printlnWarning("transaction is active now!");
                } else {
                    Tx.begin();
                    Context.txActive.set(true);
                }
                return 0;
            case "commit":
                if (!Context.txActive.get()) {
                    Stdout.printlnWarning("transaction is not active now!");
                } else {
                    Tx.commit();
                    Context.txActive.set(false);
                }
                return 0;
            case "rollback":
                if (!Context.txActive.get()) {
                    Stdout.printlnWarning("transaction is not active now!");
                } else {
                    Tx.rollback();
                    Context.txActive.set(false);
                }
                return 0;
            default:
                return -1;
        }
    }
}
