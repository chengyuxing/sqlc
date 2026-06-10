package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.completer.KeywordsCompleter;
import com.github.chengyuxing.sql.terminal.core.executor.AbstractExecutor;
import com.github.chengyuxing.sql.terminal.core.executor.SQLExecutor;
import com.github.chengyuxing.sql.terminal.core.executor.XQLExecutor;
import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.cli.component.SqlHistory;
import com.github.chengyuxing.sql.terminal.cli.interactive.Commands;
import com.github.chengyuxing.sql.terminal.core.*;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.SqlUtils;
import com.github.chengyuxing.sql.terminal.common.Stdout;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static com.github.chengyuxing.sql.terminal.common.Constants.*;
import static com.github.chengyuxing.sql.terminal.common.Constants.APP_DIR;
import static com.github.chengyuxing.sql.terminal.common.Constants.USER_HOME;

public class InteractiveMode extends AbstractMode implements Callable<Integer> {
    private static final Logger log = LoggerFactory.getLogger(InteractiveMode.class);

    private final CommandRegistry.CommandSession session;
    private final List<String> sqlBuilder;
    private final LineReader mainReader;
    private final BakiDao baki;
    private final JlineCommandRegistry commandRegistry;
    private final Prompt prompt;
    private final AbstractExecutor sqlExecutor;
    private final AbstractExecutor xqlExecutor;
    private final KeywordsCompleter keywordsCompleter = new KeywordsCompleter();

    protected InteractiveMode(App app, BakiLoader bakiLoader, Terminal terminal) {
        super(app, bakiLoader, terminal);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (Context.txActive.get()) {
                Tx.rollback();
                Stdout.printlnWarning("Transaction rollback");
            }
            bakiLoader.close();
            Stdout.printlnWarning("Bye bye :(");
        }));

        this.session = new CommandRegistry.CommandSession(terminal);
        this.sqlBuilder = new ArrayList<>();
        this.mainReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new AggregateCompleter(Commands.getCompleters(this.keywordsCompleter)))
                .variable(LineReader.HISTORY_FILE, SQLC_HISTORY_PATH.resolve("history_cmd_" + getLoginId()))
                .history(new SqlHistory(sqlBuilder))
                .build();
        this.baki = bakiLoader.getUserBaki();
        this.commandRegistry = new Builtins(CURRENT_DIR, new ConfigurationPath(APP_DIR, USER_HOME), s -> mainReader.getBuiltinWidgets().get(s));
        this.prompt = new Prompt(app.jdbcUrl);

        this.sqlExecutor = new SQLExecutor(this.baki) {
            @Override
            public LineReader paramsReader(String sql) {
                return SqlUtils.detectSQLType(sql) == SqlType.PROCEDURE
                        ? getProcParamReader()
                        : getSqlParamReader();
            }
        };

        this.xqlExecutor = new XQLExecutor(this.baki) {
            @Override
            public LineReader paramsReader(String sql) {
                return SqlUtils.detectSQLType(sql) == SqlType.PROCEDURE
                        ? getProcParamReader()
                        : getSqlParamReader();
            }
        };
        init();
    }

    private void init() {
        AutosuggestionWidgets suggest = new AutosuggestionWidgets(mainReader);
        // press ctrl+o jump to next word.
        suggest.getKeyMap().bind(new Reference(LineReader.FORWARD_WORD), KeyMap.ctrl('o'));
        suggest.enable();

        DataBaseResource dataBaseResource = new DataBaseResource(bakiLoader);

        keywordsCompleter.addVarsNames(dataBaseResource.getSqlKeyWordsOrDefault());
        CompletableFuture.supplyAsync(dataBaseResource::getNames)
                .whenCompleteAsync((tables, e) -> {
                    if (e != null) {
                        log.error("Load database objects", e);
                        return;
                    }
                    keywordsCompleter.addVarsNames(tables);
                });

        Context.promptReference.set(prompt);

        Stdout.println("Type in command or sql statement to execute query, ddl, dml.... Or try :help.");
    }

    @Override
    public Integer call() {
        while (true) {
            try {
                String line = mainReader.readLine(Context.getPromptState(app.jdbcUrl)).trim();
                if (line.isEmpty()) {
                    continue;
                }
                // execute command
                if (!Context.appending.get() && line.startsWith(":")) {
                    if (line.equals(Commands.quit.getName())) {
                        if (Context.txActive.get()) {
                            Stdout.printlnWarning("Transaction is active, please `:tx commit|rollback` before quit, [Control c], server shutdown or kill command will be rollback transaction");
                        } else {
                            break;
                        }
                        continue;
                    }
                    if (line.equals(Commands.help.getName())) {
                        Stdout.printf("Typing the statement and execute it with a ';' at the end.%n");
                        Stdout.printf("Interactive Mode Commands:%n");
                        Commands.printHelp();
                        continue;
                    }

                    if (line.equals(Commands.paste.getName())) {
                        pasteAction();
                        continue;
                    }

                    if (line.startsWith(Commands.status.getName())) {
                        String item = line.substring(Commands.status.getName().length()).trim();
                        statusAction(item);
                        continue;
                    }

                    if (line.startsWith(Commands.import_.getName())) {
                        String[] args = line.substring(Commands.import_.getName().length()).trim().split("\\s+");
                        importAction(args);
                        continue;
                    }

                    // :exec [xql name | file | sql]
                    if (line.startsWith(Commands.exec.getName())) {
                        String target = line.substring(Commands.exec.getName().length()).trim();
                        executeAction(target);
                        continue;
                    }

                    // :load my.xql
                    if (line.startsWith(Commands.load.getName())) {
                        String xql = line.substring(Commands.load.getName().length()).trim();
                        loadXqlFileAction(xql);
                        continue;
                    }

                    if (line.startsWith(Commands.transaction.getName())) {
                        String action = line.substring(Commands.transaction.getName().length()).trim();
                        action = action.isEmpty() ? "begin" : action.toLowerCase();
                        int code = transactionToggleAction(action);
                        if (code == 0) {
                            continue;
                        }
                    }

                    if (line.startsWith(Commands.toggleView.getName())) {
                        String view = line.substring(Commands.toggleView.getName().length()).trim();
                        toggleViewAction(view);
                        continue;
                    }

                    if (line.startsWith(Commands.output.getName())) {
                        String path = line.substring(Commands.output.getName().length()).trim();
                        Context.outputPath.set(path);
                        continue;
                    }

                    Stdout.printlnWarning("Command invalid, :help to get tutorial");
                    continue;
                }
                // merge sql to execute
                if (line.endsWith(";")) {
                    line = line.substring(0, line.length() - 1);
                    sqlBuilder.add(line);
                    String sql = String.join("\n", sqlBuilder);
                    // execute sql
                    if (!sql.isEmpty()) {
                        sqlExecutor.execute(sql);
                        sqlBuilder.clear();
                        Context.appending.set(false);
                    }
                } else {
                    sqlBuilder.add(line);
                    Context.appending.set(true);
                }
            } catch (UserInterruptException e) {
                // ctrl+c
                break;
            } catch (EndOfFileException e) {
                // ctrl+d
                break;
            } catch (Exception e) {
                log.error("Execute command or SQL", e);
                Stdout.printlnError(e);
                Context.appending.set(false);
                sqlBuilder.clear();
            }
        }
        return 0;
    }

    private void statusAction(String item) {
        String myItem = item.isEmpty() ? "" : " [" + item + "]";
        Stdout.printlnTitle("Status" + myItem, '-', 80, Style.SILVER);
        if (item.isEmpty()) {
            String f = "%-30s%-30s%n";
            Object[][] messages = new Object[][]{
                    {"View Mode", Context.viewMode.get()},
                    {"Display Rows", Context.printRows.get()},
                    {"Transaction", (Context.txActive.get() ? "enabled" : "disabled")},
                    {"Batch Size", baki.getBatchSize()},
                    {"XQL", "Loaded: " + baki.getXqlFileManager().getFiles().keySet()},
                    {"NamedParamPrefix", "'" + baki.getNamedParamPrefix() + "'"},
                    {"Query Redirect", Context.outputPath},
                    {"Current Dir", CURRENT_DIR},
                    {"Term", TERM}
            };
            for (Object[] message : messages) {
                Stdout.printf(f, Style.SILVER, message[0], message[1]);
            }
            // XQL resource
        } else if (item.startsWith("&")) {
            XQLFileManager xqlFileManager = baki.getXqlFileManager();
            String name = item.substring(1);
            // &
            // print all files
            if (name.isEmpty()) {
                xqlFileManager.getResources().forEach((alias, r) ->
                        Stdout.printf("- %s (%s)  %s%n", Style.SILVER, alias, r.getEntry().size(), r.getDescription()));
            } else {
                // &home[.]
                if (name.endsWith(".")) {
                    name = name.substring(0, name.length() - 1);
                }
                if (xqlFileManager.getResources().containsKey(name)) {
                    XQLFileManager.Resource resource = xqlFileManager.getResource(name);
                    if (resource != null) {
                        resource.getEntry().forEach((n, sql) -> {
                            String info = "- " + n + (sql.getDescription().isEmpty() ? "" : " -> " + sql.getDescription());
                            Stdout.printlnNotice(info);
                        });
                    }
                    // &home.getAllUsers
                } else if (xqlFileManager.contains(name)) {
                    XQLFileManager.Sql sql = xqlFileManager.getSqlObject(name);
                    if (!sql.getDescription().isEmpty()) {
                        Stdout.printlnHighlightSql("/*" + sql.getDescription() + "*/");
                    }
                    Stdout.printlnHighlightSql(sql.getSource());
                }
            }
        }
        Stdout.printlnTitle("", '-', 80, Style.SILVER);
    }

    // actions
    private void toggleViewAction(String view) {
        try {
            Context.viewMode.set(View.valueOf(view));
            Stdout.printlnNotice("Use " + view + " view");
        } catch (IllegalArgumentException e) {
            Stdout.printlnWarning("Unsupported '" + view + "' view type");
        }
    }

    private void pasteAction() throws Exception {
        Path path = SQLC_TEMP_PATH.resolve("paste_" + System.currentTimeMillis() + ".tmp");
        try {
            commandRegistry.invoke(session, "nano", "-$", path);
            if (Files.exists(path)) {
                String sqlContent = String.join("\n", Files.readAllLines(path, StandardCharsets.UTF_8)).trim();
                if (!sqlContent.isEmpty()) {
                    sqlExecutor.execute(sqlContent);
                }
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private void importAction(String[] args) throws Exception {
        if (args.length == 0) {
            Stdout.printlnWarning("Command invalid, :import file [sheetIndex [headerIndex]] | [headerIndex]");
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
            Stdout.printlnWarning("File or XQL name is required");
            return;
        }
        if (target.startsWith("&")) {
            xqlExecutor.execute(target);
        } else {
            sqlExecutor.execute(target);
        }
    }

    private void loadXqlFileAction(String xql) throws IOException {
        XQLFileManager xqlFileManager = baki.getXqlFileManager();

        Path path = PathUtils.resolve(xql);
        if (Files.isDirectory(path)) {
            String[] files = FileHelper.getFiles(path, ".xql");
            FileHelper.loadXqlFiles(xqlFileManager, files);
            return;
        }
        FileHelper.loadXqlFiles(xqlFileManager, xql);
    }

    private int transactionToggleAction(String action) {
        switch (action) {
            case "begin":
                if (Context.txActive.get()) {
                    Stdout.printlnWarning("Transaction is active");
                } else {
                    Tx.begin();
                    Context.txActive.set(true);
                }
                return 0;
            case "commit":
                if (!Context.txActive.get()) {
                    Stdout.printlnWarning("Transaction is not active");
                } else {
                    Tx.commit();
                    Context.txActive.set(false);
                }
                return 0;
            case "rollback":
                if (!Context.txActive.get()) {
                    Stdout.printlnWarning("Transaction is not active");
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
