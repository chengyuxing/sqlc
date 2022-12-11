package com.github.chengyuxing.sql.terminal;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.cli.Arguments;
import com.github.chengyuxing.sql.terminal.cli.Command;
import com.github.chengyuxing.sql.terminal.cli.SimpleReadLine;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;
import com.github.chengyuxing.sql.terminal.cli.completer.CompleterBuilder;
import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.cli.component.SqlHistory;
import com.github.chengyuxing.sql.terminal.core.*;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import com.github.chengyuxing.sql.terminal.vars.Data;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import com.github.chengyuxing.sql.transaction.Tx;
import org.jline.builtins.Completers;
import org.jline.builtins.ConfigurationPath;
import org.jline.console.CommandRegistry;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.JlineCommandRegistry;
import org.jline.keymap.KeyMap;
import org.jline.reader.*;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.AutosuggestionWidgets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.github.chengyuxing.sql.terminal.vars.Constants.*;

/**
 * sqlc 终端执行器
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger("SQLC");

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                System.out.println("-h to get some help.");
                System.exit(0);
            }
            Arguments argMap = new Arguments(args);
            if (argMap.containsKey("-u")) {
                DataSourceLoader.loadDrivers("drivers");
                DataSourceLoader dsLoader = DataSourceLoader.of(argMap.get("-u"));
                try (Terminal terminal = TerminalBuilder.builder()
                        .name("sqlc login")
                        .encoding(StandardCharsets.UTF_8)
                        .system(true)
                        .build()) {
                    LineReader lineReader = LineReaderBuilder.builder().terminal(terminal).build();
                    if (!argMap.containsKey("-n")) {
                        try {
                            dsLoader.setUsername(lineReader.readLine("username: "));
                        } catch (UserInterruptException | EndOfFileException e) {
                            System.out.println("cancel login.");
                            return;
                        }
                    } else {
                        dsLoader.setUsername(argMap.get("-n"));
                    }

                    if (!argMap.containsKey("-p")) {
                        for (int i = 5; i >= 0; i--) {
                            try {
                                if (i == 0) {
                                    System.out.println("login denied.");
                                    return;
                                }
                                dsLoader.setPassword(lineReader.readLine("password: ", '*'));
                                dsLoader.init();
                                break;
                            } catch (UserInterruptException | EndOfFileException e) {
                                System.out.println("cancel login.");
                                return;
                            } catch (Exception e) {
                                PrintHelper.printlnDanger(e.getCause().getMessage() + ", please try again.");
                            }
                        }
                    } else {
                        dsLoader.setPassword(argMap.get("-p"));
                        dsLoader.init();
                    }
                }

                log.info("Welcome to sqlc {} ({}, {})", Version.RELEASE, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
                log.info("Go to " + Command.url + " get more information about this.");
                if (argMap.containsKey("-d")) {
                    StatusManager.sqlDelimiter.set(argMap.get("-d"));
                }

                if (argMap.containsKey("-f")) {
                    String format = argMap.get("-f");
                    StatusManager.viewMode.set(format.equals("csv") ?
                            View.CSV : format.equals("json") ?
                            View.JSON : format.equals("excel") ?
                            View.EXCEL : View.TSV);
                }

                // 如果有-e参数，就执行命令模式
                if (argMap.containsKey("-e")) {
                    startCommandMode(dsLoader, argMap.get("-e"), argMap);
                    return;
                }
                // 进入交互模式
                startInteractiveMode(dsLoader);
            } else {
                Command.get(args[0]);
            }
        } catch (Exception e) {
            PrintHelper.printlnError(e);
        }
    }

    public static void startCommandMode(DataSourceLoader dataSourceLoader, String execute, Arguments args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            dataSourceLoader.release();
            System.out.println("Bye bye :(");
        }));
        SingleBaki baki = dataSourceLoader.getBaki();
        baki.metaData();
        String sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(execute);
        boolean usingTx = args.containsKey("--with-tx");
        // just execute batch insert
        if (sql.startsWith("@")) {
            int sheetIdx = Integer.parseInt(args.getIfBlank("-sheet", "0"));
            int headerIdx = Integer.parseInt(args.getIfBlank("-header", "0"));
            String filePath = sql.substring(1).trim();
            if (usingTx) {
                Tx.using(() -> {
                    try {
                        BatchInsertHelper.readFile4batch(baki, filePath, sheetIdx, headerIdx);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                BatchInsertHelper.readFile4batch(baki, filePath, sheetIdx, headerIdx);
            }
            return;
        }

        // for support prepared sql
        // e.g: -e"select ... where id = :id;..."
        // e.g: -e"insert ...(file) values (:path)"
        SimpleReadLine.readline(lb -> {
            StatusManager.promptReference.set(new Prompt(""));
            LineReader reader = lb.completer(new Completers.FilesCompleter(CURRENT_DIR)).build();
            ExecExecutor executor = new ExecExecutor(baki, sql);
            try {
                if (usingTx) {
                    Tx.using(() -> {
                        try {
                            executor.exec(reader);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                } else {
                    executor.exec(reader);
                }
            } catch (UserInterruptException | EndOfFileException e) {
                System.out.println("canceled.");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void startInteractiveMode(DataSourceLoader dataSourceLoader) throws IOException, SQLException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (StatusManager.txActive.get()) {
                Tx.rollback();
            }
            dataSourceLoader.release();
            Data.tempFiles.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception e) {
                    PrintHelper.printlnError(e);
                }
            });
            System.out.println("Bye bye :(");
        }));

        try (Terminal terminal = TerminalBuilder.builder()
                .name("sqlc terminal")
                .encoding(StandardCharsets.UTF_8)
                .system(true)
                .build()) {

            CommandRegistry.CommandSession session = new CommandRegistry.CommandSession(terminal);

            final List<String> sqlBuilder = new ArrayList<>();

            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .parser(cliParser)
                    .completer(new AggregateCompleter(CompleterBuilder.builtins.getCompleters()))
                    .variable(LineReader.HISTORY_FILE, Paths.get(Constants.USER_HOME.toString(), ".sqlc_history"))
                    .history(new SqlHistory(sqlBuilder))
                    .build();

            AutosuggestionWidgets suggest = new AutosuggestionWidgets(lineReader);
            // press ctrl+o jump to next word.
            suggest.getKeyMap().bind(new Reference(LineReader.FORWARD_WORD), KeyMap.ctrl('o'));
            suggest.enable();

            JlineCommandRegistry commandRegistry = new Builtins(CURRENT_DIR, new ConfigurationPath(APP_DIR, USER_HOME), s -> lineReader.getBuiltinWidgets().get(s));

            SingleBaki baki = dataSourceLoader.getBaki();
            DatabaseMetaData metaData = baki.metaData();
            String dbName = metaData.getDatabaseProductName().toLowerCase();

            DataBaseResource dataBaseResource = new DataBaseResource(dbName, dataSourceLoader);

            Data.keywordsCompleter.addVarsNames(dataBaseResource.getSqlKeyWordsWithDefault());
            Data.keywordsCompleter.addVarsNames(dataBaseResource.getUserTableNames());

            List<String> procedures = dataBaseResource.getUserProcedures();
            List<String> views = dataBaseResource.getUserViews();

            Data.keywordsCompleter.addVarsNames(procedures.stream().map(s -> s.substring(s.indexOf(":") + 1)).collect(Collectors.toList()));
            Data.keywordsCompleter.addVarsNames(views.stream().map(s -> s.substring(s.indexOf(":") + 1)).collect(Collectors.toList()));
            // :edit command completer words
            Data.editCmdCompleter.addVarsNames(procedures);
            Data.editCmdCompleter.addVarsNames(views);
            Data.editCmdCompleter.addVarsNames(dataBaseResource.getUserTriggers());

            Prompt prompt = new Prompt(metaData.getURL());
            StatusManager.promptReference.set(prompt);

            log.info("Type in command or sql script to execute query, ddl, dml..., or try :help.");

            exit:
            while (true) {
                try {
                    String line = lineReader.readLine(prompt.getValue()).trim();
                    if (!line.equals("")) {
                        if (line.startsWith(":")) {
                            sqlBuilder.clear();
                            label:
                            switch (line) {
                                case ":q":
                                    if (StatusManager.txActive.get()) {
                                        PrintHelper.printlnWarning("Warning: Transaction is active now, please :tx commit or :tx rollback before quit, Control c, server shutdown or kill command will be rollback transaction!");
                                        break;
                                    } else {
                                        break exit;
                                    }
                                case ":help":
                                    Command.get("--cmd");
                                    break;
                                case ":status":
                                    PrintHelper.printlnInfo("View Mode: " + StatusManager.viewMode.get());
                                    PrintHelper.printlnInfo("Transaction: " + (StatusManager.txActive.get() ? "enabled" : "disabled"));
                                    PrintHelper.printlnInfo("Multi-Sql Delimiter: '" + (StatusManager.sqlDelimiter) + "'");
                                    break;
                                case ":tx":
                                    if (StatusManager.txActive.get()) {
                                        PrintHelper.printlnWarning("transaction is active now!");
                                    } else {
                                        Tx.begin();
                                        StatusManager.setTxActive(true);
                                    }
                                    break;
                                case ":paste":
                                    String temp = "paste_panel_" + System.currentTimeMillis();
                                    Path path = Paths.get(CURRENT_DIR.toString(), temp);
                                    Data.tempFiles.add(path);
                                    try {
                                        commandRegistry.invoke(session, "nano", "-$", temp);
                                        if (Files.exists(path)) {
                                            String sqlContent = String.join("\n", Files.readAllLines(path)).trim();
                                            if (!sqlContent.equals("")) {
                                                ExecExecutor executor = new ExecExecutor(baki, sqlContent);
                                                executor.exec(lineReader);
                                                prompt.newLine();
                                            }
                                        }
                                    } finally {
                                        Files.deleteIfExists(path);
                                    }
                                    break;
                                default:
                                    Matcher bmeh = EXEC_BATCH_EXCEL_REGEX.matcher(line);
                                    if (bmeh.find()) {
                                        String file = bmeh.group("input");
                                        int sheetIdx = Integer.parseInt(bmeh.group("sheetIdx"));
                                        int headerIdx = Integer.parseInt(bmeh.group("headerIdx"));
                                        BatchInsertHelper.readFile4batch(baki, file, sheetIdx, headerIdx);
                                        break;
                                    }
                                    Matcher bmh = EXEC_BATCH_WITH_HEADER_REGEX.matcher(line);
                                    if (bmh.find()) {
                                        String file = bmh.group("input");
                                        int headerIdx = Integer.parseInt(bmh.group("headerIdx"));
                                        BatchInsertHelper.readFile4batch(baki, file, headerIdx);
                                        break;
                                    }

                                    Matcher bm = EXEC_BATCH_REGEX.matcher(line);
                                    if (bm.find()) {
                                        String file = bm.group("input");
                                        BatchInsertHelper.readFile4batch(baki, file);
                                        break;
                                    }

                                    Matcher xm = EXEC_XQL_REGEX.matcher(line);
                                    if (xm.find()) {
                                        String sqlName = xm.group("name");
                                        String sql = Data.xqlFileManager.get(sqlName);
                                        PrintHelper.printlnHighlightSql(sql);
                                        Pair<String, Map<String, Object>> pair = SqlUtil.prepareSqlWithArgs(sql, lineReader);
                                        PrintHelper.printOneSqlResultByType(baki, "&" + sqlName, pair.getItem1(), pair.getItem2());
                                        // prepared sql will change the prompt to arg name, reset to new-line prompt after executed.
                                        prompt.newLine();
                                        break;
                                    }

                                    if (line.startsWith(":exec")) {
                                        ExecExecutor executor = new ExecExecutor(baki, line.substring(5).trim());
                                        executor.exec(lineReader);
                                        prompt.newLine();
                                        break;
                                    }

                                    Matcher lm = LOAD_XQL_FILE_REGEX.matcher(line);
                                    if (lm.find()) {
                                        String input = lm.group("input");
                                        String alias = lm.group("alias");
                                        Path filePath = Paths.get(input).toRealPath();
                                        if (Data.xqlFileManager.getFiles().containsValue("file:" + filePath)) {
                                            PrintHelper.printlnWarning("unmodified " + filePath + " already loaded, cannot load again.");
                                            break;
                                        }
                                        Data.xqlFileManager.add(alias, "file:" + filePath);
                                        Data.xqlFileManager.setDelimiter(StatusManager.sqlDelimiter.get());
                                        Data.xqlFileManager.init();
                                        Data.xqlFileManager.foreach((k, v) -> System.out.println("+[" + TerminalColor.colorful(k, Color.DARK_CYAN) + "]:" + TerminalColor.highlightSql(v)));
                                        if (baki.getXqlFileManager() == null) {
                                            baki.setXqlFileManager(Data.xqlFileManager);
                                        }
                                        Data.xqlNameCompleter.setVarsNames(Data.xqlFileManager.names());
                                        PrintHelper.printlnInfo("XQLFileManager enabled, input command: ':exec& your_sql_name' to execute!");
                                        break;
                                    }

                                    if (line.startsWith(":edit")) {
                                        String name = line.substring(5).trim();
                                        switch (dbName) {
                                            case "postgresql":
                                                int colonIdx = name.indexOf(":");
                                                if (colonIdx == -1) {
                                                    throw new IllegalArgumentException("invalid object name formatter, e.g: tg(trigger):test.big.my_trigger, view:test.my_view, proc:public.hello(text)");
                                                }
                                                String type = name.substring(0, colonIdx);
                                                String tgt = name.substring(colonIdx + 1);
                                                String def = dataBaseResource.getDefinition(type, tgt);
                                                if (def.trim().equals("")) {
                                                    throw new RuntimeException(name + " definition is empty.");
                                                }
                                                String procedureTemp = name + "_" + System.currentTimeMillis();
                                                Path procedurePath = Paths.get(CURRENT_DIR.toString(), procedureTemp);
                                                Data.tempFiles.add(procedurePath);
                                                try {
                                                    Files.write(procedurePath, def.getBytes(StandardCharsets.UTF_8));
                                                    commandRegistry.invoke(session, "nano", "-$", procedureTemp);
                                                    String newDef = String.join("\n", Files.readAllLines(procedurePath));
                                                    if (!def.trim().equals(newDef.trim())) {
                                                        baki.execute(newDef);
                                                        PrintHelper.printlnNotice(name + " change submitted!");
                                                    }
                                                } finally {
                                                    Files.deleteIfExists(procedurePath);
                                                }
                                                break label;
                                            default:
                                                PrintHelper.printlnDarkWarning(":edit not support " + dbName + " currently.");
                                                break label;
                                        }
                                    }

                                    Matcher tx = TX_REGEX.matcher(line);
                                    if (tx.find()) {
                                        String op = tx.group("op");
                                        switch (op) {
                                            case "begin":
                                                if (StatusManager.txActive.get()) {
                                                    PrintHelper.printlnWarning("transaction is active now!");
                                                } else {
                                                    Tx.begin();
                                                    StatusManager.setTxActive(true);
                                                }
                                                break label;
                                            case "commit":
                                                if (!StatusManager.txActive.get()) {
                                                    PrintHelper.printlnWarning("transaction is not active now!");
                                                } else {
                                                    Tx.commit();
                                                    StatusManager.setTxActive(false);
                                                }
                                                break label;
                                            case "rollback":
                                                if (!StatusManager.txActive.get()) {
                                                    PrintHelper.printlnWarning("transaction is not active now!");
                                                } else {
                                                    Tx.rollback();
                                                    StatusManager.setTxActive(false);
                                                }
                                                break label;
                                        }
                                    }

                                    Matcher vm = VIEW_REGEX.matcher(line);
                                    if (vm.find()) {
                                        String view = vm.group("view");
                                        View viewMode = view.equals("tsv") ? View.TSV : view.equals("json") ? View.JSON : view.equals("excel") ? View.EXCEL : View.CSV;
                                        StatusManager.viewMode.set(viewMode);
                                        PrintHelper.printlnNotice("use " + view + " view!");
                                        break;
                                    }

                                    Matcher dm = DELIMITER_REGEX.matcher(line);
                                    if (dm.find()) {
                                        String d = dm.group("d");
                                        StatusManager.sqlDelimiter.set(d);
                                        PrintHelper.printlnNotice("set multi sql block delimited by '" + d + "', auto line break(\\n) delimiter if set blank.");
                                        break;
                                    }

                                    PrintHelper.printlnWarning("command invalid, command :help to get some help!");
                                    break;
                            }
                        } else {
                            if (line.endsWith(";")) {
                                sqlBuilder.add(line);
                                String sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(String.join(" ", sqlBuilder));
                                // execute sql
                                // ---------
                                if (!sql.equals("")) {
                                    ExecExecutor executor = new ExecExecutor(baki, sql);
                                    executor.exec(lineReader);
                                    sqlBuilder.clear();
                                    prompt.newLine();
                                }
                                // ---------
                            } else {
                                sqlBuilder.add(line);
                                prompt.append();
                            }
                        }
                    }
                } catch (UserInterruptException e) {
                    // ctrl+c
                    System.out.println(":q");
                    break;
                } catch (EndOfFileException e) {
                    System.out.println(":q");
                    // ctrl+d
                    break;
                } catch (Exception e) {
                    PrintHelper.printlnError(e);
                    sqlBuilder.clear();
                    prompt.newLine();
                }
            }
        }
    }
}
