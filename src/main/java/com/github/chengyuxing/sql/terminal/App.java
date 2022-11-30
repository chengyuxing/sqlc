package com.github.chengyuxing.sql.terminal;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.cli.Arguments;
import com.github.chengyuxing.sql.terminal.cli.Command;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;
import com.github.chengyuxing.sql.terminal.cli.completer.CompleterBuilder;
import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.cli.component.SqlHistory;
import com.github.chengyuxing.sql.terminal.core.*;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.Cache;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import com.github.chengyuxing.sql.terminal.vars.Data;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import com.github.chengyuxing.sql.transaction.Tx;
import org.apache.log4j.Level;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.vars.Constants.*;

/**
 * sqlc 终端执行器
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger("SQLC");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("-h to get some help.");
            System.exit(0);
        }
        Map<String, String> argMap = new Arguments(args).toMap();
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
                    dsLoader.setUsername(lineReader.readLine("username: "));
                } else {
                    dsLoader.setUsername(argMap.get("-n"));
                }
                if (!argMap.containsKey("-p")) {
                    org.apache.log4j.Logger.getLogger("com.zaxxer.hikari").setLevel(Level.FATAL);
                    for (int i = 5; i >= 0; i--) {
                        try {
                            if (i == 0) {
                                System.out.println("login rejected.");
                                return;
                            }
                            dsLoader.setPassword(lineReader.readLine("password: ", '*'));
                            dsLoader.init();
                            org.apache.log4j.Logger.getLogger("com.zaxxer.hikari").setLevel(Level.INFO);
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
            SingleBaki baki = dsLoader.getBaki();
            if (baki != null) {
                baki.metaData();
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
                    startCommandMode(baki, dsLoader, argMap.get("-e"));
                    return;
                }
                // 进入交互模式
                log.info("Type in sql script to execute query, ddl, dml..., Or try :help.");
                startInteractiveMode(baki, dsLoader);
            }
        } else {
            Command.get(args[0]);
        }
    }

    public static void startCommandMode(SingleBaki baki, DataSourceLoader dataSourceLoader, String execute) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            dataSourceLoader.release();
            System.out.println("Bye bye :(");
        }));
        String sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(execute);
        // 以@开头那么批量执行直接执行完退出
        if (sql.startsWith("@")) {
            String filePath = sql.substring(1).trim();
            BatchInsertHelper.readFile4batch(baki, filePath);
            return;
        }
        Executor executor = new Executor(baki, sql);
        executor.exec();
    }

    public static void startInteractiveMode(SingleBaki baki, DataSourceLoader dataSourceLoader) throws IOException, SQLException {
        //如果使用杀进程或ctrl+c结束，或者关机，退出程序的情况下，做一些收尾工作
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (StatusManager.txActive.get()) {
                Tx.rollback();
            }
            dataSourceLoader.release();
            System.out.println("Bye bye :(");
        }));

        try (Terminal terminal = TerminalBuilder.builder()
                .name("sqlc terminal")
                .encoding(StandardCharsets.UTF_8)
                .system(true)
                .build()) {

            final List<String> sqlBuilder = new ArrayList<>();

            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .parser(cliParser)
                    .completer(new AggregateCompleter(CompleterBuilder.builtins.getCompleters()))
                    // 历史记录文件配置
                    .variable(LineReader.HISTORY_FILE, Paths.get(Constants.USER_HOME.toString(), "sqlc.new.history"))
                    .history(new SqlHistory(sqlBuilder))
                    .build();

            // 启用自动建议
            AutosuggestionWidgets suggest = new AutosuggestionWidgets(lineReader);
            // 使用方向右键自动跳转到建议的行尾
            // 使用ctrl+o自动跳转到下一个单词
            suggest.getKeyMap().bind(new Reference(LineReader.FORWARD_WORD), KeyMap.ctrl('o'));
            suggest.enable();

            Prompt prompt = new Prompt(baki.metaData().getURL());
            StatusManager.promptReference.set(prompt);

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
                                    PrintHelper.printlnInfo("Cache: " + (StatusManager.enableCache.get() ? "enabled" : "disabled"));
                                    PrintHelper.printlnInfo("Multi Sql Delimiter: '" + (StatusManager.sqlDelimiter) + "'");
                                    break;
                                case ":c":
                                    StatusManager.enableCache.set(true);
                                    PrintHelper.printlnNotice("cache enabled!");
                                    break;
                                case ":C":
                                    StatusManager.enableCache.set(false);
                                    PrintHelper.printlnNotice("cache disabled!");
                                    break;
                                case ":C!":
                                    StatusManager.enableCache.set(false);
                                    Data.queryCaches.clear();
                                    Data.idx.set(0);
                                    Data.cacheNameCompleter.setVarsNames(Collections.emptyList());
                                    PrintHelper.printlnNotice("cache disabled and cleared!");
                                    break;
                                case ":ls":
                                    Data.queryCaches.forEach((k, v) -> {
                                        String sql = v.getSql();
                                        if (sql.length() > terminal.getWidth()) {
                                            sql = sql.substring(0, terminal.getWidth() - k.length() - 10) + "...";
                                        }
                                        PrintHelper.printlnNotice(TerminalColor.colorful(k, Color.DARK_CYAN) + " [" + v.size() + "]: " + TerminalColor.highlightSql(sql));
                                    });
                                    break;
                                case ":tx":
                                    if (StatusManager.txActive.get()) {
                                        PrintHelper.printlnWarning("transaction is active now!");
                                    } else {
                                        Tx.begin();
                                        StatusManager.setTxActive(true);
                                    }
                                    break;
                                default:
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
                                        Map<String, Object> argx = SqlUtil.prepareSqlArgIf(sql, lineReader);
                                        PrintHelper.printlnHighlightSql(sql);
                                        PrintHelper.printOneSqlResultByType(baki, "&" + sqlName, sql, argx);
                                        break;
                                    }

                                    if (line.startsWith(":exec")) {
                                        Executor executor = new Executor(baki, line.substring(5).trim());
                                        executor.exec(lineReader);
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
                                        Data.xqlFileManager.foreach((k, v) -> {
                                            System.out.println("+[" + TerminalColor.colorful(k, Color.DARK_CYAN) + "]:" + TerminalColor.highlightSql(v));
                                        });
                                        if (baki.getXqlFileManager() == null) {
                                            baki.setXqlFileManager(Data.xqlFileManager);
                                        }
                                        Data.xqlNameCompleter.setVarsNames(Data.xqlFileManager.names());
                                        PrintHelper.printlnInfo("XQLFileManager enabled, input command: ':exec& your_sql_name' to execute!");
                                        break;
                                    }

                                    if (line.startsWith(":get")) {
                                        if (line.contains(REDIRECT_SYMBOL)) {
                                            Pair<String, String> pair = SqlUtil.getSqlAndRedirect(line.substring(4).trim());
                                            Cache cache = Data.queryCaches.get(pair.getItem1());
                                            if (cache == null) {
                                                PrintHelper.printlnWarning("no cache named " + pair.getItem1());
                                            } else {
                                                PrintHelper.printlnNotice("redirect cache data to file...");
                                                FileHelper.writeFile(cache.getData().stream(), pair.getItem2());
                                            }
                                        } else {
                                            String name = line.substring(4).trim();
                                            Cache cache = Data.queryCaches.get(name);
                                            if (cache == null) {
                                                PrintHelper.printlnWarning("no cache named " + name);
                                            } else {
                                                PrintHelper.printQueryResult(cache.getData().stream());
                                            }
                                        }
                                        break;
                                    }

                                    Matcher rm = REMOVE_CACHE_REGEX.matcher(line);
                                    if (rm.find()) {
                                        String name = rm.group("name");
                                        if (!Data.queryCaches.containsKey(name)) {
                                            PrintHelper.printlnDanger("no cached named " + name);
                                        } else {
                                            Cache cache = Data.queryCaches.get(name);
                                            cache.getData().clear();
                                            cache.getArgs().clear();
                                            Data.queryCaches.remove(name);
                                            PrintHelper.printlnNotice(name + " removed!");
                                        }
                                        break;
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
                                // 执行sql
                                // ---------
                                if (!sql.equals("")) {
                                    SqlType type = SqlUtil.getType(sql);
                                    switch (type) {
                                        case QUERY:
                                            if (sql.contains(REDIRECT_SYMBOL)) {
                                                Pair<String, String> pair = SqlUtil.getSqlAndRedirect(sql);
                                                Map<String, Object> argx = SqlUtil.prepareSqlArgIf(pair.getItem1(), lineReader);
                                                try (Stream<DataRow> rowStream = WaitingPrinter.waiting("preparing...", () -> baki.query(pair.getItem1()).args(argx).stream())) {
                                                    PrintHelper.printlnNotice("redirect query to file...");
                                                    FileHelper.writeFile(rowStream, pair.getItem2());
                                                } catch (Exception e) {
                                                    PrintHelper.printlnError(e);
                                                }
                                            } else {
                                                boolean hasName = false;
                                                String prefix = "";
                                                String name = "";
                                                Matcher ncm = NAME_QUERY_CACHE_REGEX.matcher(sql);
                                                if (ncm.find()) {
                                                    hasName = true;
                                                    sql = sql.substring(ncm.end(0));
                                                    //prefix = ncm.group("prefix");
                                                    name = ncm.group("name");
                                                }
                                                final String query = sql;
                                                Map<String, Object> argx = SqlUtil.prepareSqlArgIf(query, lineReader);
                                                try (Stream<DataRow> rowStream = WaitingPrinter.waiting(() -> baki.query(query).args(argx).stream())) {
                                                    // 查询缓存结果
                                                    if (StatusManager.enableCache.get()) {
                                                        if (!hasName) {
                                                            name = "res" + Data.idx.getAndIncrement();
                                                        }
                                                        List<DataRow> queryResult = new ArrayList<>();
                                                        PrintHelper.printQueryResult(rowStream, queryResult::add);
                                                        Cache cache = new Cache(query, queryResult);
                                                        cache.setArgs(argx);
                                                        Data.queryCaches.put(name, cache);
                                                        Data.cacheNameCompleter.setVarsNames(Data.queryCaches.keySet());
                                                        PrintHelper.printlnNotice(name + ": added to cache!");
                                                    } else {
                                                        PrintHelper.printQueryResult(rowStream);
                                                    }
                                                } catch (Exception e) {
                                                    PrintHelper.printlnError(e);
                                                }
                                            }
                                            break;
                                        case FUNCTION:
                                            PrintHelper.printlnWarning("function not support now!");
                                            break;
                                        case OTHER:
                                            try {
                                                if (sql.contains(REDIRECT_SYMBOL)) {
                                                    PrintHelper.printlnWarning("only query support redirect operation!");
                                                } else {
                                                    PrintHelper.printQueryResult(PrintHelper.executedRow2Stream(baki, sql, SqlUtil.prepareSqlArgIf(sql, lineReader)));
                                                }
                                            } catch (Exception e) {
                                                PrintHelper.printlnError(e);
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                // ---------
                                sqlBuilder.clear();
                                prompt.newLine();
                            } else {
                                sqlBuilder.add(line);
                                prompt.append();
                            }
                        }
                    }
                } catch (UserInterruptException e) {
                    // ctrl+c
                } catch (EndOfFileException e) {
                    System.out.println(":q");
                    // ctrl+d
                    break;
                } catch (Exception e) {
                    PrintHelper.printlnError(e);
                }
            }
        }
    }
}
