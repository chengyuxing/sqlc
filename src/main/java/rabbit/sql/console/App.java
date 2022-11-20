package rabbit.sql.console;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.transaction.Tx;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.sql.console.core.DataSourceLoader;
import rabbit.sql.console.core.ScannerHelper;
import rabbit.sql.console.core.SingleBaki;
import rabbit.sql.console.core.StatusManager;
import rabbit.sql.console.progress.impl.NumberProgressPrinter;
import rabbit.sql.console.types.SqlType;
import rabbit.sql.console.types.View;
import rabbit.sql.console.util.SqlUtil;
import rabbit.sql.console.util.TimeUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static rabbit.sql.console.core.FileHelper.writeFile;
import static rabbit.sql.console.core.PrintHelper.*;
import static rabbit.sql.console.core.StatusManager.*;
import static rabbit.sql.console.util.SqlUtil.multiSqlList;
import static rabbit.sql.console.util.SqlUtil.prepareSqlArgIf;

public class App {
    private static final Logger log = LoggerFactory.getLogger("SQLC");
    // 查询输出重定向正则
    private static final Pattern QUERY_R_FILE = Pattern.compile("(?<sql>[\\s\\S]+\\S)\\s*>\\s*(?<path>\\.*" + File.separator + "\\S+)$");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("--help to get some help.");
            System.exit(0);
        }
        Map<String, String> argMap = DataSourceLoader.resolverArgs(args);
        if (argMap.containsKey("-u")) {
            DataSourceLoader.loadDrivers("drivers");
            DataSourceLoader dsLoader = DataSourceLoader.of(argMap.get("-u"),
                    Optional.ofNullable(argMap.get("-n")).orElse(""),
                    Optional.ofNullable(argMap.get("-p")).orElse(""));

            log.info("Welcome to sqlc {} ({}, {})", Version.RELEASE, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
            log.info("Go to \33[4mhttps://github.com/chengyuxing/sqlc\33[0m get more information about this.");
            SingleBaki baki = dsLoader.getBaki(argMap.get("-n"));
            if (baki != null) {
                if (argMap.containsKey("-d")) {
                    sqlDelimiter.set(argMap.get("-d"));
                }

                if (argMap.containsKey("-f")) {
                    String format = argMap.get("-f");
                    viewMode.set(format.equals("csv") ?
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
                log.info("Type in sql script to execute query, ddl, dml..., Or try :help");
                startInteractiveMode(baki, dsLoader);
            }
        } else {
            String msg = Command.get(args[0]);
            if (msg != null) {
                System.out.println(msg);
            } else {
                System.out.println("-u(jdbc url) is required or -h to get some help.");
            }
        }
    }

    public static void startCommandMode(SingleBaki baki, DataSourceLoader dataSourceLoader, String execute) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            dataSourceLoader.release();
            System.out.println("Bye bye :(");
        }));
        String sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(execute);
        // 以@开头那么批量执行直接执行完退出如果有重定向>，则不进行执行
        if (sql.startsWith("@")) {
            if (sql.contains(">")) {
                printlnWarning("batch(@) execute not support redirect operation!");
            } else {
                executeBatch(baki, sql);
            }
            return;
        }
        // 这里sql名可能还是 /usr/input.sql > /usr/local/output，处理一下
        // 这里sql名可能还是 select * from table > /usr/local/output，处理一下
        boolean redirect = false;
        Matcher mCheck = QUERY_R_FILE.matcher(sql);
        String output = null;
        if (mCheck.find()) {
            redirect = true;
            // 路径或sql
            sql = mCheck.group("sql");
            output = mCheck.group("path");
        }
        // 这里判断是 -e参数可能后面没有跟其他字符
        if (!sql.trim().equals("")) {
            try {
                List<String> sqls = multiSqlList(sql);
                if (sqls.size() == 1) {
                    sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(sqls.get(0));
                    SqlType sqlType = SqlUtil.getType(sql);
                    printlnHighlightSql(sql);
                    // 如果是查询是重定向操作
                    if (redirect && output != null) {
                        if (sqlType == SqlType.QUERY) {
                            try (Stream<DataRow> rowStream = baki.query(sql).stream()) {
                                printlnNotice("redirect query to file...");
                                writeFile(rowStream, output);
                            } catch (Exception e) {
                                printlnError(e);
                            }
                        } else {
                            printlnWarning("only query support redirect operation!");
                        }
                    } else {
                        printOneSqlResultByType(baki, sql, sql, Collections.emptyMap());
                    }
                } else {
                    // 如果是多条sql并且如果是重定向操作，则不让其执行
                    if (redirect) {
                        printlnWarning("only single query support redirect operation!");
                    } else {
                        printMultiSqlResult(baki, sqls);
                    }
                }
            } catch (Exception e) {
                printlnError(e);
            }
        } else {
            printlnWarning("no sql to execute, please check the -e format, is whitespace between -e and it's arg?");
        }
    }

    public static void startInteractiveMode(SingleBaki baki, DataSourceLoader dataSourceLoader) {
        isInteractive.set(true);

        XQLFileManager xqlFileManager = null;
        // 获取全部结果正则
        final Pattern GET_FORMAT = Pattern.compile("^:get\\s+(?<key>[\\s\\S]+)$");
        // 删除缓存正则
        final Pattern RM_CACHE_FORMAT = Pattern.compile("^:rm\\s+(?<key>\\S+)$");
        // 载入sql文件正则
        final Pattern LOAD_SQL_FORMAT = Pattern.compile("^:load\\s+(?<path>[\\s\\S]+\\S)$");
        // 设置多行sql分隔符正则
        final Pattern SQL_DELIMITER_FORMAT = Pattern.compile("^:d\\s+(?<key>[\\S\\s]+)$");

        final Scanner scanner = new Scanner(System.in);
        final StringBuilder inputStr = new StringBuilder();

        //如果使用杀进程或ctrl+c结束，或者关机，退出程序的情况下，做一些收尾工作
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (txActive.get()) {
                Tx.rollback();
            }
            dataSourceLoader.release();
            System.out.println("Bye bye :(");
        }));

        ScannerHelper.newLine();
        exit:
        while (true) {
            String line = scanner.nextLine().trim();
            if (line.startsWith(":")) {
                // 如果在输入多行sql的情况下，输入:号，都会清空之前的sql并执行相应命令
                // 此种情况可用来取消之前输入错误的sql
                inputStr.setLength(0);
                switch (line) {
                    case ":q":
                        if (txActive.get()) {
                            printlnWarning("Warning: Transaction is active now, please :commit or :rollback before quit, Control c, server shutdown or kill command will be rollback transaction!");
                            break;
                        } else {
                            break exit;
                        }
                    case ":help":
                    case ":h":
                        System.out.println(Command.get("--help"));
                        break;
                    case ":status":
                        printlnInfo("View Mode: " + viewMode.get());
                        printlnInfo("Transaction: " + (txActive.get() ? "enabled" : "disabled"));
                        printlnInfo("Cache: " + (enableCache.get() ? "enabled" : "disabled"));
                        printlnInfo("Multi Sql Delimiter: '" + (sqlDelimiter) + "'");
                        break;
                    case ":c":
                        enableCache.set(true);
                        printlnNotice("cache enabled!");
                        break;
                    case ":C":
                        enableCache.set(false);
                        printlnNotice("cache disabled!");
                        break;
                    case ":C!":
                        enableCache.set(false);
                        CACHE.clear();
                        idx.set(0);
                        printlnNotice("cache disabled and cleared!");
                        break;
                    case ":ls":
                        printlnNotice(CACHE.keySet().toString());
                        break;
                    case ":json":
                        viewMode.set(View.JSON);
                        printlnNotice("use json view!");
                        break;
                    case ":tsv":
                        viewMode.set(View.TSV);
                        printlnNotice("use tsv!");
                        break;
                    case ":csv":
                        viewMode.set(View.CSV);
                        printlnNotice("use csv!");
                        break;
                    case ":excel":
                        viewMode.set(View.EXCEL);
                        printlnNotice("use excel(grid) view!");
                        break;
                    case ":begin":
                        if (txActive.get()) {
                            printlnNotice("transaction is active now!");
                        } else {
                            Tx.begin();
                            txActive.set(true);
                            printlnInfo("open transaction: *sqlc> means transaction is active now!");
                        }
                        break;
                    case ":commit":
                        if (!txActive.get()) {
                            printlnNotice("transaction is not active now!");
                        } else {
                            Tx.commit();
                            txActive.set(false);
                        }
                        break;
                    case ":rollback":
                        if (!txActive.get()) {
                            printlnNotice("transaction is not active now!");
                        } else {
                            Tx.rollback();
                            txActive.set(false);
                        }
                        break;
                    default:
                        Matcher m_get = GET_FORMAT.matcher(line);
                        if (m_get.matches()) {
                            String keyFormat = m_get.group("key");
                            // '>' 代表将结果重定向输出到文件
                            if (keyFormat.contains(">")) {
                                Pattern CACHE_OP_FORMAT = Pattern.compile("(?<key>\\S+)\\s*>\\s*(?<path>\\.*" + File.separator + "\\S+)$");
                                Matcher m = CACHE_OP_FORMAT.matcher(keyFormat);
                                if (m.find()) {
                                    String key = m.group("key");
                                    String outputPath = m.group("path");
                                    List<DataRow> cache = CACHE.get(key);
                                    if (cache == null) {
                                        printlnWarning("no cache named " + key);
                                    } else {
                                        printlnNotice("redirect cache data to file...");
                                        writeFile(cache.stream(), outputPath);
                                    }
                                } else {
                                    printlnWarning("e.g. :get cacheName > /usr/local/you_file_name");
                                }
                            } else {
                                // 读取sql解析器内的sql
                                if (keyFormat.startsWith("&")) {
                                    try {
                                        if (xqlFileManager == null) {
                                            printlnWarning("XQLFileManager init failed, :load /you_path/you.xql to enable it.");
                                            break;
                                        }
                                        String sqlName = "x." + keyFormat.substring(1);
                                        // 这里不进行动态sql解析，因为需要先获取sql用来计算出需要输入的参数
                                        String sql = xqlFileManager.get(sqlName);
                                        Map<String, Object> argx = prepareSqlArgIf(sql, scanner);
                                        // 真正执行时使用baki来进行动态sql解析
                                        printlnHighlightSql(sql);
                                        printOneSqlResultByType(baki, "&" + sqlName, sql, argx);
                                    } catch (Exception e) {
                                        printlnError(e);
                                    }
                                    // 获取缓存
                                } else {
                                    List<DataRow> cache = CACHE.get(keyFormat);
                                    if (cache == null) {
                                        printlnWarning("no cache named " + keyFormat);
                                    } else {
                                        printQueryResult(cache.stream(), viewMode);
                                    }
                                }
                            }
                            break;
                        }

                        Matcher m_rm = RM_CACHE_FORMAT.matcher(line);
                        if (m_rm.matches()) {
                            String key = m_rm.group("key");
                            if (!CACHE.containsKey(key)) {
                                printlnDanger("no cached named " + key);
                            } else {
                                List<DataRow> rows = CACHE.get(key);
                                CACHE.remove(key);
                                rows.clear();
                                printlnNotice(key + " removed!");
                            }
                            break;
                        }

                        Matcher m_load_sql = LOAD_SQL_FORMAT.matcher(line);
                        if (m_load_sql.matches()) {
                            if (enableCache.get()) {
                                printlnWarning("WARN: cache will not work with :load...");
                            }
                            String path = m_load_sql.group("path").trim();
                            if (!path.equals("")) {
                                if (path.startsWith("@")) {
                                    if (path.contains(">")) {
                                        printlnWarning("batch(@) execute not support redirect operation!");
                                    } else {
                                        executeBatch(baki, path);
                                        newlineInThread.set(true);
                                    }
                                } else {
                                    if (path.contains(">")) {
                                        try {
                                            String[] input_output = path.split(">");
                                            String input = input_output[0].trim();
                                            String output = input_output[1].trim();
                                            List<String> sqls = multiSqlList(input);
                                            if (sqls.size() > 1) {
                                                printlnWarning("only single query support redirect operation!");
                                            } else if (SqlUtil.getType(sqls.get(0)) == SqlType.QUERY) {
                                                printlnHighlightSql(sqls.get(0));
                                                String sql = sqls.get(0);
                                                try (Stream<DataRow> s = baki.query(sql).args(prepareSqlArgIf(sql, scanner)).stream()) {
                                                    writeFile(s, output);
                                                }
                                            } else {
                                                printlnWarning("only query support redirect operation!");
                                            }
                                        } catch (Exception e) {
                                            printlnError(e);
                                        }
                                    } else {
                                        // 简单装载sql的处理，如果是xql文件，则使用XQLFileManager文件管理器来解析
                                        // 否则就常规解法
                                        try {
                                            if (path.endsWith(".xql")) {
                                                xqlFileManager = new XQLFileManager();
                                                xqlFileManager.setDelimiter(sqlDelimiter.get());
                                                xqlFileManager.add("x", "file:" + path);
                                                baki.setXqlFileManager(xqlFileManager);
                                                printlnInfo("XQLFileManager enabled, input command: ':get &you_sql_name' to execute!");
                                            } else {
                                                List<String> sqls = multiSqlList(path);
                                                if (sqls.size() > 0) {
                                                    if (sqls.size() == 1) {
                                                        String sql = sqls.get(0);
                                                        Map<String, Object> argx = prepareSqlArgIf(sql, scanner);
                                                        printlnHighlightSql(sql);
                                                        printOneSqlResultByType(baki, sql, sql, argx);
                                                    } else {
                                                        printMultiSqlResult(baki, sqls);
                                                        if (txActive.get()) {
                                                            printlnWarning("NOTICE: transaction is active...");
                                                        }
                                                    }
                                                } else {
                                                    printlnDanger("no sql script to execute.");
                                                }
                                            }
                                        } catch (Exception e) {
                                            printlnError(e);
                                        }
                                    }
                                }
                            } else {
                                printlnDanger("please input the file path.");
                            }
                            break;
                        }

                        Matcher m_sql_delimiter = SQL_DELIMITER_FORMAT.matcher(line);
                        if (m_sql_delimiter.matches()) {
                            String d = m_sql_delimiter.group("key");
                            sqlDelimiter.set(d.trim());
                            printlnNotice("set multi sql block delimited by '" + d.trim() + "', auto line break(\\n) delimiter if set blank.");
                            break;
                        }
                        printlnWarning("command not found or format invalid, command :help to get some help!");
                        break;
                }
                if (!newlineInThread.get()) {
                    ScannerHelper.newLine();
                } else {
                    newlineInThread.set(false);
                }
            } else {
                //此分支为累加sql语句执行sql
                inputStr.append(line);
                // 如果sql没有以分号结尾，则进入连续输入模式
                if (!line.endsWith(";")) {
                    if (inputStr.length() == 0) {
                        ScannerHelper.newLine();
                    } else {
                        inputStr.append("\n");
                        ScannerHelper.append();
                    }
                } else {
                    // 否则直接执行sql
                    String sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(inputStr.toString());
                    if (!sql.equals("")) {
                        SqlType type = SqlUtil.getType(sql);
                        switch (type) {
                            case QUERY:
                                Matcher m = QUERY_R_FILE.matcher(sql);
                                if (m.find()) {
                                    sql = m.group("sql");
                                    String output = m.group("path");
                                    try (Stream<DataRow> rowStream = baki.query(sql).args(prepareSqlArgIf(sql, scanner)).stream()) {
                                        printlnNotice("redirect query to file...");
                                        writeFile(rowStream, output);
                                    } catch (Exception e) {
                                        printlnError(e);
                                    }
                                    // 这里不输出，因为进度更新是独立的线程，防止重复显示顺序不对
                                    // ScannerHelper.newLine();
                                } else {
                                    try (Stream<DataRow> rowStream = baki.query(sql).args(prepareSqlArgIf(sql, scanner)).stream()) {
                                        // 查询缓存结果
                                        if (enableCache.get()) {
                                            List<DataRow> queryResult = new ArrayList<>();
                                            String key = "res" + idx.getAndIncrement();
                                            CACHE.put(key, queryResult);
                                            printQueryResult(rowStream, viewMode, queryResult::add);
                                            printlnNotice(key + ": added to cache!");
                                        } else {
                                            printQueryResult(rowStream, viewMode);
                                        }
                                        if (txActive.get()) {
                                            printlnWarning("NOTICE: transaction is active...");
                                        }
                                    } catch (Exception e) {
                                        printlnError(e);
                                    }
                                    ScannerHelper.newLine();
                                }
                                break;
                            case FUNCTION:
                                printlnWarning("function not support now!");
                                ScannerHelper.newLine();
                                break;
                            case OTHER:
                                try {
                                    if (sql.matches(QUERY_R_FILE.pattern())) {
                                        printlnWarning("only query support redirect operation!");
                                    } else {
                                        printQueryResult(executedRow2Stream(baki, sql, prepareSqlArgIf(sql, scanner)), viewMode);
                                    }
                                } catch (Exception e) {
                                    printlnError(e);
                                }
                                ScannerHelper.newLine();
                                break;
                            default:
                                break;
                        }
                    }
                    inputStr.setLength(0);
                }
            }
        }
    }

    public static void executeBatch(Baki baki, String path) {
        String delimiter = sqlDelimiter.get();
        String bPath = path.substring(1);
        Path path1 = Paths.get(bPath);
        if (Files.exists(path1)) {
            printlnPrimary("prepare to batch execute, default chunk size is 1000, waiting...");
            FastList<String> chunk = new FastList<>(String.class);
            AtomicReference<String> example = new AtomicReference<>("");
            NumberProgressPrinter pp = NumberProgressPrinter.of("chunk ", " executed.");
            pp.setStep(2);
            pp.valueFormatter(v -> v + "(" + v * 1000 + ")");
            pp.start((value, during) -> {
                if (value % 1000 != 0) {
                    value = value - 1;
                }
                long rows = value * 1000 + Math.max(chunk.size(), 1);
                long chunks = value;
                if (!chunk.isEmpty()) {
                    chunks += 1;
                }
                printlnHighlightSql(example.get() + ", more...");
                printlnPrimary("all of " + chunks + " chunks(" + rows + ") execute completed.(" + TimeUtil.format(during) + ")");
                chunk.clear();
                if (StatusManager.isInteractive.get()) {
                    ScannerHelper.newLine();
                }
            });
            try (Stream<String> lineStream = Files.lines(path1)) {
                StringBuilder sb = new StringBuilder();
                lineStream.map(String::trim)
                        .filter(sql -> !sql.equals("") && !StringUtil.startsWithsIgnoreCase(sql, "--", "#", "/*"))
                        .forEach(sql -> {
                            if (delimiter.equals("")) {
                                chunk.add(sql);
                            } else {
                                sb.append(sql).append("\n");
                                if (sql.endsWith(delimiter)) {
                                    chunk.add(sb.substring(0, sb.length() - delimiter.length() - 1));
                                    sb.setLength(0);
                                }
                            }
                            if (chunk.size() == 1000) {
                                if (example.get().equals("")) {
                                    example.set(chunk.get(0));
                                }
                                baki.batchExecute(chunk);
                                chunk.clear();
                                pp.increment();
                            }
                        });
                if (sb.length() > 0) {
                    chunk.add(sb.toString());
                    sb.setLength(0);
                }
                if (!chunk.isEmpty()) {
                    baki.batchExecute(chunk);
                    pp.increment();
                }
                pp.stop();
            } catch (Exception e) {
                pp.interrupt();
                printlnError(e);
            }
        } else {
            printlnDanger("sql file [ " + path + " ] not exists.");
        }
    }

    @SuppressWarnings("unchecked")
    public static Stream<DataRow> executedRow2Stream(Baki baki, String sql, Map<String, Object> args) {
        DataRow row = baki.execute(sql, args);
        Object res = row.getFirst();
        Stream<DataRow> stream;
        if (res instanceof DataRow) {
            stream = Stream.of((DataRow) res);
        } else if (res instanceof List) {
            stream = ((List<DataRow>) res).stream();
        } else {
            stream = Stream.of(row);
        }
        return stream;
    }

    public static void printOneSqlResultByType(Baki baki, String sqlOrAddress, String tempString, Map<String, Object> args) {
        SqlType sqlType = SqlUtil.getType(tempString);
        if (sqlType == SqlType.QUERY) {
            try (Stream<DataRow> s = baki.query(sqlOrAddress).args(args).stream()) {
                printQueryResult(s, viewMode);
            } catch (Exception e) {
                printlnError(e);
            }
        } else if (sqlType == SqlType.OTHER) {
            try {
                printQueryResult(executedRow2Stream(baki, sqlOrAddress, args), viewMode);
            } catch (Exception e) {
                printlnError(e);
            }
        } else if (sqlType == SqlType.FUNCTION) {
            printlnWarning("function not support now");
        }
    }

    public static void printMultiSqlResult(Baki baki, List<String> sqls) {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        sqls.forEach(sql -> {
            try {
                printlnHighlightSql(sql);
                printQueryResult(executedRow2Stream(baki, sql, Collections.emptyMap()), viewMode);
                success.incrementAndGet();
            } catch (Exception e) {
                fail.incrementAndGet();
                printlnError(e);
            }
        });
        printlnNotice("Execute finished, success: " + success + ", fail: " + fail);

    }
}
