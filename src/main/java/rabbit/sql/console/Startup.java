package rabbit.sql.console;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.transaction.Tx;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.sql.console.util.DataSourceLoader;
import rabbit.sql.console.types.SqlType;
import rabbit.sql.console.types.View;
import rabbit.sql.console.util.SqlUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static rabbit.sql.console.util.FileHelper.writeFile;
import static rabbit.sql.console.util.PrintHelper.*;

public class Startup {
    private static final Logger log = LoggerFactory.getLogger("SQLC");

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
            Baki baki = dsLoader.getBaki();

            log.info("Welcome to sqlc {} ({}, {})", Version.RELEASE, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
            log.info("Go to \33[4mhttps://github.com/chengyuxing/sqlc\33[0m get more information about this.");

            if (baki != null) {
                // 多行sql分隔符
                final AtomicReference<String> sqlDelimiter = new AtomicReference<>(";;");
                if (argMap.containsKey("-d")) {
                    sqlDelimiter.set(argMap.get("-d"));
                }
                // 输出的结果视图与结果保存类型
                final AtomicReference<View> viewMode = new AtomicReference<>(View.TSV);
                if (argMap.containsKey("-f")) {
                    String format = argMap.get("-f");
                    viewMode.set(format.equals("csv") ?
                            View.CSV : format.equals("json") ?
                            View.JSON : format.equals("excel") ?
                            View.EXCEL : View.TSV);
                }
                // 查询sql重定向输出文件正则
                final Pattern QUERY_R_FILE = Pattern.compile("(?<sql>[\\s\\S]+\\S)\\s*>\\s*(?<path>\\.*" + File.separator + "\\S+)$");

                if (argMap.containsKey("-e")) {
                    String sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(argMap.get("-e"));
                    // 以@开头那么批量执行直接执行完退出如果有重定向>，则不进行执行
                    if (sql.startsWith("@")) {
                        if (sql.contains(">")) {
                            printWarning("batch(@) execute not support redirect operation!");
                        } else {
                            executeBatch(baki, sql, sqlDelimiter);
                        }
                        System.exit(0);
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
                            List<String> sqls = multiSqlList(sql, sqlDelimiter);
                            if (sqls.size() == 1) {
                                sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(sqls.get(0));
                                SqlType sqlType = SqlUtil.getType(sql);
                                printHighlightSql(sql);
                                // 如果是查询是重定向操作
                                if (redirect && output != null) {
                                    if (sqlType == SqlType.QUERY) {
                                        try (Stream<DataRow> rowStream = baki.query(sql)) {
                                            printNotice("redirect query to file...");
                                            writeFile(rowStream, viewMode, output);
                                        } catch (Exception e) {
                                            printError(e);
                                        }
                                    } else {
                                        printWarning("only query support redirect operation!");
                                    }
                                } else {
                                    printOneSqlResultByType(baki, sql, viewMode);
                                }
                            } else {
                                // 如果是多条sql并且如果是重定向操作，则不让其执行
                                if (redirect) {
                                    printWarning("only single query support redirect operation!");
                                } else {
                                    printMultiSqlResult(baki, sqls, viewMode);
                                }
                            }
                        } catch (Exception e) {
                            printError(e);
                        }
                    } else {
                        printWarning("no sql to execute, please check the -e format, is whitespace between -e and it's arg?");
                    }
                    dsLoader.release();
                    System.exit(0);
                }

                // 进入交互模式
                log.info("Type in sql script to execute query, ddl, dml..., Or try :help");
                Scanner scanner = new Scanner(System.in);
                printPrefix(new AtomicBoolean(false), "sqlc>");

                // 数据缓存
                Map<String, List<DataRow>> CACHE = new LinkedHashMap<>();
                // 输入字符串缓冲
                StringBuilder inputStr = new StringBuilder();
                // 事务是否活动标志
                AtomicBoolean txActive = new AtomicBoolean(false);
                // 是否开启缓存
                AtomicBoolean enableCache = new AtomicBoolean(false);
                // 结果集缓存key自增
                AtomicInteger idx = new AtomicInteger(0);
                // 获取全部结果正则
                Pattern GET_FORMAT = Pattern.compile("^:get\\s+(?<key>[\\s\\S]+)$");
                // 删除缓存正则
                Pattern RM_CACHE_FORMAT = Pattern.compile("^:rm\\s+(?<key>\\S+)$");
                // 载入sql文件正则
                Pattern LOAD_SQL_FORMAT = Pattern.compile("^:load\\s+(?<path>[\\s\\S]+\\S)$");
                // 设置多行sql分隔符正则
                Pattern SQL_DELIMITER_FORMAT = Pattern.compile("^:d\\s+(?<key>[\\S\\s]+)$");

                //如果使用杀进程或ctrl+c结束，或者关机，退出程序的情况下，做一些收尾工作
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (txActive.get()) {
                        Tx.rollback();
                    }
                    dsLoader.release();
                    System.out.println("Bye bye :(");
                }));

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
                                    printWarning("Warning: Transaction is active now, please :commit or :rollback before quit, Control c, server shutdown or kill command will be rollback transaction!");
                                    break;
                                } else {
                                    break exit;
                                }
                            case ":help":
                            case ":h":
                                System.out.println(Command.get("--help"));
                                break;
                            case ":status":
                                printInfo("View Mode: " + viewMode.get());
                                printInfo("Transaction: " + (txActive.get() ? "enabled" : "disabled"));
                                printInfo("Cache: " + (enableCache.get() ? "enabled" : "disabled"));
                                printInfo("Multi Sql Delimiter: '" + (sqlDelimiter) + "'");
                                break;
                            case ":c":
                                enableCache.set(true);
                                printNotice("cache enabled!");
                                break;
                            case ":C":
                                enableCache.set(false);
                                printNotice("cache disabled!");
                                break;
                            case ":C!":
                                enableCache.set(false);
                                CACHE.clear();
                                idx.set(0);
                                printNotice("cache disabled and cleared!");
                                break;
                            case ":ls":
                                printNotice(CACHE.keySet().toString());
                                break;
                            case ":json":
                                viewMode.set(View.JSON);
                                printNotice("use json view!");
                                break;
                            case ":tsv":
                                viewMode.set(View.TSV);
                                printNotice("use tsv!");
                                break;
                            case ":csv":
                                viewMode.set(View.CSV);
                                printNotice("use csv!");
                                break;
                            case ":excel":
                                viewMode.set(View.EXCEL);
                                printNotice("use excel(grid) view!");
                                break;
                            case ":begin":
                                if (txActive.get()) {
                                    printNotice("transaction is active now!");
                                } else {
                                    Tx.begin();
                                    txActive.set(true);
                                    printInfo("open transaction: [*]sqlc> means transaction is active now!");
                                }
                                break;
                            case ":commit":
                                if (!txActive.get()) {
                                    printNotice("transaction is not active now!");
                                } else {
                                    Tx.commit();
                                    txActive.set(false);
                                }
                                break;
                            case ":rollback":
                                if (!txActive.get()) {
                                    printNotice("transaction is not active now!");
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
                                                printWarning("no cache named " + key);
                                            } else {
                                                printNotice("redirect cache data to file...");
                                                writeFile(cache.stream(), viewMode, outputPath);
                                            }
                                        } else {
                                            printWarning("e.g. :get cacheName > /usr/local/you_file_name");
                                        }
                                    } else {
                                        List<DataRow> cache = CACHE.get(keyFormat);
                                        if (cache == null) {
                                            printWarning("no cache named " + keyFormat);
                                        } else {
                                            printQueryResult(cache.stream(), viewMode);
                                        }
                                    }
                                    break;
                                }

                                Matcher m_rm = RM_CACHE_FORMAT.matcher(line);
                                if (m_rm.matches()) {
                                    String key = m_rm.group("key");
                                    if (!CACHE.containsKey(key)) {
                                        printDanger("no cached named " + key);
                                    } else {
                                        List<DataRow> rows = CACHE.get(key);
                                        CACHE.remove(key);
                                        rows.clear();
                                        printNotice(key + " removed!");
                                    }
                                    break;
                                }

                                Matcher m_load_sql = LOAD_SQL_FORMAT.matcher(line);
                                if (m_load_sql.matches()) {
                                    if (enableCache.get()) {
                                        printWarning("WARN: cache will not work with :load...");
                                    }
                                    String path = m_load_sql.group("path").trim();
                                    if (!path.equals("")) {
                                        if (path.startsWith("@")) {
                                            if (path.contains(">")) {
                                                printWarning("batch(@) execute not support redirect operation!");
                                            } else {
                                                executeBatch(baki, path, sqlDelimiter);
                                            }
                                        } else {
                                            if (path.contains(">")) {
                                                try {
                                                    String[] input_output = path.split(">");
                                                    String input = input_output[0].trim();
                                                    String output = input_output[1].trim();
                                                    List<String> sqls = multiSqlList(input, sqlDelimiter);
                                                    if (sqls.size() > 1) {
                                                        printWarning("only single query support redirect operation!");
                                                    } else if (SqlUtil.getType(sqls.get(0)) == SqlType.QUERY) {
                                                        printHighlightSql(sqls.get(0));
                                                        try (Stream<DataRow> s = baki.query(sqls.get(0))) {
                                                            writeFile(s, viewMode, output);
                                                        }
                                                    } else {
                                                        printWarning("only query support redirect operation!");
                                                    }
                                                } catch (Exception e) {
                                                    printError(e);
                                                }
                                            } else {
                                                try {
                                                    List<String> sqls = multiSqlList(path, sqlDelimiter);
                                                    if (sqls.size() > 0) {
                                                        if (sqls.size() == 1) {
                                                            printHighlightSql(sqls.get(0));
                                                            printOneSqlResultByType(baki, sqls.get(0), viewMode);
                                                        } else {
                                                            printMultiSqlResult(baki, sqls, viewMode);
                                                            if (txActive.get()) {
                                                                printWarning("NOTICE: transaction is active...");
                                                            }
                                                        }
                                                    } else {
                                                        printDanger("no sql script to execute.");
                                                    }
                                                } catch (Exception e) {
                                                    printError(e);
                                                }
                                            }
                                        }
                                    } else {
                                        printDanger("please input the file path.");
                                    }
                                    break;
                                }

                                Matcher m_sql_delimiter = SQL_DELIMITER_FORMAT.matcher(line);
                                if (m_sql_delimiter.matches()) {
                                    String d = m_sql_delimiter.group("key");
                                    sqlDelimiter.set(d.trim());
                                    printNotice("set multi sql block delimited by '" + d.trim() + "', auto line break(\\n) delimiter if set blank.");
                                    break;
                                }
                                printWarning("command not found or format invalid, command :help to get some help!");
                                break;
                        }
                        printPrefix(txActive, "sqlc>");
                    } else {
                        //此分支为累加sql语句执行sql
                        inputStr.append(line);
                        // 如果sql没有以分号结尾，则进入连续输入模式
                        if (!line.endsWith(";")) {
                            if (inputStr.length() == 0) {
                                printPrefix(txActive, "sqlc>");
                            } else {
                                inputStr.append("\n");
                                printPrefix(txActive, ">>");
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
                                            try (Stream<DataRow> rowStream = baki.query(sql)) {
                                                printNotice("redirect query to file...");
                                                writeFile(rowStream, viewMode, output);
                                            } catch (Exception e) {
                                                printError(e);
                                            }
                                        } else {
                                            try (Stream<DataRow> rowStream = baki.query(sql)) {
                                                // 查询缓存结果
                                                if (enableCache.get()) {
                                                    List<DataRow> queryResult = new ArrayList<>();
                                                    String key = "res" + idx.getAndIncrement();
                                                    CACHE.put(key, queryResult);
                                                    printQueryResult(rowStream, viewMode, queryResult::add);
                                                    printNotice(key + ": added to cache!");
                                                } else {
                                                    printQueryResult(rowStream, viewMode);
                                                }
                                                if (txActive.get()) {
                                                    printWarning("NOTICE: transaction is active...");
                                                }
                                            } catch (Exception e) {
                                                printError(e);
                                            }
                                        }
                                        break;
                                    case FUNCTION:
                                        printWarning("function not support now!");
                                        break;
                                    case OTHER:
                                        try {
                                            if (sql.matches(QUERY_R_FILE.pattern())) {
                                                printWarning("only query support redirect operation!");
                                            } else {
                                                printQueryResult(executedRow2Stream(baki, sql), viewMode);
                                            }
                                        } catch (Exception e) {
                                            printError(e);
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }
                            printPrefix(txActive, "sqlc>");
                            inputStr.setLength(0);
                        }
                    }
                }

                // 正常退出：如果在事务中，则回滚事务
                if (txActive.get()) {
                    Tx.rollback();
                }
                System.exit(0);
            }
        } else {
            String msg = Command.get(args[0]);
            if (msg != null) {
                System.out.println(msg);
            } else {
                System.out.println("-u(jdbc url) is required or -h to get some help.");
            }
        }
        System.exit(0);
    }

    public static void executeBatch(Baki baki, String path, AtomicReference<String> delimiterR) {
        String delimiter = delimiterR.get();
        String bPath = path.substring(1);
        if (Files.exists(Paths.get(bPath))) {
            printPrimary("Prepare to batch execute, default chunk size is 1000, waiting...");
            FastList<String> chunk = new FastList<>(String.class);
            AtomicInteger chunkNum = new AtomicInteger(0);
            try (Stream<String> lineStream = Files.lines(Paths.get(bPath))) {
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
                                printPrimary("waiting...");
                                baki.batchExecute(chunk);
                                printNotice("chunk" + chunkNum.getAndIncrement() + " executed!");
                                for (int i = 0; i < 3; i++) {
                                    printHighlightSql(chunk.get(i));
                                }
                                printNotice("more(" + (chunk.size() - 3) + ")......");
                                chunk.clear();
                            }
                        });
                if (sb.length() > 0) {
                    chunk.add(sb.toString());
                    sb.setLength(0);
                }
                if (!chunk.isEmpty()) {
                    printPrimary("waiting...");
                    baki.batchExecute(chunk);
                    printNotice("chunk" + chunkNum.getAndIncrement() + " executed!");
                    for (int i = 0, j = Math.min(chunk.size(), 3); i < j; i++) {
                        printHighlightSql(chunk.get(i));
                    }
                    int rest = Math.max(chunk.size() - 3, 0);
                    printNotice("more(" + rest + ")......");
                    chunk.clear();
                }
            } catch (Exception e) {
                printError(e);
            }
        } else {
            printDanger("sql file [ " + path + " ] not exists.");
        }
    }

    @SuppressWarnings("unchecked")
    public static Stream<DataRow> executedRow2Stream(Baki baki, String sql) {
        DataRow row = baki.execute(sql);
        Object res = row.get(0);
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

    /**
     * 如果是文件路径就读取文件返回sql
     *
     * @param sqlOrPath sql字符串或路径
     * @return sql字符串
     * @throws IOException 如果读取文件发生异常或文件不存在
     */
    public static String getSqlByFileIf(String sqlOrPath) throws IOException {
        if (sqlOrPath.startsWith(File.separator) || sqlOrPath.startsWith("." + File.separator)) {
            if (!Files.exists(Paths.get(sqlOrPath))) {
                throw new FileNotFoundException("sql file [" + sqlOrPath + "] not exists.");
            }
            return String.join("\n", Files.readAllLines(Paths.get(sqlOrPath)));
        }
        return sqlOrPath;
    }

    public static void printOneSqlResultByType(Baki baki, String sql, AtomicReference<View> viewMode) {
        SqlType sqlType = SqlUtil.getType(sql);
        if (sqlType == SqlType.QUERY) {
            try (Stream<DataRow> s = baki.query(sql)) {
                printQueryResult(s, viewMode);
            } catch (Exception e) {
                printError(e);
            }
        } else if (sqlType == SqlType.OTHER) {
            try {
                printQueryResult(executedRow2Stream(baki, sql), viewMode);
            } catch (Exception e) {
                printError(e);
            }
        } else if (sqlType == SqlType.FUNCTION) {
            printWarning("function not support now");
        }
    }

    public static void printMultiSqlResult(Baki baki, List<String> sqls, AtomicReference<View> viewMode) {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        sqls.forEach(sql -> {
            try {
                printHighlightSql(sql);
                printQueryResult(executedRow2Stream(baki, sql), viewMode);
                success.incrementAndGet();
            } catch (Exception e) {
                fail.incrementAndGet();
                printError(e);
            }
        });
        printNotice("Execute finished, success: " + success + ", fail: " + fail);

    }

    /**
     * 整个大字符串或sql文件根据分隔符分块
     *
     * @param multiSqlOrFilePath sql或文件路径
     * @param delimiter          分隔符
     * @return 一组sql
     * @throws IOException 如果读取文件发生异常或文件不存在
     */
    public static List<String> multiSqlList(String multiSqlOrFilePath, AtomicReference<String> delimiter) throws IOException {
        String sqls = getSqlByFileIf(multiSqlOrFilePath);
        return Stream.of(sqls.split(delimiter.get()))
                .filter(sql -> !sql.trim().equals("") && !sql.matches("^[;\r\t\n]$"))
                .collect(Collectors.toList());
    }
}
