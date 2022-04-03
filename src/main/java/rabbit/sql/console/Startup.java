package rabbit.sql.console;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.transaction.Tx;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.sql.console.core.Command;
import rabbit.sql.console.core.DataSourceLoader;
import rabbit.sql.console.core.Version;
import rabbit.sql.console.types.SqlType;
import rabbit.sql.console.types.View;
import rabbit.sql.console.util.SqlUtil;

import java.io.File;
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

import static rabbit.sql.console.core.FileHelper.writeFile;
import static rabbit.sql.console.core.PrintHelper.*;

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
                    if (sql.startsWith("@")) {
                        if (sql.matches(QUERY_R_FILE.pattern())) {
                            printWarning("only single query support redirect operation!");
                            System.exit(0);
                        }
                        executeBatch(baki, sql, sqlDelimiter);
                        System.exit(0);
                    }

                    String sourceSql = sql;
                    // 这里sql名可能还是 /usr/input.sql > /usr/local/output，处理一下
                    Matcher mCheck = QUERY_R_FILE.matcher(sql);
                    if (mCheck.find()) {
                        sql = mCheck.group("sql");
                    }
                    // 如果是sql是文件路径的话，将sql读取出来
                    if (sql.startsWith(File.separator) || sql.startsWith("." + File.separator)) {
                        if (!Files.exists(Paths.get(sql))) {
                            printDanger("sql file [" + sql + "] not exists.");
                            System.exit(0);
                        }
                        sql = String.join("\n", Files.readAllLines(Paths.get(sql)));
                    }
                    if (!sql.trim().equals("")) {
                        if (sql.contains(sqlDelimiter.get())) {
                            List<String> sqls = Stream.of(sql.split(sqlDelimiter.get()))
                                    .filter(s -> !s.trim().equals("") && !s.matches("^[;\r\t\n]$"))
                                    .collect(Collectors.toList());
                            if (sqls.size() > 1) {
                                // 如果是多条sql并且用户输入的-e参数是查询重定向输出文件，则不让其执行
                                if (sourceSql.matches(QUERY_R_FILE.pattern())) {
                                    printWarning("only single query support redirect operation!");
                                    System.exit(0);
                                }
                                AtomicInteger success = new AtomicInteger(0);
                                AtomicInteger fail = new AtomicInteger(0);
                                sqls.forEach(sbql -> {
                                    try {
                                        printHighlightSql(sbql);
                                        printQueryResult(executedRow2Stream(baki, sbql), viewMode);
                                        success.incrementAndGet();
                                    } catch (Exception e) {
                                        printError(e);
                                        fail.incrementAndGet();
                                    }
                                });
                                printNotice("Execute finished, success: " + success + ", fail: " + fail);
                                dsLoader.release();
                                System.exit(0);
                            }
                        }
                        SqlType sqlType = SqlUtil.getType(sql);
                        sql = com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(sql);
                        printHighlightSql(sql);
                        if (sqlType == SqlType.QUERY) {
                            Matcher m = QUERY_R_FILE.matcher(sourceSql);
                            if (m.find()) {
                                String output = m.group("path");
                                try (Stream<DataRow> rowStream = baki.query(sql)) {
                                    printNotice("redirect query to file...");
                                    writeFile(rowStream, viewMode, output);
                                } catch (Exception e) {
                                    printError(e);
                                }
                            } else {
                                try (Stream<DataRow> s = baki.query(sql)) {
                                    printQueryResult(s, viewMode);
                                } catch (Exception e) {
                                    printError(e);
                                }
                            }
                        } else if (sqlType == SqlType.OTHER) {
                            try {
                                printQueryResult(executedRow2Stream(baki, sql), viewMode);
                            } catch (Exception e) {
                                printError(e);
                            }
                        } else if (sqlType == SqlType.FUNCTION) {
                            printWarning("function not support now");
                        } else {
                            printWarning("unKnow sql type, will not be execute!");
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
                // 保存文件格式验证正则
                Pattern SAVE_FILE_FORMAT = Pattern.compile("^:save\\s+\\$(?<key>res[\\d]+)\\s*>\\s*(?<path>[\\S]+)$");
                // 直接保存查询结果到文件正则
                Pattern SAVE_QUERY_FORMAT = Pattern.compile("^:save\\s+\\$\\{\\s*(?<sql>[\\s\\S]+\\S)\\s*}\\s*>\\s*(?<path>[\\S]+)$");
                // 获取全部结果正则
                Pattern GET_FORMAT = Pattern.compile("^:get\\s+\\$(?<key>res[\\s\\S]+)$");
                // 删除缓存正则
                Pattern RM_CACHE_FORMAT = Pattern.compile("^:rm\\s+\\$(?<key>res[\\d]+)$");
                // 载入sql文件正则
                Pattern LOAD_SQL_FORMAT = Pattern.compile("^:load\\s+(?<path>[\\S]+)$");
                // 设置多行sql分隔符正则
                Pattern SQL_DELIMITER_FORMAT = Pattern.compile("^:d\\s+(?<key>[\\S\\s]+)$");

                //如果使用杀进程或ctrl+c结束，或者关机，退出程序的情况下，做一些收尾工作
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (txActive.get()) {
                        Tx.rollback();
                    }
                    dsLoader.release();
                    scanner.close();
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
                                CACHE.clear();
                                idx.set(0);
                                printNotice("cache disabled!");
                                break;
                            case ":clear":
                                CACHE.clear();
                                idx.set(0);
                                printNotice("cache cleared!");
                                break;
                            case ":keys":
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
                                Matcher m_save = SAVE_FILE_FORMAT.matcher(line);
                                if (m_save.matches()) {
                                    String key = m_save.group("key");
                                    // 如果存在缓存
                                    if (CACHE.containsKey(key)) {
                                        Stream<DataRow> rows = CACHE.get(key).stream();
                                        String path = m_save.group("path");
                                        writeFile(rows, viewMode, path);
                                    } else {
                                        printDanger("cache of " + key + " not exist!");
                                    }
                                    break;
                                }

                                Matcher m_get = GET_FORMAT.matcher(line);
                                if (m_get.matches()) {
                                    if (!enableCache.get()) {
                                        printWarning("cache is disabled, :c to enable.");
                                    } else {
                                        String keyFormat = m_get.group("key");
                                        // '>' 代表将结果重定向输出到文件
                                        if (keyFormat.contains(">")) {
                                            Pattern CACHE_OP_FORMAT = Pattern.compile("(?<key>res\\d+)\\s*>\\s*(?<path>\\.*" + File.separator + "\\S+)$");
                                            Matcher m = CACHE_OP_FORMAT.matcher(keyFormat);
                                            if (m.find()) {
                                                String key = m.group("key");
                                                String outputPath = m.group("path");
                                                List<DataRow> cache = CACHE.get(key);
                                                if (cache == null || cache.isEmpty()) {
                                                    printWarning("0 rows cached!");
                                                } else {
                                                    printNotice("redirect cache data to file...");
                                                    writeFile(cache.stream(), viewMode, outputPath);
                                                }
                                            } else {
                                                printWarning("e.g. $res0 > /usr/local/you_file_name");
                                            }
                                        } else {
                                            List<DataRow> cache = CACHE.get(keyFormat);
                                            if (cache == null || cache.isEmpty()) {
                                                printWarning("0 rows cached!");
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
                                        printDanger("no cached named " + key);
                                    } else {
                                        List<DataRow> rows = CACHE.get(key);
                                        CACHE.remove(key);
                                        rows.clear();
                                        printNotice(key + " removed!");
                                    }
                                    break;
                                }

                                Matcher m_query_save = SAVE_QUERY_FORMAT.matcher(line);
                                if (m_query_save.matches()) {
                                    // 查询直接导出记录
                                    try {
                                        String sql = m_query_save.group("sql");
                                        // 以路径开头，则认为是要读取sql查询呢脚本文件
                                        if (sql.startsWith(File.separator) || sql.startsWith("." + File.separator)) {
                                            try {
                                                sql = String.join("\n", Files.readAllLines(Paths.get(sql)));
                                                printHighlightSql(sql);
                                            } catch (Exception e) {
                                                throw new IOException(e);
                                            }
                                        }
                                        String path = m_query_save.group("path");
                                        try (Stream<DataRow> s = baki.query(sql)) {
                                            writeFile(s, viewMode, path);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    } catch (Exception e) {
                                        printError(e);
                                    }
                                    break;
                                }

                                Matcher m_load_sql = LOAD_SQL_FORMAT.matcher(line);
                                if (m_load_sql.matches()) {
                                    if (enableCache.get()) {
                                        printWarning("WARN: cache will not work with :load...");
                                    }
                                    String path = m_load_sql.group("path").trim();
                                    if (path.length() > 0) {
                                        if (path.startsWith("@")) {
                                            executeBatch(baki, path, sqlDelimiter);
                                        } else if (Files.exists(Paths.get(path))) {
                                            try {
                                                AtomicInteger success = new AtomicInteger(0);
                                                AtomicInteger fail = new AtomicInteger(0);
                                                Stream.of(String.join("\n", Files.readAllLines(Paths.get(path))).split(sqlDelimiter.get()))
                                                        .filter(sql -> !sql.trim().equals("") && !sql.matches("^[;\r\t\n]$"))
                                                        .forEach(sql -> {
                                                            try {
                                                                printHighlightSql(sql);
                                                                printQueryResult(executedRow2Stream(baki, sql), viewMode);
                                                                success.incrementAndGet();
                                                            } catch (Exception e) {
                                                                fail.incrementAndGet();
                                                                printError(e);
                                                            }
                                                        });
                                                if (txActive.get()) {
                                                    printWarning("NOTICE: transaction is active...");
                                                }
                                                printNotice("Execute finished, success: " + success + ", fail: " + fail);
                                            } catch (Exception e) {
                                                printError(e);
                                            }
                                        } else {
                                            printDanger("sql file [ " + path + " ] not exists.");
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
                    printNotice("more(" + (chunk.size() - 3) + ")......");
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
}
