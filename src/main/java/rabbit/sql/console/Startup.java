package rabbit.sql.console;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.io.Lines;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.excel.io.BigExcelLineWriter;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.transaction.Tx;
import com.zaxxer.hikari.util.FastList;
import org.apache.poi.ss.usermodel.Sheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.sql.console.core.Command;
import rabbit.sql.console.core.DataSourceLoader;
import rabbit.sql.console.core.ViewPrinter;
import rabbit.sql.console.types.SqlType;
import rabbit.sql.console.types.View;
import rabbit.sql.console.util.SqlUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                if (argMap.containsKey("-e")) {
                    String sql = argMap.get("-e");
                    if (sql.startsWith("@")) {
                        if (argMap.containsKey("-s")) {
                            printWarning("WARN: batch execute(@) will not work with -s, only print executed result.");
                        }
                        executeBatch(baki, sql, sqlDelimiter);
                        System.exit(0);
                    }

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
                            // 如果有多段sql脚本，则批量执行并打印结果，但不能配合 -s 输出文件
                            if (sqls.size() > 1) {
                                if (argMap.containsKey("-s")) {
                                    printWarning("WARN: multi block sql script will not work with -s, only print executed result.");
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
                        printHighlightSql(sql);
                        if (sqlType == SqlType.QUERY) {
                            try (Stream<DataRow> s = baki.query(sql)) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    writeFile(s, viewMode, path);
                                } else {
                                    printQueryResult(s, viewMode);
                                }
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
                        } else {
                            printWarning("unKnow sql type, will not be execute!");
                        }
                    } else {
                        printWarning("no sql to execute, please check the -e format, is whitespace between -e and it's arg?");
                    }
                    dsLoader.release();
                    System.exit(0);
                }

                log.info("Welcome to sqlc {} ({}, {})", Version.RELEASE, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
                log.info("Go to \33[4mhttps://github.com/chengyuxing/sqlc\33[0m get more information about this.");
                log.info("Type in sql script to execute query,ddl,dml..., Or try :help");

                // 进入交互模式
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
                Pattern SAVE_FILE_FORMAT = Pattern.compile("^:save +\\$(?<key>res[\\d]+) *> *(?<path>[\\S]+)$");
                // 直接保存查询结果到文件正则
                Pattern SAVE_QUERY_FORMAT = Pattern.compile("^:save +\\$\\{\\s*(?<sql>[\\s\\S]+\\S)\\s*} *> *(?<path>[\\S]+)$");
                // 获取结果集区间正则
                Pattern GET_RES_RANGE_FORMAT = Pattern.compile("^:get +\\$(?<key>res[\\d]+) *< *(?<start>\\d+) *: *(?<end>\\d+)$");
                // 获取指定索引的结果正则
                Pattern GET_RES_IDX_FORMAT = Pattern.compile("^:get +\\$(?<key>res[\\d]+) *< *(?<index>\\d+)$");
                // 获取全部结果正则
                Pattern GET_ALL_FORMAT = Pattern.compile("^:get +\\$(?<key>res[\\d]+)$");
                // 删除缓存正则
                Pattern RM_CACHE_FORMAT = Pattern.compile("^:rm +\\$(?<key>res[\\d]+)$");
                // 查询缓存大小正则
                Pattern GET_SIZE_FORMAT = Pattern.compile("^:size +\\$(?<key>res[\\d]+)$");
                // 载入sql文件正则
                Pattern LOAD_SQL_FORMAT = Pattern.compile("^:load +(?<path>[\\S]+)$");
                // 设置多行sql分隔符正则
                Pattern SQL_DELIMITER_FORMAT = Pattern.compile("^:d +(?<key>[\\S\\s]+)$");

                Scanner scanner = new Scanner(System.in);
                printPrefix(txActive, "sqlc> ");

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
                    if (line.length() > 1 && line.startsWith(":")) {
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

                                Matcher m_getAll = GET_ALL_FORMAT.matcher(line);
                                if (m_getAll.matches()) {
                                    String key = m_getAll.group("key");
                                    List<DataRow> rows = CACHE.get(key);
                                    if (rows == null || rows.isEmpty()) {
                                        printWarning("0 rows cached!");
                                    } else {
                                        printQueryResult(rows.stream(), viewMode);
                                        printInfo(key + " loaded!");
                                    }
                                    break;
                                }

                                Matcher m_getByIdx = GET_RES_IDX_FORMAT.matcher(line);
                                if (m_getByIdx.matches()) {
                                    String key = m_getByIdx.group("key");
                                    int index = Integer.parseInt(m_getByIdx.group("index"));
                                    List<DataRow> rows = CACHE.get(key);
                                    if (rows == null || rows.isEmpty()) {
                                        printWarning("0 rows cached!");
                                    } else {
                                        if (index < 0 || index > rows.size() - 1) {
                                            printDanger("index " + index + " of " + key + " out of range.");
                                        } else {
                                            printQueryResult(Stream.of(rows.get(index)), viewMode);
                                            printInfo("line " + index + " of " + key + " loaded!");
                                        }
                                    }
                                    break;
                                }

                                Matcher m_getByRange = GET_RES_RANGE_FORMAT.matcher(line);
                                if (m_getByRange.matches()) {
                                    String key = m_getByRange.group("key");
                                    int start = Integer.parseInt(m_getByRange.group("start"));
                                    int end = Integer.parseInt(m_getByRange.group("end"));
                                    List<DataRow> rows = CACHE.get(key);
                                    if (rows == null || rows.isEmpty()) {
                                        printWarning("0 rows cached!");
                                    } else {
                                        if (start < 0 || start > end || end > rows.size() - 1) {
                                            printDanger("invalid range!");
                                        } else {
                                            printQueryResult(rows.subList(start, end).stream(), viewMode);
                                            printInfo("line " + start + " to " + end + " of " + key + " loaded!");
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

                                Matcher m_size = GET_SIZE_FORMAT.matcher(line);
                                if (m_size.matches()) {
                                    String key = m_size.group("key");
                                    if (!CACHE.containsKey(key)) {
                                        printDanger("no cached named " + key);
                                    } else {
                                        printInfo(CACHE.get(key).size() + "");
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
                                                                if (txActive.get()) {
                                                                    printWarning("WARN: transaction is active now, go on...");
                                                                }
                                                                success.incrementAndGet();
                                                            } catch (Exception e) {
                                                                fail.incrementAndGet();
                                                                printError(e);
                                                            }
                                                        });
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
                        //此分支为执行sql
                    } else {
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
                            String sql = inputStr.toString();
                            if (!com.github.chengyuxing.sql.utils.SqlUtil.trimEnd(sql).equals("")) {
                                SqlType type = SqlUtil.getType(sql);
                                switch (type) {
                                    case QUERY:
                                        // 查询缓存结果
                                        List<DataRow> queryResult = new ArrayList<>();
                                        String key = "";
                                        boolean cacheEnabled = enableCache.get();
                                        if (cacheEnabled) {
                                            key = "res" + idx.getAndIncrement();
                                            CACHE.put(key, queryResult);
                                        }
                                        try (Stream<DataRow> rowStream = baki.query(sql)) {
                                            if (cacheEnabled) {
                                                printQueryResult(rowStream, viewMode, queryResult::add);
                                            } else {
                                                printQueryResult(rowStream, viewMode);
                                            }
                                            if (cacheEnabled) {
                                                printNotice(key + ": added to cache!");
                                            }
                                            if (txActive.get()) {
                                                printWarning("WARN: transaction is active now, go on...");
                                            }
                                        } catch (Exception e) {
                                            printError(e);
                                        }
                                        break;
                                    case FUNCTION:
                                        printWarning("function not support now!");
                                        break;
                                    case OTHER:
                                        try {
                                            DataRow res = baki.execute(sql);
                                            printInfo("execute " + res.getString("type") + ":" + res.getInt("result"));
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

    public static void printPrefix(AtomicBoolean isTxActive, String mode) {
        String txActiveFlag = isTxActive.get() ? "[*]" : "";
        Printer.printf("%s%s ", Color.PURPLE, txActiveFlag, mode);
    }

    public static void writeFile(Stream<DataRow> stream, AtomicReference<View> mode, String path) {
        if (path.endsWith(".sql")) {
            writeInsertSqlFile(stream, path);
            return;
        }
        switch (mode.get()) {
            case JSON:
                writeJSON(stream, path);
                break;
            case TSV:
            case CSV:
                writeDSV(stream, mode, path);
                break;
            case EXCEL:
                writeExcel(stream, path);
                break;
        }
    }

    public static void writeDSV(Stream<DataRow> s, AtomicReference<View> mode, String path) {
        String fileName = path;
        if (!StringUtil.endsWithsIgnoreCase(fileName, ".tsv", ".csv")) {
            String suffix = mode.get() == View.TSV ? ".tsv" : ".csv";
            fileName += suffix;
        }
        AtomicReference<FileOutputStream> outputStreamAtomicReference = new AtomicReference<>(null);
        try {
            outputStreamAtomicReference.set(new FileOutputStream(fileName));
            FileOutputStream out = outputStreamAtomicReference.get();
            String d = mode.get() == View.TSV ? "\t" : ",";
            printPrimary("waiting...");
            AtomicLong i = new AtomicLong(1);
            AtomicBoolean first = new AtomicBoolean(true);
            s.forEach(row -> {
                try {
                    if (first.get()) {
                        Lines.writeLine(out, row.getNames(), d);
                        first.set(false);
                    }
                    Lines.writeLine(out, row.getValues(), d);
                    long offset = i.getAndIncrement();
                    if (offset % 10000 == 0) {
                        Printer.printf("[%s] %s rows has written.", Color.DARK_CYAN, LocalDateTime.now(), offset);
                        System.out.println();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            Printer.printf("[%s] %s rows write completed.", Color.DARK_CYAN, LocalDateTime.now(), i.get());
            System.out.println();
            printNotice(fileName + " saved!");
        } catch (Exception e) {
            try {
                FileOutputStream out = outputStreamAtomicReference.get();
                if (out != null) {
                    out.close();
                    Files.deleteIfExists(Paths.get(fileName));
                }
            } catch (IOException ex) {
                e.addSuppressed(ex);
            }
            printError(e);
        }
    }

    public static void writeJSON(Stream<DataRow> s, String path) {
        String fileName = path;
        if (!fileName.endsWith(".json")) {
            fileName += ".json";
        }
        Path filePath = Paths.get(fileName);
        AtomicReference<BufferedWriter> bufferedWriterAtomicReference = new AtomicReference<>(null);
        try {
            bufferedWriterAtomicReference.set(Files.newBufferedWriter(filePath));
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            printPrimary("waiting...");
            AtomicBoolean first = new AtomicBoolean(true);
            AtomicLong i = new AtomicLong(1);
            s.forEach(row -> {
                try {
                    if (first.get()) {
                        writer.write("[");
                        writer.write(ViewPrinter.getJson(row));
                        first.set(false);
                    } else {
                        writer.write(", " + ViewPrinter.getJson(row));
                    }
                    long offset = i.getAndIncrement();
                    if (offset % 10000 == 0) {
                        Printer.printf("[%s] %s object has written.", Color.DARK_CYAN, LocalDateTime.now(), offset);
                        System.out.println();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            writer.write("]");
            Printer.printf("[%s] %s object write completed.", Color.DARK_CYAN, LocalDateTime.now(), i.get());
            System.out.println();
            printNotice(fileName + " saved!");
            writer.close();
        } catch (Exception e) {
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            if (writer != null) {
                try {
                    writer.close();
                    Files.deleteIfExists(filePath);
                } catch (IOException ex) {
                    e.addSuppressed(ex);
                }
            }
            printError(e);
        }
    }

    public static void writeExcel(Stream<DataRow> rowStream, String path) {
        printPrimary("waiting...");
        String filePath = path;
        if (!filePath.endsWith(".xlsx")) {
            filePath += ".xlsx";
        }
        BigExcelLineWriter writer = new BigExcelLineWriter(true);
        try {
            Sheet sheet = writer.createSheet("Sheet1");
            AtomicBoolean first = new AtomicBoolean(true);
            rowStream.forEach(row -> {
                if (first.get()) {
                    writer.writeRow(sheet, row.getNames().toArray());
                    first.set(false);
                }
                writer.writeRow(sheet, row.getValues());
            });
            writer.saveTo(filePath);
            printNotice(filePath + " saved!");
            writer.close();
        } catch (Exception e) {
            try {
                writer.close();
                Files.deleteIfExists(Paths.get(filePath));
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            printError(e);
        }
    }

    public static void writeInsertSqlFile(Stream<DataRow> stream, String outputPath) {
        // e.g. /usr/local/qbpt_deve.pinyin_ch.sql
        String tableName = outputPath.substring(outputPath.lastIndexOf(File.separator) + 1, outputPath.lastIndexOf("."));
        printWarning("Ignore view mode(-f and :[tsv|csv|json|excel]), output file name will as the insert sql script target table name!!!");
        printWarning("e.g. " + outputPath + " --> insert into " + tableName + "(...) values(...);;");
        AtomicReference<BufferedWriter> bufferedWriterAtomicReference = new AtomicReference<>(null);
        try {
            bufferedWriterAtomicReference.set(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath))));
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            printPrimary("waiting...");
            AtomicInteger rows = new AtomicInteger(0);
            stream.forEach(d -> {
                try {
                    String insert = com.github.chengyuxing.sql.utils.SqlUtil.generateInsert(tableName, d.toMap(), d.getNames()).replace("\n", "") + ";;\n";
                    writer.write(insert);
                    int i = rows.incrementAndGet();
                    if (i % 10000 == 0) {
                        Printer.printf("[%s] %s rows has written.", Color.DARK_CYAN, LocalDateTime.now(), i);
                        System.out.println();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            Printer.printf("[%s] %s rows write completed.", Color.DARK_CYAN, LocalDateTime.now(), rows.get());
            System.out.println();
            printNotice(outputPath + " saved!");
            writer.close();
        } catch (IOException e) {
            try {
                BufferedWriter writer = bufferedWriterAtomicReference.get();
                if (writer != null) {
                    writer.close();
                    Files.deleteIfExists(Paths.get(outputPath));
                }
            } catch (IOException ioException) {
                e.addSuppressed(ioException);
            }
            printError(e);
        }
    }

    public static void executeBatch(Baki baki, String path, AtomicReference<String> delimiterR) {
        String delimiter = delimiterR.get();
        String bPath = path.substring(1);
        if (Files.exists(Paths.get(bPath))) {
            printPrimary("Prepare to batch execute, chunk size is 1000, waiting...");
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

    public static void printQueryResult(Stream<DataRow> s, AtomicReference<View> viewMode, Consumer<DataRow> eachRowFunc) {
        AtomicBoolean first = new AtomicBoolean(true);
        try {
            switch (viewMode.get()) {
                case JSON:
                    if (eachRowFunc == null) {
                        s.forEach(row -> {
                            try {
                                ViewPrinter.printJSON(row, first);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } else {
                        s.forEach(row -> {
                            try {
                                ViewPrinter.printJSON(row, first);
                                eachRowFunc.accept(row);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                    if (viewMode.get() == View.JSON) {
                        printWarning("]");
                        System.out.println();
                    }
                    break;
                case TSV:
                    if (eachRowFunc == null) {
                        s.forEach(row -> ViewPrinter.printDSV(row, "\t", first));
                    } else {
                        s.forEach(row -> {
                            ViewPrinter.printDSV(row, "\t", first);
                            eachRowFunc.accept(row);
                        });
                    }
                    break;
                case CSV:
                    if (eachRowFunc == null) {
                        s.forEach(row -> ViewPrinter.printDSV(row, ",", first));
                    } else {
                        s.forEach(row -> {
                            ViewPrinter.printDSV(row, ",", first);
                            eachRowFunc.accept(row);
                        });
                    }
                    break;
                case EXCEL:
                    if (eachRowFunc == null) {
                        s.forEach(row -> ViewPrinter.printDSV(row, " | ", first));
                    } else {
                        s.forEach(row -> {
                            ViewPrinter.printDSV(row, " | ", first);
                            eachRowFunc.accept(row);
                        });
                    }
                    break;
            }
        } catch (Exception e) {
            printError(e);
        }
    }

    public static void printQueryResult(Stream<DataRow> s, AtomicReference<View> viewMode) {
        printQueryResult(s, viewMode, null);
    }

    public static void printHighlightSql(String sql) {
        printNotice(">>> ");
        System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sql.trim()));
    }

    public static void printError(Throwable e) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out)), true)) {
            writer.println(new Object() {
                @Override
                public String toString() {
                    StringWriter stringWriter = new StringWriter();
                    PrintWriter writer = new PrintWriter(stringWriter);
                    e.printStackTrace(writer);
                    StringBuffer buffer = stringWriter.getBuffer();
                    return buffer.toString();
                }
            });
            printDanger(out.toString());
        } catch (IOException ioException) {
            printDanger(ioException.toString());
        }
    }

    public static void printDanger(String msg) {
        Printer.println(msg, Color.RED);
    }

    public static void printWarning(String msg) {
        Printer.println(msg, Color.YELLOW);
    }

    public static void printInfo(String msg) {
        Printer.println(msg, Color.CYAN);
    }

    public static void printNotice(String msg) {
        Printer.println(msg, Color.SILVER);
    }

    public static void printPrimary(String msg) {
        Printer.println(msg, Color.DARK_CYAN);
    }
}
