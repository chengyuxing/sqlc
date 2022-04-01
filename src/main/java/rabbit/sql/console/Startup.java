package rabbit.sql.console;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.io.Lines;
import com.github.chengyuxing.excel.Excels;
import com.github.chengyuxing.excel.io.BigExcelLineWriter;
import com.github.chengyuxing.excel.io.ExcelWriter;
import com.github.chengyuxing.excel.type.XSheet;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.transaction.Tx;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Startup {
    private static final Logger log = LoggerFactory.getLogger("SQLC");

    @SuppressWarnings("unchecked")
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
            Baki light = dsLoader.getBaki();

            if (light != null) {
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
                    if (sql.startsWith("/")) {
                        if (!Files.exists(Paths.get(sql))) {
                            Printer.println("sql file [" + argMap.get("-b") + "] not exists.", Color.RED);
                            System.exit(0);
                        }
                        sql = String.join("\n", Files.readAllLines(Paths.get(sql)));
                    }
                    if (sql.contains(";;")) {
                        List<String> sqls = Stream.of(sql.split(";;"))
                                .filter(s -> !s.trim().equals("") && !s.matches("^[;\r\t\n]$"))
                                .collect(Collectors.toList());
                        // 如果有多段sql脚本，则批量执行并打印结果，但不能配合 -s 输出文件
                        if (sqls.size() > 1) {
                            if (argMap.containsKey("-s")) {
                                Printer.println("multi block sql script will not work with -s, only print executed result.", Color.YELLOW);
                            }
                            AtomicInteger success = new AtomicInteger(0);
                            AtomicInteger fail = new AtomicInteger(0);
                            sqls.forEach(sbql -> {
                                try {
                                    Printer.print(">>>: ", Color.SILVER);
                                    System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sbql));
                                    DataRow row = light.execute(sbql);
                                    Object res = row.get(0);
                                    Stream<DataRow> stream;
                                    if (res instanceof DataRow) {
                                        stream = Stream.of((DataRow) res);
                                    } else if (res instanceof List) {
                                        stream = ((List<DataRow>) res).stream();
                                    } else {
                                        stream = Stream.of(row);
                                    }
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    stream.forEach(rr -> ViewPrinter.printQueryResult(rr, viewMode, first));
                                    if (viewMode.get() == View.JSON) {
                                        Printer.print("]", Color.YELLOW);
                                        System.out.println();
                                    }
                                    success.incrementAndGet();
                                } catch (Exception e) {
                                    printError(e);
                                    fail.incrementAndGet();
                                }
                            });
                            Printer.println("Execute finished, success: " + success + ", fail: " + fail, Color.SILVER);
                            dsLoader.release();
                            System.exit(0);
                        }
                    }
                    SqlType sqlType = SqlUtil.getType(sql);
                    Printer.print(">>>: ", Color.SILVER);
                    System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sql));
                    if (sqlType == SqlType.QUERY) {
                        try (Stream<DataRow> s = light.query(sql)) {
                            if (viewMode.get() == View.TSV || viewMode.get() == View.CSV) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    String suffix = viewMode.get() == View.TSV ? ".tsv" : ".csv";
                                    writeDSV(s, viewMode, path, suffix);
                                } else {
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    s.forEach(row -> ViewPrinter.printQueryResult(row, viewMode, first));
                                }
                            } else if (viewMode.get() == View.JSON) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    writeJSON(s, path);
                                } else {
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    s.forEach(row -> ViewPrinter.printQueryResult(row, viewMode, first));
                                    if (viewMode.get() == View.JSON) {
                                        Printer.print("]", Color.YELLOW);
                                        System.out.println();
                                    }
                                }
                            } else if (viewMode.get() == View.EXCEL) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    writeExcel(s, path);
                                } else {
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    s.forEach(row -> ViewPrinter.printQueryResult(row, viewMode, first));
                                }
                            }
                        } catch (Exception e) {
                            printError(e);
                        }
                    } else if (sqlType == SqlType.OTHER) {
                        try {
                            DataRow res = light.execute(sql);
                            Printer.println("execute " + res.getString("type") + ": " + res.getInt("result"), Color.CYAN);
                        } catch (Exception e) {
                            printError(e);
                        }
                    } else if (sqlType == SqlType.FUNCTION) {
                        System.out.println("function not support now");
                    } else {
                        System.out.println("unKnow sql type, will not be execute!");
                    }
                    dsLoader.release();
                    System.exit(0);
                }
                if (argMap.containsKey("-b")) {
                    try {
                        if (Files.exists(Paths.get(argMap.get("-b")))) {
                            AtomicInteger success = new AtomicInteger(0);
                            AtomicInteger fail = new AtomicInteger(0);
                            Stream.of(String.join("\n", Files.readAllLines(Paths.get(argMap.get("-b")))).split(";;"))
                                    .filter(sql -> !sql.trim().equals("") && !sql.matches("^[;\r\t\n]$"))
                                    .forEach(sql -> {
                                        try {
                                            Printer.print(">>>: ", Color.SILVER);
                                            System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sql));
                                            DataRow row = light.execute(sql);
                                            Object res = row.get(0);
                                            Stream<DataRow> stream;
                                            if (res instanceof DataRow) {
                                                stream = Stream.of((DataRow) res);
                                            } else if (res instanceof List) {
                                                stream = ((List<DataRow>) res).stream();
                                            } else {
                                                stream = Stream.of(row);
                                            }
                                            AtomicBoolean first = new AtomicBoolean(true);
                                            stream.forEach(rr -> ViewPrinter.printQueryResult(rr, viewMode, first));
                                            if (viewMode.get() == View.JSON) {
                                                Printer.print("]", Color.YELLOW);
                                                System.out.println();
                                            }
                                            success.incrementAndGet();
                                        } catch (Exception e) {
                                            printError(e);
                                            fail.incrementAndGet();
                                        }
                                    });
                            Printer.println("Execute finished, success: " + success + ", fail: " + fail, Color.SILVER);
                        } else {
                            Printer.println("sql file [" + argMap.get("-b") + "] not exists.", Color.RED);
                        }
                    } catch (Exception e) {
                        printError(e);
                    }
                    dsLoader.release();
                    System.exit(0);
                }
                log.info("Welcome to sqlc {} ({}, {})", Version.RELEASE, System.getProperty("java.runtime.version"), System.getProperty("java.vm.name"));
                log.info("Type in sql script to execute query,ddl,dml..., Or try :help");
                // 进入交互模式
                Scanner scanner = new Scanner(System.in);
                Printer.print("sqlc> ", Color.PURPLE);
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
                Pattern SAVE_QUERY_FORMAT = Pattern.compile("^:save +\\$\\{(?<sql>[\\s\\S]+)} *> *(?<path>[\\S]+)$");
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

                //如果使用杀进程或ctrl+c结束，或者关机，退出程序的情况下，做一些收尾工作
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (txActive.get()) {
                        Tx.rollback();
                    }
                    dsLoader.release();
                    scanner.close();
                    System.out.println("Love you，Bye bye :(");
                }));

                exit:
                while (true) {
                    String line = scanner.nextLine().trim();
                    if (line.length() > 1 && line.startsWith(":")) {
                        switch (line) {
                            case ":q":
                                if (txActive.get()) {
                                    Printer.println("Warning: Transaction is active now, please :commit or :rollback before quit, Control c, server shutdown or kill command will be rollback transaction!", Color.YELLOW);
                                    break;
                                } else {
                                    break exit;
                                }
                            case ":help":
                            case ":h":
                                System.out.println(Command.get("--help"));
                                break;
                            case ":status":
                                Printer.println("View Mode:" + viewMode.get(), Color.CYAN);
                                Printer.println("Transaction:" + (txActive.get() ? "enabled" : "disabled"), Color.CYAN);
                                Printer.println("Cache:" + (enableCache.get() ? "enabled" : "disabled"), Color.CYAN);
                                break;
                            case ":c":
                                enableCache.set(true);
                                System.out.println("cache enabled!");
                                break;
                            case ":C":
                                enableCache.set(false);
                                CACHE.clear();
                                idx.set(0);
                                System.out.println("cache disabled!");
                                break;
                            case ":clear":
                                CACHE.clear();
                                idx.set(0);
                                System.out.println("cache cleared!");
                                break;
                            case ":keys":
                                System.out.println(CACHE.keySet());
                                break;
                            case ":json":
                                viewMode.set(View.JSON);
                                System.out.println("use json view!");
                                break;
                            case ":tsv":
                                viewMode.set(View.TSV);
                                System.out.println("use tsv!");
                                break;
                            case ":csv":
                                viewMode.set(View.CSV);
                                System.out.println("use csv!");
                                break;
                            case ":excel":
                                viewMode.set(View.EXCEL);
                                System.out.println("use excel(grid) view!");
                                break;
                            case ":begin":
                                if (txActive.get()) {
                                    System.out.println("transaction is active now!");
                                } else {
                                    Tx.begin();
                                    txActive.set(true);
                                    System.out.println("open transaction: [*]sqlc> means transaction is active now!");
                                }
                                break;
                            case ":commit":
                                if (!txActive.get()) {
                                    System.out.println("transaction is not active now!");
                                } else {
                                    Tx.commit();
                                    txActive.set(false);
                                }
                                break;
                            case ":rollback":
                                if (!txActive.get()) {
                                    System.out.println("transaction is not active now!");
                                } else {
                                    Tx.rollback();
                                    txActive.set(false);
                                }
                                break;
                            default:
                                Matcher m_getAll = GET_ALL_FORMAT.matcher(line);
                                Matcher m_getByIdx = GET_RES_IDX_FORMAT.matcher(line);
                                Matcher m_getByRange = GET_RES_RANGE_FORMAT.matcher(line);
                                Matcher m_rm = RM_CACHE_FORMAT.matcher(line);
                                Matcher m_save = SAVE_FILE_FORMAT.matcher(line);
                                Matcher m_size = GET_SIZE_FORMAT.matcher(line);
                                Matcher m_query_save = SAVE_QUERY_FORMAT.matcher(line);
                                Matcher m_load_sql = LOAD_SQL_FORMAT.matcher(line);

                                if (m_save.matches()) {
                                    String key = m_save.group("key");
                                    // 如果存在缓存
                                    if (CACHE.containsKey(key)) {
                                        List<DataRow> rows = CACHE.get(key);
                                        String path = m_save.group("path").trim();
                                        View mode = viewMode.get();
                                        if (mode == View.TSV || mode == View.CSV) {
                                            String suffix = mode == View.TSV ? ".tsv" : ".csv";
                                            String d = mode == View.TSV ? "\t" : ",";
                                            try (FileOutputStream out = new FileOutputStream(path + suffix)) {
                                                Printer.println("waiting...", Color.DARK_CYAN);
                                                boolean first = true;
                                                for (DataRow row : rows) {
                                                    if (first) {
                                                        Lines.writeLine(out, row.getNames(), d);
                                                        first = false;
                                                    }
                                                    Lines.writeLine(out, row.getValues(), d);
                                                }
                                            } catch (Exception e) {
                                                printError(e);
                                            }
                                            System.out.println(path + suffix + " saved!");
                                        } else if (mode == View.JSON) {
                                            Printer.println("waiting...", Color.DARK_CYAN);
                                            ViewPrinter.writeJsonArray(rows, path + ".json");
                                            System.out.println(path + ".json saved!");
                                        } else if (mode == View.EXCEL) {
                                            Printer.println("waiting...", Color.DARK_CYAN);
                                            try (ExcelWriter writer = Excels.writer()) {
                                                XSheet sheet = XSheet.of(key, rows);
                                                writer.write(sheet).saveTo(path + ".xlsx");
                                                System.out.println(path + ".xlsx saved!");
                                            } catch (Exception e) {
                                                printError(e);
                                            }
                                        }
                                    } else {
                                        System.out.println("result:$" + key + "not exist!");
                                    }
                                } else if (m_getAll.matches()) {
                                    String key = m_getAll.group("key");
                                    List<DataRow> rows = CACHE.get(key);
                                    if (rows == null || rows.isEmpty()) {
                                        System.out.println("0 rows cached!");
                                    } else {
                                        AtomicBoolean first = new AtomicBoolean(true);
                                        for (DataRow row : rows) {
                                            ViewPrinter.printQueryResult(row, viewMode, first);
                                        }
                                        if (viewMode.get() == View.JSON) {
                                            Printer.print("]", Color.YELLOW);
                                            System.out.println();
                                        }
                                        System.out.println(key + " loaded!");
                                    }
                                } else if (m_getByIdx.matches()) {
                                    String key = m_getByIdx.group("key");
                                    int index = Integer.parseInt(m_getByIdx.group("index"));
                                    List<DataRow> rows = CACHE.get(key);
                                    if (rows == null || rows.isEmpty()) {
                                        System.out.println("0 rows cached!");
                                    } else {
                                        if (index < 1 || index > rows.size()) {
                                            System.out.println("invalid index!");
                                        } else {
                                            ViewPrinter.printQueryResult(rows.get(index - 1), viewMode, new AtomicBoolean(true));
                                            if (viewMode.get() == View.JSON) {
                                                Printer.print("]", Color.YELLOW);
                                                System.out.println();
                                            }
                                            System.out.println("line " + index + " of " + key + " loaded!");
                                        }
                                    }
                                } else if (m_getByRange.matches()) {
                                    String key = m_getByRange.group("key");
                                    int start = Integer.parseInt(m_getByRange.group("start"));
                                    int end = Integer.parseInt(m_getByRange.group("end"));
                                    List<DataRow> rows = CACHE.get(key);
                                    if (rows == null || rows.isEmpty()) {
                                        System.out.println("0 rows cached!");
                                    } else {
                                        AtomicBoolean first = new AtomicBoolean(true);
                                        if (start < 1 || start > end || end > rows.size()) {
                                            System.out.println("invalid range!");
                                        } else {
                                            for (int i = start - 1; i <= end - 1; i++) {
                                                ViewPrinter.printQueryResult(rows.get(i), viewMode, first);
                                            }
                                            if (viewMode.get() == View.JSON) {
                                                Printer.print("]", Color.YELLOW);
                                                System.out.println();
                                            }
                                            System.out.println("line " + start + " to " + end + " of " + key + " loaded!");
                                        }
                                    }
                                } else if (m_rm.matches()) {
                                    String key = m_rm.group("key");
                                    if (!CACHE.containsKey(key)) {
                                        System.out.println("no cached named " + key);
                                    } else {
                                        List<DataRow> rows = CACHE.get(key);
                                        CACHE.remove(key);
                                        rows.clear();
                                        System.out.println(key + " removed!");
                                    }
                                } else if (m_size.matches()) {
                                    String key = m_size.group("key");
                                    if (!CACHE.containsKey(key)) {
                                        System.out.println("no cached named " + key);
                                    } else {
                                        System.out.println(CACHE.get(key).size());
                                    }
                                } else if (m_query_save.matches()) {
                                    // 查询直接导出记录
                                    String sql = m_query_save.group("sql");
                                    String path = m_query_save.group("path");
                                    try (Stream<DataRow> s = light.query(sql)) {
                                        View mode = viewMode.get();
                                        if (mode == View.TSV || mode == View.CSV) {
                                            String suffix = mode == View.TSV ? ".tsv" : ".csv";
                                            writeDSV(s, viewMode, path, suffix);
                                        } else if (mode == View.JSON) {
                                            writeJSON(s, path);
                                        } else if (mode == View.EXCEL) {
                                            writeExcel(s, path);
                                        }
                                    } catch (Exception e) {
                                        printError(e);
                                    }
                                } else if (m_load_sql.matches()) {
                                    String path = m_load_sql.group("path").trim();
                                    if (path.length() > 0) {
                                        if (Files.exists(Paths.get(path))) {
                                            try {
                                                AtomicInteger success = new AtomicInteger(0);
                                                AtomicInteger fail = new AtomicInteger(0);
                                                Stream.of(String.join("\n", Files.readAllLines(Paths.get(path))).split(";;"))
                                                        .filter(sql -> !sql.trim().equals("") && !sql.matches("^[;\r\t\n]$"))
                                                        .forEach(sql -> {
                                                            try {
                                                                Printer.print(">>>: ", Color.SILVER);
                                                                System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sql));
                                                                DataRow row = light.execute(sql);
                                                                Object res = row.get(0);
                                                                Stream<DataRow> stream;
                                                                if (res instanceof DataRow) {
                                                                    stream = Stream.of((DataRow) res);
                                                                } else if (res instanceof List) {
                                                                    stream = ((List<DataRow>) res).stream();
                                                                } else {
                                                                    stream = Stream.of(row);
                                                                }

                                                                List<DataRow> queryResult = new ArrayList<>();
                                                                String key = "";
                                                                boolean cacheEnabled = enableCache.get();
                                                                if (cacheEnabled) {
                                                                    key = "res" + idx.getAndIncrement();
                                                                    CACHE.put(key, queryResult);
                                                                }
                                                                AtomicBoolean first = new AtomicBoolean(true);
                                                                stream.forEach(rr -> {
                                                                    ViewPrinter.printQueryResult(rr, viewMode, first);
                                                                    queryResult.add(rr);
                                                                });
                                                                if (viewMode.get() == View.JSON) {
                                                                    Printer.print("]", Color.YELLOW);
                                                                    System.out.println();
                                                                }
                                                                if (cacheEnabled) {
                                                                    System.out.println(key + ": added to cache!");
                                                                }
                                                                if (txActive.get()) {
                                                                    Printer.println("WARN: transaction is active now, go on...", Color.YELLOW);
                                                                }
                                                            } catch (Exception e) {
                                                                printError(e);
                                                            }
                                                        });
                                                Printer.println("Execute finished, success: " + success + ", fail: " + fail, Color.SILVER);
                                            } catch (Exception e) {
                                                printError(e);
                                            }
                                        } else {
                                            Printer.println("sql file [ " + path + " ] not exists.", Color.RED);
                                        }
                                    } else {
                                        Printer.println("please input the file path.", Color.YELLOW);
                                    }
                                } else {
                                    System.out.println("command not found or format invalid, command :help to get some help!");
                                }
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
                                        try (Stream<DataRow> rowStream = light.query(sql)) {
                                            AtomicBoolean first = new AtomicBoolean(true);
                                            rowStream.forEach(row -> {
                                                ViewPrinter.printQueryResult(row, viewMode, first);
                                                if (cacheEnabled) {
                                                    queryResult.add(row);
                                                }
                                            });
                                            if (viewMode.get() == View.JSON) {
                                                Printer.print("]", Color.YELLOW);
                                                System.out.println();
                                            }
                                            if (cacheEnabled) {
                                                System.out.println(key + ": added to cache!");
                                            }
                                            if (txActive.get()) {
                                                Printer.println("WARN: transaction is active now, go on...", Color.YELLOW);
                                            }
                                        } catch (Exception e) {
                                            printError(e);
                                        }
                                        break;
                                    case FUNCTION:
                                        System.out.println("function not support now!");
                                        break;
                                    case OTHER:
                                        try {
                                            DataRow res = light.execute(sql);
                                            System.out.println("execute " + res.getString("type") + ":" + res.getInt("result"));
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

    public static void writeDSV(Stream<DataRow> s, AtomicReference<View> mode, String path, String suffix) {
        String fileName = path + suffix;
        AtomicReference<FileOutputStream> outputStreamAtomicReference = new AtomicReference<>(null);
        try {
            outputStreamAtomicReference.set(new FileOutputStream(fileName));
            FileOutputStream out = outputStreamAtomicReference.get();
            String d = mode.get() == View.TSV ? "\t" : ",";
            Printer.println("waiting...", Color.DARK_CYAN);
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
            Printer.printf("[%s] %s rows completed.", Color.DARK_CYAN, LocalDateTime.now(), i.get());
            System.out.println();
            System.out.println(fileName + " saved!");
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
        Path filePath = Paths.get(path + ".json");
        AtomicReference<BufferedWriter> bufferedWriterAtomicReference = new AtomicReference<>(null);
        try {
            bufferedWriterAtomicReference.set(Files.newBufferedWriter(filePath));
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            Printer.println("waiting...", Color.DARK_CYAN);
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
            Printer.printf("[%s] %s object completed.", Color.DARK_CYAN, LocalDateTime.now(), i.get());
            System.out.println();
            System.out.println(path + ".json saved!");
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
        String filePath = path + ".xlsx";
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
            System.out.println(filePath + " saved!");
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
            Printer.println(out.toString(), Color.RED);
        } catch (IOException ioException) {
            Printer.println(ioException.toString(), Color.RED);
        }
    }
}
