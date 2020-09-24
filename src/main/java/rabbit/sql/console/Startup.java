package rabbit.sql.console;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.io.DSVWriter;
import rabbit.common.io.TSVWriter;
import rabbit.common.types.DataRow;
import rabbit.excel.Excels;
import rabbit.excel.io.ExcelWriter;
import rabbit.excel.type.ISheet;
import rabbit.sql.Light;
import rabbit.sql.console.core.CSVWriter;
import rabbit.sql.console.core.Command;
import rabbit.sql.console.core.DataSourceLoader;
import rabbit.sql.console.core.ViewPrinter;
import rabbit.sql.console.types.SqlType;
import rabbit.sql.console.types.View;
import rabbit.sql.console.util.SqlUtil;
import rabbit.sql.transaction.Tx;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
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
            Light light = dsLoader.getLight();

            if (light != null) {
                if (argMap.containsKey("-e")) {
                    String sql = argMap.get("-e");
                    SqlType sqlType = SqlUtil.getType(sql);
                    if (sqlType == SqlType.QUERY) {
                        try (Stream<DataRow> s = light.query(sql)) {
                            AtomicReference<View> mode = new AtomicReference<>(View.TSV);
                            if (argMap.containsKey("-f")) {
                                String format = argMap.get("-f");
                                mode.set(format.equals("csv") ?
                                        View.CSV : format.equals("json") ?
                                        View.JSON : format.equals("excel") ?
                                        View.EXCEL : View.TSV);
                            }
                            if (mode.get() == View.TSV || mode.get() == View.CSV) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    String suffix = mode.get() == View.TSV ? ".tsv" : ".csv";
                                    writeDSV(s, mode, path, suffix);
                                } else {
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    s.forEach(row -> ViewPrinter.printQueryResult(row, mode, first));
                                }
                            } else if (mode.get() == View.JSON) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    writeJSON(s, path);
                                } else {
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    s.forEach(row -> ViewPrinter.printQueryResult(row, mode, first));
                                    if (mode.get() == View.JSON) {
                                        System.out.print("\033[93m]\033[0m");
                                        System.out.println();
                                    }
                                }
                            } else if (mode.get() == View.EXCEL) {
                                if (argMap.containsKey("-s")) {
                                    String path = argMap.get("-s");
                                    int size = Optional.ofNullable(argMap.get("-b"))
                                            .map(Integer::parseInt).filter(v -> v > 0).orElse(-1);
                                    writeExcel(s, path, size);
                                } else {
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    s.forEach(row -> ViewPrinter.printQueryResult(row, mode, first));
                                }
                            }
                        }
                    } else if (sqlType == SqlType.OTHER) {
                        DataRow res = light.execute(sql);
                        System.out.println("execute " + res.getString("type") + ":" + res.getInt("result"));
                    } else {
                        System.out.println("function not support now!");
                    }
                    dsLoader.release();
                    System.exit(0);
                }

                // 进入交互模式
                Scanner scanner = new Scanner(System.in);
                System.out.print("\033[95msqlc> \033[0m");
                // 数据缓存
                Map<String, List<DataRow>> CACHE = new LinkedHashMap<>();
                // 输入字符串缓冲
                StringBuilder inputStr = new StringBuilder();
                // 事务是否活动标志
                AtomicBoolean txActive = new AtomicBoolean(false);
                // 输出的结果视图
                AtomicReference<View> viewMode = new AtomicReference<>(View.TSV);
                // 是否开启缓存
                AtomicBoolean enableCache = new AtomicBoolean(false);
                // 结果集缓存key自增
                AtomicInteger idx = new AtomicInteger(0);
                // 批量下载excel数据分页大小（默认不分页）
                AtomicInteger pageSize = new AtomicInteger(-1);
                // 保存文件格式验证正则
                Pattern SAVE_FILE_FORMAT = Pattern.compile("^:save *\\$(?<key>res[\\d]+) *> *(?<path>[\\S]+)$");
                // 直接保存查询结果到文件正则
                Pattern SAVE_QUERY_FORMAT = Pattern.compile("^:save *\\$\\{(?<sql>[\\s\\S]+)} *> *(?<path>[\\S]+)$");
                // 获取结果集区间正则
                Pattern GET_RES_RANGE_FORMAT = Pattern.compile("^:get *\\$(?<key>res[\\d]+) *< *(?<start>\\d+) *: *(?<end>\\d+)$");
                // 获取指定索引的结果正则
                Pattern GET_RES_IDX_FORMAT = Pattern.compile("^:get *\\$(?<key>res[\\d]+) *< *(?<index>\\d+)$");
                // 获取全部结果正则
                Pattern GET_ALL_FORMAT = Pattern.compile("^:get *\\$(?<key>res[\\d]+)$");
                // 删除缓存正则
                Pattern RM_CACHE_FORMAT = Pattern.compile("^:rm *\\$(?<key>res[\\d]+)$");
                // 查询缓存大小正则
                Pattern GET_SIZE_FORMAT = Pattern.compile("^:size *\\$(?<key>res[\\d]+)$");
                // 判断是否是内置指令正则
                Pattern IS_CMD_FORMAT = Pattern.compile("^:[a-z]+");
                // 判断分页下载大小正则
                Pattern PAGE_SIZE_FORMAT = Pattern.compile("^:batch *(?<size>\\d+)$");

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
                    Matcher m_cmd = IS_CMD_FORMAT.matcher(line);
                    if (m_cmd.find()) {
                        switch (line) {
                            case ":q":
                                break exit;
                            case ":help":
                                System.out.println(Command.get("--help"));
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":status":
                                System.out.println("\033[96mView Mode:" + viewMode.get() + "\033[0m");
                                System.out.println("\033[96mTransaction:" + (txActive.get() ? "enabled" : "disabled") + "\033[0m");
                                System.out.println("\033[96mCache:" + (enableCache.get() ? "enabled" : "disabled") + "\033[0m");
                                System.out.println("\033[96mExcel Batch save size:" + (pageSize.get() == -1 ? "unset" : pageSize.get()) + "\033[0m");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":c":
                                enableCache.set(true);
                                System.out.println("cache enabled!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":C":
                                enableCache.set(false);
                                System.out.println("cache disabled!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":clear":
                                CACHE.clear();
                                idx.set(0);
                                System.out.println("cache cleared!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":keys":
                                System.out.println(CACHE.keySet());
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":json":
                                viewMode.set(View.JSON);
                                System.out.println("use json view!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":tsv":
                                viewMode.set(View.TSV);
                                System.out.println("use tsv!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":csv":
                                viewMode.set(View.CSV);
                                System.out.println("use csv!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":excel":
                                viewMode.set(View.EXCEL);
                                System.out.println("use excel(grid) view!");
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":begin":
                                if (txActive.get()) {
                                    System.out.println("transaction is active now!");
                                } else {
                                    Tx.begin();
                                    txActive.set(true);
                                    System.out.println("open transaction!");
                                }
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":commit":
                                if (!txActive.get()) {
                                    System.out.println("transaction is not active now!");
                                } else {
                                    Tx.commit();
                                    txActive.set(false);
                                    System.out.println("commit transaction!");
                                }
                                printPrefix(txActive, "sqlc>");
                                break;
                            case ":rollback":
                                if (!txActive.get()) {
                                    System.out.println("transaction is not active now!");
                                } else {
                                    Tx.rollback();
                                    txActive.set(false);
                                    System.out.println("rollback transaction!");
                                }
                                printPrefix(txActive, "sqlc>");
                                break;
                            default:
                                Matcher m_getAll = GET_ALL_FORMAT.matcher(line);
                                Matcher m_getByIdx = GET_RES_IDX_FORMAT.matcher(line);
                                Matcher m_getByRange = GET_RES_RANGE_FORMAT.matcher(line);
                                Matcher m_rm = RM_CACHE_FORMAT.matcher(line);
                                Matcher m_save = SAVE_FILE_FORMAT.matcher(line);
                                Matcher m_size = GET_SIZE_FORMAT.matcher(line);
                                Matcher m_query_save = SAVE_QUERY_FORMAT.matcher(line);
                                Matcher m_page_size = PAGE_SIZE_FORMAT.matcher(line);

                                if (m_save.matches()) {
                                    String key = m_save.group("key");
                                    // 如果存在缓存
                                    if (CACHE.containsKey(key)) {
                                        List<DataRow> rows = CACHE.get(key);
                                        String path = m_save.group("path").trim();
                                        View mode = viewMode.get();
                                        if (mode == View.TSV || mode == View.CSV) {
                                            String suffix = mode == View.TSV ? ".tsv" : ".csv";
                                            DSVWriter writer = mode == View.TSV ? TSVWriter.of(path + suffix) : new CSVWriter(new FileOutputStream(path + suffix));
                                            System.out.println("\033[36mwaiting...\033[0m");
                                            for (DataRow row : rows) {
                                                writer.writeLine(row);
                                            }
                                            writer.close();
                                            System.out.println(path + suffix + " saved!");
                                        } else if (mode == View.JSON) {
                                            System.out.println("\033[36mwaiting...\033[0m");
                                            ViewPrinter.writeJsonArray(rows, path + ".json");
                                            System.out.println(path + ".json saved!");
                                        } else if (mode == View.EXCEL) {
                                            System.out.println("\033[36mwaiting...\033[0m");
                                            try (ExcelWriter writer = Excels.writer()) {
                                                ISheet sheet = ISheet.of(key, rows);
                                                writer.write(sheet).saveTo(path + ".xlsx");
                                                System.out.println(path + ".xlsx saved!");
                                            } catch (Exception e) {
                                                System.out.println(e.getMessage());
                                            }
                                        }
                                    } else {
                                        System.out.println("result:$" + key + "not exist!");
                                    }
                                    printPrefix(txActive, "sqlc>");
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
                                            System.out.print("\033[93m]\033[0m");
                                            System.out.println();
                                        }
                                        System.out.println(key + " loaded!");
                                    }
                                    printPrefix(txActive, "sqlc>");
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
                                                System.out.print("\033[93m]\033[0m");
                                                System.out.println();
                                            }
                                            System.out.println("line " + index + " of " + key + " loaded!");
                                        }
                                    }
                                    printPrefix(txActive, "sqlc>");
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
                                                System.out.print("\033[93m]\033[0m");
                                                System.out.println();
                                            }
                                            System.out.println("line " + start + " to " + end + " of " + key + " loaded!");
                                        }
                                    }
                                    printPrefix(txActive, "sqlc>");
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
                                    printPrefix(txActive, "sqlc>");
                                } else if (m_size.matches()) {
                                    String key = m_size.group("key");
                                    if (!CACHE.containsKey(key)) {
                                        System.out.println("no cached named " + key);
                                    } else {
                                        System.out.println(CACHE.get(key).size());
                                    }
                                    printPrefix(txActive, "sqlc>");
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
                                            writeExcel(s, path, pageSize.get());
                                        }
                                    } catch (Exception e) {
                                        System.out.println(e.getMessage());
                                    }
                                    printPrefix(txActive, "sqlc>");
                                } else if (m_page_size.matches()) {
                                    int size = Integer.parseInt(m_page_size.group("size"));
                                    pageSize.set(size);
                                    System.out.println("size: " + size + " !");
                                    printPrefix(txActive, "sqlc>");
                                } else {
                                    System.out.println("command not found or format invalid, command :help to get some help!");
                                    printPrefix(txActive, "sqlc>");
                                }
                                break;
                        }
                        //此分支为执行sql
                    } else {
                        inputStr.append(line);
                        // 如果sql没有以分号结尾，进入连续输入模式
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

                                    Stream<DataRow> rowStream = light.query(sql);
                                    AtomicBoolean first = new AtomicBoolean(true);
                                    rowStream.forEach(row -> {
                                        ViewPrinter.printQueryResult(row, viewMode, first);
                                        if (cacheEnabled) {
                                            queryResult.add(row);
                                        }
                                    });
                                    if (viewMode.get() == View.JSON) {
                                        System.out.print("\033[93m]\033[0m");
                                        System.out.println();
                                    }
                                    if (cacheEnabled) {
                                        System.out.println(key + ": added to cache!");
                                    }
                                    // 如果事务还在活动，则不关闭单前查询流对象，统一由用户输入指令提交或回滚
                                    // 如果当前没有事务，则查询完毕后就直接关闭
                                    if (!txActive.get()) {
                                        rowStream.close();
                                    }
                                    break;
                                case FUNCTION:
                                    System.out.println("function not support now!");
                                    break;
                                case OTHER:
                                    DataRow res = light.execute(sql);
                                    System.out.println("execute " + res.getString("type") + ":" + res.getInt("result"));
                                    break;
                                default:
                                    break;
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
            System.out.println(Command.get(args[0]));
        }
        System.exit(0);
    }

    public static void printPrefix(AtomicBoolean isTxActive, String mode) {
        String txActiveFlag = isTxActive.get() ? "[*]" : "";
        System.out.printf("\033[95m%s%s \033[0m", txActiveFlag, mode);
    }

    public static void writeDSV(Stream<DataRow> s, AtomicReference<View> mode, String path, String suffix) throws Exception {
        DSVWriter writer = mode.get() == View.TSV ? TSVWriter.of(path + suffix) : new CSVWriter(new FileOutputStream(path + suffix));
        System.out.println("\033[36mwaiting...\033[0m");
        AtomicLong i = new AtomicLong(1);
        s.forEach(row -> {
            try {
                writer.writeLine(row);
                long offset = i.getAndIncrement();
                if (offset % 10000 == 0) {
                    System.out.printf("\033[36m[%s] %s rows has written.\033[0m", LocalDateTime.now(), offset);
                    System.out.println();
                }
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        });
        writer.close();
        System.out.printf("\033[36m[%s] %s rows completed.\033[0m", LocalDateTime.now(), i.get());
        System.out.println();
        System.out.println(path + suffix + " saved!");
    }

    public static void writeJSON(Stream<DataRow> s, String path) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path + ".json"))) {
            System.out.println("\033[36mwaiting...\033[0m");
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
                        System.out.printf("\033[36m[%s] %s object has written.\033[0m", LocalDateTime.now(), offset);
                        System.out.println();
                    }
                } catch (JsonProcessingException e) {
                    log.error("json parse error:{}", e.getMessage());
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            });
            writer.write("]");
            System.out.printf("\033[36m[%s] %s object completed.\033[0m", LocalDateTime.now(), i.get());
            System.out.println();
            System.out.println(path + ".json saved!");
        }
    }

    public static void writeExcel(List<DataRow> rows, String path) {
        try (ExcelWriter writer = Excels.writer()) {
            ISheet sheet = ISheet.of("Sheet1", rows);
            writer.write(sheet).saveTo(path + ".xlsx");
            System.out.println(path + ".xlsx saved!");
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public static void writeExcel(Stream<DataRow> s, String path, int size) {
        System.out.println("\033[36mwaiting...\033[0m");
        if (size == -1) {
            writeExcel(s.collect(Collectors.toList()), path);
        } else {
            AtomicLong i = new AtomicLong(1);
            AtomicInteger z = new AtomicInteger(1);
            List<DataRow> pagedResource = new ArrayList<>();
            s.forEach(row -> {
                pagedResource.add(row);
                long offset = i.getAndIncrement();
                if (offset % size == 0) {
                    writeExcel(pagedResource, path + z.getAndIncrement());
                    pagedResource.clear();
                }
            });
            if (!pagedResource.isEmpty()) {
                writeExcel(pagedResource, path + z.getAndIncrement());
                pagedResource.clear();
            }
        }
    }
}
