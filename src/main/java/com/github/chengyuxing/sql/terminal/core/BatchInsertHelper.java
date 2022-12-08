package com.github.chengyuxing.sql.terminal.core;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.Lines;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.excel.Excels;
import com.github.chengyuxing.excel.io.ExcelReader;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import com.github.chengyuxing.sql.transaction.Tx;
import com.zaxxer.hikari.util.FastList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BatchInsertHelper {
    static final ObjectMapper JSON = new ObjectMapper();

    public static void readFile4batch(SingleBaki baki, String filePath, int headerIdx) throws Exception {
        Path file = Paths.get(filePath);
        if (Files.exists(file)) {
            String fileName = file.getFileName().toString();
            int dotIdx = fileName.lastIndexOf(".");
            if (dotIdx != -1) {
                String ext = fileName.substring(dotIdx);
                String tableName = fileName.substring(0, fileName.lastIndexOf(ext));
                PrintHelper.printlnPrimary("prepare to batch execute, default chunk size is 1000, waiting...");
                switch (ext) {
                    case ".sql":
                        readInsertSqlScriptBatchExecute(baki, file);
                        break;
                    case ".json":
                        readJson4batch(baki, file, tableName);
                        break;
                    case ".csv":
                        readDSV4batch(baki, file, tableName, ",", headerIdx);
                        break;
                    case ".tsv":
                        readDSV4batch(baki, file, tableName, "\t", headerIdx);
                        break;
                    case ".xlsx":
                    case ".xls":
                        readExcel4batch(baki, file, tableName, headerIdx);
                        break;
                    default:
                        throw new UnsupportedOperationException("extension'" + ext + "' file type not support.");
                }
            }
        } else {
            throw new FileNotFoundException("file [ " + file + " ] does not exists.");
        }
    }

    public static void readInsertSqlScriptBatchExecute(SingleBaki baki, Path path) {
        String delimiter = StatusManager.sqlDelimiter.get();
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        AtomicBoolean prepared = new AtomicBoolean(false);

        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("rows", "executed"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "rows", "execute")).start();
        try (Stream<String> lineStream = Files.lines(path)) {
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
                        if (example.get().equals("")) {
                            if (!chunk.isEmpty()) {
                                example.set(chunk.get(0));
                                boolean isPrepared = !SqlUtil.sqlTranslator.getPreparedSql(chunk.get(0), Collections.emptyMap()).getItem2().isEmpty();
                                prepared.set(isPrepared);
                            }
                        }
                        if (chunk.size() == 1000) {
                            if (prepared.get()) {
                                try {
                                    preparedInsert4BlobBatchExecute(baki, chunk, path);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                baki.batchExecute(chunk);
                            }
                            chunk.clear();
                            pp.increment();
                        }
                    });
            if (sb.length() > 0) {
                chunk.add(sb.toString());
                sb.setLength(0);
            }
            if (!chunk.isEmpty()) {
                if (prepared.get()) {
                    preparedInsert4BlobBatchExecute(baki, chunk, path);
                } else {
                    baki.batchExecute(chunk);
                }
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    public static boolean preparedInsert4BlobBatchExecute(SingleBaki baki, List<String> sqls, Path path) throws IOException {
        Path blobsDir = Paths.get(path.getParent().toString() + File.separator + "blobs");
        if (Files.exists(blobsDir)) {
            if (!StatusManager.txActive.get()) {
                Tx.using(() -> {
                    for (String sql : sqls) {
                        List<String> names = SqlUtil.sqlTranslator.getPreparedSql(sql, Collections.emptyMap()).getItem2();
                        Map<String, Object> arg = new HashMap<>();
                        for (String name : names) {
                            arg.put(name, Paths.get(blobsDir + File.separator + name).toFile());
                        }
                        baki.executeNonQuery(sql, Collections.singletonList(arg));
                    }
                });
            }
            return true;
        }
        throw new FileNotFoundException("cannot find 'blobs' folder on " + path.getParent() + ".");
    }


    public static void readJson4batch(SingleBaki baki, Path path, String tableName) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("objects", "inserted"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "objects", "insert")).start();
        try (MappingIterator<Map<String, Object>> iterator = JSON.reader().forType(Map.class).readValues(path.toFile())) {
            while (iterator.hasNext()) {
                Map<String, Object> obj = iterator.next();
                chunk.add(SqlUtil.sqlTranslator.generateInsert(tableName, obj, Collections.emptyList()));
                if (example.get().equals("")) {
                    example.set(chunk.get(0));
                }
                if (chunk.size() == 1000) {
                    baki.batchExecute(chunk);
                    chunk.clear();
                    pp.increment();
                }
            }
            if (!chunk.isEmpty()) {
                baki.batchExecute(chunk);
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void readDSV4batch(SingleBaki baki, Path path, String tableName, String delimiter, int headerIdx) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("lines", "inserted"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "lines", "insert")).start();
        int start = headerIdx;
        String[] nameGeneric = new String[0];
        try (Stream<List<String>> lines = Lines.readLines(path, delimiter, StandardCharsets.UTF_8)) {
            List<String> tableFields = new ArrayList<>();
            if (start < 0) {
                tableFields.addAll(baki.getTableFields(tableName));
                start = 0;
            }
            int next = headerIdx < 0 ? 0 : 1;
            lines.skip(start)
                    .peek(cols -> {
                        if (tableFields.isEmpty()) {
                            tableFields.addAll(cols);
                        }
                    })
                    .skip(next)
                    .map(cols -> {
                        DataRow row = DataRow.of(tableFields.toArray(nameGeneric), cols.toArray());
                        return SqlUtil.sqlTranslator.generateInsert(tableName, row, Collections.emptyList());
                    })
                    .forEach(insert -> {
                        chunk.add(insert);
                        if (example.get().equals("")) {
                            example.set(chunk.get(0));
                        }
                        if (chunk.size() == 1000) {
                            baki.batchExecute(chunk);
                            chunk.clear();
                            pp.increment();
                        }
                    });
            if (!chunk.isEmpty()) {
                baki.batchExecute(chunk);
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void readExcel4batch(SingleBaki baki, Path path, String tableName, int headerIdx) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("rows", "inserted"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "rows", "insert")).start();
        try {
            ExcelReader reader = Excels.reader(path);
            int skip = 0;
            if (headerIdx >= 0) {
                reader.namedHeaderAt(headerIdx,true);
                skip = 1;
            } else {
                reader.namedHeaderAt(-1, true);
                reader.fieldMap(baki.getTableFields(tableName).toArray(new String[0]));
            }
            try (Stream<DataRow> s = reader.stream()) {
                s.skip(skip)
                        .peek(d -> d.removeIf((k, v) -> k == null || k.trim().equals("")))
                        .peek(d -> d.removeIf((k, v) -> v == null || v.toString().equals("")))
                        .filter(d -> !d.isEmpty())
                        .map(d -> SqlUtil.sqlTranslator.generateInsert(tableName, d, Collections.emptyList()))
                        .forEach(insert -> {
                            chunk.add(insert);
                            if (example.get().equals("")) {
                                example.set(chunk.get(0));
                            }
                            if (chunk.size() == 999) {
                                baki.batchExecute(chunk);
                                chunk.clear();
                                pp.increment();
                            }
                        });
                if (!chunk.isEmpty()) {
                    baki.batchExecute(chunk);
                    pp.increment();
                }
                pp.stop();
            }
        } catch (Exception e) {
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    static BiConsumer<Long, Long> whenStoppedFunc(List<String> chunk, AtomicReference<String> example, String name, String op) {
        return (v, c) -> {
            long i = v;
            if (!chunk.isEmpty()) {
                i -= 1;
            }
            long rows = i * 1000 + chunk.size();
            PrintHelper.printlnHighlightSql(example.get() + ", more...");
            PrintHelper.printlnPrimary("all of " + v + " chunks(" + rows + " " + name + ") " + op + " completed.(" + TimeUtil.format(c) + ")");
            chunk.clear();
        };
    }

    static BiFunction<Long, Long, String> formatter(String name, String op) {
        return (v, c) -> "chunk " + v + "(" + v * 1000 + " " + name + ") " + op + ".(" + TimeUtil.format(c) + ")";
    }
}
