package com.github.chengyuxing.sql.terminal.core;

import com.fasterxml.jackson.databind.MappingIterator;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.excel.Excels;
import com.github.chengyuxing.excel.io.ExcelReader;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;
import com.github.chengyuxing.sql.util.SqlUtils;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.JSON;

public class BatchInsertHelper {
    private static final Logger log = LoggerFactory.getLogger(BatchInsertHelper.class);

    public static void readFile4batch(BakiDao baki, String filePath, int sheetIdx, int headerIdx) throws Exception {
        Path file = Paths.get(filePath.trim());
        if (Files.exists(file)) {
            String fileName = file.getFileName().toString();
            int dotIdx = fileName.lastIndexOf(".");
            if (dotIdx != -1) {
                String ext = fileName.substring(dotIdx);
                String tableName = fileName.substring(0, fileName.lastIndexOf(ext));
                Stdout.printlnPrimary("prepare to batch execute, default chunk size is 1000, waiting...");
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
                        readExcel4batch(baki, file, tableName, sheetIdx, headerIdx);
                        break;
                    default:
                        throw new UnsupportedOperationException("extension'" + ext + "' file type not support.");
                }
            }
        } else {
            throw new FileNotFoundException("file [ " + file + " ] does not exists.");
        }
    }

    public static void readInsertSqlScriptBatchExecute(BakiDao baki, Path path) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        AtomicBoolean prepared = new AtomicBoolean(false);

        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("rows", "executed"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "rows", "execute")).start();
        try (Stream<String> s = Files.lines(path, StandardCharsets.UTF_8)) {
            StringBuilder sb = new StringBuilder();
            s.map(String::trim)
                    .filter(sql -> !sql.isEmpty() && !StringUtils.startsWithsIgnoreCase(sql, "--", "#", "/*"))
                    .forEach(sql -> {
                        sb.append(sql).append("\n");
                        if (sql.endsWith(";")) {
                            chunk.add(sb.substring(0, sb.length()));
                            sb.setLength(0);
                        }
                        if (example.get().isEmpty()) {
                            if (!chunk.isEmpty()) {
                                example.set(chunk.get(0));
                                boolean isPrepared = !baki.getSqlGenerator().generatePreparedSql(chunk.get(0), Collections.emptyMap()).getArgNameIndexMapping().isEmpty();
                                prepared.set(isPrepared);
                            }
                        }
                        if (chunk.size() == 1000) {
                            if (prepared.get()) {
                                try {
                                    preparedInsert4BlobBatchExecute(baki, chunk, path);
                                } catch (IOException e) {
                                    log.error("batch insert sql file error", e);
                                    throw new UncheckedIOException(e);
                                }
                            } else {
                                baki.execute(chunk);
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
                    baki.execute(chunk);
                }
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            log.error("batch insert sql file error", e);
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void preparedInsert4BlobBatchExecute(BakiDao baki, List<String> sqls, Path path) throws IOException {
        Path blobsDir = path.getParent().resolve("blobs");
        if (!Files.exists(blobsDir)) {
            throw new FileNotFoundException("Cannot find 'blobs' folder on " + path.getParent() + ".");
        }
        // Because the SQL only contains the blob fields for prepare,
        // other fields is just the literal value
        // insert into table (name, img) values ('cyx', :blob)
        for (String sql : sqls) {
            Set<String> names = baki.getSqlGenerator().generatePreparedSql(sql, Collections.emptyMap()).getArgNameIndexMapping().keySet();
            Map<String, Object> args = new HashMap<>();
            for (String name : names) {
                args.put(name, blobsDir.resolve(name).toFile());
            }
            baki.execute(sql, Collections.singletonList(args), Function.identity());
        }
    }

    public static void readJson4batch(BakiDao baki, Path path, String tableName) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("objects", "inserted"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "objects", "insert")).start();
        try (MappingIterator<Map<String, Object>> iterator = JSON.reader().forType(Map.class).readValues(path.toFile())) {
            while (iterator.hasNext()) {
                Map<String, Object> obj = iterator.next();

                if (example.get().isEmpty()) {
                    example.set(baki.getSqlGenerator().generateNamedParamInsert(tableName, obj.keySet()));
                }

                String insert = baki.getSqlGenerator().generateSql(example.get(), obj, v -> SqlUtils.toSqlLiteral(v, true));
                chunk.add(insert);

                if (chunk.size() == 1000) {
                    baki.execute(chunk);
                    chunk.clear();
                    pp.increment();
                }
            }
            if (!chunk.isEmpty()) {
                baki.execute(chunk);
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            log.error("batch insert json file error", e);
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void readDSV4batch(BakiDao baki, Path path, String tableName, String delimiter, int headerIdx) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("lines", "inserted"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "lines", "insert")).start();

        try (Stream<String> s = Files.lines(path, StandardCharsets.UTF_8)) {

            // tsv header index
            // -1 : do query table fields by tsv file name
            // >= 0 : collect current row as the fields and skip the line to the next
            // next all lines as the insert data

            AtomicReference<String[]> tableFields = new AtomicReference<>();
            int start = headerIdx;
            if (start < 0) {
                tableFields.set(baki.table(tableName).fields().toArray(new String[0]));
                start = 0;
            }
            int next = headerIdx < 0 ? 0 : 1;
            s.map(l -> l.split(delimiter))
                    .skip(start)
                    .peek(cols -> {
                        if (tableFields.get() == null) {
                            tableFields.set(cols);
                        }
                    })
                    .skip(next)
                    .forEach(cols -> {
                        if (example.get().isEmpty()) {
                            example.set(baki.getSqlGenerator().generateNamedParamInsert(tableName, Arrays.asList(tableFields.get())));
                        }

                        DataRow row = DataRow.of(tableFields, cols);
                        String insert = baki.getSqlGenerator().generateSql(example.get(), row, v -> SqlUtils.toSqlLiteral(v, true));
                        chunk.add(insert);

                        if (chunk.size() == 1000) {
                            baki.execute(chunk);
                            chunk.clear();
                            pp.increment();
                        }
                    });
            if (!chunk.isEmpty()) {
                baki.execute(chunk);
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            log.error("batch insert dsv file error", e);
            pp.interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void readExcel4batch(BakiDao baki, Path path, String tableName, int sheetIdx, int headerIdx) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("rows", "inserted"));
        pp.whenStopped(whenStoppedFunc(chunk, example, "rows", "insert")).start();
        try {
            ExcelReader reader = Excels.reader(path).sheetAt(sheetIdx);
            int skip = 0;
            if (headerIdx >= 0) {
                reader.namedHeaderAt(headerIdx, true);
                skip = 1;
            } else {
                reader.namedHeaderAt(-1, true);
                reader.fieldMap(baki.table(tableName).fields().toArray(new String[0]));
            }
            try (Stream<DataRow> s = reader.stream()) {
                s.skip(skip)
                        // Sometimes Excel file has much empty rows.
                        // skip the empty rows
                        .peek(d -> d.removeIf((k, v) -> k == null || k.trim().isEmpty()))
                        .peek(d -> d.removeIf((k, v) -> v == null || v.toString().isEmpty()))
                        .filter(d -> !d.isEmpty())
                        .forEach(d -> {
                            if (example.get().isEmpty()) {
                                example.set(baki.getSqlGenerator().generateNamedParamInsert(tableName, d.keySet()));
                            }
                            String insert = baki.getSqlGenerator().generateSql(example.get(), d, v -> SqlUtils.toSqlLiteral(v, true));
                            chunk.add(insert);

                            if (chunk.size() == 1000) {
                                baki.execute(chunk);
                                chunk.clear();
                                pp.increment();
                            }
                        });
                if (!chunk.isEmpty()) {
                    baki.execute(chunk);
                    pp.increment();
                }
                pp.stop();
            }
        } catch (Exception e) {
            log.error("batch insert excel file error", e);
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
            Stdout.printlnHighlightSql(example.get() + ", more...");
            Stdout.printlnPrimary("all of " + v + " chunks(" + rows + " " + name + ") " + op + " completed.(" + TimeUtil.format(c) + ")");
            chunk.clear();
        };
    }

    static BiFunction<Long, Long, String> formatter(String name, String op) {
        return (v, c) -> "chunk " + v + " " + op + ".(" + TimeUtil.format(c) + ")";
    }
}
