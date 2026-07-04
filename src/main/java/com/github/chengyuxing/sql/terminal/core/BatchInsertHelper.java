package com.github.chengyuxing.sql.terminal.core;

import com.fasterxml.jackson.databind.MappingIterator;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.excel.Excels;
import com.github.chengyuxing.excel.io.ExcelReader;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.TimeUtils;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtils.JSON;

public class BatchInsertHelper {
    private static final Logger log = LoggerFactory.getLogger(BatchInsertHelper.class);

    private static final int chunkSize = 1000;

    public static void readFile4batch(BakiDao baki, String filePath, int sheetIdx, int headerIdx) throws Exception {
        Path file = PathUtils.resolve(filePath);
        if (Files.exists(file)) {
            String fileName = file.getFileName().toString();
            int dotIdx = fileName.lastIndexOf(".");
            if (dotIdx != -1) {
                String ext = fileName.substring(dotIdx);
                String tableName = fileName.substring(0, fileName.lastIndexOf(ext));
                Stdout.printlnPrimary("Prepare to batch execute, waiting...");
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
                        throw new UnsupportedOperationException("Extension'" + ext + "' file type not support");
                }
            } else {
                throw new UnsupportedOperationException("Unknow file type");
            }
        } else {
            throw new FileNotFoundException("File [ " + file + " ] does not exists");
        }
    }

    public static void readInsertSqlScriptBatchExecute(BakiDao baki, Path path) {
        FastList<String> nonPreparedChunk = new FastList<>(String.class);
        Map<String, List<Map<String, Object>>> preparedChunk = new HashMap<>();
        AtomicReference<String> example = new AtomicReference<>("");
        Path blobsDir = path.getParent().resolve("blobs");

        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("rows", "executed"));
        try (Stream<String> s = Files.lines(path, StandardCharsets.UTF_8)) {
            pp.finalize((v, c) -> {
                long i = v;
                long prepareSize = prepareArgsSize(preparedChunk);
                long total = nonPreparedChunk.size() + prepareSize;
                if (total != 0) {
                    i -= 1;
                }
                long rows = i * chunkSize + total;
                Stdout.printlnHighlightSql(example.get() + ", more...");
                Stdout.printlnPrimary("All of " + v + " chunks(" + rows + " rows) execute completed (" + TimeUtils.format(c) + ")");
            }).start();
            StringBuilder sb = new StringBuilder();
            s.map(String::trim)
                    .filter(sql -> !sql.isEmpty() && !StringUtils.startsWithsIgnoreCase(sql, "--", "#", "/*"))
                    .forEach(sql -> {
                        sb.append(sql).append("\n");
                        if (sql.endsWith(";")) {
                            String fullSql = sb.substring(0, sb.length());
                            sb.setLength(0);

                            if (example.get().isEmpty()) {
                                example.set(fullSql);
                            }

                            // Because the SQL only contains the blob fields for prepare,
                            // other fields is just the literal value
                            // insert into table (name, img) values ('cyx', :blob)

                            Map<String, List<Integer>> mapping = baki.getSqlGenerator()
                                    .generatePreparedSql(fullSql, Collections.emptyMap())
                                    .getArgNameIndexMapping();
                            if (mapping.isEmpty()) {
                                nonPreparedChunk.add(fullSql);
                            } else {
                                Map<String, Object> args = new HashMap<>();
                                for (String name : mapping.keySet()) {
                                    args.put(name, blobsDir.resolve(name).toFile());
                                }
                                List<Map<String, Object>> argsList = preparedChunk.computeIfAbsent(fullSql, k -> new FastList<>(Map.class));
                                argsList.add(args);
                            }
                        }
                        if (nonPreparedChunk.size() + prepareArgsSize(preparedChunk) >= chunkSize) {
                            try {
                                prepareInsertWithBlobBatchExecute(baki, nonPreparedChunk, preparedChunk);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            nonPreparedChunk.clear();
                            preparedChunk.clear();
                            pp.increment();
                        }
                    });
            if (!nonPreparedChunk.isEmpty() || !preparedChunk.isEmpty()) {
                prepareInsertWithBlobBatchExecute(baki, nonPreparedChunk, preparedChunk);
                pp.increment();
            }
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            log.error("Batch insert sql file error", e);
            throw new RuntimeException(e);
        }
    }

    private static int prepareArgsSize(Map<String, List<Map<String, Object>>> prepared) {
        int i = 0;
        for (Map.Entry<String, List<Map<String, Object>>> e : prepared.entrySet()) {
            i += e.getValue().size();
        }
        return i;
    }

    public static void prepareInsertWithBlobBatchExecute(BakiDao baki, List<String> nonPrepared, Map<String, List<Map<String, Object>>> prepared) throws IOException {
        baki.execute(nonPrepared);
        for (Map.Entry<String, List<Map<String, Object>>> e : prepared.entrySet()) {
            baki.execute(e.getKey(), e.getValue());
        }
    }

    public static void readJson4batch(BakiDao baki, Path path, String tableName) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("objects", "inserted"));
        try (MappingIterator<Map<String, Object>> iterator = JSON.reader().forType(Map.class).readValues(path.toFile())) {
            pp.finalize(whenStoppedFunc(chunk, example, "objects")).start();
            while (iterator.hasNext()) {
                Map<String, Object> obj = iterator.next();

                if (example.get().isEmpty()) {
                    example.set(baki.getSqlGenerator().generateNamedParamInsert(tableName, obj.keySet(), null));
                }

                String insert = baki.getSqlGenerator()
                        .generateSql(example.get(), obj, v -> SqlUtils.toSqlLiteral(v, true));
                chunk.add(insert);

                if (chunk.size() == chunkSize) {
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
            pp.interrupt();
            log.error("Batch insert json file error", e);
            throw new RuntimeException(e);
        }
    }

    public static void readDSV4batch(BakiDao baki, Path path, String tableName, String delimiter, int headerIdx) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("lines", "inserted"));

        try (Stream<String> s = Files.lines(path, StandardCharsets.UTF_8)) {
            pp.finalize(whenStoppedFunc(chunk, example, "lines")).start();

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
                            example.set(baki.getSqlGenerator().generateNamedParamInsert(tableName, Arrays.asList(tableFields.get()), null));
                        }

                        DataRow row = DataRow.of(tableFields.get(), cols);
                        String insert = baki.getSqlGenerator()
                                .generateSql(example.get(), row, v -> SqlUtils.toSqlLiteral(v, true));
                        chunk.add(insert);

                        if (chunk.size() == chunkSize) {
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
            pp.interrupt();
            log.error("Batch insert dsv file error", e);
            throw new RuntimeException(e);
        }
    }

    public static void readExcel4batch(BakiDao baki, Path path, String tableName, int sheetIdx, int headerIdx) {
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter(formatter("rows", "inserted"));
        try {
            pp.finalize(whenStoppedFunc(chunk, example, "rows")).start();

            ExcelReader reader = Excels.reader(path);
            // sheet index <= -1 then filename is table name,
            // otherwise sheet name is table name
            String finalTableName = sheetIdx >= 0
                    ? reader.getSheets().get(sheetIdx).getName()
                    : tableName;
            int finalSheetIdx = Math.max(sheetIdx, 0);

            reader.sheetAt(finalSheetIdx);

            int skip = 0;
            if (headerIdx >= 0) {
                reader.namedHeaderAt(headerIdx, true);
                skip = 1;
            } else {
                reader.namedHeaderAt(-1, true);
                reader.fieldMap(baki.table(finalTableName).fields().toArray(new String[0]));
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
                                example.set(baki.getSqlGenerator().generateNamedParamInsert(finalTableName, d.keySet(), null));
                            }
                            String insert = baki.getSqlGenerator()
                                    .generateSql(example.get(), d, v -> SqlUtils.toSqlLiteral(v, true));
                            chunk.add(insert);

                            if (chunk.size() == chunkSize) {
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
            pp.interrupt();
            log.error("Batch insert excel file error", e);
            throw new RuntimeException(e);
        }
    }

    static BiConsumer<Long, Long> whenStoppedFunc(List<String> chunk, AtomicReference<String> example, String name) {
        return (v, c) -> {
            long i = v;
            if (!chunk.isEmpty()) {
                i -= 1;
            }
            long rows = i * chunkSize + chunk.size();
            Stdout.printlnHighlightSql(example.get() + ", more...");
            Stdout.printlnPrimary("All of " + v + " chunks(" + rows + " " + name + ") insert completed (" + TimeUtils.format(c) + ")");
            chunk.clear();
        };
    }

    static BiFunction<Long, Long, String> formatter(String name, String op) {
        return (v, c) -> "chunk " + v + " " + op + ".(" + TimeUtils.format(c) + ")";
    }
}
