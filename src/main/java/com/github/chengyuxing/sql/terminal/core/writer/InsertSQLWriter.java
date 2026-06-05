package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.io.IOutput;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.SqlUtils.safeQuote;

public class InsertSQLWriter implements IWriter {
    private static final Logger log = LoggerFactory.getLogger(InsertSQLWriter.class);

    @Override
    public void write(Stream<DataRow> data, String output) throws IOException {
        // e.g: /usr/local/qbpt_deve.pinyin_ch.sql
        Path source = PathUtils.resolve(output.endsWith(".sql") ? output : output + ".sql");
        String filename = source.getFileName().toString();
        String tablename = filename.substring(0, filename.lastIndexOf("."));

        Stdout.printlnWarning("Extension '.sql' has been detected");
        Stdout.printlnWarning("Generate insert statement: " + filename + " -> insert into " + tablename + " ... ");
        Stdout.printlnPrimary("Waiting...");

        ProgressPrinter pp = ProgressPrinter.of("", " rows has written");

        // my_table_29103810820124.tmp
        //    |- my_table.sql
        //    |- blobs
        //       |- file_1
        //       |- file_2
        //       |- img_1
        //       |- ...

        Path tempDir = Files.createDirectory(PathUtils.createTmpFile(source));
        Path tempFile = tempDir.resolve(filename);
        Path tempBlobDir = Files.createDirectory(tempDir.resolve("blobs"));
        Path targetDir = source.getParent().resolve(tablename + "_" + System.currentTimeMillis());
        AtomicBoolean hasBlob = new AtomicBoolean(false);
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
            pp.finalize((value, during) -> {
                Stdout.printlnPrimary(value + " rows write completed (" + TimeUtils.format(during) + ")");
                if (hasBlob.get()) {
                    Stdout.printf("Built file (%s, blobs) : %s%n", Style.DARK_CYAN, filename, targetDir.toString());
                } else {
                    Stdout.printlnPrimary("Built file: " + source);
                }
            }).start();

            data.forEach(row -> {
                try {
                    Pair<String, List<String>> insertAndBlobKeys = generateInsert(tablename, row, pp.getValue());
                    writer.write(insertAndBlobKeys.getItem1() + ";");
                    writer.newLine();
                    // write blob files
                    if (!insertAndBlobKeys.getItem2().isEmpty()) {
                        for (String k : insertAndBlobKeys.getItem2()) {
                            IOutput out = () -> (byte[]) row.get(k);
                            out.saveTo(tempBlobDir.resolve(createBlobKey(pp.getValue(), k)));
                        }
                    }
                    pp.increment();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (PathUtils.isDirectoryEmpty(tempBlobDir)) {
                Files.move(tempFile, source, StandardCopyOption.REPLACE_EXISTING);
            } else {
                hasBlob.set(true);
                String readme = String.format("# Notice\n\n" +
                                "Please do not change files if you will batch insert to another table:\n\n" +
                                "-----------------\n\n" +
                                "- blobs\n" +
                                "- %s",
                        source.getFileName());
                Files.write(tempDir.resolve("README.md"), readme.getBytes(StandardCharsets.UTF_8));
                Files.move(tempDir, targetDir, StandardCopyOption.REPLACE_EXISTING);
            }
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            log.error("Write sql insert file error", e);
            throw new RuntimeException(e);
        } finally {
            PathUtils.deleteFileRecursive(tempDir);
        }
    }

    /**
     * Create blob unique key
     *
     * @param index row index
     * @param key   column name
     * @return key
     */
    private static String createBlobKey(long index, String key) {
        return key + "_" + index;
    }

    /**
     * Generate insert statement from row data, only the byte type to use prepare holder :blob, other just the literal value
     *
     * @param tableName table name
     * @param row       data row from query
     * @param rownum    row num index
     * @return statement and blob keys
     */
    private static Pair<String, List<String>> generateInsert(final String tableName, final Map<String, ?> row, long rownum) {
        StringJoiner f = new StringJoiner(", ");
        StringJoiner v = new StringJoiner(", ");
        List<String> blobKeys = new ArrayList<>();
        for (Map.Entry<String, ?> e : row.entrySet()) {
            if (e.getValue() instanceof byte[]) {
                f.add(e.getKey());
                v.add(":" + createBlobKey(rownum, e.getKey()));
                blobKeys.add(e.getKey());
            } else {
                f.add(e.getKey());
                v.add(safeQuote(e.getValue()));
            }
        }
        return Pair.of("insert into " + tableName + "(" + f + ") values (" + v + ")", blobKeys);
    }
}
