package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.IOutput;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.SqlUtil.safeQuote;

public class InsertSQLWriter implements IWriter {
    private static final Logger log = LoggerFactory.getLogger(InsertSQLWriter.class);

    @Override
    public void write(Stream<DataRow> data, String output) throws IOException {
        // e.g: /usr/local/qbpt_deve.pinyin_ch.sql
        Path path = Paths.get(output.endsWith(".sql") ? output : output + ".sql");
        String currentDir = path.getParent().toString();
        // qbpt_deve.pinyin_ch.sql
        String fileName = path.getFileName().toString();
        // qbpt_deve.pinyin_ch
        String tableName = fileName.substring(0, fileName.lastIndexOf("."));

        Stdout.printlnWarning("Extension '.sql' has been detected!");
        Stdout.printlnWarning("Generate insert statement file: '" + fileName + "' -> 'insert into " + tableName + " ... '");
        Stdout.printlnPrimary("waiting...");

        // if the data doesn't contain the blob type, just save the insert file, otherwise
        // 1. write the insert file
        // 2. create new file dir
        // 3. create blobs dir to save blob data
        // 4. move the insert file to the new file dir

        // my_table_29103810820124
        //    |- my_table.sql
        //    |- blobs
        //       |- file_1
        //       |- file_2
        //       |- img_1
        //       |- ...

        Path fileDir = Paths.get(currentDir, tableName + "_" + System.currentTimeMillis());
        Path blobDir = fileDir.resolve("blobs");
        AtomicBoolean hasBlob = new AtomicBoolean(false);

        ProgressPrinter pp = ProgressPrinter.of("", " rows has written.");
        pp.whenStopped((value, during) -> {
            if (hasBlob.get()) {
                try {
                    Files.move(path, fileDir.resolve(fileName));

                    String readme = StringUtils.FMT.format("# Notice\n\n" +
                                    "Please do not change files if you will batch insert to another table:\n\n" +
                                    "-----------------\n\n" +
                                    "- ${insert}\n" +
                                    "- ${blobs}",
                            Args.of("blobs", blobDir.getFileName().toString(),
                                    "insert", path.getFileName().toString()));

                    Files.write(fileDir.resolve("README.md"), readme.getBytes(StandardCharsets.UTF_8));

                    Stdout.printlnNotice(StringUtils.FMT.format("${a}(${b}, blobs) saved!",
                            Args.of("a", fileDir.toString(), "b", fileName)));

                    Stdout.printlnPrimary(value + " rows write completed.(" + TimeUtil.format(during) + ")");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                Stdout.printlnNotice(path + " saved!");
            }
        }).start();

        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            data.forEach(row -> {
                try {
                    Pair<String, List<String>> insertAndBlobKeys = generateInsert(tableName, row, pp.getValue());
                    writer.write(insertAndBlobKeys.getItem1() + ";");
                    writer.newLine();

                    if (!insertAndBlobKeys.getItem2().isEmpty()) {
                        if (!hasBlob.get()) {
                            hasBlob.set(true);
                            Files.createDirectory(fileDir);
                            Files.createDirectory(blobDir);
                            log.info("create file dirs: {} , {}", fileDir, blobDir);
                        }
                        // do save blob file
                        for (String k : insertAndBlobKeys.getItem2()) {
                            IOutput out = () -> (byte[]) row.get(k);
                            out.saveTo(blobDir.resolve(createBlobKey(pp.getValue(), k)));
                        }
                    }
                    pp.increment();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            pp.stop();
        } catch (Exception e) {
            log.error("Write sql insert file error", e);
            Files.deleteIfExists(path);
            Files.deleteIfExists(fileDir);
            pp.interrupt();
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
