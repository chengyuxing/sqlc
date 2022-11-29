package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import com.github.chengyuxing.sql.transaction.Tx;
import com.zaxxer.hikari.util.FastList;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class BatchInsertHelper {
    public static void readFile4batch(SingleBaki baki, String filePath) {
        Path file = Paths.get(filePath);
        if (Files.exists(file)) {
            try {
                String fileName = file.getFileName().toString();
                String ext = fileName.substring(fileName.lastIndexOf(".") + 1);
                PrintHelper.printlnPrimary("prepare to batch execute, default chunk size is 1000, waiting...");
                switch (ext) {
                    case "sql":
                        readInsertSqlScriptBatchExecute(baki, file);
                        break;
                    case "json":
                        break;
                    case "csv":
                        break;
                    case "tsv":
                        break;
                    case "xlsx":
                        break;
                }
            } catch (Exception e) {
                PrintHelper.printlnError(e);
            }
        } else {
            PrintHelper.printlnDanger(" file [ " + file + " ] does not exists.");
        }
    }

    public static void readInsertSqlScriptBatchExecute(SingleBaki baki, Path path) throws IOException {
        String delimiter = StatusManager.sqlDelimiter.get();
        FastList<String> chunk = new FastList<>(String.class);
        AtomicReference<String> example = new AtomicReference<>("");
        AtomicBoolean prepared = new AtomicBoolean(false);

        ProgressPrinter pp = new ProgressPrinter();
        pp.setStep(2);
        pp.setFormatter((v, c) -> "chunk " + v + "(" + v * 1000 + ") executed.(" + TimeUtil.format(c) + ")");
        pp.whenStopped((value, during) -> {
            long i = value;
            if (!chunk.isEmpty()) {
                i -= 1;
            }
            long rows = i * 1000 + chunk.size();
            PrintHelper.printlnHighlightSql(example.get() + ", more...");
            PrintHelper.printlnPrimary("all of " + value + " chunks(" + rows + ") execute completed.(" + TimeUtil.format(during) + ")");
            chunk.clear();
        }).start();

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
            throw e;
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


}
