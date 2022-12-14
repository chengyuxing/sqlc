package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.Lines;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.excel.io.BigExcelLineWriter;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.terminal.util.Bytes2File;
import com.github.chengyuxing.sql.terminal.util.ObjectUtil;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import org.apache.poi.ss.usermodel.Sheet;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public final class FileHelper {
    public static void writeFile(Stream<DataRow> stream, String path) {
        path = Paths.get(path).toAbsolutePath().toString();
        if (path.endsWith(".sql")) {
            writeInsertSqlFile(stream, path);
            return;
        }
        switch (StatusManager.viewMode.get()) {
            case JSON:
                writeJSON(stream, path);
                break;
            case TSV:
            case CSV:
                writeDSV(stream, path);
                break;
            case EXCEL:
                writeExcel(stream, path);
                break;
        }
    }

    public static void writeDSV(Stream<DataRow> s, String path) {
        String fileName = path;
        if (!StringUtil.endsWithsIgnoreCase(fileName, ".tsv", ".csv")) {
            String suffix = StatusManager.viewMode.get() == View.TSV ? ".tsv" : ".csv";
            fileName += suffix;
        }
        final String resultFileName = fileName;
        AtomicReference<FileOutputStream> outputStreamAtomicReference = new AtomicReference<>(null);
        ProgressPrinter pp = ProgressPrinter.of("", " rows has written.");
        try {
            outputStreamAtomicReference.set(new FileOutputStream(fileName));
            BufferedOutputStream out = new BufferedOutputStream(outputStreamAtomicReference.get());
            String d = StatusManager.viewMode.get() == View.TSV ? "\t" : ",";
            PrintHelper.printlnPrimary("waiting...");
            pp.whenStopped((value, during) -> {
                PrintHelper.printlnPrimary(value + " rows write completed.( " + TimeUtil.format(during) + ")");
                PrintHelper.printlnNotice(resultFileName + " saved!");
            }).start();
            AtomicBoolean first = new AtomicBoolean(true);
            s.forEach(row -> {
                try {
                    if (first.get()) {
                        Lines.writeLine(out, row.names(), d);
                        first.set(false);
                    }
                    Lines.writeLine(out, row.values(), d);
                    pp.increment();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            out.close();
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            try {
                FileOutputStream out = outputStreamAtomicReference.get();
                if (out != null) {
                    out.close();
                    Files.deleteIfExists(Paths.get(fileName));
                }
            } catch (IOException ex) {
                e.addSuppressed(ex);
            }
            throw new RuntimeException(e);
        }
    }

    public static void writeJSON(Stream<DataRow> s, String path) {
        String fileName = path;
        if (!fileName.endsWith(".json")) {
            fileName += ".json";
        }
        Path filePath = Paths.get(fileName);
        AtomicReference<BufferedWriter> bufferedWriterAtomicReference = new AtomicReference<>(null);
        ProgressPrinter pp = ProgressPrinter.of("", " object has written.");
        try {
            bufferedWriterAtomicReference.set(Files.newBufferedWriter(filePath,StandardCharsets.UTF_8));
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            PrintHelper.printlnPrimary("waiting...");
            pp.whenStopped((value, during) -> {
                PrintHelper.printlnPrimary(value + " object write completed.(" + TimeUtil.format(during) + ")");
                PrintHelper.printlnNotice(filePath + " saved!");
            }).start();
            AtomicBoolean first = new AtomicBoolean(true);
            writer.write("[");
            s.forEach(row -> {
                try {
                    if (first.get()) {
                        writer.write(ObjectUtil.getJson(row));
                        first.set(false);
                    } else {
                        writer.write(", " + ObjectUtil.getJson(row));
                    }
                    pp.increment();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            writer.write("]");
            writer.close();
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            if (writer != null) {
                try {
                    writer.close();
                    Files.deleteIfExists(filePath);
                } catch (IOException ex) {
                    e.addSuppressed(ex);
                }
            }
            throw new RuntimeException(e);
        }
    }

    public static void writeExcel(Stream<DataRow> rowStream, String path) {
        String filePath = path;
        if (!filePath.endsWith(".xlsx")) {
            filePath += ".xlsx";
        }
        final String resultFilename = filePath;
        BigExcelLineWriter writer = new BigExcelLineWriter(true);
        ProgressPrinter pp = ProgressPrinter.of("", " rows has written.");
        try {
            PrintHelper.printlnPrimary("waiting...");
            pp.whenStopped((value, during) -> {
                PrintHelper.printlnPrimary(value + " rows write completed.(" + TimeUtil.format(during) + ")");
                PrintHelper.printlnNotice(resultFilename + " saved!");
            }).start();
            Sheet sheet = writer.createSheet("Sheet1");
            AtomicBoolean first = new AtomicBoolean(true);
            rowStream.forEach(row -> {
                if (first.get()) {
                    writer.writeRow(sheet, row.names().toArray());
                    first.set(false);
                }
                writer.writeRow(sheet, row.values());
                pp.increment();
            });
            writer.saveTo(filePath);
            writer.close();
            pp.stop();
        } catch (Exception e) {
            pp.interrupt();
            try {
                writer.close();
                Files.deleteIfExists(Paths.get(filePath));
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            throw new RuntimeException(e);
        }
    }

    public static void writeInsertSqlFile(Stream<DataRow> stream, String outputPath) {
        // e.g: /usr/local/qbpt_deve.pinyin_ch.sql
        Path path = Paths.get(outputPath);
        String currentDir = path.getParent().toString();
        // qbpt_deve.pinyin_ch.sql
        String fileName = path.getFileName().toString();
        // qbpt_deve.pinyin_ch
        String tableName = fileName.substring(0, fileName.lastIndexOf("."));
        PrintHelper.printlnWarning("Ignore view mode, output file name will as the insert sql script target table name!!!");
        PrintHelper.printlnWarning("e.g: " + fileName + " --> insert into " + tableName + " (...) values (...);");
        AtomicReference<BufferedWriter> bufferedWriterAtomicReference = new AtomicReference<>(null);
        ProgressPrinter pp = ProgressPrinter.of("", " rows has written.");
        try {
            bufferedWriterAtomicReference.set(Files.newBufferedWriter(path, StandardCharsets.UTF_8));
            BufferedWriter writer = bufferedWriterAtomicReference.get();
            AtomicBoolean hasBlob = new AtomicBoolean(false);

            final AtomicReference<String> fileDir = new AtomicReference<>("");
            final AtomicReference<String> blobsDir = new AtomicReference<>("");

            PrintHelper.printlnPrimary("waiting...");
            pp.whenStopped((value, during) -> {
                PrintHelper.printlnPrimary(value + " rows write completed.(" + TimeUtil.format(during) + ")");
                if (hasBlob.get()) {
                    try {
                        Files.move(path, Paths.get(fileDir.get(), fileName));
                        String readme = StringUtil.format("please do not change files if you will batch insert to another table:\n-----------------\n${blobs}\n${insert}", Args.create("blobs", blobsDir, "insert", path));
                        Files.write(Paths.get(fileDir.get(), "readme.txt"), readme.getBytes(StandardCharsets.UTF_8));
                        PrintHelper.printlnNotice(StringUtil.format("${a}(${b} and blobs) saved!", Args.create("a", fileDir, "b", fileName)));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    PrintHelper.printlnNotice(outputPath + " saved!");
                }
            }).start();

            stream.forEach(d -> {
                try {
                    Pair<String, List<String>> insertAndBlobKeys = SqlUtil.generateInsert(tableName, d, pp.getValue());
                    writer.write(insertAndBlobKeys.getItem1() + ";\n");
                    if (!insertAndBlobKeys.getItem2().isEmpty()) {
                        if (!hasBlob.get()) {
                            hasBlob.set(true);
                        }
                        for (String k : insertAndBlobKeys.getItem2()) {
                            if (fileDir.get().equals("")) {
                                fileDir.set(Paths.get(currentDir, tableName + "_" + System.currentTimeMillis()).toString());
                                blobsDir.set(Paths.get(fileDir.get(), "blobs").toString());
                                Files.createDirectory(Paths.get(fileDir.get()));
                                Files.createDirectory(Paths.get(blobsDir.get()));
                            }
                            Bytes2File b2f = new Bytes2File((byte[]) d.get(k));
                            b2f.saveTo(Paths.get(blobsDir.get(), "blob_" + pp.getValue() + "_" + k));
                        }
                    }
                    pp.increment();
                } catch (IOException e) {
                    throw new UncheckedIOException("write blob file error:" + blobsDir + "; " + fileDir, e);
                }
            });
            writer.close();
            pp.stop();
        } catch (IOException e) {
            pp.interrupt();
            try {
                BufferedWriter writer = bufferedWriterAtomicReference.get();
                if (writer != null) {
                    writer.close();
                    Files.deleteIfExists(path);
                }
            } catch (IOException ioException) {
                e.addSuppressed(ioException);
            }
            throw new UncheckedIOException(e);
        }
    }

    public static long lineNumber(String file) throws IOException {
        try (FileReader fr = new FileReader(file);
             LineNumberReader lr = new LineNumberReader(fr)) {
            //noinspection ResultOfMethodCallIgnored
            lr.skip(Long.MAX_VALUE);
            return lr.getLineNumber() + 1;
        }
    }

    public static boolean isFilePath(String s) {
        String sep = File.separator;
        return s.startsWith(sep) || s.startsWith("." + sep) || s.startsWith(".." + sep);
    }
}
