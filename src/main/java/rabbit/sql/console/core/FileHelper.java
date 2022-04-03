package rabbit.sql.console.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.Lines;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.excel.io.BigExcelLineWriter;
import org.apache.poi.ss.usermodel.Sheet;
import rabbit.sql.console.types.View;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static rabbit.sql.console.core.PrintHelper.*;
import static rabbit.sql.console.core.PrintHelper.printError;

public final class FileHelper {
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
            AtomicLong i = new AtomicLong(0);
            AtomicBoolean first = new AtomicBoolean(true);
            s.forEach(row -> {
                try {
                    if (first.get()) {
                        Lines.writeLine(out, row.getNames(), d);
                        first.set(false);
                    }
                    Lines.writeLine(out, row.getValues(), d);
                    long offset = i.incrementAndGet();
                    if (offset % 10000 == 0) {
                        printPrimary(offset + " rows has written.");
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            printPrimary(i.get() + " rows write completed.");
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
            AtomicLong i = new AtomicLong(0);
            s.forEach(row -> {
                try {
                    if (first.get()) {
                        writer.write("[");
                        writer.write(PrintHelper.getJson(row));
                        first.set(false);
                    } else {
                        writer.write(", " + PrintHelper.getJson(row));
                    }
                    long offset = i.incrementAndGet();
                    if (offset % 10000 == 0) {
                        printPrimary(offset + " object has written.");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            writer.write("]");
            printPrimary(i.get() + " object write completed.");
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
            AtomicLong i = new AtomicLong(0);
            rowStream.forEach(row -> {
                if (first.get()) {
                    writer.writeRow(sheet, row.getNames().toArray());
                    first.set(false);
                }
                writer.writeRow(sheet, row.getValues());
                long offset = i.incrementAndGet();
                if (offset % 10000 == 0) {
                    printPrimary(offset + " rows has written.");
                }
            });
            writer.saveTo(filePath);
            printPrimary(i.get() + " rows write completed.");
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
                        printPrimary(i + " rows has written.");
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            printPrimary(rows.get() + " rows write completed.");
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
}
