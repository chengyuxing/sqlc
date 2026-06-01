package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.excel.io.BigExcelLineWriter;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;
import org.apache.poi.ss.usermodel.Sheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ExcelWriter implements IWriter {
    private static final Logger log = LoggerFactory.getLogger(ExcelWriter.class);

    @Override
    public void write(Stream<DataRow> data, String output) throws IOException {
        Path path = Paths.get(output.endsWith(".xlsx") ? output : output + ".xlsx");

        Stdout.printlnPrimary("waiting...");

        ProgressPrinter pp = ProgressPrinter.of("", " rows has written.");
        pp.whenStopped((value, during) -> {
            Stdout.printlnPrimary(value + " rows write completed.(" + TimeUtil.format(during) + ")");
            Stdout.printlnNotice(path + " saved!");
        }).start();

        try (BigExcelLineWriter writer = new BigExcelLineWriter(true)) {
            Sheet sheet = writer.createSheet("Sheet1");
            AtomicBoolean first = new AtomicBoolean(true);
            data.forEach(row -> {
                if (first.get()) {
                    writer.writeRow(sheet, row.names().toArray());
                    first.set(false);
                }
                writer.writeRow(sheet, row.values());
                pp.increment();
            });
            writer.saveTo(path);
            pp.stop();
        } catch (Exception e) {
            log.error("write excel error", e);
            Files.deleteIfExists(path);
            pp.interrupt();
        }
    }
}
