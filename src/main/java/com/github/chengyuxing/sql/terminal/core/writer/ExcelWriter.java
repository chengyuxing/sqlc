package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.excel.io.BigExcelLineWriter;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.TimeUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ExcelWriter implements IWriter {
    private static final Logger log = LoggerFactory.getLogger(ExcelWriter.class);

    @Override
    public void write(Stream<DataRow> data, String output) throws IOException {
        Path path = PathUtils.resolve(output.endsWith(".xlsx") ? output : output + ".xlsx");

        Stdout.printlnPrimary("Waiting...");

        ProgressPrinter pp = ProgressPrinter.of("", " rows has written");

        PathUtils.usingTmpFile(path, temp -> {
            try (BigExcelLineWriter writer = new BigExcelLineWriter(true)) {
                pp.finalize((value, during) -> {
                    Stdout.printlnPrimary(value + " rows write completed (" + TimeUtils.format(during) + ")");
                    Stdout.printlnNotice(path + " saved");
                }).start();

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
                writer.saveTo(temp);
                pp.stop();
            } catch (Exception e) {
                pp.interrupt();
                log.error("Write excel error", e);
                throw new RuntimeException(e);
            }
        });
    }
}
