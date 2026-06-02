package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DSVWriter implements IWriter {
    private static final Logger log = LoggerFactory.getLogger(DSVWriter.class);

    @Override
    public void write(Stream<DataRow> data, String output) throws IOException {
        String fileName = output;
        if (!StringUtils.endsWithsIgnoreCase(fileName, ".tsv", ".csv")) {
            String suffix = Context.viewMode.get() == View.tsv ? ".tsv" : ".csv";
            fileName += suffix;
        }
        Path path = PathUtils.resolve(fileName);
        String d = Context.viewMode.get() == View.tsv ? "\t" : ",";

        Stdout.printlnPrimary("waiting...");

        ProgressPrinter pp = ProgressPrinter.of("", " rows has written.");
        pp.whenStopped((value, during) -> {
            Stdout.printlnPrimary(value + " rows write completed.( " + TimeUtil.format(during) + ")");
            Stdout.printlnNotice(path + " saved!");
        }).start();

        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            AtomicBoolean first = new AtomicBoolean(true);
            data.forEach(row -> {
                try {
                    if (first.get()) {
                        String line = String.join(d, row.names());
                        writer.write(line);
                        writer.newLine();
                        first.set(false);
                    }
                    String line = row.values().stream().map(v -> Objects.isNull(v) ? "" : v.toString()).collect(Collectors.joining(d));
                    writer.write(line);
                    writer.newLine();
                    pp.increment();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            pp.stop();
        } catch (Exception e) {
            log.error("write dsv({}) error", d, e);
            Files.deleteIfExists(path);
            pp.interrupt();
        }
    }
}
