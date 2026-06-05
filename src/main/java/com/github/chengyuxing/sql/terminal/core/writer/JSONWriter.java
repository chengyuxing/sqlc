package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;
import com.github.chengyuxing.sql.terminal.util.ObjectUtils;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class JSONWriter implements IWriter {
    private static final Logger log = LoggerFactory.getLogger(JSONWriter.class);

    @Override
    public void write(Stream<DataRow> data, String output) throws IOException {
        Path path = PathUtils.resolve(output.endsWith(".json") ? output : output + ".json");

        Stdout.printlnPrimary("Waiting...");

        ProgressPrinter pp = ProgressPrinter.of("", " object has written");

        PathUtils.usingTmpFile(path, temp -> {
            try (BufferedWriter writer = Files.newBufferedWriter(temp, StandardCharsets.UTF_8)) {
                pp.finalize((value, during) -> {
                    Stdout.printlnPrimary(value + " object write completed (" + TimeUtils.format(during) + ")");
                    Stdout.printlnNotice(path + " saved");
                }).start();

                AtomicBoolean first = new AtomicBoolean(true);
                writer.write("[");
                data.forEach(row -> {
                    try {
                        if (first.get()) {
                            writer.write(ObjectUtils.getJson(row));
                            first.set(false);
                        } else {
                            writer.write(", " + ObjectUtils.getJson(row));
                        }
                        pp.increment();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                writer.write("]");
                pp.stop();
            } catch (Exception e) {
                pp.interrupt();
                log.error("Write json error", e);
                throw new RuntimeException(e);
            }
        });
    }
}
