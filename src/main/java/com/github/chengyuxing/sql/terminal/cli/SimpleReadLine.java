package com.github.chengyuxing.sql.terminal.cli;

import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class SimpleReadLine {
    public static void readline(Consumer<LineReaderBuilder> consumer) throws IOException {
        try (Terminal terminal = TerminalBuilder.builder()
                .name("sqlc terminal")
                .encoding(StandardCharsets.UTF_8)
                .system(true)
                .build()) {
            LineReaderBuilder lb = LineReaderBuilder.builder().terminal(terminal);
            consumer.accept(lb);
        }
    }
}
