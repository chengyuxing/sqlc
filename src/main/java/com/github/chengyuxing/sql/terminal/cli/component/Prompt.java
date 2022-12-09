package com.github.chengyuxing.sql.terminal.cli.component;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Prompt {
    private final String host;
    private String value;
    private static final String DEFAULT = "sqlc> ";
    private static final String APPEND = ">> ";
    private Color color = Color.PURPLE;
    private Status status = Status.NEWLINE;

    public Prompt(String jdbcUrl) {
        Pattern p = Pattern.compile("(?<host>\\d{1,3}(\\.\\d{1,3}){3}(:\\d{1,5})?)");
        Matcher m = p.matcher(jdbcUrl);
        if (m.find()) {
            this.host = m.group("host") + "> ";
        } else {
            this.host = DEFAULT;
        }
        this.value = this.host;
    }

    public void newLine() {
        this.value = host;
        this.status = Status.NEWLINE;
    }

    public void append() {
        this.value = APPEND;
        this.status = Status.APPEND;
    }

    public void custom(String content) {
        this.value = content;
        this.status = Status.CUSTOM;
    }

    public String getValue() {
        return TerminalColor.colorful(value, color);
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public Status getStatus() {
        return status;
    }

    public static enum Status {
        NEWLINE,
        APPEND,
        CUSTOM
    }
}
