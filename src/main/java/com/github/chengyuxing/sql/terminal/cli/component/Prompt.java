package com.github.chengyuxing.sql.terminal.cli.component;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.terminal.cli.Help;
import com.github.chengyuxing.sql.terminal.common.Stdout;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Prompt {
    private final String host;
    private String value;
    private static final String DEFAULT = Help.appSimpleName + "> ";
    private static final String APPEND = ">> ";
    private Style style = Style.PURPLE;
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

    public void param(String content) {
        this.value = content;
        this.status = Status.PARAM;
    }

    public String getValue() {
        return Stdout.colorful(value, style);
    }

    public void setStyle(Style style) {
        this.style = style;
    }

    public Status getStatus() {
        return status;
    }

    public enum Status {
        NEWLINE,
        APPEND,
        CUSTOM,
        PARAM
    }
}
