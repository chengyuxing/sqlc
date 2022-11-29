package com.github.chengyuxing.sql.terminal.cli.component;

import org.jline.reader.impl.history.DefaultHistory;

import java.time.Instant;
import java.util.List;

public class SqlHistory extends DefaultHistory {
    private final List<String> sqlBuilder;

    public SqlHistory(List<String> sqlBuilder) {
        this.sqlBuilder = sqlBuilder;
    }

    @Override
    public void add(Instant time, String line) {
        if (line.startsWith(":")) {
            super.add(time, line);
            return;
        }
        if (line.endsWith(";")) {
            if (sqlBuilder.isEmpty()) {
                super.add(time, line);
                return;
            }
            String sql = String.join(" ", sqlBuilder) + " " + line;
            super.add(time, sql);
        }
    }
}
