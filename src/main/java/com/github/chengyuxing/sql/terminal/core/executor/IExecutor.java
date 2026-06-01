package com.github.chengyuxing.sql.terminal.core.executor;

import org.jline.reader.LineReader;

public interface IExecutor {
    LineReader paramsReader(String sql);
}
