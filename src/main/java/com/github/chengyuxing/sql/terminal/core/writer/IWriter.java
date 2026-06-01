package com.github.chengyuxing.sql.terminal.core.writer;

import com.github.chengyuxing.common.DataRow;

import java.io.IOException;
import java.util.stream.Stream;

public interface IWriter {
    void write(Stream<DataRow> data, String output) throws IOException;
}
