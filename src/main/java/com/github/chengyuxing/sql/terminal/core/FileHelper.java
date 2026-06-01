package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.terminal.core.writer.*;
import com.github.chengyuxing.sql.terminal.cli.Context;

import java.io.*;
import java.util.stream.Stream;

public final class FileHelper {
    public static final IWriter dsvWriter = new DSVWriter();
    public static final IWriter excelWriter = new ExcelWriter();
    public static final IWriter jsonWriter = new JSONWriter();
    public static final InsertSQLWriter sqlWriter = new InsertSQLWriter();

    public static void writeFile(Stream<DataRow> stream, String path) throws IOException {
        if (path.endsWith(".sql")) {
            sqlWriter.write(stream, path);
            return;
        }
        switch (Context.viewMode.get()) {
            case json:
                jsonWriter.write(stream, path);
                break;
            case tsv:
            case csv:
                dsvWriter.write(stream, path);
                break;
            case excel:
                excelWriter.write(stream, path);
                break;
        }
    }

    public static long lineNumber(String file) throws IOException {
        try (FileReader fr = new FileReader(file);
             LineNumberReader lr = new LineNumberReader(fr)) {
            //noinspection ResultOfMethodCallIgnored
            lr.skip(Long.MAX_VALUE);
            return lr.getLineNumber() + 1;
        }
    }

    public static boolean isFilePath(String s) {
        String sep = File.separator;
        return s.startsWith(sep) || s.startsWith("." + sep) || s.startsWith(".." + sep);
    }
}
