package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.core.writer.*;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.util.Stdout;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public final class FileHelper {

    public static final IWriter dsvWriter = new DSVWriter();
    public static final IWriter excelWriter = new ExcelWriter();
    public static final IWriter jsonWriter = new JSONWriter();
    public static final InsertSQLWriter sqlWriter = new InsertSQLWriter();

    public static void writeFile(BakiDao baki, String sqlOrRef, Map<String, Object> args, String output) throws IOException {
        try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...",
                () -> baki.query(sqlOrRef).args(args).stream())) {
            Stdout.printlnNotice("redirect query to file...");
            Path path = Paths.get(output);
            if (Files.isDirectory(path)) {
                path = path.resolve("query_result_" + System.currentTimeMillis());
            }
            writeFile(s, path.toString());
        }
    }

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

    public static boolean isFileURI(String path) {
        return new FileResource(path) {
            public boolean isURIPath() {
                return isURI();
            }
        }.isURIPath();
    }
}
