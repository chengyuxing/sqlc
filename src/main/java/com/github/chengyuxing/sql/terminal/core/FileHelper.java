package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.completer.ExecCompleter;
import com.github.chengyuxing.sql.terminal.cli.interactive.Commands;
import com.github.chengyuxing.sql.terminal.core.writer.*;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.util.PathUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.PathUtils.isFileURI;

public final class FileHelper {

    public static final IWriter dsvWriter = new DSVWriter();
    public static final IWriter excelWriter = new ExcelWriter();
    public static final IWriter jsonWriter = new JSONWriter();
    public static final InsertSQLWriter sqlWriter = new InsertSQLWriter();

    public static void writeFile(BakiDao baki, String sqlOrRef, Map<String, Object> args, String output) throws IOException {
        try (Stream<DataRow> s = WaitingPrinter.waiting("preparing...",
                () -> baki.query(sqlOrRef).args(args).stream())) {
            Stdout.printlnNotice("redirect query to file...");
            Path path = PathUtils.resolve(output);
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

    public static String[] getFiles(Path dir, String extension) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile)
                    .map(Path::toString)
                    .filter(p -> p.endsWith(extension))
                    .toArray(String[]::new);
        }
    }

    public static void loadXqlFiles(XQLFileManager xqlFileManager, String... files) {
        for (String file : files) {
            if (!file.endsWith(".xql")) {
                continue;
            }
            if (isFileURI(file)) {
                xqlFileManager.add(file);
                continue;
            }
            String uri = PathUtils.resolve(file).toUri().toString();
            xqlFileManager.add(uri);
        }
        xqlFileManager.init();
        // print details log unless only 1 file
        if (files.length == 1) {
            xqlFileManager.foreach((a, r) -> {
                Stdout.printlnTitle(a, '-', 80, Style.SILVER);
                for (Map.Entry<String, XQLFileManager.Sql> entry : r.getEntry().entrySet()) {
                    String name = entry.getKey();
                    XQLFileManager.Sql sql = entry.getValue();
                    String info = XQLFileManager.encodeSqlReference(a, name) + (sql.getDescription().isEmpty() ? "" : " -> " + sql.getDescription());
                    Stdout.printlnNotice(info);
                }
            });
        } else {
            xqlFileManager.getResources().forEach((alias, r) ->
                    Stdout.printf("+ %s (%s)  %s%n", Style.SILVER, alias, r.getEntry().size(), r.getDescription()));
        }
        ExecCompleter.setXQLNames(xqlFileManager.names());

        if (!xqlFileManager.getResources().isEmpty()) {
            Stdout.println("Type '" + Commands.exec.getName() + " &sql_name' to execute!");
        }
    }
}
