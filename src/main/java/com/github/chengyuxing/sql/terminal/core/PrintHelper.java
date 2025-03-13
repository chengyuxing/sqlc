package com.github.chengyuxing.sql.terminal.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.ExceptionUtil;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import de.vandermeer.asciitable.AsciiTable;
import org.jline.reader.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.getJson;
import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.wrapObjectForSerialized;

public final class PrintHelper {
    private static final Logger log = LoggerFactory.getLogger(PrintHelper.class);

    public static void printQueryResult(Stream<DataRow> s) {
        AtomicBoolean first = new AtomicBoolean(true);
        switch (StatusManager.viewMode.get()) {
            case JSON:
                TerminalColor.print("[", Color.YELLOW);
                s.forEach(row -> {
                    try {
                        PrintHelper.printJSON(row, first);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
                TerminalColor.println("]", Color.YELLOW);
                break;
            case TSV:
                s.forEach(row -> PrintHelper.printDSV(row, "\t", first));
                break;
            case CSV:
                s.forEach(row -> PrintHelper.printDSV(row, ",", first));
                break;
            case EXCEL:
                printPrettyTable(s, first);
                break;
        }
    }

    @SuppressWarnings("unchecked")
    public static Stream<DataRow> executedRow2Stream(Baki baki, String sql, Map<String, Object> args) {
        DataRow row = WaitingPrinter.waiting(() -> baki.of(sql).execute(args));
        Object res = row.getFirst();
        Stream<DataRow> stream;
        if (res instanceof DataRow) {
            stream = Stream.of((DataRow) res);
        } else if (res instanceof List) {
            stream = ((List<DataRow>) res).stream();
        } else {
            stream = Stream.of(row);
        }
        return stream;
    }

    public static void printOneSqlResultByType(Baki baki, String sqlOrAddress, String tempString, Map<String, Object> args) {
        SqlType sqlType = SqlUtil.getType(tempString);
        if (sqlType == SqlType.QUERY) {
            try (Stream<DataRow> s = WaitingPrinter.waiting(() -> baki.query(sqlOrAddress).args(args).stream())) {
                printQueryResult(s);
            }
        } else if (sqlType == SqlType.FUNCTION) {
            ProcedureExecutor procedureExecutor = new ProcedureExecutor(baki, tempString);
            procedureExecutor.exec(SqlUtil.toInOutParam(args));
        } else if (sqlType == SqlType.OTHER) {
            printQueryResult(executedRow2Stream(baki, sqlOrAddress, args));
        }
    }

    public static void printMultiSqlResult(Baki baki, List<String> sqls, LineReader reader) {
        AtomicInteger success = new AtomicInteger(0);
        sqls.forEach(sql -> {
            try {
                printlnHighlightSql(sql);
                Pair<String, Map<String, Object>> pair = SqlUtil.prepareSqlWithArgs(sql, reader);
                printQueryResult(executedRow2Stream(baki, pair.getItem1(), pair.getItem2()));
                success.incrementAndGet();
            } catch (Exception e) {
                printlnNotice("Execute " + success + "/" + sqls.size() + " finished.");
                throw new RuntimeException(e);
            }
        });
    }

    public static void printlnHighlightSql(String sql) {
        TerminalColor.print(">>> ", Color.SILVER);
        System.out.println(TerminalColor.highlightSql(sql.trim()));
    }

    public static void printlnError(Throwable e) {
        if (log.isDebugEnabled()) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out)), true)) {
                writer.println(new Object() {
                    @Override
                    public String toString() {
                        StringWriter stringWriter = new StringWriter();
                        PrintWriter writer = new PrintWriter(stringWriter);
                        e.printStackTrace(writer);
                        StringBuffer buffer = stringWriter.getBuffer();
                        return buffer.toString();
                    }
                });
                printlnDanger(out.toString());
            } catch (IOException ioException) {
                printlnDanger(ioException.toString());
            }
        } else {
            ExceptionUtil.getCauseMessages(e).forEach(PrintHelper::printlnDanger);
            // PrintHelper.printlnDanger(ExceptionUtil.getCauseMessage(e));
        }
    }

    public static void printlnDanger(String msg) {
        TerminalColor.println(msg, Color.RED);
    }

    public static void printlnWarning(String msg) {
        TerminalColor.println(msg, Color.YELLOW);
    }

    public static void printlnDarkWarning(String msg) {
        TerminalColor.println(msg, Color.DARK_YELLOW);
    }

    public static void printlnInfo(String msg) {
        TerminalColor.println(msg, Color.CYAN);
    }

    public static void printlnNotice(String msg) {
        TerminalColor.println(msg, Color.SILVER);
    }

    public static void printlnPrimary(String msg) {
        TerminalColor.println(msg, Color.DARK_CYAN);
    }

    public static void printPrimary(String msg) {
        TerminalColor.print(msg, Color.DARK_CYAN);
    }

    public static void printGrid(List<List<String>> gridData) {
        AsciiTable asciiTable = new AsciiTable();
        asciiTable.getContext().setWidth(StatusManager.terminalReference.get().getWidth());
        for (int i = 0; i < gridData.size(); i++) {
            if (i == 0) {
                asciiTable.addRule();
                asciiTable.addRow(gridData.get(i));
                asciiTable.addRule();
            } else {
                asciiTable.addRow(gridData.get(i));
            }
        }
        asciiTable.addRule();
        if (asciiTable.getColNumber() > 0) {
            TerminalColor.println(asciiTable.render(), Color.CYAN);
        }
    }

    public static void printJSON(DataRow data, AtomicBoolean firstLine) throws JsonProcessingException {
        if (firstLine.get()) {
            TerminalColor.print(getJson(data), Color.CYAN);
            firstLine.set(false);
        } else {
            TerminalColor.print(", " + getJson(data), Color.CYAN);
        }
    }

    public static void printDSV(DataRow data, String d, AtomicBoolean firstLine) {
        if (firstLine.get()) {
            String namesLine = String.join(d, data.keySet());
            TerminalColor.println(namesLine, Color.DARK_CYAN);
            firstLine.set(false);
        }
        String valuesLine = data.values().stream().map(v -> {
            if (null == v) {
                return "null";
            }
            return wrapObjectForSerialized(v).toString();
        }).collect(Collectors.joining(d));
        TerminalColor.println(valuesLine, Color.CYAN);
    }

    public static void printPrettyTable(Stream<DataRow> s, AtomicBoolean firstLine) {
        AsciiTable asciiTable = new AsciiTable();
        asciiTable.getContext().setWidth(StatusManager.terminalReference.get().getWidth());
        s.forEach(d -> {
            if (firstLine.get()) {
                asciiTable.addRule();
                asciiTable.addRow(d.keySet());
                asciiTable.addRule();
                firstLine.set(false);
            }
            List<Object> values = d.values().stream().map(v -> {
                if (null == v) {
                    return "null";
                }
                return wrapObjectForSerialized(v).toString();
            }).collect(Collectors.toList());
            asciiTable.addRow(values);
        });
        asciiTable.addRule();
        if (asciiTable.getColNumber() > 0) {
            TerminalColor.println(asciiTable.render(), Color.CYAN);
        }
    }
}
