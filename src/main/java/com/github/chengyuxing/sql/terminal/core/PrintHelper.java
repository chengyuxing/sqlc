package com.github.chengyuxing.sql.terminal.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtils;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.types.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtils.getJson;
import static com.github.chengyuxing.sql.terminal.util.ObjectUtils.wrapObjectForSerialized;
import static com.github.chengyuxing.sql.terminal.util.SqlUtils.hasOutParam;

public final class PrintHelper {
    private static final Logger log = LoggerFactory.getLogger(PrintHelper.class);

    public static void printStreamData(Stream<DataRow> s) {
        long argRows = Context.printRows.get();
        long maxRows = argRows <= 0 ? Long.MAX_VALUE : argRows;
        AtomicLong n = new AtomicLong();
        switch (Context.viewMode.get()) {
            case json:
                Stdout.printData("[");
                s.limit(maxRows).forEach(row -> {
                    try {
                        PrintHelper.printJSON(row, n.get());
                        n.incrementAndGet();
                    } catch (JsonProcessingException e) {
                        log.error("Print query result as json", e);
                        throw new RuntimeException(e);
                    }
                });
                Stdout.printlnData("]");
                break;
            case tsv:
                s.limit(maxRows).forEach(row -> {
                    PrintHelper.printDSV(row, "\t", n.get());
                    n.incrementAndGet();
                });
                break;
            case csv:
                s.limit(maxRows).forEach(row -> {
                    PrintHelper.printDSV(row, ",", n.get());
                    n.incrementAndGet();
                });
                break;
            case excel:
                s.limit(maxRows).forEach(row -> {
                    PrintHelper.printDSV(row, " | ", n.get());
                    n.incrementAndGet();
                });
                break;
        }
        if (n.get() >= maxRows) {
            Stdout.printlnTitle("", '-', 80, Style.SILVER);
            Stdout.printf("Showing first %s rows...%n", Style.SILVER, maxRows);
        }
    }

    @SuppressWarnings("unchecked")
    public static void printProcedureResult(Baki baki, String procedure, Map<String, Param> args) {
        boolean hasOutParam = hasOutParam(args);
        DataRow result = WaitingPrinter.waiting(() -> baki.call(procedure, args));
        if (hasOutParam) {
            result.forEach((k, v) -> {
                if (v instanceof List) {
                    Stdout.printlnTitle(k, '-', 80, Style.DARK_YELLOW);
                    printStreamData(((List<DataRow>) v).stream());
                    Stdout.println();
                } else {
                    printStreamData(Stream.of(DataRow.of(k, v)));
                }
            });
            return;
        }
        Object first = result.getFirst();
        if (first instanceof List) {
            printStreamData(((List<DataRow>) first).stream());
            return;
        }
        printStreamData(Stream.of(result));
    }

    @SuppressWarnings("unchecked")
    public static Stream<DataRow> executedRow2Stream(Baki baki, String sql, Map<String, Object> args) {
        DataRow result = WaitingPrinter.waiting(() -> baki.execute(sql, args));
        Object first = result.getFirst();
        Stream<DataRow> stream;
        if (first instanceof DataRow) {
            stream = Stream.of((DataRow) first);
        } else if (first instanceof List) {
            stream = ((List<DataRow>) first).stream();
        } else {
            stream = Stream.of(result);
        }
        return stream;
    }

    public static void printExecuteResultByType(Baki baki, String sqlOrRef, SqlType type, Map<String, Object> args) {
        switch (type) {
            case QUERY:
                try (Stream<DataRow> s = WaitingPrinter.waiting(() -> baki.query(sqlOrRef).args(args).stream())) {
                    printStreamData(s);
                }
                break;
            case PROCEDURE:
                printProcedureResult(baki, sqlOrRef, SqlUtils.toInOutParam(args));
                break;
            case OTHER:
                Stream<DataRow> s = executedRow2Stream(baki, sqlOrRef, args);
                printStreamData(s);
                break;
        }
    }

    public static void printJSON(DataRow data, long n) throws JsonProcessingException {
        if (n == 0) {
            Stdout.printData(getJson(data));
        } else {
            Stdout.printData(", " + getJson(data));
        }
    }

    public static void printDSV(DataRow data, String d, long n) {
        if (n == 0) {
            String namesLine = String.join(d, data.keySet());
            String border = StringUtils.repeat("-", Math.max(namesLine.length(), 80));
            Stdout.println(border, Style.SILVER);
            Stdout.printlnData(namesLine);
            Stdout.println(border, Style.SILVER);
        }
        String valuesLine = data.values().stream().map(v -> {
            if (null == v) {
                return "null";
            }
            return wrapObjectForSerialized(v).toString();
        }).collect(Collectors.joining(d));
        Stdout.printlnData(valuesLine);
    }
}
