package com.github.chengyuxing.sql.terminal.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.types.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.getJson;
import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.wrapObjectForSerialized;
import static com.github.chengyuxing.sql.terminal.util.SqlUtil.hasOutParam;

public final class PrintHelper {
    private static final Logger log = LoggerFactory.getLogger(PrintHelper.class);

    public static void printStreamData(Stream<DataRow> s) {
        AtomicBoolean first = new AtomicBoolean(true);
        switch (Context.viewMode.get()) {
            case json:
                Stdout.printData("[");
                s.forEach(row -> {
                    try {
                        PrintHelper.printJSON(row, first);
                    } catch (JsonProcessingException e) {
                        log.error("print query result as json.", e);
                        throw new RuntimeException(e);
                    }
                });
                Stdout.printlnData("]");
                break;
            case tsv:
                s.forEach(row -> PrintHelper.printDSV(row, "\t", first));
                break;
            case csv:
                s.forEach(row -> PrintHelper.printDSV(row, ",", first));
                break;
            case excel:
                s.forEach(row -> PrintHelper.printDSV(row, " | ", first));
                break;
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

    public static void printExecuteResultByType(Baki baki, String sqlOrAddress, SqlType type, Map<String, Object> args) {
        switch (type) {
            case QUERY:
                try (Stream<DataRow> s = WaitingPrinter.waiting(() -> baki.query(sqlOrAddress).args(args).stream())) {
                    printStreamData(s);
                }
                break;
            case PROCEDURE:
                printProcedureResult(baki, sqlOrAddress, SqlUtil.toInOutParam(args));
                break;
            case OTHER:
                Stream<DataRow> s = executedRow2Stream(baki, sqlOrAddress, args);
                printStreamData(s);
                break;
        }
    }

    public static void printJSON(DataRow data, AtomicBoolean firstLine) throws JsonProcessingException {
        if (firstLine.get()) {
            Stdout.printData(getJson(data));
            firstLine.set(false);
        } else {
            Stdout.printData(", " + getJson(data));
        }
    }

    public static void printDSV(DataRow data, String d, AtomicBoolean firstLine) {
        if (firstLine.get()) {
            String namesLine = String.join(d, data.keySet());
            Stdout.println(namesLine, Style.SILVER);
            Stdout.println(StringUtils.repeat("-", namesLine.length()), Style.SILVER);
            firstLine.set(false);
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
