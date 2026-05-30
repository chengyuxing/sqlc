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
import com.github.chengyuxing.sql.terminal.common.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.getJson;
import static com.github.chengyuxing.sql.terminal.util.ObjectUtil.wrapObjectForSerialized;

public final class PrintHelper {
    private static final Logger log = LoggerFactory.getLogger(PrintHelper.class);

    public static void printQueryResult(Stream<DataRow> s) {
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
    public static Stream<DataRow> executedRow2Stream(Baki baki, String sql, Map<String, Object> args) {
        DataRow row = WaitingPrinter.waiting(() -> baki.execute(sql, args));
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

    public static void printGrid(List<List<String>> gridData) {
        int[] maxes = new int[gridData.get(0).size()];
        Arrays.fill(maxes, 0);
        StringJoiner fmt = new StringJoiner("\t");
        for (List<String> gridDatum : gridData) {
            for (int j = 0; j < gridDatum.size(); j++) {
                int now = gridDatum.get(j).length();
                if (maxes[j] < now) {
                    maxes[j] = now;
                }
            }
        }
        for (int len : maxes) {
            fmt.add("%-" + len + "s");
        }
        for (int i = 0; i < gridData.size(); i++) {
            String content = String.format(fmt.toString(), gridData.get(i).toArray());
            if (i == 0) {
                Stdout.println(content, Style.CYAN);
                Stdout.print(StringUtils.repeat("-", content.length()), Style.CYAN);
            } else {
                Stdout.print(content, Style.DARK_CYAN);
            }
            System.err.println();
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
