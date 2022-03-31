package rabbit.sql.console.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import rabbit.sql.console.types.View;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ViewPrinter {
    private final static ObjectMapper JSON = new ObjectMapper();

    public static void writeJsonArray(List<DataRow> rows, String path) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path))) {
            if (rows == null || rows.isEmpty()) {
                writer.write("[]");
            } else {
                for (int i = 0, j = rows.size(); i < j; i++) {
                    String json = getJson(rows.get(i));
                    if (i == 0) {
                        writer.write("[");
                        writer.write(json);
                    } else {
                        writer.write(", " + json);
                    }
                }
                writer.write("]");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getJson(DataRow row) throws JsonProcessingException {
        return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(row.toMap());
    }

    public static void printJSON(DataRow data, AtomicBoolean firstLine) throws JsonProcessingException {
        if (!firstLine.get()) {
            Printer.print(", " + getJson(data), Color.CYAN);
        } else {
            System.out.print(Printer.colorful("[", Color.YELLOW) + Printer.colorful(getJson(data), Color.CYAN));
            firstLine.set(false);
        }
    }

    public static void printDSV(DataRow data, String d, AtomicBoolean firstLine) {
        if (firstLine.get()) {
            String typesLine = data.getNames().stream()
                    .map(n -> {
                        String v = data.getType(n);
                        int idx = v.lastIndexOf(".");
                        if (idx == -1) {
                            return v;
                        }
                        return v.substring(idx + 1);
                    }).collect(Collectors.joining(d, "[", "]"));
            Printer.println(typesLine, Color.YELLOW);
            String namesLine = data.getNames().stream()
                    .collect(Collectors.joining(d, "[", "]"));
            Printer.println(namesLine, Color.YELLOW);
            firstLine.set(false);
        }
        String valuesLine = data.getValues().stream().map(v -> {
            if (null == v) {
                return "null";
            }
            return v.toString();
        }).collect(Collectors.joining(d, "[", "]"));
        Printer.println(valuesLine, Color.CYAN);
    }

    public static void printQueryResult(DataRow row, AtomicReference<View> viewMode, AtomicBoolean first) {
        switch (viewMode.get()) {
            case JSON:
                try {
                    ViewPrinter.printJSON(row, first);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            case CSV:
                ViewPrinter.printDSV(row, ", ", first);
                break;
            case EXCEL:
                ViewPrinter.printDSV(row, " | ", first);
                break;
            default:
                ViewPrinter.printDSV(row, "\t", first);
                break;
        }
    }
}
