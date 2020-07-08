package rabbit.sql.console.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import rabbit.common.types.DataRow;
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
            if (rows == null) {
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
            System.out.print(", \033[96m" + getJson(data) + "\033[0m");
        } else {
            System.out.print("\033[93m[\033[0m\033[96m" + getJson(data) + "\033[0m");
            firstLine.set(false);
        }
    }

    public static void printDSV(DataRow data, String d, AtomicBoolean firstLine) {
        if (firstLine.get()) {
            System.out.println("\033[93m" + data.getTypes().stream()
                    .map(v -> {
                        int idx = v.lastIndexOf(".");
                        if (idx == -1) {
                            return v;
                        }
                        return v.substring(idx + 1);
                    }).collect(Collectors.joining(d, "[", "]")) + "\033[0m");

            System.out.println("\033[93m" + data.getNames().stream()
                    .collect(Collectors.joining(d, "[", "]")) + "\033[0m");
            firstLine.set(false);
        }
        System.out.println("\033[96m" + data.getValues().stream().map(v -> {
            if (null == v) {
                return "null";
            }
            return v.toString();
        }).collect(Collectors.joining(d, "[", "]")) + "\033[0m");
    }

    public static void printQueryResult(DataRow row, AtomicReference<View> viewMode, AtomicBoolean first) {
        if (viewMode.get() == View.TSV) {
            ViewPrinter.printDSV(row, "\t", first);
        } else if (viewMode.get() == View.JSON) {
            try {
                ViewPrinter.printJSON(row, first);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        } else if (viewMode.get() == View.EXCEL) {
            ViewPrinter.printDSV(row, " | ", first);
        } else {
            ViewPrinter.printDSV(row, ", ", first);
        }
    }
}
