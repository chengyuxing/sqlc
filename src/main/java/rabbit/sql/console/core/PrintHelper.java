package rabbit.sql.console.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.utils.StringUtil;
import rabbit.sql.console.types.View;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static rabbit.sql.console.util.ObjectUtil.getJson;
import static rabbit.sql.console.util.ObjectUtil.wrapObjectForSerialized;

public final class PrintHelper {
    public static boolean isWindows = StringUtil.containsIgnoreCase(System.getProperty("os.name"), "windows");

    public static void printQueryResult(Stream<DataRow> s, AtomicReference<View> viewMode, Consumer<DataRow> eachRowFunc) {
        AtomicBoolean first = new AtomicBoolean(true);
        try {
            switch (viewMode.get()) {
                case JSON:
                    if (eachRowFunc == null) {
                        s.forEach(row -> {
                            try {
                                PrintHelper.printJSON(row, first);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } else {
                        s.forEach(row -> {
                            try {
                                PrintHelper.printJSON(row, first);
                                eachRowFunc.accept(row);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                    if (viewMode.get() == View.JSON) {
                        printlnWarning("]");
                        System.out.println();
                    }
                    break;
                case TSV:
                    if (eachRowFunc == null) {
                        s.forEach(row -> PrintHelper.printDSV(row, "\t", first));
                    } else {
                        s.forEach(row -> {
                            PrintHelper.printDSV(row, "\t", first);
                            eachRowFunc.accept(row);
                        });
                    }
                    break;
                case CSV:
                    if (eachRowFunc == null) {
                        s.forEach(row -> PrintHelper.printDSV(row, ",", first));
                    } else {
                        s.forEach(row -> {
                            PrintHelper.printDSV(row, ",", first);
                            eachRowFunc.accept(row);
                        });
                    }
                    break;
                case EXCEL:
                    if (eachRowFunc == null) {
                        s.forEach(row -> PrintHelper.printDSV(row, " | ", first));
                    } else {
                        s.forEach(row -> {
                            PrintHelper.printDSV(row, " | ", first);
                            eachRowFunc.accept(row);
                        });
                    }
                    break;
            }
        } catch (Exception e) {
            printlnError(e);
        }
    }

    public static void printQueryResult(Stream<DataRow> s, AtomicReference<View> viewMode) {
        printQueryResult(s, viewMode, null);
    }

    public static void printlnHighlightSql(String sql) {
        if (!isWindows) {
            Printer.print(">>> ", Color.SILVER);
            System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sql.trim()));
        } else {
            System.out.println(">>> ");
            System.out.println(sql.trim());
        }
    }

    public static void printlnError(Throwable e) {
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
    }

    public static void printlnDanger(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.RED);
        } else {
            System.out.println(msg);
        }
    }

    public static void printlnWarning(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.YELLOW);
        } else {
            System.out.println(msg);
        }
    }

    public static void printlnInfo(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.CYAN);
        } else {
            System.out.println(msg);
        }
    }

    public static void printlnNotice(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.SILVER);
        } else {
            System.out.println(msg);
        }
    }

    public static void printlnPrimary(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.DARK_CYAN);
        } else {
            System.out.println(msg);
        }
    }

    public static void printPrimary(String msg) {
        if (!isWindows) {
            Printer.print(msg, Color.DARK_CYAN);
        } else {
            System.out.print(msg);
        }
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
            String typesLine = data.keySet().stream()
                    .map(n -> {
                        Class<?> type = data.getType(n);
                        if (type == null) {
                            return "unKnow";
                        }
                        String v = type.getName();
                        int idx = v.lastIndexOf(".");
                        if (idx == -1) {
                            return v;
                        }
                        return v.substring(idx + 1);
                    }).collect(Collectors.joining(d, "[", "]"));
            Printer.println(typesLine, Color.YELLOW);
            String namesLine = data.keySet().stream()
                    .collect(Collectors.joining(d, "[", "]"));
            Printer.println(namesLine, Color.YELLOW);
            firstLine.set(false);
        }
        String valuesLine = data.values().stream().map(v -> {
            if (null == v) {
                return "null";
            }
            return wrapObjectForSerialized(v).toString();
        }).collect(Collectors.joining(d, "[", "]"));
        Printer.println(valuesLine, Color.CYAN);
    }
}
