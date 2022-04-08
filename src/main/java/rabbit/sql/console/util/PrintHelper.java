package rabbit.sql.console.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.utils.StringUtil;
import rabbit.sql.console.types.View;

import java.io.*;
import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;
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
                        printWarning("]");
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
            printError(e);
        }
    }

    public static void printQueryResult(Stream<DataRow> s, AtomicReference<View> viewMode) {
        printQueryResult(s, viewMode, null);
    }

    public static void printHighlightSql(String sql) {
        if (!isWindows) {
            Printer.print(">>> ", Color.SILVER);
            System.out.println(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(sql.trim()));
        } else {
            System.out.println(">>> ");
            System.out.println(sql.trim());
        }
    }

    public static void printError(Throwable e) {
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
            printDanger(out.toString());
        } catch (IOException ioException) {
            printDanger(ioException.toString());
        }
    }

    public static void printPrefix(AtomicBoolean isTxActive, String text) {
        String txActiveFlag = isTxActive.get() ? "*" : "";
        if (!isWindows) {
            System.out.print(Printer.colorful(txActiveFlag, Color.SILVER) + Printer.colorful(text, Color.PURPLE) + " ");
        } else {
            System.out.print(txActiveFlag + text + " ");
        }
    }

    public static void printPrefix(String text) {
        printPrefix(new AtomicBoolean(false), text);
    }

    public static void printDanger(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.RED);
        } else {
            System.out.println(msg);
        }
    }

    public static void printWarning(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.YELLOW);
        } else {
            System.out.println(msg);
        }
    }

    public static void printInfo(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.CYAN);
        } else {
            System.out.println(msg);
        }
    }

    public static void printNotice(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.SILVER);
        } else {
            System.out.println(msg);
        }
    }

    public static void printPrimary(String msg) {
        if (!isWindows) {
            Printer.println(msg, Color.DARK_CYAN);
        } else {
            System.out.println(msg);
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
            String typesLine = data.getNames().stream()
                    .map(n -> {
                        String v = data.getType(n);
                        if (v == null) {
                            return "unKnow";
                        }
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
            return wrapObjectForSerialized(v).toString();
        }).collect(Collectors.joining(d, "[", "]"));
        Printer.println(valuesLine, Color.CYAN);
    }

    public static void printPercentProgress(int value, int max, String prefix, String suffix) throws InterruptedException {
        float maxValue = Float.parseFloat(max + ".0");
        NumberFormat num = NumberFormat.getPercentInstance();
        num.setMaximumIntegerDigits(4);
        num.setMaximumFractionDigits(2);
        double percent = value / maxValue;
        String temp = prefix + num.format(percent) + suffix;
        System.out.print(temp);
        TimeUnit.MILLISECONDS.sleep(500);
        if (value == max) {
            System.out.println();
        } else {
            for (int j = 0, l = temp.length() + suffix.length(); j < l; j++) {
                System.out.print("\b");
            }
        }
    }
}
