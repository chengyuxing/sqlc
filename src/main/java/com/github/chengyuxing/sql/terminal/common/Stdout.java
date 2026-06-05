package com.github.chengyuxing.sql.terminal.common;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.terminal.util.ExceptionUtils;
import com.github.chengyuxing.sql.util.SqlHighlighter;

import static com.github.chengyuxing.sql.terminal.common.Constants.IS_XTERM;

public final class Stdout {
    public static void printData(String s) {
        System.out.print(s);
    }

    public static void printlnData() {
        System.out.println();
    }

    public static void printlnData(String s) {
        System.out.println(s);
    }

    public static void println(String s) {
        System.err.println(s);
    }

    public static void println() {
        System.err.println();
    }

    public static void print(String s) {
        System.err.print(s);
    }

    public static void printf(String s, Object... args) {
        System.err.printf(s, args);
    }

    public static void println(String str, Style color) {
        if (IS_XTERM) {
            Printer.println(str, color);
        } else {
            println(str);
        }
    }

    public static void print(String str, Style color) {
        if (IS_XTERM) {
            Printer.print(str, color);
        } else {
            print(str);
        }
    }

    public static void printf(String str, Style color, Object... args) {
        if (IS_XTERM) {
            Printer.printf(str, color, args);
        } else {
            printf(str, args);
        }
    }

    public static void printlnDanger(String msg) {
        println(colorful(msg, Style.RED));
    }

    public static void printlnWarning(String msg) {
        println(colorful(msg, Style.YELLOW));
    }

    public static void printlnDarkWarning(String msg) {
        println(colorful(msg, Style.DARK_YELLOW));
    }

    public static void printlnInfo(String msg) {
        println(colorful(msg, Style.CYAN));
    }

    public static void printlnNotice(String msg) {
        println(colorful(msg, Style.SILVER));
    }

    public static void printNotice(String msg) {
        print(colorful(msg, Style.SILVER));
    }

    public static void printlnPrimary(String msg) {
        println(colorful(msg, Style.DARK_CYAN));
    }

    public static void printPrimary(String msg) {
        print(colorful(msg, Style.DARK_CYAN));
    }

    public static void printlnHighlightSql(String sql) {
        print(colorful(">>> ", Style.SILVER));
        println(SqlHighlighter.highlightIfAnsiCapable(sql.trim()));
    }

    public static void printlnError(Throwable e) {
        printlnTitle("Error", '-', 80, Style.RED);
        ExceptionUtils.getCauseMessages(e).forEach(msg -> {
            printlnDanger(msg);
            printlnTitle("", '-', 79, Style.RED);
        });
    }

    public static void printlnTitle(String title, char border, int max, Style color) {
        if (title.isEmpty()) {
            println(StringUtils.repeat(String.valueOf(border), max), color);
            return;
        }
        if (title.length() >= max - 2) {
            println(border + title + border, color);
            return;
        }
        int half = (max - title.length()) / 2;
        String b = StringUtils.repeat(String.valueOf(border), half);
        String result = b + title + b;
        println(result, color);
    }

    public static String colorful(String str, Style... color) {
        if (IS_XTERM) {
            return Printer.colorful(str, color);
        }
        return str;
    }
}
