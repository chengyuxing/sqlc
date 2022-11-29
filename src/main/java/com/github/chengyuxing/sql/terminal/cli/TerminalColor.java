package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.sql.utils.SqlUtil;

import static com.github.chengyuxing.sql.terminal.vars.Constants.IS_XTERM;

public class TerminalColor {
    public static String colorful(String str, Color color) {
        if (IS_XTERM) {
            return Printer.colorful(str, color);
        }
        return str;
    }

    public static String underline(String str) {
        if (IS_XTERM) {
            return "\33[4m" + str + "\33[0m";
        }
        return str;
    }

    public static void print(String str, Color color) {
        System.out.print(colorful(str, color));
    }

    public static void println(String str, Color color) {
        System.out.println(colorful(str, color));
    }

    public static void printf(String str, Color color, Object... args) {
        System.out.printf(colorful(str, color), args);
    }

    public static String highlightSql(String sql) {
        return SqlUtil.buildPrintSql(sql, IS_XTERM);
    }
}
