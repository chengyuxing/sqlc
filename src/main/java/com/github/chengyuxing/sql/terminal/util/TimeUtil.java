package com.github.chengyuxing.sql.terminal.util;

import java.util.Formatter;

public class TimeUtil {
    public static String format(long milliseconds) {
        final Formatter fmt = new Formatter();
        String time;
        if (milliseconds > 60000) {
            time = fmt.format("%.2f", milliseconds / 60000.0) + " m";
        } else {
            time = fmt.format("%.2f", milliseconds / 1000.0) + " s";
        }
        return time;
    }
}
