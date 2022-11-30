package com.github.chengyuxing.sql.terminal.util;

import java.util.LinkedHashSet;
import java.util.Set;

public class ExceptionUtil {
    public static String getCauseMessage(Throwable throwable) {
        while (throwable != null) {
            Throwable cause = throwable.getCause();
            if (cause == null) {
                return throwable.toString();
            }
            throwable = cause;
        }
        return "";
    }
}
