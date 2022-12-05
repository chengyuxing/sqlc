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

    public static Set<String> getCauseMessages(Throwable throwable) {
        Set<String> messages = new LinkedHashSet<>();
        while (throwable != null) {
            Throwable cause = throwable.getCause();
            if (cause == null) {
                messages.add(throwable.toString());
            } else {
                messages.add(cause.toString());
            }
            throwable = cause;
        }
        return messages;
    }
}
