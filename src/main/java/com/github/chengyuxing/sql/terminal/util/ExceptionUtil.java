package com.github.chengyuxing.sql.terminal.util;

import java.util.*;

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

    public static List<String> getCauseMessages(Throwable throwable) {
        List<String> messages = new ArrayList<>();
        while (throwable != null) {
            Throwable cause = throwable.getCause();
            String msg;
            if (cause == null) {
                msg = throwable.toString();
            } else {
                msg = cause.toString();
            }
            if (!messages.isEmpty()) {
                int last = messages.size() - 1;
                if (messages.get(last).contains(msg)) {
                    messages.remove(last);
                }
            }
            messages.add(msg);
            throwable = cause;
        }
        return messages;
    }
}
