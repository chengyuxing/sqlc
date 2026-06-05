package com.github.chengyuxing.sql.terminal.util;

import java.util.*;

public class ExceptionUtils {
    public static String getCauseMessage(Throwable throwable) {
        while (throwable != null) {
            Throwable cause = throwable.getCause();
            if (cause == null) {
                return throwable.getMessage();
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
                msg = throwable.getMessage();
            } else {
                msg = cause.getMessage();
            }
            if (!messages.isEmpty()) {
                int last = messages.size() - 1;
                if (messages.get(last).contains(msg)) {
                    messages.remove(last);
                }
            }
            if (msg != null) {
                messages.add(msg);
            }
            throwable = cause;
        }
        return messages;
    }
}
