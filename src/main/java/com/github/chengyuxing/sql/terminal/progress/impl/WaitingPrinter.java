package com.github.chengyuxing.sql.terminal.progress.impl;

import com.github.chengyuxing.sql.terminal.util.TimeUtil;

import java.util.concurrent.Callable;

public class WaitingPrinter extends ProgressPrinter {
    public WaitingPrinter(String prompt) {
        step = 2;
        formatter = (v, d) -> {
            if (d >= 180) {
                return prompt + " (" + TimeUtil.format(d) + ")";
            }
            return "";
        };
    }

    public static <T> T waiting(String prompt, Callable<T> callable) throws Exception {
        WaitingPrinter wp = new WaitingPrinter(prompt);
        try {
            wp.start();
            T result = callable.call();
            wp.stop();
            return result;
        } catch (Exception e) {
            wp.interrupt();
            throw e;
        }
    }

    public static <T> T waiting(Callable<T> callable) throws Exception {
        return waiting("waiting...", callable);
    }
}
