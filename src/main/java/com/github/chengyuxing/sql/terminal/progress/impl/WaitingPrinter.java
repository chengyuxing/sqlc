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

    /**
     * 等待进度逻辑
     *
     * @param prompt   题词
     * @param callable 运行逻辑
     * @param <T>      结果类型参数
     * @return 运行结果
     */
    public static <T> T waiting(String prompt, Callable<T> callable) {
        WaitingPrinter wp = new WaitingPrinter(prompt);
        try {
            wp.start();
            T result = callable.call();
            wp.stop();
            return result;
        } catch (Exception e) {
            wp.interrupt();
            throw new RuntimeException("an error in waiting: ", e);
        }
    }

    /**
     * 等待进度逻辑
     *
     * @param callable 运行逻辑
     * @param <T>      结果类型参数
     * @return 运行结果
     */
    public static <T> T waiting(Callable<T> callable) {
        return waiting("waiting...", callable);
    }
}
