package com.github.chengyuxing.sql.terminal.progress.impl;

import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.progress.Progress;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class ProgressPrinter extends Progress {

    protected long step = 10000;
    protected BiFunction<Long, Long, String> formatter;

    public ProgressPrinter() {

    }

    public ProgressPrinter whenStopped(BiConsumer<Long, Long> whenStopped) {
        this.whenStopped = whenStopped;
        return this;
    }

    public static ProgressPrinter of(String prefix, String suffix) {
        ProgressPrinter pp = new ProgressPrinter();
        pp.prefix = prefix;
        pp.suffix = suffix;
        return pp;
    }

    protected void updateValue(String content) throws InterruptedException {
        if (content != null && !content.equals("")) {
            PrintHelper.printPrimary(content);
            TimeUnit.MILLISECONDS.sleep(200);
            if (stop.get()) {
                System.out.println();
            } else {
                for (int i = 0, j = content.length() + content.length() >> 1; i < j; i++) {
                    System.out.print("\b");
                }
            }
        }
    }

    @Override
    protected void listening(long cost) {
        if (value.get() % step == 0) {
            try {
                if (formatter == null) {
                    updateValue(prefix + value.get() + suffix + "(" + cost + ")");
                } else {
                    updateValue(formatter.apply(value.get(), cost));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setFormatter(BiFunction<Long, Long, String> formatter) {
        this.formatter = formatter;
    }

    public void setStep(long step) {
        this.step = step;
    }
}
