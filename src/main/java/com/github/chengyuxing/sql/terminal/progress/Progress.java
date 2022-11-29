package com.github.chengyuxing.sql.terminal.progress;

import com.github.chengyuxing.sql.terminal.core.PrintHelper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public abstract class Progress {
    protected final AtomicBoolean stop = new AtomicBoolean(false);
    protected final AtomicLong value = new AtomicLong(0);
    protected String prefix = "";
    protected String suffix = "";
    protected Thread current;

    protected BiConsumer<Long, Long> whenStopped;

    protected abstract void listening(long cost);

    public void start() {
        current = new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                while (!isStopped()) {
                    if (current.isInterrupted()) {
                        break;
                    }
                    listening(System.currentTimeMillis() - start);
                }
                long during = System.currentTimeMillis() - start;
                if (whenStopped != null) {
                    whenStopped.accept(value.get(), during);
                }
            } catch (Exception e) {
                PrintHelper.printlnError(e);
            }
        });
        current.start();
    }

    public long getValue() {
        return value.get();
    }

    public long increment() {
        return value.incrementAndGet();
    }

    public boolean isStopped() {
        return stop.get();
    }

    public void stop() {
        stop.set(true);
        try {
            current.join();
        } catch (InterruptedException e) {
            PrintHelper.printlnError(e);
        }
    }

    public void interrupt() {
        if (current != null) {
            if (!current.isInterrupted())
                current.interrupt();
        }
    }

    public void reset() {
        if (current != null && !current.isAlive()) {
            stop.set(false);
            value.set(0);
        }
    }
}
