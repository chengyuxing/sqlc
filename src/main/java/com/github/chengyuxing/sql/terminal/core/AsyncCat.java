package com.github.chengyuxing.sql.terminal.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class AsyncCat implements AutoCloseable {
    private ExecutorService service;
    private int nThreads = 1;

    public AsyncCat(int nThreads) {
        this.nThreads = nThreads;
    }

    public AsyncCat() {

    }

    @Override
    public void close() {
        if (service == null) {
            return;
        }
        if (!service.isShutdown()) {
            service.shutdown();
        }
    }

    public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        if (service == null) {
            if (nThreads == 1) {
                service = Executors.newSingleThreadExecutor();
            } else if (nThreads > 1) {
                service = Executors.newFixedThreadPool(nThreads);
            } else {
                throw new IllegalArgumentException("thread count must not less than 1");
            }
        }
        return CompletableFuture.supplyAsync(supplier, service);
    }
}
