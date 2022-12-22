package com.github.chengyuxing.sql.terminal.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class AsyncCat implements AutoCloseable {
    private final ExecutorService service = Executors.newSingleThreadExecutor();

    @Override
    public void close() {
        if (!service.isShutdown()) {
            service.shutdown();
        }
    }

    public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier, service);
    }
}
