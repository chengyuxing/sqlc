package com.github.chengyuxing.sql.terminal.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncCat implements AutoCloseable {
    private final ExecutorService service = Executors.newSingleThreadExecutor();

    @Override
    public void close() {
        if (!service.isShutdown()) {
            service.shutdown();
        }
    }

    public ExecutorService getService() {
        return service;
    }
}
