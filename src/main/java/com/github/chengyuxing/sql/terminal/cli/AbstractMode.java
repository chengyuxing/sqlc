package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.sql.terminal.core.BakiLoader;

public abstract class AbstractMode {
    protected final StartupShell shell;
    protected final BakiLoader bakiLoader;

    protected AbstractMode(StartupShell shell, BakiLoader bakiLoader) {
        this.shell = shell;
        this.bakiLoader = bakiLoader;
    }
}
