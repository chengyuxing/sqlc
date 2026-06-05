package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.sql.terminal.util.IOUtils;
import picocli.CommandLine;

import java.io.IOException;

public enum StartupMode {
    STDIN,
    COMMAND,
    INTERACTIVE;

    public static StartupMode detect(App app) throws IOException {
        if (app.ioOptions != null) {
            if (app.ioOptions.executeOptions != null) {
                App.ExecuteOptions executeOptions = app.ioOptions.executeOptions;
                if (executeOptions.sql.length == 0) {
                    if (!IOUtils.isPipedInput()) {
                        throw new CommandLine.ParameterException(app.spec.commandLine(), "Error: Missing SQL. Use `-e, --execute=<sql>` or pipe SQL via stdin");
                    }
                    return STDIN;
                }
                if (!executeOptions.output.isEmpty() && executeOptions.sql.length != 1) {
                    throw new CommandLine.ParameterException(app.spec.commandLine(), "Error: -o can only be used when exactly one `-e, --execute=<sql>` is specified");
                }
            }
            return COMMAND;
        }
        if (IOUtils.isPipedInput()) {
            return STDIN;
        }
        return INTERACTIVE;
    }
}
