package com.github.chengyuxing.sql.terminal;

import com.github.chengyuxing.sql.terminal.cli.StartupShell;
import picocli.CommandLine;

public class App {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new StartupShell()).execute(args);
        System.exit(exitCode);
    }
}
