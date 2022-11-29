package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.cli.completer.CompleterBuilder;
import com.github.chengyuxing.sql.terminal.Version;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Command {
    public static final String url = TerminalColor.underline("https://github.com/chengyuxing/sqlc/tree/2.x");
    public static final String header = "A Command Line sql tool, support Query, DDL, DML, Transaction, (@)Batch Execute, Export File(json, csv, tsv, excel, sql)!\nHome page: " + url;
    public static final String footer = "When no '-e' command is given, sqlc starts in interactive mode.\nType :help in interactive mode for information on available commands and settings.";
    public static final Map<String, Pair<List<String>, List<String>>> cmdDesc = CompleterBuilder.builtins.getCmdDesc();
    public static final Map<String, String> cliDesc = new LinkedHashMap<String, String>() {{
        put("-u[url]", "jdbc url, e.g.: -ujdbc:postgresql://... (require)");
        put("-n[username]", "database username.");
        put("-p[password]", "database password.");
        put("-e\"[sql|path[&> output]]\"", "execute sql or redirect single query result to file\n(if ends with '.sql', will ignore -f and generate insert \nsql script and file name will as table name) with format(-f).");
        put("-e\"[@path]\"", "read file(sql(delimiter default ';')|json) for execute batch insert.");
        put("-d\"[delimiter]\"", "delimiter for multi-sql, default ';'(single semicolon)");
        put("-f[tsv|csv|json|excel]", "format of query result which will be executed.(default tsv)");
        put("-v", "version");
        put("-h[elp]", "get some help.");
    }};
    public static final Map<String, Runnable> command = new LinkedHashMap<String, Runnable>() {{
        put("--cli", () -> cliDesc.forEach((k, v) -> {
            if (v.contains("\n")) {
                List<String> lines = Arrays.asList(v.split("\n"));
                TerminalColor.printf("%-2s %-25s %-10s\n", Color.CYAN, "", k, lines.get(0));
                lines.subList(1, lines.size()).forEach(s -> TerminalColor.printf("%-2s %-25s %-10s\n", Color.CYAN, "", "", s));
            } else {
                TerminalColor.printf("%-2s %-25s %-10s\n", Color.CYAN, "", k, v);
            }
        }));
        put("--cmd", () -> cmdDesc.forEach((k, v) -> {
            if (!k.equals("")) {
                List<String> mainDesc = v.getItem1();
                List<String> argDesc = v.getItem2();
                if (argDesc.isEmpty()) {
                    TerminalColor.printf("%-2s %-10s %-10s\n", Color.CYAN, "", k, mainDesc.get(0));
                    mainDesc.subList(1, mainDesc.size()).forEach(s -> TerminalColor.printf("%-2s %-10s %-10s\n", Color.CYAN, "", "", s));
                } else {
                    TerminalColor.printf("%-2s %-10s %-10s\n", Color.DARK_CYAN, "", k, String.join(" ", argDesc));
                    mainDesc.forEach(s -> TerminalColor.printf("%-2s %-10s %-10s\n", Color.CYAN, "", "", s));
                }
            }
        }));
        put("-h", () -> {
            TerminalColor.println(header, Color.CYAN);
            TerminalColor.println("Command Mode:", Color.CYAN);
            get("--cli").run();
            TerminalColor.println("Interactive Mode:", Color.CYAN);
            get("--cmd").run();
            TerminalColor.println(footer, Color.CYAN);
        });
        put("-v", () -> TerminalColor.println(Version.RELEASE, Color.CYAN));
    }};

    public static void get(String key) {
        switch (key) {
            case "-h":
            case "-help":
            case "--help":
                command.get("-h").run();
                break;
            case "-v":
            case "-version":
            case "--version":
                command.get("-v").run();
                break;
            default:
                if (command.containsKey(key)) {
                    command.get(key).run();
                } else {
                    System.out.println("command not found, -h to get help.");
                }
                break;
        }
    }
}
