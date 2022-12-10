package com.github.chengyuxing.sql.terminal.vars;

import org.jline.reader.impl.DefaultParser;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class Constants {
    public static final String REDIRECT_SYMBOL = "&>";
    public static final Path ROOT_DIR = Paths.get(File.separator);
    public static final Path USER_HOME = Paths.get(System.getProperty("user.home"));
    public static final Path CURRENT_DIR = Paths.get(System.getProperty("user.dir"));
    public static final Path CLASS_PATH = Paths.get(System.getProperty("java.class.path"));
    public static final Path APP_DIR = CLASS_PATH.getParent();
    public static final String TERM = System.getenv("TERM");
    public static final boolean IS_XTERM = TERM != null && TERM.startsWith("xterm");
    public static final DefaultParser cliParser = new DefaultParser().regexCommand(":?[a-zA-Z]+[a-zA-Z0-9_\\-@&]*!?");
    public static final Pattern EXEC_BATCH_REGEX = Pattern.compile("^:exec@\\s+(?<input>.+)");
    public static final Pattern EXEC_BATCH_WITH_HEADER_REGEX = Pattern.compile("^:exec@\\s+(?<input>.+)\\s+(?<headerIdx>-?(0|[1-9]\\d*))");
    public static final Pattern EXEC_BATCH_EXCEL_REGEX = Pattern.compile("^:exec@\\s+(?<input>.+)\\s+(?<sheetIdx>(0|[1-9]\\d*))(\\s+(?<headerIdx>-?(0|[1-9]\\d*)))");
    public static final Pattern EXEC_XQL_REGEX = Pattern.compile("^:exec&\\s+(?<name>.+)");
    public static final Pattern LOAD_XQL_FILE_REGEX = Pattern.compile("^:load\\s+(?<input>.+\\.xql)\\s+as\\s+(?<alias>.+)");
    public static final Pattern REMOVE_CACHE_REGEX = Pattern.compile("^:rm\\s+(?<name>.+)");
    public static final Pattern TX_REGEX = Pattern.compile("^:tx\\s+(?<op>begin|commit|rollback)", Pattern.CASE_INSENSITIVE);
    public static final Pattern VIEW_REGEX = Pattern.compile("^:view\\s+(?<view>csv|tsv|json|excel)", Pattern.CASE_INSENSITIVE);
    public static final Pattern DELIMITER_REGEX = Pattern.compile("^:d\\s+(?<d>.+)");
    public static final Pattern NAME_QUERY_CACHE_REGEX = Pattern.compile("^(?<prefix>var|val)\\s+(?<name>[\\w._-]+)\\s*=");
    public static final Pattern PROCEDURE_OUT_REGEX = Pattern.compile("^OUT\\s+(?<out>-?(0|[1-9]\\d*))", Pattern.CASE_INSENSITIVE);
    public static final Pattern PROCEDURE_IN_OUT_REGEX = Pattern.compile("^(IN([_/]?|\\s+)OUT)\\s+(?<out>-?(0|[1-9]\\d*))\\s+(?<in>.+)", Pattern.CASE_INSENSITIVE);
    public static final Pattern SQL_TEMPLATE_ARG_REGEX = Pattern.compile("\\$\\{\\s*:?(?<key>[\\w._-]+)\\s*}");
    public static final Pattern GET_MYSQL_SCHEMA = Pattern.compile(":\\d{1,5}/(?<schema>[\\w_]+)");
}
