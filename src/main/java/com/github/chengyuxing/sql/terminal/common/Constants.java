package com.github.chengyuxing.sql.terminal.common;

import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.terminal.util.PathUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class Constants {
    public static final Path USER_HOME = Paths.get(System.getProperty("user.home"));
    public static final Path CURRENT_DIR = Paths.get(System.getProperty("user.dir"));
    public static final Path APP_DIR = PathUtils.getAppDir();

    public static final Path SQLC_USER_PATH = USER_HOME.resolve(".sqlc");
    public static final Path SQLC_TEMP_PATH = SQLC_USER_PATH.resolve("temp");
    public static final Path SQLC_HISTORY_PATH = SQLC_USER_PATH.resolve("history");

    public static final String TERM = System.getenv("TERM");
    public static final boolean IS_XTERM = TERM != null && TERM.startsWith("xterm");

    public static final Pattern PROCEDURE_OUT_REGEX = Pattern.compile("^OUT\\s+(?<out>-?(0|[1-9]\\d*|\\w+))", Pattern.CASE_INSENSITIVE);
    public static final Pattern PROCEDURE_IN_OUT_REGEX = Pattern.compile("^(INOUT)\\s+(?<out>-?(0|[1-9]\\d*|\\w+))\\s+(?<in>.+)", Pattern.CASE_INSENSITIVE);
    public static final Pattern SQL_TEMPLATE_ARG_REGEX = StringUtils.FMT.getPattern();
}
