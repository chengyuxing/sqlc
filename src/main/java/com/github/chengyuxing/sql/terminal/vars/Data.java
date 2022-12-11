package com.github.chengyuxing.sql.terminal.vars;

import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.completer.DynamicVarsCompleter;
import com.github.chengyuxing.sql.terminal.cli.completer.KeywordsCompleter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public abstract class Data {
    /**
     * xql名字自动完成数据
     */
    public static final DynamicVarsCompleter xqlNameCompleter = new DynamicVarsCompleter();
    /**
     * 存储过程名字自动完成
     */
    public static final DynamicVarsCompleter editCmdCompleter = new DynamicVarsCompleter();
    /**
     * sql关键字自动完成
     */
    public static final KeywordsCompleter keywordsCompleter = new KeywordsCompleter();
    /**
     * xql管理器
     */
    public static final XQLFileManager xqlFileManager = new XQLFileManager() {{
        setHighlightSql(Constants.IS_XTERM);
    }};
    /**
     * 临时文件缓存
     */
    public static final List<Path> tempFiles = new ArrayList<>();
}
