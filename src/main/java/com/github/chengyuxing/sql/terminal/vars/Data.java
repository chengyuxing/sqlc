package com.github.chengyuxing.sql.terminal.vars;

import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.completer.DynamicVarsCompleter;
import com.github.chengyuxing.sql.terminal.cli.completer.KeywordsCompleter;
import com.github.chengyuxing.sql.terminal.types.Cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Data {
    /**
     * 结果集缓存key自增
     */
    public static final AtomicInteger idx = new AtomicInteger(0);
    /**
     * 查询结果缓存
     */
    public static final Map<String, Cache> queryCaches = new LinkedHashMap<>();
    /**
     * 缓存名自动完成数据
     */
    public static final DynamicVarsCompleter cacheNameCompleter = new DynamicVarsCompleter();
    /**
     * xql名字自动完成数据
     */
    public static final DynamicVarsCompleter xqlNameCompleter = new DynamicVarsCompleter();

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
}
