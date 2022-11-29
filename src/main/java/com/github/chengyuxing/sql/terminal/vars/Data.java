package com.github.chengyuxing.sql.terminal.vars;

import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.completer.DynamicVarsCompleter;
import com.github.chengyuxing.sql.terminal.types.Cache;

import java.util.*;
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
    public static DynamicVarsCompleter cacheNameCompleter = new DynamicVarsCompleter();
    /**
     * xql名字自动完成数据
     */
    public static DynamicVarsCompleter xqlNameCompleter = new DynamicVarsCompleter();
    /**
     * xql管理器
     */
    public static XQLFileManager xqlFileManager = new XQLFileManager() {{
        setHighlightSql(Constants.IS_XTERM);
    }};
}
