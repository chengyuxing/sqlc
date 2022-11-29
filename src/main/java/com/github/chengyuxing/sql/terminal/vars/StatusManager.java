package com.github.chengyuxing.sql.terminal.vars;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.types.View;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class StatusManager {
    /**
     * 事务是否活动标志
     */
    public static final AtomicBoolean txActive = new AtomicBoolean(false);
    /**
     * 是否开启缓存
     */
    public static final AtomicBoolean enableCache = new AtomicBoolean(false);
    /**
     * 输出的结果视图与结果保存类型
     */
    public static final AtomicReference<View> viewMode = new AtomicReference<>(View.TSV);
    /**
     * 多行sql分隔符
     */
    public static final AtomicReference<String> sqlDelimiter = new AtomicReference<>(";");
    /**
     * baki是否第一次加载
     */
    public static final AtomicBoolean bakiFirstLoad = new AtomicBoolean(true);
    /**
     * 命令行终端题词
     */
    public static final AtomicReference<Prompt> promptReference = new AtomicReference<>();

    /**
     * 设置事务状态
     *
     * @param active 是否活动
     */
    public static void setTxActive(boolean active) {
        txActive.set(active);
        if (promptReference.get() != null) {
            Color color = active ? Color.DARK_YELLOW : Color.PURPLE;
            promptReference.get().setColor(color);
        }
    }
}
