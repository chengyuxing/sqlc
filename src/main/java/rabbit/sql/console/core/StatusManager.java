package rabbit.sql.console.core;

import com.github.chengyuxing.common.DataRow;
import rabbit.sql.console.types.View;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class StatusManager {
    /**
     * 数据缓存
     */
    public static final Map<String, List<DataRow>> CACHE = new LinkedHashMap<>();
    /**
     * 事务是否活动标志
     */
    public static final AtomicBoolean txActive = new AtomicBoolean(false);
    /**
     * 是否开启缓存
     */
    public static final AtomicBoolean enableCache = new AtomicBoolean(false);
    /**
     * 结果集缓存key自增
     */
    public static final AtomicInteger idx = new AtomicInteger(0);
    /**
     * 输出的结果视图与结果保存类型
     */
    public static final AtomicReference<View> viewMode = new AtomicReference<>(View.TSV);
    /**
     * 多行sql分隔符
     */
    public static final AtomicReference<String> sqlDelimiter = new AtomicReference<>(";");
    /**
     * 是否是交互模式
     */
    public static final AtomicBoolean isInteractive = new AtomicBoolean(false);
    /**
     * baki是否第一次加载
     */
    public static final AtomicBoolean bakiFirstLoad = new AtomicBoolean(true);
    /**
     * 交互模式下是否在其他线程中已经输出了新行
     */
    public static final AtomicBoolean newlineInThread = new AtomicBoolean(false);
}
