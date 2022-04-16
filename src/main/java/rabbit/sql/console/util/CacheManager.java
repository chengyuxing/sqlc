package rabbit.sql.console.util;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Baki;
import rabbit.sql.console.types.Cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CacheManager extends ConcurrentHashMap<String, Cache> {
    private final ExecutorService worker = Executors.newFixedThreadPool(8);
    private final Map<String, CompletableFuture<?>> tasks = new HashMap<>();
    private final Baki baki;

    public CacheManager(Baki baki) {
        this.baki = baki;
    }

    public Cache update(String name) {
        if (containsKey(name)) {
            Cache cache = get(name);
            try (Stream<DataRow> stream = baki.query(cache.getSql())) {
                List<DataRow> rows = stream.collect(Collectors.toList());
                cache.setData(rows);
            } catch (Exception e) {
                cache.setStatus("error");
                cache.setError(e.toString());
            }
            return cache;
        }
        return null;
    }

    public void updateBackground(String name) {
        if (containsKey(name)) {
            Cache cache = get(name);
            CompletableFuture<?> task = CompletableFuture.supplyAsync(() -> {
                cache.setStatus("pending");
                try (Stream<DataRow> stream = baki.query(cache.getSql())) {
                    List<DataRow> rows = stream.collect(Collectors.toList());
                    cache.setData(rows);
                    cache.setStatus("");
                } catch (Exception e) {
                    cache.setStatus("error");
                    cache.setError(e.toString());
                }
                return cache;
            }, worker).whenComplete((status, err) -> {
                cache.setError(err.toString());
                put(name, cache);
            });
            tasks.put(name, task);
        }
    }

    public void killTask(String name) {
        if (tasks.containsKey(name))
            tasks.get(name).cancel(true);
    }

    public String info() {
        return keySet().stream().map(k -> {
            String status = get(k).getStatus();
            return k + "(" + status + ")";
        }).collect(Collectors.joining(", "));
    }
}
