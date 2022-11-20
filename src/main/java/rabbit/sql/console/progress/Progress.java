package rabbit.sql.console.progress;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public abstract class Progress {
    protected final AtomicBoolean stop = new AtomicBoolean(false);
    protected final AtomicLong value = new AtomicLong(0);
    protected String prefix = "";
    protected String suffix = "";
    private Thread current;

    protected abstract void listening(long cost);

    public void start(BiConsumer<Long, Long> whenStopped) {
        current = new Thread(() -> {
            long start = System.currentTimeMillis();
            while (!isStopped()) {
                if (current.isInterrupted()) {
                    break;
                }
                listening(System.currentTimeMillis() - start);
            }
            long during = System.currentTimeMillis() - start;
            whenStopped.accept(value.get(), during);
        });
        current.start();
    }

    public long getValue() {
        return value.get();
    }

    public long increment() {
        return value.incrementAndGet();
    }

    public boolean isStopped() {
        return stop.get();
    }

    public void stop() {
        stop.set(true);
    }

    public void interrupt() {
        if (current != null) {
            if (!current.isInterrupted())
                current.interrupt();
        }
    }

    public void reset() {
        if (current != null && !current.isAlive()) {
            stop.set(false);
            value.set(0);
        }
    }
}
