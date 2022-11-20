package rabbit.sql.console.progress.impl;

import rabbit.sql.console.core.PrintHelper;
import rabbit.sql.console.progress.Progress;
import rabbit.sql.console.util.TimeUtil;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class NumberProgressPrinter extends Progress {

    private long step = 10000;
    private Function<Long, String> valueFormatter;

    public NumberProgressPrinter() {

    }

    public static NumberProgressPrinter of(String prefix, String suffix) {
        NumberProgressPrinter pp = new NumberProgressPrinter();
        pp.prefix = prefix;
        pp.suffix = suffix;
        return pp;
    }

    public void updateValue(Object value, long cost) throws InterruptedException {
        String costSuffix = suffix + "(" + TimeUtil.format(cost) + ")";
        String newestContent = prefix + value + costSuffix;
        PrintHelper.printPrimary(newestContent);
        TimeUnit.MILLISECONDS.sleep(200);
        if (stop.get()) {
            System.out.println();
        } else {
            for (int i = 0, j = newestContent.length() + costSuffix.length(); i < j; i++) {
                System.out.print("\b");
            }
        }
    }

    @Override
    protected void listening(long cost) {
        if (value.get() % step == 0) {
            try {
                if (valueFormatter == null) {
                    updateValue(value.get(), cost);
                } else {
                    updateValue(valueFormatter.apply(value.get()), cost);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void valueFormatter(Function<Long, String> valueFormatter) {
        this.valueFormatter = valueFormatter;
    }

    public void setStep(long step) {
        this.step = step;
    }
}
