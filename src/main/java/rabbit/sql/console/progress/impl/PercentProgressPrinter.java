package rabbit.sql.console.progress.impl;

import rabbit.sql.console.core.PrintHelper;
import rabbit.sql.console.progress.Progress;

import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;

public class PercentProgressPrinter extends Progress {
    private final NumberFormat num = NumberFormat.getPercentInstance();
    private long max;

    PercentProgressPrinter() {
        num.setMaximumIntegerDigits(4);
        num.setMaximumFractionDigits(2);
    }

    public static PercentProgressPrinter of(long max, String prefix, String suffix) {
        PercentProgressPrinter p = new PercentProgressPrinter();
        p.max = max;
        p.prefix = prefix;
        p.suffix = suffix;
        return p;
    }

    public static PercentProgressPrinter of(long max) {
        return of(max, "", "");
    }

    public void updatePercentValue(long value, long max) throws InterruptedException {
        float maxValue = Float.parseFloat(max + ".0");
        double percent = value / maxValue;
        String newestContent = prefix + num.format(percent) + suffix;
        PrintHelper.printPrimary(newestContent);
        TimeUnit.MILLISECONDS.sleep(200);
        if (value == max || stop.get()) {
            System.out.println();
        } else {
            for (int j = 0, l = newestContent.length() + suffix.length(); j < l; j++) {
                System.out.print("\b");
            }
        }
    }

    @Override
    protected void listening(long cost) {
        try {
            updatePercentValue(value.get(), max);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
