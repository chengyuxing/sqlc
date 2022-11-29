package com.github.chengyuxing.sql.terminal.progress.formatter;

import com.github.chengyuxing.sql.terminal.util.TimeUtil;

import java.text.NumberFormat;
import java.util.function.BiFunction;

public class PercentFormatter implements BiFunction<Long, Long, String> {
    private static final NumberFormat num = NumberFormat.getPercentInstance();
    private final long max;

    public PercentFormatter(long max) {
        this.max = max;
        num.setMaximumIntegerDigits(4);
        num.setMaximumFractionDigits(2);
    }

    @Override
    public String apply(Long aLong, Long aLong2) {
        float maxValue = Float.parseFloat(max + ".0");
        double percent = aLong / maxValue;
        String sp = num.format(percent);
        return "chunk " + aLong + "(" + aLong * 1000 + ") " + sp + " executed.(" + TimeUtil.format(aLong2) + ")";
    }
}
