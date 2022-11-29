package rabbit.sql.connsole.test;

import com.github.chengyuxing.sql.terminal.progress.formatter.PercentFormatter;
import com.github.chengyuxing.sql.terminal.progress.impl.ProgressPrinter;

public class PrinterTests {
    static final ProgressPrinter pp = ProgressPrinter.of("data of ", "");

    public static void main(String[] args) throws InterruptedException {
        int max = 1034;
        pp.setFormatter(new PercentFormatter(max));
        pp.setStep(2);
        pp.whenStopped((v, d) -> {
            System.out.println("已完成：" + v);
            System.out.println(d);
        }).start();

        for (int i = 0; i < max; i++) {
            pp.increment();
            Thread.sleep(10);
        }
        pp.stop();
    }
}
