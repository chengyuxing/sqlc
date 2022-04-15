package rabbit.sql.connsole.test;

import rabbit.sql.console.util.PrintHelper;

public class PrinterTests {
    public static void main(String[] args) throws InterruptedException {
        String prefix = "chunk";
        String suffix = " executed!" +
                "                \">>> insert into test.region(id,name,pid) values (1,'中国',0)" +
                "                \"more(22)......";
        for (int i = 1; i <= 34; i++) {
            PrintHelper.printPercentProgress(i, 34, prefix, suffix);
        }
    }
}
