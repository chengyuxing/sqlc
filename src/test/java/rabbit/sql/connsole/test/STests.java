package rabbit.sql.connsole.test;

import org.junit.Test;

public class STests {
    @Test
    public void test1() throws Exception {
        System.getProperties().forEach((k, v) -> {
            System.out.println(k + ":" + v);
        });
    }
}
