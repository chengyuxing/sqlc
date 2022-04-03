package rabbit.sql.connsole.test;

import org.junit.Test;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class STests {
    @Test
    public void test1() throws Exception {
        System.getProperties().forEach((k, v) -> {
            System.out.println(k + ":" + v);
        });
    }

    @Test
    public void test2() throws Exception {
        String sql = "select * from test.region limit 10 > asd/bbb.sql";
        Pattern p = Pattern.compile("\\s*>\\s*(?<path>\\.*/\\S+)$");
        Matcher m = p.matcher(sql);
        if (m.find()) {
            System.out.println(m.group("path"));
        }
    }
    @Test
    public void test3() throws Exception{
        Pattern p = Pattern.compile("\\$(?<key>res\\d+)\\s*>\\s*(?<path>\\.*" + File.separator + "\\S+)$");
        Matcher m = p.matcher("$res2 > /usr/local/aaa.txt");
        if (m.find()) {
            System.out.println(m.group("key"));
            System.out.println(m.group("path"));
        }
    }
}
