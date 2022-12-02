package rabbit.sql.connsole.test;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import org.junit.Test;
import rabbit.sql.console.core.DataSourceLoader;

import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class STests {
    public static void main(String[] args) throws Exception {
        System.out.print("Progress:");
        NumberFormat num = NumberFormat.getPercentInstance();
        num.setMaximumIntegerDigits(4);
        num.setMaximumFractionDigits(3);
        for (int i = 1; i <= 13; i++) {
            double percent = i / 13.0;
            String temp = num.format(percent);
            System.out.print(temp);
            Thread.sleep(1000);
            // 退格
            if (i != 13) {
                for (int j = 0; j < temp.length(); j++) {
                    System.out.print("\b");
                }
            }
        }
        System.out.println();
    }

    @Test
    public void test1() throws Exception {
        System.getProperties().forEach((k, v) -> {
            System.out.println(k + ":" + v);
        });
    }

    @Test
    public void test2() throws Exception {
        String sql = "select * from test.region limit 7>/Users/chengyuxing/Downloads/bbbbb.sql";
        Pattern p = Pattern.compile("(?<sql>[\\s\\S]+)\\s*>\\s*(?<path>\\.*/\\S+)$");
        System.out.println(sql.matches(p.pattern()));
        Matcher m = p.matcher(sql);
        if (m.find()) {
            System.out.println(m.group("sql"));
            System.out.println(m.group("path"));
        }
    }

    @Test
    public void test3() throws Exception {
        Pattern p = Pattern.compile("^:get\\s+\\$(?<key>[\\s\\S]+)$");
        Matcher m = p.matcher(":get $res2    ".trim());
        if (m.find()) {
            System.out.println(m.group("key"));
        }
    }

    @Test
    public void testargs() throws Exception {
        System.out.println(DataSourceLoader.resolverArgs("-ncyx", "-p123456", "-skipHeader"));
    }

    @Test
    public void test() throws Exception {
        System.out.println(Object[].class.isAssignableFrom(byte[].class));
    }

    @Test
    public void testSqlFile() throws Exception {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("x", "file:/Users/chengyuxing/Downloads/xql_file_manager.xql");
//        xqlFileManager.init();
        xqlFileManager.foreach((k, v) -> System.out.println(k + " -> " + v));

        Args<Object> args = Args.of("id", "");

        BakiDao bakiDao = DataSourceLoader.of("jdbc:postgresql://127.0.0.1:5432/postgres", "chengyuxing", "")
                .getBaki("");
        bakiDao.setDebugFullSql(true);
        bakiDao.setXqlFileManager(xqlFileManager);
        Thread.sleep(5000);
        bakiDao.query("&x.query_region", args).forEach(System.out::println);
    }
}
