package rabbit.sql.connsole.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.Light;
import rabbit.sql.console.core.DataSourceLoader;
import rabbit.sql.console.core.ViewPrinter;
import rabbit.sql.console.util.SqlUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DBTests {

    @Test
    public void aaa() throws Exception{
        System.out.println(SqlUtil.getType("((  select 1"));
    }

    public static void m(String[] args) {
        System.out.println(System.getProperty("java.class.path"));
    }

    @Test
    public void test1() throws Exception {
        DataSourceLoader loader = DataSourceLoader.of("jdbc:postgresql://127.0.0.1:5432/postgres", "chengyuxing", "123456");
        Light light = loader.getLight();
        Stream<DataRow> s = light.query("select * from test.user");
        s.limit(5).forEach(System.out::println);
        s.limit(5).forEach(System.out::println);
    }

    @Test
    public void regex() throws Exception {
        Pattern p = Pattern.compile(":save *\\$(?<key>res[\\d]) *> *(?<path>[^ ]+)");
        String s = ":save   $res0  >  abc";
        Matcher matcher = p.matcher(s);
        if (matcher.matches()) {
            System.out.println(matcher.group("key"));
            System.out.println(matcher.group("path"));
        }
    }

    @Test
    public void regex2() throws Exception {
        Pattern GET_RES_RANGE_FORMAT = Pattern.compile("^:save *\\$\\{(?<sql>[\\s\\S]+)} *> *(?<path>[\\S]+)$");
        String s = ":save  ${select * from user where id > 1 and name \n like '%ddd%';}  >  /Users/chengyuxing/test/a.xlsx";
        Matcher matcher = GET_RES_RANGE_FORMAT.matcher(s);
        if (matcher.matches()) {
            System.out.println(matcher.group("sql"));
            System.out.println(matcher.group("path"));
        }
    }

    @Test
    public void cmdTest() throws Exception{
        Pattern IS_CMD_FORMAT = Pattern.compile("^:[\\w]+");
        String s = ":get $res0 *dd://\\";
        Matcher m = IS_CMD_FORMAT.matcher(s);
        if (m.find()) {
            System.out.println(1);
        }
    }

    @Test
    public void con() throws Exception{
        System.out.println(System.getProperty("java.class.path"));
    }

    @Test
    public void argTest() throws Exception {
        String[] args = new String[]{
                "jdbc:postgresql",
                "-uchengyuxing",
                "-p123456",
                "-f/Users/chengyuxing/test/files",
                "-e\"select",
                "*",
                "from",
                "test.user\"",
                "-texcel",
        };
        Map<String, String> map = DataSourceLoader.resolverArgs(args);
        System.out.println(map);
    }

    @Test
    public void sss() throws Exception{
        System.out.println(DataSourceLoader.resolverArgs("-n","-p123456","-asss"));
    }

    @Test
    public void as() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "cyx");
        map.put("age", 27);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "jackson");
        map1.put("age", 33);
        List<Map<String, Object>> list = Arrays.asList(map, map1);
        AtomicBoolean first = new AtomicBoolean(true);
        list.forEach(m -> {
            try {
                ViewPrinter.printJSON(DataRow.fromMap(m), first);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }
}
