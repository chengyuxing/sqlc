package rabbit.sql.connsole.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.Arguments;
import com.github.chengyuxing.sql.terminal.core.*;
import org.junit.Test;
import org.postgresql.util.PGobject;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBTests {

    @Test
    public void aaa() throws Exception {
        System.out.println(SqlUtil.getType("((  select 1"));
    }

    public static void m(String[] args) {
        System.out.println(System.getProperty("java.class.path"));
    }

    @Test
    public void testPg() throws Exception {
        DataSourceLoader loader = DataSourceLoader.of("jdbc:postgresql://127.0.0.1:5432/postgres");
        loader.setUsername("chengyuxing");
        loader.init();
        DataBaseResource dataBaseResource = new DataBaseResource(loader);
//        dataBaseResource.getUserTableNames().forEach(System.out::println);
//        System.out.println(dataBaseResource.getProcedureDefinition("test.slow_query(integer, integer)"));
//        System.out.println(dataBaseResource.getViewDefinition("test.big_top5"));
//        System.out.println(dataBaseResource.getTriggerDefinition("test.big.notice_big_bad"));
//        System.out.println(dataBaseResource.getUserTriggers());
//        System.out.println(dataBaseResource.getUserViews());
//        System.out.println(dataBaseResource.getUserProcedures());
//        System.out.println(dataBaseResource.getTableDefinition("test.hello"));

        List<List<String>> tableDesc = dataBaseResource.getTableDesc("test.big");
        PrintHelper.printGrid(tableDesc);
    }

    @Test
    public void testMysql() throws Exception {
        DataSourceLoader loader = DataSourceLoader.of("jdbc:mysql://139.198.19.116:3306/test");
        loader.setUsername("sp");
        loader.setPassword("A14_sp_123");
        loader.init();
        DataBaseResource dataBaseResource = new DataBaseResource(loader);
        dataBaseResource.getUserTableNames().forEach(System.out::println);
    }

    @Test
    public void test2() throws Exception {
        DataSourceLoader loader = DataSourceLoader.of("jdbc:postgresql://127.0.0.1:5432/postgres");
        Baki baki = loader.getUserBaki();
        baki.query("select '{\"a\":\"cyx\"}'::jsonb as x").findFirst().ifPresent(d -> {
            Object v = d.get("x");
            if (v instanceof PGobject) {
                System.out.println(((PGobject) v).getValue());
            }
            System.out.println(d.getType(0));
            System.out.println(d.get("x"));
        });
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
    public void cmdTest() throws Exception {
        Pattern IS_CMD_FORMAT = Pattern.compile("^:[\\w]+");
        String s = ":get $res0 *dd://\\";
        Matcher m = IS_CMD_FORMAT.matcher(s);
        if (m.find()) {
            System.out.println(1);
        }
    }

    @Test
    public void con() throws Exception {
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
        Arguments map = new Arguments(args);
        System.out.println(map);
    }

    @Test
    public void sss() throws Exception {
        System.out.println(new Arguments("-n", "-p123456", "-asss"));
    }

    @Test
    public void sqlFile() throws Exception {
        XQLFileManager manager = new XQLFileManager(Args.of("sql", "/Users/chengyuxing/Downloads/sqlc.sql"));
        manager.init();
        manager.foreachEntry((k, r) -> r.forEach((n, v) -> {
            if (!n.startsWith("${")) {
                Printer.println("Execute sql [ " + n + " ] ::: ", Color.DARK_CYAN);
            }
        }));
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
                PrintHelper.printJSON(DataRow.fromMap(m), first);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }
}
