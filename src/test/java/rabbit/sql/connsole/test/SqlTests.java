package rabbit.sql.connsole.test;

import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

public class SqlTests {
    @Test
    public void test1() throws Exception {
        String sql = "select * from test.region where id < :id";
//        System.out.println(new SqlTranslator(':').generateSql(sql, Collections.emptyMap(), true));
    }

    @Test
    public void test2() throws Exception {
        Object v = SqlUtil.stringValue2Object("[1,2,3,4,5]::string[]");
        if (v instanceof Object[]) {
            System.out.println(Arrays.toString((Object[]) v));
        } else
            System.out.println(v + ": " + Objects.requireNonNull(v).getClass());
    }
}
