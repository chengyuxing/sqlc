package rabbit.sql.connsole.test;

import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.Objects;

public class SqlTests {
    @Test
    public void test1() throws Exception {
        String sql = "select * from test.region where id < :id";
        System.out.println(SqlUtil.generateSql(sql, Collections.emptyMap(), true));
    }

    @Test
    public void test2() throws Exception {
        Object v = rabbit.sql.console.util.SqlUtil.stringValue2Object("/Users/chengyuxing/Downloads/prepare.sql");
        System.out.println(v + ": " + Objects.requireNonNull(v).getClass());
    }
}
