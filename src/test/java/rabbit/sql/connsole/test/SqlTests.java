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
}
