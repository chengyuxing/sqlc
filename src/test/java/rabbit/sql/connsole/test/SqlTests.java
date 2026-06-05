package rabbit.sql.connsole.test;

import org.junit.Test;

public class SqlTests {
    @Test
    public void test1() throws Exception {
        String sql = "select * from test.region where id < :id";
//        System.out.println(new SqlTranslator(':').generateSql(sql, Collections.emptyMap(), true));
    }
}
