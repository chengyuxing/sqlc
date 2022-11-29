package rabbit.sql.connsole.test;

import com.github.chengyuxing.sql.utils.SqlTranslator;
import org.junit.Test;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Stream;

public class ArgsResolveTests {
    public static void main(String[] args) {
        try {
            Stream.iterate(0, i -> i + 1)
                    .limit(10)
                    .peek(i -> {
                        if (i == 7) {
                            try {
                                makeError();
                            } catch (FileNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    })
                    .forEach(System.out::println);
        } catch (Exception e) {
            PrintHelper.printlnWarning(e.toString());
        }

    }

    public static void makeError() throws FileNotFoundException {
        throw new FileNotFoundException("aaaaa");
    }

    @Test
    public void test1() throws Exception {
        System.out.println("/Users/chengyuxing/Downloads/blob_2_photo".matches(".+/blob_\\d+_.+"));
    }

    @Test
    public void testf() throws Exception {
        String path = "/usr/sss/user";
        System.out.println(Paths.get(path).getFileName().toString());
    }

    @Test
    public void tests() throws Exception {
        SqlTranslator sqlTranslator = new SqlTranslator(':');
        String sql = "insert into skynet.files(id, photo, file) values (1, :blob_0_photo, :blob_0_file);";
        System.out.println(sqlTranslator.getPreparedSql(sql, Collections.emptyMap()));
    }

    @Test
    public void testx() throws Exception {
        String fn = "/Users/chengyuxing/Downloads/big.tsv";
        System.out.println(fn.substring(fn.lastIndexOf(".") + 1));
        System.out.println(Paths.get(fn).getFileName());
        System.out.println(Files.exists(Paths.get("/Users/chengyuxing/Downloads")));
    }
}
