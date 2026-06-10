package rabbit.sql.connsole.test;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.terminal.cli.App;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import com.github.chengyuxing.sql.terminal.core.FileHelper;
import com.github.chengyuxing.sql.terminal.core.writer.*;
import com.github.chengyuxing.sql.terminal.types.View;
import com.github.chengyuxing.sql.terminal.util.PathUtils;
import com.github.chengyuxing.sql.terminal.util.SqlUtils;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import org.junit.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class CommandShellTests {
    public static void main(String[] args) {
        new CommandLine(new App()).execute(
                "-ujdbc:postgresql://127.0.0.1:5432/postgres",
                "-nchengyuxing"
//                "-eselect current_timestamp",
//                "-eselect 1",
//                "-eselect"
        );
    }

    @Test
    public void test1() {
        System.out.println("abc");
        System.err.println("def");
        System.out.println("\033[1;4;34mRabbit-SQL\033[0m");
    }

    @Test
    public void testBaki() {
        BakiLoader loader = BakiLoader.of("jdbc:postgresql://127.0.0.1:5432/postgres");
        loader.setUsername("chengyuxing");
        loader.init();
//        loader.getUserBaki().query("&selectb").stream().forEach(System.out::println);
    }

    @Test
    public void testP() throws IOException {
        Path p = Paths.get("/Users/chengyuxing/Downloads");
        System.out.println(p);
        System.out.println(Files.isDirectory(p));
        System.out.println(p.resolveSibling(p.getFileName() + ".tmp"));
        Stream.of(FileHelper.getFiles(p, ".xql"))
                .forEach(System.out::println);
        System.out.println(Paths.get("a", "b", "c").toUri());
        System.out.println(PathUtils.resolve("~/home.xql"));
        System.out.println(Files.exists(Paths.get("./README.md")));
        System.out.println(System.currentTimeMillis());
        System.out.println(System.nanoTime());
    }

    @Test
    public void testR() throws IOException {
        System.out.println(SqlUtils.detectSQLType("with a as ('insert')select"));
        System.out.println(SqlUtils.parseValueFromLiteral("{\"name\":\"cyx\"}"));
        System.out.println(SqlUtils.resolveProcedureArgs("out integer  "));
        Stdout.printlnTitle("chengyuxing", '-', 100, Style.SILVER);
    }

    static Stream<DataRow> stream = Stream.of(
            DataRow.of("id", 1, "name", "cyx", "photo", new byte[50]),
            DataRow.of("id", 2, "name", "cyx", "photo", new byte[50]),
            DataRow.of("id", 3, "name", "cyx", "photo", new byte[50]),
            DataRow.of("id", 4, "name", "cyx", "photo", null)
    );

    @Test
    public void testDsvWriter() throws IOException {
        Stream<DataRow> s = Stream.iterate(0, i -> i + 1)
                .limit(50000)
                .map(i -> DataRow.of("name", "chengyuxing_qq", "address", "kunming", "age", i));

        Context.viewMode.set(View.csv);
        IWriter writer = new InsertSQLWriter();
        writer.write(s, "/Users/chengyuxing/Downloads/test.guest");
    }
}
