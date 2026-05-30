package rabbit.sql.connsole.test;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.terminal.cli.StartupShell;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import org.junit.Test;
import picocli.CommandLine;

public class CommandShellTests {
    public static void main(String[] args) {
        new CommandLine(new StartupShell()).execute(
                "-ujdbc:postgresql://127.0.0.1:5432/postgres",
                "-nchengyuxing",
//                "-eselect current_timestamp",
//                "-eselect 1",
                "-eselect"
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
        DataRow row = loader.getUserBaki().execute("", Args.of());
        System.out.println(row);
    }
}
