package rabbit.sql.connsole.test;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.Arguments;
import com.github.chengyuxing.sql.terminal.cli.Command;
import com.github.chengyuxing.sql.terminal.util.ExceptionUtil;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import org.junit.Test;
import com.github.chengyuxing.sql.terminal.core.DataSourceLoader;
import com.github.chengyuxing.sql.terminal.core.FileHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class STests {
    public static void main(String[] args) throws Exception {
        Stream<DataRow> rowStream = Stream.iterate(0, (i) -> i + 1)
//                .limit(10006093)
                .limit(906093)
                .map(i -> DataRow.fromPair("id", i, "name", "cyx", "address", "昆明市西山区", "age", 27));
//        FileHelper.writeJSON(rowStream, "/Users/chengyuxing/Downloads/big.json");
//        FileHelper.writeDSV(rowStream, new AtomicReference<>(View.TSV), "/Users/chengyuxing/Downloads/big.tsv");
//        FileHelper.writeExcel(rowStream, "/Users/chengyuxing/Downloads/big.xlsx");
        FileHelper.writeInsertSqlFile(rowStream, "/Users/chengyuxing/Downloads/test.big.sql");
//        ObjectMapper mapper = new ObjectMapper();
//        SequenceWriter writer = mapper.writer().withDefaultPrettyPrinter().writeValuesAsArray(Files.newOutputStream(Paths.get("/Users/chengyuxing/Downloads/big.json")));
//        rowStream.forEach(d->{
//            try {
//                writer.write(d);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
    }

    @Test
    public void testFileLines() throws Exception {
        System.out.println(FileHelper.lineNumber("/Users/chengyuxing/Downloads/big.tsv"));
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
        System.out.println(new Arguments("-ncyx", "-p123456", "-skipHeader"));
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

        BakiDao bakiDao = DataSourceLoader.of("jdbc:postgresql://127.0.0.1:5432/postgres")
                .getBaki();
        bakiDao.setDebugFullSql(true);
        bakiDao.setXqlFileManager(xqlFileManager);
        Thread.sleep(5000);
        bakiDao.query("&x.query_region").args(args).stream().forEach(System.out::println);
    }

    @Test
    public void testBuildText() throws Exception {
        List<String> sqls = Files.readAllLines(Paths.get("/Users/chengyuxing/Downloads/skynet.files_1669005346688/skynet.files.sql"));
//        App.createBlobArgs(sqls, Paths.get("/Users/chengyuxing/Downloads/skynet.files_1669005346688/skynet.files.sql"));
    }

    @Test
    public void testFile() throws Exception {
        FileResource fr = new FileResource("file:" + "/Users/chengyuxing/Downloads/skynet.files_1669005346688/skynet.files.sql");
        System.out.println(fr.getFilenameExtension());
        System.out.println(fr.getFileName());
    }

    @Test
    public void testRegex() throws Exception {
        System.out.println(Constants.USER_HOME);
        System.out.println(Constants.CURRENT_DIR);
        System.out.println(Constants.TERM);
        System.out.println(Constants.IS_XTERM);
    }

    @Test
    public void testFilePath() throws Exception {
        String path = "./README.md";
        System.out.println(Paths.get(path));
        System.out.println(Paths.get(path).toAbsolutePath());
    }

    @Test
    public void testFilesCompare() throws Exception {
        try {
            catchError();
        } catch (Exception e) {
            System.out.println(ExceptionUtil.getCauseMessage(e));;
        }
    }

    public static void catchError() {
        try {
            Path path = Paths.get("/Users/chengyuxing/Downloads/xql_file_manager.xqlm").toRealPath();
            System.out.println(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void testCmdDesc() throws Exception {
        Command.get("--cmd");
    }

    @Test
    public void testArgs() throws Exception {
        System.out.println(new Arguments("-nchengyuxing", "-p").toMap().get("-p"));
        for (int i = 5; i >= 0; i--) {
            System.out.println(i);
        }
    }
}
