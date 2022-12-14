package rabbit.sql.connsole.test;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.excel.Excels;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.cli.Arguments;
import com.github.chengyuxing.sql.terminal.cli.Help;
import com.github.chengyuxing.sql.terminal.cli.completer.DbObjectCompleter;
import com.github.chengyuxing.sql.terminal.util.ExceptionUtil;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import org.junit.Test;
import com.github.chengyuxing.sql.terminal.core.DataSourceLoader;
import com.github.chengyuxing.sql.terminal.core.FileHelper;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Types;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class STests {
    public static void main(String[] args) throws Exception {
        Stream<DataRow> rowStream = Stream.iterate(0, (i) -> i + 1)
//                .limit(10006093)
                .limit(906093)
                .map(i -> DataRow.fromPair("id", i, "name", "cyx", "address", "昆明市西山区", "age", 27));
        FileHelper.writeJSON(rowStream, "/Users/chengyuxing/Downloads/big.json");
//        FileHelper.writeDSV(rowStream, new AtomicReference<>(View.TSV), "/Users/chengyuxing/Downloads/big.tsv");
//        FileHelper.writeExcel(rowStream, "/Users/chengyuxing/Downloads/big.xlsx");
//        FileHelper.writeInsertSqlFile(rowStream, "/Users/chengyuxing/Downloads/test.big.sql");
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
                .getUserBaki();
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
            System.out.println(ExceptionUtil.getCauseMessage(e));
            ;
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
        Help.get("--cmd");
    }

    @Test
    public void testArr() throws Exception {
        List<String> strings = new ArrayList<>();
        strings.add("abcde");
        strings.add("abc");
        strings.add("bc");
        System.out.println(strings);
    }

    @Test
    public void testArgs() throws Exception {
        Arguments args = new Arguments("-nchengyuxing", "-p", "-header-10", "--with-tx");
        System.out.println(args);
        System.out.println(args.getIfBlank("-vp", "10"));
    }

    @Test
    public void testNum() throws Exception {
        Integer i = 2147483647;
        System.out.println(Integer.parseInt("2147483649"));
    }

    @Test
    public void testjacksonRead() throws Exception {

    }

    @Test
    public void testJson() throws Exception {
        String json = "[{\n" +
                "  \"name\": \"cyx\",\n" +
                "  \"age\": 28,\n" +
                "  \"address\": \"昆明市西山区\"\n" +
                "},{\n" +
                "\"name\": \"cyx\",\n" +
                "\"age\": 28,\n" +
                "\"address\": \"昆明市西山区\"\n" +
                "},{\n" +
                "\"name\": \"cyx\",\n" +
                "\"age\": 28,\n" +
                "\"address\": \"昆明市西山区\"\n" +
                "},{\n" +
                "\"name\": \"cyx\",\n" +
                "\"age\": 28,\n" +
                "\"address\": \"昆明市西山区\"\n" +
                "},{\n" +
                "\"name\": \"cyx\",\n" +
                "\"age\": 28,\n" +
                "\"address\": \"昆明市西山区\"\n" +
                "}]";

        String[] jsonLine = json.split("\n");

        // json中字符串是不跨行的，
        // 查找 json是是否有转义 \"
        // 转义的 \" 替换为别的特殊字符，
        // 在将字符串字段 "" 里的内容临时存起来（避免字符串内有括号造成解析异常），找完括号在将其还原回来

        if (json.startsWith("[")) {
            for (int i = 0; i < jsonLine.length; i++) {
                String line = jsonLine[i];
                int start = line.indexOf("{");
                if (start != -1) {
                    StringJoiner jsonObj = new StringJoiner("\n");
                    jsonObj.add(line.substring(start));
                    int count = 1;
                    while (true) {
                        String next = jsonLine[++i];
                        int nextStartIdx = next.indexOf("{");
                        int nextEndIdx = next.indexOf("}");
                        if (nextEndIdx != -1) {
                            count--;
                            if (count == 0) {
                                jsonObj.add(next.substring(0, nextEndIdx + 1));
                                // },{
                                if (nextStartIdx != -1) {
                                    --i;
                                }
                                break;
                            }
                        }
                        if (nextStartIdx != -1) {
                            count++;
                        }
                        if (count != 0) {
                            jsonObj.add(next);
                        }
                    }
                    System.out.println(jsonObj);
                }
            }
        }
    }

    @Test
    public void testGetSqlTypes() throws Exception {
        Class<?> clazz = Types.class;
        Field[] fields = clazz.getDeclaredFields();
        Stream.of(fields)
                .forEach(f -> {
                    try {
                        System.out.println(f.getName() + "(" + f.get(null) + ")");
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testXql() throws Exception {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("me", "file:/Users/chengyuxing/Downloads/xql_file_manager.xql");
        xqlFileManager.init();
        String sql = xqlFileManager.get("me.query_region", Args.create("id", 131, "age", 28));
        System.out.println(sql);
    }

    @Test
    public void testSqlTemp() throws Exception {
        Path path = Paths.get("/Users/chengyuxing/IdeaProjects/sqlc/build/./completion/mysql.cnf");
        try (Stream<String> lines = Files.lines(path)) {
            lines.map(line -> Arrays.asList(line.split("\\s+")))
                    .flatMap(Collection::stream)
                    .forEach(System.out::println);
        } catch (IOException e) {
        }
    }

    @Test
    public void testM() throws Exception {
        System.out.println(Paths.get("/Users/chengyuxing/Downloads\\").toString());
    }

    @Test
    public void testExcel() throws Exception {
        Excels.reader(Paths.get("/Users/chengyuxing/Downloads/big.xlsx"))
                .namedHeaderAt(-1)
                .fieldMap(new String[]{"id", "name", "address", "age"})
                .stream()
                .skip(0)
                .peek(d -> d.removeIf((k, v) -> k == null || k.trim().equals("")))
                .peek(d -> d.removeIf((k, v) -> v == null || v.toString().equals("")))
                .filter(d -> !d.isEmpty())
                .forEach(System.out::println);
    }

    @Test
    public void testTemp() throws Exception{
        System.out.println(System.getProperty("java.class.path"));
        System.getenv().forEach((k,v)->{
            System.out.println(k + ": " + v);
        });
    }

    @Test
    public void testList() throws Exception{

    }
}
