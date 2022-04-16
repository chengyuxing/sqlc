package rabbit.sql.console;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;

import java.util.LinkedHashMap;
import java.util.Map;

public class Command {
    private static final Map<String, String> commands = new LinkedHashMap<>();
    static {
        commands.put("--help", Printer.colorful("A Command Line sql tool, support Query, DDL, DML, Transaction, (@)Batch Execute, Export File(json, csv, tsv, excel, sql)!", Color.CYAN) + "\n" +
                Printer.colorful("Home page: ", Color.CYAN) + "\33[96;4mhttps://github.com/chengyuxing/sqlc\33[0m\n" +
                Printer.colorful("Command:\n" +
                        "\t-u[url]\t\t\t\t--jdbc url, e.g.: -ujdbc:postgresql://...\n" +
                        "\t-n[username]\t\t\t--database username.\n" +
                        "\t-p[password]\t\t\t--database password.\n" +
                        "\t-e\"[sql|[@]path][>output]\"\t--(file path start with '@' to batch) execute sql or redirect single query result to file(if ends with '.sql', will ignore -f and generate insert sql script and file name will as table name) with format(-f).\n" +
                        "\t-d\"[delimiter]\"\t\t\t--delimiter for sqls to batch execute, default ';'(single semicolon)\n" +
                        "\t-f[tsv|csv|json|excel]\t\t--format of query result which will be executed.(default tsv)\n" +
                        "\t-v\t\t\t\t--version\n" +
                        "\t-h\t\t\t\t--help\n" +
                        "Interactive Mode:\n" +
                        "\t:q\t\t\t\t--quit.\n" +
                        "\t:c\t\t\t\t--enable cache query results.\n" +
                        "\t:C[!]\t\t\t\t--disable cache(if !, disable and clear all cache)\n" +
                        "\t:ls\t\t\t\t--list all of cache.\n" +
                        "\t:status\t\t\t\t--show current status.\n" +
                        "\t:begin\t\t\t\t--begin transaction.\n" +
                        "\t:commit\t\t\t\t--commit transaction.\n" +
                        "\t:rollback\t\t\t--rollback transaction.\n" +
                        "\t:[tsv|csv|json|excel]\t\t--format of query and exported file(default tsv).\n" +
                        "\t:get [&]key[>output]\t\t--get cache or execute sql by key or redirect cache to file.\n" +
                        "\t:rm key\t\t\t\t--remove the cache by key.\n" +
                        "\t:d [delimiter]\t\t\t--delimiter for sqls to batch execute, default ';'(single semicolon).\n" +
                        "\t:load [@]path[.sql|.xql][>output]\t\t--(file path start with '@' to batch) execute sql or redirect single query result to file(if ends with '.sql', will ignore -f and generate insert sql script and file name will as table name) with format(-f).\n" +
                        "\t:h[elp]\t\t\t\t--get some help.", Color.CYAN));
        commands.put("-h", commands.get("--help"));
        commands.put("-v", Version.RELEASE);
    }

    public static String get(String key) {
        return commands.get(key);
    }
}
