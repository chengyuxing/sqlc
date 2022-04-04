package rabbit.sql.console.core;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;

import java.util.LinkedHashMap;
import java.util.Map;

public class Command {
    private static final Map<String, String> commands = new LinkedHashMap<>();

    // :load增加重定向方法，就可以完全移除:save方法了
    // 增加一个跳过表头行数的参数
    static {
        commands.put("--help", Printer.colorful("A Command Line sql tool, support Query, DDL, DML, Transaction!", Color.CYAN) + "\n" +
                Printer.colorful("Home page: ", Color.CYAN) + "\33[96;4mhttps://github.com/chengyuxing/sqlc\33[0m\n" +
                Printer.colorful("Notice: add your jdbc driver into the drivers folder;\n" +
                        "command history(require c lib: readline, rlwrap);\n" +
                        "sql keywords completion(require c lib: readline, rlwrap);\n" +
                        "Control+r (require c lib: readline, rlwrap);\n" +
                        "Author: chengyuxing@gmail.com\n" +
                        "Command:\n" +
                        "\t-u[url]\t\t\t\t--jdbc url, e.g.: -ujdbc:postgresql://...\n" +
                        "\t-n[username]\t\t\t--database username.\n" +
                        "\t-p[password]\t\t\t--database password.\n" +
                        "\t-e\"[sql|[@]path][>outputPath]\"\t--execute one or more sql script(or in sql file) or redirect single query to file(if ends with '.sql', will ignore -f and generate insert sql script and file name will as table name) with format(-f) e.g. -e\"select * from user;;create function...\" or -e/usr/local/one.sql, warn: if path starts with '@', batch execute ddl or dml statement for each block(multi line sql delimited by ;;(-d)) faster.\n" +
                        "\t-d\"[delimiter]\"\t\t\t--use for delimit multi and single line sql block to batch execute, default ';;'(double semicolon)\n" +
                        "\t-f[tsv|csv|json|excel]\t\t--format of query result which will be executed.(default tsv)\n" +
                        "\t-v\t\t\t\t--version\n" +
                        "\t-h\t\t\t\t--help\n" +
                        "Interactive Mode:\n" +
                        "\t:q\t\t\t\t--quit.\n" +
                        "\t:c\t\t\t\t--enable cache query results (Warning: be careful out of memory).\n" +
                        "\t:C\t\t\t\t--disable cache query results. (default)\n" +
                        "\t:status\t\t\t\t--show current status.\n" +
                        "\t:clear\t\t\t\t--clear query results cache.\n" +
                        "\t:begin\t\t\t\t--begin transaction, and the prefix[*] means transaction is active now!\n" +
                        "\t:commit\t\t\t\t--commit transaction.\n" +
                        "\t:rollback\t\t\t--rollback transaction.\n" +
                        "\t:[tsv|csv|json|excel]\t\t--as query format and exported file format. (default tsv)\n" +
                        "\t:keys\t\t\t\t--list all keys of cache.\n" +
                        "\t:get $[key][>outputPath]\t--query cache by key or redirect cache data to file.\n" +
                        "\t:rm $[key]\t\t\t--remove the cache by key.\n" +
                        "\t:d [delimiter]\t\t\t--use for delimit multi and single line sql block to batch execute, default ';;'(double semicolon).\n" +
                        "\t:load [[@]path][>outputPath]\t\t\t--load local sql file and execute  or redirect single query to file(if ends with '.sql', will ignore -f and generate insert sql script and file name will as table name) with format(-f), if path starts with '@', batch execute ddl or dml statement for each line faster('delimited by \\n'), otherwise delimited by ';;'(double semicolon).\n" +
                        "\t:h[elp]\t\t\t\t--get some help.", Color.CYAN));
        commands.put("-h", commands.get("--help"));
        commands.put("-v", Version.RELEASE);
    }

    public static String get(String key) {
        return commands.get(key);
    }
}
