package rabbit.sql.console.core;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import rabbit.sql.console.Version;

import java.util.LinkedHashMap;
import java.util.Map;

public class Command {
    private static final Map<String, String> commands = new LinkedHashMap<>();

    static {
        commands.put("--help", Printer.colorful("A Command Line sql tool, support Query, DDL, DML, Transaction!\n" +
                "Notice: add your jdbc driver into the drivers folder;\n" +
                "command history(require c lib: readline, rlwrap);\n" +
                "sql key words completion(require c lib: readline, rlwrap);\n" +
                "Control+r (require c lib: readline, rlwrap);\n" +
                "Author: chengyuxing@gmail.com\n" +
                "Command:\n" +
                "\t-u[url]\t\t\t\t--jdbc url, e.g.: -ujdbc:postgresql://...\n" +
                "\t-n[username]\t\t\t--database username.\n" +
                "\t-p[password]\t\t\t--database password.\n" +
                "\t-e\"[sql|[@]path]\"\t\t--execute one or more sql script(or in sql file) e.g. -e\"select * from user;;create function...\" or -e/usr/local/one.sql, warn: if path starts with '@', batch execute ddl or dml statement for each line faster('delimited by \\n') \n" +
                "\t-d\"[delimiter]\"\t\t--use for delimit multi and single line sql block, default ';;'(double semicolon)\n" +
                "\t-f[tsv|csv|json|excel]\t\t--format of query result which will be executed.(default tsv)\n" +
                "\t-s/[path]\t\t\t--full file path of query result which will be saved(with -e or -x).\n" +
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
                "\t:get $key[<lineNum][:endNum]\t--query cache by key, 1 is first index.\n" +
                "\t:rm $key\t\t\t--remove the cache by key.\n" +
                "\t:size $key\t\t\t--query cache size by key.\n" +
                "\t:d [delimiter]\t\t\t--use for delimit multi and single line sql block, default ';;'(double semicolon).\n" +
                "\t:load [[@]path]\t\t\t--load local sql file and execute, if path starts with '@', batch execute ddl or dml statement for each line faster('delimited by \\n'), otherwise delimited by ';;'(double semicolon).\n" +
                "\t:save $key|${query}>[path]\t--save the cache or query result to local file.(faster than cached result, no cache and it doesn't print the result.)\n" +
                "\t:h[elp]\t\t\t\t--get some help.", Color.CYAN));
        commands.put("-h", commands.get("--help"));
        commands.put("-v", Version.RELEASE);
    }

    public static String get(String key) {
        return commands.get(key);
    }
}
