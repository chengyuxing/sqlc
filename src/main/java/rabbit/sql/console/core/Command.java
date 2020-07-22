package rabbit.sql.console.core;

import java.util.LinkedHashMap;
import java.util.Map;

public class Command {
    private static final Map<String, String> commands = new LinkedHashMap<>();

    static {
        commands.put("--help", "\033[96mA Command Line sql tool, support Query, DDL, DML, Transaction!\n" +
                "Notice: add your jdbc driver into the drivers folder;\n" +
                "command history(require c lib: readline, rlwrap);\n" +
                "sql key words completion(require c lib: readline, rlwrap);\n" +
                "Control+r (require c lib: readline, rlwrap);\n" +
                "Author: chengyuxing@gmail.com\n" +
                "Command:\n" +
                "\t-u[url]\t\t\t\t--jdbc url, e.g.: -ujdbc:postgresql://...\n" +
                "\t-n[username]\t\t\t--database username.\n" +
                "\t-p[password]\t\t\t--database password.\n" +
                "\t-e\"[sql]\"\t\t\t--execute a sql(query or ddl/dml) e.g. -e\"select * from user;\"\n" +
                "\t-f[tsv|csv|json|excel]\t\t--format of query result which will be executed.(default tsv)\n" +
                "\t-s[path]\t\t\t--full file path of query result which will be saved.\n" +
                "\t-b[size]\t\t\t--set size for paged query result which will be batch saved(only valid in excel format).\n" +
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
                "\t:tsv\t\t\t\t--use TSV(Tab-separated) as query format. (default)\n" +
                "\t:csv\t\t\t\t--use CSV(Comma-separated) as query format.\n" +
                "\t:json\t\t\t\t--use JSON as query format.\n" +
                "\t:excel\t\t\t\t--use excel(grid) as query format.\n" +
                "\t:batch [size]\t\t\t--set size for paged uncached query result which will be batch saved(only valid in excel format).\n" +
                "\t:keys\t\t\t\t--list all keys of cache.\n" +
                "\t:get $[key]\t\t\t--query all items of cache by key.\n" +
                "\t:get $[key]<[index]\t\t--query indexed item of cache by key.\n" +
                "\t:get $[key]<[start]:[end]\t--query ranged items of cache by key.\n" +
                "\t:rm $[key]\t\t\t--remove the cache by key.\n" +
                "\t:size $[key]\t\t\t--query cache size by key.\n" +
                "\t:save $[key]>[path]\t\t--if cache enabled, save the selected cache to local file.\n" +
                "\t:save ${[query]}>[path]\t\t--save the query result to local file.(faster than cached result, no cache and it doesn't print the result.)\n" +
                "\t:help\t\t\t\t--get some help.\033[0m");
        commands.put("-h", commands.get("--help"));
        commands.put("-v", "1.0.0");
    }

    public static String get(String key) {
        return commands.get(key);
    }
}
