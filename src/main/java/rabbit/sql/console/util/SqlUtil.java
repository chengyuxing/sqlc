package rabbit.sql.console.util;

import rabbit.sql.console.types.SqlType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtil {
    static Pattern p = Pattern.compile("^[(\\s]*(select|with)\\s+");

    public static SqlType getType(final String sql) {
        String trimSql = sql.trim();
        Matcher m = p.matcher(trimSql);
        if (m.find()) {
            return SqlType.QUERY;
        } else if (trimSql.startsWith("call") || trimSql.startsWith("{")) {
            return SqlType.FUNCTION;
        } else {
            return SqlType.OTHER;
        }
    }
}
