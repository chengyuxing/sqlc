package rabbit.sql.console.util;

import rabbit.sql.console.types.SqlType;

public class SqlUtil {
    public static SqlType getType(final String sql) {
        String trimSql = sql.trim();
        if (trimSql.startsWith("insert") ||
                trimSql.startsWith("update") ||
                trimSql.startsWith("delete") ||
                trimSql.startsWith("drop") ||
                trimSql.startsWith("alter") ||
                trimSql.startsWith("truncate") ||
                trimSql.startsWith("create")) {
            return SqlType.OTHER;
        } else if (trimSql.startsWith("{") && trimSql.endsWith("}")) {
            return SqlType.FUNCTION;
        } else {
            return SqlType.QUERY;
        }
    }
}
