package rabbit.sql.console.util;

import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.utils.StringUtil;
import rabbit.sql.console.types.SqlType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtil {
    static Pattern p = Pattern.compile("^[(\\s]*(select|with)\\s+");
    static Pattern TYPE_PARSE = Pattern.compile("::(?<type>[a-zA-Z]+)$");

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

    public static Object stringValue2Object(String str) {
        String value = str;
        Matcher m = TYPE_PARSE.matcher(str);
        if (m.find()) {
            value = str.substring(0, str.lastIndexOf("::"));
            String parse = m.group("type");
            if (parse.equals("date")) {
                return DateTimes.toDate(value);
            }
            throw new UnsupportedOperationException(parse);
        }
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        if (value.matches("[\\d\\-]+")) {
            return Integer.parseInt(value);
        }
        if (value.matches("[\\d.\\-]+")) {
            return Double.parseDouble(value);
        }
        if (StringUtil.equalsAnyIgnoreCase(value, "true", "false")) {
            return Boolean.parseBoolean(value);
        }
        if (StringUtil.equalsAnyIgnoreCase(value, "null")) {
            return null;
        }
        if (StringUtil.startsWiths(value, File.separator, "." + File.separator)) {
            return Paths.get(value).toFile();
        }
        return value;
    }
}
