package rabbit.sql.console.util;

import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.utils.StringUtil;
import rabbit.sql.console.types.SqlType;

import java.io.File;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SqlUtil {
    public static Pattern p = Pattern.compile("^[(\\s]*(select|with)\\s+");
    public static Pattern TYPE_PARSE = Pattern.compile("::(?<type>[a-zA-Z]+\\[*)((?<delimiter>[\\s\\S]*)])*$");

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
            value = str.substring(0, m.start("type") - 2);
            String parse = m.group("type");
            if (parse.equals("date")) {
                return DateTimes.toDate(value);
            }
            if (value.startsWith("[") && value.endsWith("]")) {
                String delimiter = m.group("delimiter");
                if (delimiter != null && parse.endsWith("[")) {
                    delimiter = delimiter.trim();
                    if (delimiter.equals("")) {
                        delimiter = ",";
                    }
                    String[] filteredArrS = Stream.of(value.substring(1, value.length() - 1).split(delimiter))
                            .map(String::trim)
                            .filter(v -> !v.equals(""))
                            .toArray(String[]::new);
                    if (parse.equals("string[")) {
                        return filteredArrS;
                    }
                    int length = filteredArrS.length;
                    if (parse.equals("int[")) {
                        int[] ints = new int[length];
                        for (int i = 0; i < length; i++) {
                            ints[i] = Integer.parseInt(filteredArrS[i]);
                        }
                        return ints;
                    }
                    if (parse.equals("double[")) {
                        double[] doubles = new double[length];
                        for (int i = 0; i < length; i++) {
                            doubles[i] = Double.parseDouble(filteredArrS[i]);
                        }
                        return doubles;
                    }
                    if (parse.equals("float[")) {
                        float[] floats = new float[length];
                        for (int i = 0; i < length; i++) {
                            floats[i] = Float.parseFloat(filteredArrS[i]);
                        }
                        return floats;
                    }
                    if (parse.equals("long[")) {
                        long[] longs = new long[length];
                        for (int i = 0; i < length; i++) {
                            longs[i] = Long.parseLong(filteredArrS[i]);
                        }
                        return longs;
                    }
                }
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
