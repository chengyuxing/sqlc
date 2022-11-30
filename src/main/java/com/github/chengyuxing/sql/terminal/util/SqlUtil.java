package com.github.chengyuxing.sql.terminal.util;

import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import org.jline.reader.LineReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.utils.SqlUtil.quoteFormatValueIfNecessary;

public class SqlUtil {
    public static Pattern p = Pattern.compile("^[(\\s]*(select|with)\\s+");
    public static Pattern TYPE_PARSE = Pattern.compile("::(?<type>[a-zA-Z]+\\[*)((?<delimiter>[\\s\\S]*)])*$");
    public static final SqlTranslator sqlTranslator = new SqlTranslator(':');

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
        if (value.startsWith("'") && value.startsWith("'")) {
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
        if (StringUtil.startsWiths(value, File.separator, "." + File.separator, ".." + File.separator)) {
            return Paths.get(value).toFile();
        }
        return value;
    }

    public static Pair<String, List<String>> generateInsert(final String tableName, final Map<String, ?> row, long blobRowNum) {
        StringJoiner f = new StringJoiner(", ");
        StringJoiner v = new StringJoiner(", ");
        List<String> blobKeys = new ArrayList<>();
        for (Map.Entry<String, ?> e : row.entrySet()) {
            if (e.getValue() instanceof byte[]) {
                f.add(e.getKey());
                v.add(":blob_" + blobRowNum + "_" + e.getKey());
                blobKeys.add(e.getKey());
            } else {
                f.add(e.getKey());
                v.add(quoteFormatValueIfNecessary(e.getValue()));
            }
        }
        return Pair.of("insert into " + tableName + "(" + f + ") values (" + v + ")", blobKeys);
    }

    public static Map<String, Object> prepareSqlArgIf(String sql, LineReader lineReader) {
        Pair<String, List<String>> pSql = sqlTranslator.generateSql(sql, Collections.emptyMap(), true);
        List<String> pNames = pSql.getItem2();
        if (pNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Set<String> distinctArgs = new LinkedHashSet<>(pSql.getItem2());
        Map<String, Object> parameters = new HashMap<>();
        for (String name : distinctArgs) {
            StatusManager.promptReference.get().custom(name + " = ");
            Object value = SqlUtil.stringValue2Object(lineReader.readLine(StatusManager.promptReference.get().getValue()).trim());
            parameters.put(name, value);
        }
        return parameters;
    }

    /**
     * 整个大字符串或sql文件根据分隔符分块
     *
     * @param multiSqlOrFilePath sql或文件路径
     * @return 一组sql
     * @throws IOException 如果读取文件发生异常或文件不存在
     */
    public static List<String> multiSqlList(String multiSqlOrFilePath) throws IOException {
        String sqls = getSqlByFileIf(multiSqlOrFilePath);
        return Stream.of(sqls.split(StatusManager.sqlDelimiter.get()))
                .filter(sql -> !sql.trim().equals("") && !sql.matches("^[;\r\t\n]$"))
                .collect(Collectors.toList());
    }

    /**
     * 如果是文件路径就读取文件返回sql
     *
     * @param sqlOrPath sql字符串或路径
     * @return sql字符串
     * @throws IOException 如果读取文件发生异常或文件不存在
     */
    public static String getSqlByFileIf(String sqlOrPath) throws IOException {
        if (sqlOrPath.startsWith(File.separator) || sqlOrPath.startsWith("." + File.separator)) {
            Path path = Paths.get(sqlOrPath).toAbsolutePath();
            if (!Files.exists(path)) {
                throw new FileNotFoundException("sql file [" + sqlOrPath + "] not exists.");
            }
            return String.join("\n", Files.readAllLines(path));
        }
        return sqlOrPath;
    }

    public static Pair<String, String> getSqlAndRedirect(String s) {
        String[] parts = s.split(Constants.REDIRECT_SYMBOL);
        return Pair.of(parts[0].trim(), parts[1].trim());
    }
}
