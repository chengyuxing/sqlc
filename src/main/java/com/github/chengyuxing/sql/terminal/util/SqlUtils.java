package com.github.chengyuxing.sql.terminal.util;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.common.script.ast.impl.KeyExpressionParser;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.common.Constants;
import com.github.chengyuxing.sql.terminal.cli.Context;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.types.StandardOutParamType;
import com.github.chengyuxing.sql.util.SqlGenerator;
import org.jline.reader.LineReader;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.sql.terminal.util.ObjectUtils.JSON;
import static com.github.chengyuxing.sql.util.SqlUtils.formatSqlTemplate;

public class SqlUtils {
    public static Pattern WORD_PATTERN = Pattern.compile("([a-zA-Z]+)|'(?:''|[^'])*'");
    private static final Map<String, Integer> OUT_PARAM_TYPES = new LinkedHashMap<>();

    public static SqlType detectSQLType(final String sql) {
        Matcher m = WORD_PATTERN.matcher(sql);
        if (m.find()) {
            String w = m.group();
            if (w.equalsIgnoreCase("select")) {
                return SqlType.QUERY;
            }
            if (w.equalsIgnoreCase("with")) {
                while (m.find()) {
                    String token = m.group();
                    if (StringUtils.equalsAnyIgnoreCase(token, "update", "insert", "delete", "merge")) {
                        return SqlType.OTHER;
                    }
                    if (token.equalsIgnoreCase("call")) {
                        return SqlType.PROCEDURE;
                    }
                }
                return SqlType.QUERY;
            }
            if (w.equalsIgnoreCase("call") || sql.trim().startsWith("{")) {
                return SqlType.PROCEDURE;
            }
        }
        return SqlType.OTHER;
    }

    public static Object parseValueFromLiteral(Object literal) throws IOException {
        if (literal == null) {
            return "";
        }
        String ts = literal.toString().trim();
        if (ts.startsWith("[") && ts.endsWith("]")) {
            return JSON.readValue(ts, List.class);
        }
        if (ts.startsWith("{") && ts.endsWith("}")) {
            return JSON.readValue(ts, Map.class);
        }
        if (StringUtils.isNumber(ts)) {
            if (ts.contains(".")) {
                return Double.parseDouble(ts);
            }
            long l = Long.parseLong(ts);
            if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                return (int) l;
            }
            return l;
        }
        if (StringUtils.equalsAnyIgnoreCase(ts, "null", "blank")) {
            return null;
        }
        if (StringUtils.equalsAnyIgnoreCase(ts, "true", "false")) {
            return Boolean.parseBoolean(ts);
        }
        if (isQuote(ts)) {
            return ts.substring(1, ts.length() - 1);
        }
        if (PathUtils.isFilePath(ts)) {
            return PathUtils.resolve(ts).toFile();
        }
        return literal;
    }

    public static boolean isQuote(String s) {
        return (s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"));
    }

    public static String safeQuote(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return "0x" + Arrays.toString((byte[]) value);
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        return value.toString();
    }

    /**
     * 格式化sql，处理其中的字符串模版
     *
     * @param sql        sql字符串
     * @param lineReader readline
     * @return 处理后的sql
     */
    public static String formatSql(String sql, LineReader lineReader) {
        List<String> tempNames = getTemplateNames(sql);
        if (tempNames.isEmpty()) {
            return sql;
        }
        Map<String, Object> templates = new HashMap<>();
        for (String name : tempNames) {
            Context.promptReference.get().param("${" + name + "} = ");
            String template = lineReader.readLine(Context.promptReference.get().getValue()).trim();
            templates.put(name, template);
        }
        return formatSql(formatSqlTemplate(sql, templates), lineReader);
    }

    /**
     * 预编译sql处理构建参数字典
     *
     * @param sql        sql字符串
     * @param lineReader readline
     * @return 解析完成的sql和参数字典
     */
    public static Pair<String, Map<String, Object>> prepareSqlWithArgs(SqlGenerator sqlGenerator, String sql, LineReader lineReader) throws IOException {
        String fmtSql = formatSql(sql, lineReader);
        if (!sql.equals(fmtSql)) {
            Stdout.printlnHighlightSql(fmtSql);
        }
        RabbitScriptParamParser paramParser = new RabbitScriptParamParser(fmtSql, sqlGenerator);
        paramParser.parse();
        Map<String, Set<String>> params = paramParser.getParamsMap();
        if (params.isEmpty()) {
            return Pair.of(fmtSql, Collections.emptyMap());
        }

        Map<String, Object> args = new HashMap<>();
        SqlType type = detectSQLType(fmtSql);
        if (type == SqlType.PROCEDURE) {
            // OUT formatter: num1 = OUT -2017
            // IN_OUT formatter: num1 = IN_OUT -5 126
            // IN formatter: num1 = 180
            Stdout.printlnTitle("OUT Param Types Example", '-', 80, Style.SILVER);

            int i = 1;
            for (Map.Entry<String, Integer> e : getProcedureOutParamTypes().entrySet()) {
                String v = Stdout.colorful(e.getKey() + "(", Style.SILVER) + Stdout.colorful(e.getValue().toString(), Style.DARK_CYAN) + Stdout.colorful(")", Style.SILVER);
                Stdout.printf("%-48s", v);
                if (i % 4 == 0 || v.length() >= 48) {
                    Stdout.println();
                }
                i++;
            }
            Stdout.printlnTitle("", '-', 80, Style.SILVER);
            Stdout.printlnDarkWarning("Format: <in value> | out <name|code> | inout <name|code> <value>");

            for (Map.Entry<String, Set<String>> entry : paramParser.getParamsMap().entrySet()) {
                String name = entry.getKey();
                Context.promptReference.get().param(name + " = ");
                Param param = resolveProcedureArgs(lineReader.readLine(Context.promptReference.get().getValue()).trim());
                args.put(name, param);
            }
        } else {
            for (Map.Entry<String, Set<String>> entry : paramParser.getParamsMap().entrySet()) {
                String name = entry.getKey();
                Context.promptReference.get().param(name + " = ");
                Object value = parseValueFromLiteral(lineReader.readLine(Context.promptReference.get().getValue()).trim());
                args.put(name, value);
            }
        }
        return Pair.of(fmtSql, args);
    }

    public static Map<String, Integer> getProcedureOutParamTypes() {
        if (OUT_PARAM_TYPES.isEmpty()) {
            StandardOutParamType oracle_cursor = StandardOutParamType.ORACLE_CURSOR;
            OUT_PARAM_TYPES.put(oracle_cursor.getName(), oracle_cursor.typeNumber());
            Field[] fields = Types.class.getFields();
            try {
                for (Field f : fields) {
                    int n = f.getModifiers();
                    if (Modifier.isFinal(n) && Modifier.isStatic(n)
                            && (f.getType() == int.class || f.getType() == Integer.class)) {
                        OUT_PARAM_TYPES.put(f.getName().toLowerCase(), (Integer) f.get(null));
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return OUT_PARAM_TYPES;
    }

    public static Map<String, Param> toInOutParam(Map<String, Object> args) {
        Map<String, Param> inOutParams = new HashMap<>();
        args.forEach((k, v) -> inOutParams.put(k, (Param) v));
        return inOutParams;
    }

    public static boolean hasOutParam(Map<String, Param> args) {
        for (Map.Entry<String, Param> entry : args.entrySet()) {
            Param param = entry.getValue();
            if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                return true;
            }
        }
        return false;
    }

    public static int parseOutParamCode(String input) {
        if (StringUtils.isNumber(input)) {
            return Integer.parseInt(input);
        }
        Integer n = OUT_PARAM_TYPES.get(input);
        if (n == null) {
            throw new IllegalArgumentException("Out param type name '" + input + "' not found");
        }
        return n;
    }

    public static Param resolveProcedureArgs(String input) throws IOException {
        Matcher outM = Constants.PROCEDURE_OUT_REGEX.matcher(input);
        if (outM.find()) {
            return Param.OUT(() -> parseOutParamCode(outM.group("out")));
        }
        Matcher inOutM = Constants.PROCEDURE_IN_OUT_REGEX.matcher(input);
        if (inOutM.find()) {
            String in = inOutM.group("in");
            String out = inOutM.group("out");
            return Param.IN_OUT(parseValueFromLiteral(in), () -> parseOutParamCode(out));
        }
        return Param.IN(parseValueFromLiteral(input));
    }

    public static List<String> getTemplateNames(String sql) {
        Matcher m = Constants.SQL_TEMPLATE_ARG_REGEX.matcher(sql);
        List<String> names = new ArrayList<>();
        while (m.find()) {
            String name = m.group("key");
            int idx = KeyExpressionParser.getFirstDotIndex(name);
            if (idx == -1) {
                names.add(name);
            } else {
                names.add(name.substring(0, idx));
            }
        }
        return names;
    }
}
