package com.github.chengyuxing.sql.terminal.util;

import com.github.chengyuxing.common.script.ast.impl.KeyExpressionParser;
import com.github.chengyuxing.common.script.lang.Token;
import com.github.chengyuxing.common.script.lang.TokenType;
import com.github.chengyuxing.common.script.lexer.RabbitScriptLexer;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.util.SqlGenerator;
import com.github.chengyuxing.sql.util.SqlUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.util.StringUtils.scan;

public class RabbitScriptParamParser {
    private final SqlGenerator sqlGenerator;
    private final List<Token> tokens;
    private int currentTokenIndex;
    private Token currentToken;

    private final Map<String, Set<String>> paramsMap = new LinkedHashMap<>();
    private final Set<String> localParams = new HashSet<>();
    private final List<String> plainSqlParts = new ArrayList<>();

    public RabbitScriptParamParser(String input, SqlGenerator sqlGenerator) {
        this.sqlGenerator = sqlGenerator;
        RabbitScriptLexer lexer = new RabbitScriptLexer(input) {
            @Override
            protected String normalizeDirectiveLine(String line) {
                int idx = SqlUtils.indexOfWholeLineComment(line);
                if (idx != -1) {
                    return line.substring(idx + 2);
                }
                return line;
            }
        };
        this.tokens = lexer.tokenize();
        this.currentTokenIndex = 0;
        this.currentToken = tokens.get(currentTokenIndex);
    }

    private void advance() {
        currentTokenIndex++;
        if (currentTokenIndex < tokens.size()) {
            currentToken = tokens.get(currentTokenIndex);
        } else {
            Token lastToken = tokens.get(currentTokenIndex - 1);
            currentToken = new Token(TokenType.EOF, "", lastToken.getLine(), lastToken.getColumn());
        }
    }

    public static Pair<String, String> getKeyAndProp(String name) {
        String key = name;
        String prop = "";

        int idx = KeyExpressionParser.getFirstDotIndex(name);

        if (idx != -1) {
            key = name.substring(0, idx);
            prop = name.substring(idx);
        }
        return Pair.of(key, prop);
    }

    private void appendToken(StringJoiner sb, Token token) {
        if (token.getType() == TokenType.STRING) {
            sb.add("'" + token.getValue() + "'");
            return;
        }
        sb.add(token.getValue());
    }

    private String replaceVarHolder(String var, String content) {
        Pattern p = Pattern.compile("(?<var>:" + var + ")");
        StringBuilder sb = new StringBuilder();
        scan(content, p, (seg, hit) -> {
            if (hit) {
                sb.append("_");
            } else {
                sb.append(seg);
            }
        });
        return sb.toString();
    }

    private void parsePlainText(String text) {
        sqlGenerator.generatePreparedSql(text, Args.of()).getArgNameIndexMapping()
                .keySet()
                .forEach(key -> {
                    Pair<String, String> kp = getKeyAndProp(key);
                    String var = "_";
                    String prop = kp.getItem2();
                    if (!prop.isEmpty()) {
                        var += prop;
                    }
                    paramsMap.computeIfAbsent(kp.getItem1(), k -> new LinkedHashSet<>()).add(var);
                });

        Matcher m = StringUtils.FMT.getPattern().matcher(text);
        while (m.find()) {
            String key = m.group("key");
            if (key.startsWith("!")) {
                key = key.substring(1);
            }
            Pair<String, String> kp = getKeyAndProp(key);
            String var = "_";
            String prop = kp.getItem2();
            if (!prop.isEmpty()) {
                var += prop;
            }
            var = "${" + var + "}";
            paramsMap.computeIfAbsent(kp.getItem1(), k -> new LinkedHashSet<>()).add(var);
        }
    }

    /**
     * Parse to (key, key expression), e.g. (user, user.addresses[0])
     *
     * @return pair
     */
    private Pair<String, String> parseExpression() {
        StringBuilder sb = new StringBuilder();
        sb.append(currentToken.getValue());
        advance();

        String keyStart = currentToken.getValue();
        sb.append(keyStart);
        advance();
        while (currentToken.getType() != TokenType.NEWLINE) {
            if (currentToken.getType() == TokenType.DOT) {
                sb.append(".");
                advance();
                sb.append(currentToken.getValue()); //number
                advance();
            } else if (currentToken.getType() == TokenType.LBRACKET) {
                sb.append("[");
                advance();
                sb.append(currentToken.getValue());
                advance();
                sb.append("]");
                advance();
            } else {
                break;
            }
        }
        return Pair.of(keyStart, sb.toString());
    }

    private Set<String> parseExpressions(StringJoiner sb) {
        Set<String> vars = new LinkedHashSet<>();
        while (currentToken.getType() != TokenType.NEWLINE) {
            if (currentToken.getType() == TokenType.COLON) {
                Pair<String, String> pair = parseExpression();
                vars.add(pair.getItem1());
                sb.add(pair.getItem2());
            } else {
                appendToken(sb, currentToken);
                advance();
            }
        }
        return vars;
    }

    private Set<String> parseForExpressions(StringJoiner sb) {
        Set<String> vars = new LinkedHashSet<>();
        while (currentToken.getType() != TokenType.NEWLINE) {
            if (currentToken.getType() == TokenType.COLON) {
                Pair<String, String> pair = parseExpression();
                vars.add(pair.getItem1());
                sb.add(pair.getItem2());
            } else if (currentToken.getType() == TokenType.FOR_PROPERTY_AS) {
                sb.add(currentToken.getValue()); // as
                advance();
                localParams.add(currentToken.getValue());
                sb.add(currentToken.getValue());
                advance();
            } else {
                appendToken(sb, currentToken);
                advance();
            }
        }
        return vars;
    }

    private void parseVarStatement() {
        StringJoiner sb = new StringJoiner(" ");
        sb.add(currentToken.getValue()); // #var
        advance();
        sb.add(currentToken.getValue()); // var name
        localParams.add(currentToken.getValue());
        advance();
        Set<String> vars = parseExpressions(sb);
        vars.forEach(var -> {
            String summary = replaceVarHolder(var, sb.toString());
            paramsMap.computeIfAbsent(var, k -> new LinkedHashSet<>()).add(summary);
        });
    }

    private void parseConditionStatement() {
        StringJoiner sb = new StringJoiner(" ");
        sb.add(currentToken.getValue());
        advance();
        Set<String> vars = parseExpressions(sb);
        vars.forEach(var -> {
            String summary = replaceVarHolder(var, sb.toString());
            paramsMap.computeIfAbsent(var, k -> new LinkedHashSet<>()).add(summary);
        });
    }

    private void parseForStatement() {
        StringJoiner sb = new StringJoiner(" ");
        sb.add(currentToken.getValue()); // #for
        advance();
        localParams.add(currentToken.getValue());
        sb.add(currentToken.getValue());    // item
        advance();
        sb.add(currentToken.getValue()); // of
        advance();
        Set<String> vars = parseForExpressions(sb);
        vars.forEach(var -> {
            String summary = replaceVarHolder(var, sb.toString());
            paramsMap.computeIfAbsent(var, k -> new LinkedHashSet<>()).add(summary);
        });
    }

    public void parse() {
        paramsMap.clear();
        localParams.clear();
        plainSqlParts.clear();
        if (this.tokens.isEmpty()) {
            return;
        }
        while (currentToken.getType() != TokenType.EOF) {
            switch (currentToken.getType()) {
                case PLAIN_TEXT:
                    plainSqlParts.add(currentToken.getValue());
                    advance();
                    break;
                case IF:
                case WHEN:
                case GUARD:
                case SWITCH:
                case CHECK:
                case CASE:
                    parseConditionStatement();
                    break;
                case DEFINE_VAR:
                    parseVarStatement();
                    break;
                case FOR:
                    parseForStatement();
                    break;
                default:
                    advance();
                    break;
            }
        }
        String sql = String.join("\n", plainSqlParts);
        parsePlainText(sql);
        paramsMap.entrySet().removeIf(entry -> localParams.contains(entry.getKey()));
    }

    public Map<String, Set<String>> getParamsMap() {
        return paramsMap;
    }
}
