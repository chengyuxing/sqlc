package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.utils.StringUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class Arguments {
    private static final String[] argNames = new String[]{
            "-u",   //url
            "-p",   //password
            "-n",   //name
            "-f",   //format
            "-e",   //sql string or sql file
            "-d",    //multi sql block delimiter;
            "-header",   // tsv,csv，excel header index
            "--with-tx"   //using transaction wrapper
    };
    private final String[] args;
    private Map<String, String> argMap = new HashMap<>();

    public Arguments(String... args) {
        this.args = args;
        init();
    }

    void init() {
        argMap = Stream.of(args).filter(arg -> StringUtil.startsWiths(arg, argNames))
                .reduce(new HashMap<>(), (acc, curr) -> {
                    for (String name : argNames) {
                        if (curr.startsWith(name)) {
                            int prefixLength = name.length();
                            String key = curr.substring(0, prefixLength);
                            String value = curr.substring(key.length()).trim();
                            acc.put(key, value);
                            break;
                        }
                    }
                    return acc;
                }, (a, b) -> a);
    }

    public boolean containsKey(String key) {
        return argMap.containsKey(key);
    }

    public String get(String key) {
        return argMap.get(key);
    }

    public String getIfBlank(String key, String defaultValue) {
        if (!argMap.containsKey(key)) {
            return defaultValue;
        }
        if (argMap.get(key).equals("")) {
            return defaultValue;
        }
        return argMap.get(key);
    }

    @Override
    public String toString() {
        return argMap.toString();
    }
}
