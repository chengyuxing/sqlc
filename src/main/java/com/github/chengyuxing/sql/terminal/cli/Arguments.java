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
            "-d"    //multi sql block delimiter};
    };
    private final String[] args;

    public Arguments(String... args) {
        this.args = args;
    }

    public Map<String, String> toMap() {
        return Stream.of(args).filter(arg -> StringUtil.startsWiths(arg, argNames))
                .reduce(new HashMap<>(), (acc, curr) -> {
                    for (String name : argNames) {
                        if (curr.startsWith(name)) {
                            int prefixLength = name.length();
                            String key = curr.substring(0, prefixLength);
                            String value = curr.substring(key.length());
                            acc.put(key, value);
                            break;
                        }
                    }
                    return acc;
                }, (a, b) -> a);
    }
}
