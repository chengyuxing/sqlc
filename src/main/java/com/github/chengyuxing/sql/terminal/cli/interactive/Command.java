package com.github.chengyuxing.sql.terminal.cli.interactive;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

public class Command {
    private final String name;
    private final String description;
    private final String argsDescription;
    private final ArgumentCompleter completer;

    public Command(String name, String description, String argsDescription, Completer... completers) {
        this.name = name;
        this.description = description;
        this.argsDescription = argsDescription;

        if (name.isEmpty()) {
            this.completer = new ArgumentCompleter(completers);
            return;
        }

        if (completers.length == 0) {
            this.completer = new ArgumentCompleter(new StringsCompleter(name));
            return;
        }

        Completer[] cs = new Completer[completers.length + 1];
        cs[0] = new StringsCompleter(name);
        System.arraycopy(completers, 0, cs, 1, completers.length);
        this.completer = new ArgumentCompleter(cs);
    }

    public Command(String name, String description) {
        this(name, description, "");
    }

    public String getName() {
        return name;
    }

    public String getArgsDescription() {
        return argsDescription;
    }

    public String getDescription() {
        return description;
    }

    public ArgumentCompleter getCompleter() {
        return completer;
    }
}
