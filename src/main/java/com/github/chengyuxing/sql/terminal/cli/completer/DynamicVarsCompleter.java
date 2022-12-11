package com.github.chengyuxing.sql.terminal.cli.completer;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class DynamicVarsCompleter implements Completer {

    Completer completer;
    private final Set<String> vars;

    public DynamicVarsCompleter() {
        this.completer = new StringsCompleter();
        this.vars = new LinkedHashSet<>();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        completer.complete(reader, line, candidates);
    }

    public void setVarsNames(Collection<String> vars) {
        this.completer = new StringsCompleter(vars);
    }

    public void addVarsNames(Collection<String> vars) {
        if (this.vars != vars) {
            this.vars.addAll(vars);
            this.completer = new StringsCompleter(this.vars);
        }
    }
}
