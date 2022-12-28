package com.github.chengyuxing.sql.terminal.cli.completer;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Collection;
import java.util.List;

public class DynamicVarsCompleter implements Completer {

    Completer completer;

    public DynamicVarsCompleter() {
        this.completer = new StringsCompleter();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        completer.complete(reader, line, candidates);
    }

    public void setVarsNames(Collection<String> vars) {
        this.completer = new StringsCompleter(vars);
    }
}
