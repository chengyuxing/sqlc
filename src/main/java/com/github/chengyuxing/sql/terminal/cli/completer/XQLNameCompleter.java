package com.github.chengyuxing.sql.terminal.cli.completer;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.*;

public class XQLNameCompleter implements Completer {
    public static final XQLNameCompleter INSTANCE = new XQLNameCompleter();

    private Completer refNameCompleter = new StringsCompleter();

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        refNameCompleter.complete(reader, line, candidates);
    }

    public void setResource(Collection<String> names) {
        Set<String> refs = new HashSet<>();
        for (String var : names) {
            refs.add("&" + var);
        }
        this.refNameCompleter = new StringsCompleter(refs);
    }
}
