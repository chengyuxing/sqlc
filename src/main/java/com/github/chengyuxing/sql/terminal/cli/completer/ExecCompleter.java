package com.github.chengyuxing.sql.terminal.cli.completer;

import org.jline.builtins.Completers;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.*;

public class ExecCompleter implements Completer {

    private Completer xqlNamesCompleter;
    private final Completer fileCompleter;

    public ExecCompleter() {
        this.xqlNamesCompleter = new StringsCompleter();
        this.fileCompleter = new Completers.FileNameCompleter();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        String w = line.word();
        if (w.startsWith("&")) {
            List<Candidate> temp = new ArrayList<>();
            xqlNamesCompleter.complete(reader, line, temp);
            candidates.addAll(temp);
        } else {
            fileCompleter.complete(reader, line, candidates);
        }
    }

    public void setXqlNames(Collection<String> vars) {
        Set<String> refs = new HashSet<>();
        for (String var : vars) {
            refs.add("&" + var);
        }
        this.xqlNamesCompleter = new StringsCompleter(refs);
    }
}
