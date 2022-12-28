package com.github.chengyuxing.sql.terminal.cli.completer;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class DynamicCompleter implements Completer {

    Completer completer;
    Collection<String> suggests;
    Function<String, Collection<String>> function;

    public DynamicCompleter() {
        this.completer = new StringsCompleter();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        String w = line.word();
        if (function != null) {
            setSuggestions(function.apply(w));
        }
        completer.complete(reader, line, candidates);
    }

    public void listening(Function<String, Collection<String>> suggestionsFunc) {
        this.function = suggestionsFunc;
    }

    public void setSuggestions(Collection<String> vars) {
        if (vars != suggests) {
            suggests = vars;
            this.completer = new StringsCompleter(suggests);
        }
    }
}
