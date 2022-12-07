package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class KeywordsCompleter implements Completer {
    private StringsCompleter completer;
    private final Set<String> words;

    public KeywordsCompleter() {
        this.completer = new StringsCompleter();
        this.words = new LinkedHashSet<>();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        String l = line.line().trim();
        if (l.equals("")) {
            return;
        }
        if (!l.startsWith(":") && StatusManager.promptReference.get().getStatus() != Prompt.Status.CUSTOM) {
            completer.complete(reader, line, candidates);
        }
    }

    public void addVarsNames(Collection<String> words) {
        if (this.words != words) {
            this.words.addAll(words);
            this.completer = new StringsCompleter(this.words);
        }
    }
}
