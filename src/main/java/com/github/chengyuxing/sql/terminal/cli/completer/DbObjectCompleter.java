package com.github.chengyuxing.sql.terminal.cli.completer;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.ArrayList;
import java.util.List;

public class DbObjectCompleter implements Completer {
    List<String> triggers = new ArrayList<>();
    List<String> procedures = new ArrayList<>();
    List<String> tables = new ArrayList<>();
    List<String> views = new ArrayList<>();
    Completer completer;

    public DbObjectCompleter() {
        this.completer = new StringsCompleter();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        String l = line.line();
        String word = line.word();
        // :edit command completer words
        if (l.startsWith(":edit")) {
            if (word.startsWith("proc:")) {
                setProcedures();
            } else if (word.startsWith("tg:")) {
                setTriggers();
            } else if (word.startsWith("view:")) {
                setViews();
            } else {
                setProcedures();
            }
            this.completer.complete(reader, line, candidates);
            return;
        }
        // :ddl command
        if (l.startsWith(":ddl")) {
            if (word.startsWith("proc:")) {
                setProcedures();
            } else if (word.startsWith("tg:")) {
                setTriggers();
            } else if (word.startsWith("view:")) {
                setViews();
            } else {
                setTables();
            }
            this.completer.complete(reader, line, candidates);
            return;
        }
        // :desc command
        if (l.startsWith(":desc")) {
            setTables();
            this.completer.complete(reader, line, candidates);
            return;
        }

        setEmpty();
        this.completer.complete(reader, line, candidates);
    }

    void setTriggers() {
        this.completer = new StringsCompleter(triggers);
    }

    void setProcedures() {
        this.completer = new StringsCompleter(procedures);
    }

    void setTables() {
        this.completer = new StringsCompleter(tables);
    }

    void setViews() {
        this.completer = new StringsCompleter(views);
    }

    void setEmpty() {
        this.completer = new StringsCompleter();
    }

    void setDefault(String... vars) {
        this.completer = new StringsCompleter(vars);
    }

    public void setProcedures(List<String> procedures) {
        this.procedures = procedures;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public void setTriggers(List<String> triggers) {
        this.triggers = triggers;
    }

    public void setViews(List<String> views) {
        this.views = views;
    }
}
