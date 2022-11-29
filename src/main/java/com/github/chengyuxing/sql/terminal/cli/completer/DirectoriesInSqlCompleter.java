package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.common.utils.StringUtil;
import org.jline.builtins.Completers;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

public class DirectoriesInSqlCompleter extends Completers.DirectoriesCompleter {

    public DirectoriesInSqlCompleter(Path currentDir) {
        super(currentDir);
    }

    @Override
    public void complete(LineReader reader, ParsedLine commandLine, List<Candidate> candidates) {
        if (!commandLine.line().startsWith(":")) {
            String word = commandLine.word();
            if (StringUtil.startsWiths(word, File.separator, "." + File.separator, ".." + File.separator)) {
                super.complete(reader, commandLine, candidates);
            }
        }
    }
}
