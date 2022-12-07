package com.github.chengyuxing.sql.terminal.cli.completer;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import com.github.chengyuxing.sql.terminal.vars.Data;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.chengyuxing.sql.terminal.vars.Constants.CURRENT_DIR;

public class CliCompleters {
    /**
     * 读取一个普通sql文件执行查询，并可以做出重定向输出结果
     */
    public static Pair<String, Completer> readQuery(String mainCmd) {
        ArgumentCompleter completer = new ArgumentCompleter(
                new StringsCompleter(mainCmd),
                new Completers.FilesCompleter(CURRENT_DIR, "*.sql"),
                new StringsCompleter("&>"),
                new Completers.DirectoriesCompleter(CURRENT_DIR),
                NullCompleter.INSTANCE
        );
        return Pair.of(mainCmd, completer);
    }

    /**
     * 读取文件执行批量插入操作
     */
    public static Pair<String, Completer> read4Batch(String mainCmd) {
        ArgumentCompleter completer = new ArgumentCompleter(
                new StringsCompleter(mainCmd),
                new Completers.FilesCompleter(CURRENT_DIR, "*.sql|*.json|*.csv|*.tsv|*.xlsx"),
                NullCompleter.INSTANCE
        );
        return Pair.of(mainCmd, completer);
    }

    /**
     * 读取xql文件并使用as命名别名
     */
    public static Pair<String, Completer> readXqlFile(String mainCmd) {
        ArgumentCompleter completer = new ArgumentCompleter(
                new StringsCompleter(mainCmd),
                new Completers.FilesCompleter(CURRENT_DIR, "*.xql"),
                new StringsCompleter("as"),
                NullCompleter.INSTANCE
        );
        return Pair.of(mainCmd, completer);
    }

    public static Pair<String, Completer> transaction(String mainCmd) {
        return Pair.of(mainCmd, new ArgumentCompleter(
                new StringsCompleter(mainCmd),
                new StringsCompleter("begin", "commit", "rollback"),
                NullCompleter.INSTANCE
        ));
    }

    public static Pair<String, Completer> view(String mainCmd) {
        return Pair.of(mainCmd, new ArgumentCompleter(
                new StringsCompleter(mainCmd),
                new StringsCompleter("csv", "tsv", "json", "excel"),
                NullCompleter.INSTANCE
        ));
    }

    public static Pair<String, Completer> cmdBuilder(String mainCmd, Completer... argCompleters) {
        List<Completer> completers = new ArrayList<>();
        completers.add(new StringsCompleter(mainCmd));
        completers.addAll(Arrays.asList(argCompleters));
        return Pair.of(mainCmd, new ArgumentCompleter(completers));
    }

    public static Pair<String, Completer> singleCmd(String mainCmd) {
        return Pair.of(mainCmd, new ArgumentCompleter(
                new StringsCompleter(mainCmd),
                NullCompleter.INSTANCE
        ));
    }

    public static Pair<String, Completer> directoriesCmd(Path path) {
        return Pair.of("", new DirectoriesInSqlCompleter(path));
    }

    public static Pair<String, Completer> keywordsCmd() {
        return Pair.of("", Data.keywordsCompleter);
    }
}
