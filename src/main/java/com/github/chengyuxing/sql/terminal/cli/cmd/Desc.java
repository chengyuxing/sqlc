package com.github.chengyuxing.sql.terminal.cli.cmd;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.core.DataBaseResource;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static com.github.chengyuxing.sql.terminal.vars.Constants.REDIRECT_SYMBOL;

public class Desc {
    private final DataBaseResource dataBaseResource;

    public Desc(DataBaseResource dataBaseResource) {
        this.dataBaseResource = dataBaseResource;
    }

    public void exec(String cmd) throws IOException {
        if (cmd.contains(REDIRECT_SYMBOL)) {
            Pair<String, String> pair = SqlUtil.getSqlAndRedirect(cmd);
            String obj = pair.getItem1();
            Path output = Paths.get(pair.getItem2());
            if (Files.isDirectory(output)) {
                output = output.resolve(obj + ".tsv");
            }
            String tsv = dataBaseResource.getTableDesc(obj)
                    .stream()
                    .map(cols -> String.join("\t", cols))
                    .collect(Collectors.joining("\n"));
            Files.write(output, tsv.getBytes(StandardCharsets.UTF_8));
            PrintHelper.printlnNotice("table fields desc saved to: " + output);
            return;
        }
        PrintHelper.printlnNotice("\n------------------------" + cmd + "------------------------");
        PrintHelper.printGrid(dataBaseResource.getTableDesc(cmd));
        PrintHelper.printlnNotice("----------------------" + cmd + " end----------------------\n");
    }
}
