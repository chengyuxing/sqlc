package com.github.chengyuxing.sql.terminal.cli.cmd;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;
import com.github.chengyuxing.sql.terminal.core.DataBaseResource;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.chengyuxing.sql.terminal.vars.Constants.REDIRECT_SYMBOL;

public class Ddl {
    private final DataBaseResource dataBaseResource;

    public Ddl(DataBaseResource dataBaseResource) {
        this.dataBaseResource = dataBaseResource;
    }

    public void exec(String cmd) throws IOException {
        if (dataBaseResource.getDbName().equals("mysql")) {
            PrintHelper.printlnWarning("mysql not support :ddl command, use mysql 'show ...' syntax instead.");
            return;
        }
        if (cmd.contains(REDIRECT_SYMBOL)) {
            Pair<String, String> pair = SqlUtil.getSqlAndRedirect(cmd);
            String obj = pair.getItem1();
            Path output = Paths.get(pair.getItem2());
            if (Files.isDirectory(output)) {
                output = output.resolve(obj.replaceAll("\\s+", "").replace(":", "_") + ".sql");
            }
            Files.write(output, dataBaseResource.getDefinition(obj).getBytes(StandardCharsets.UTF_8));
            PrintHelper.printlnNotice("ddl script saved to: " + output);
            return;
        }
        PrintHelper.printlnNotice("\n-------------------" + cmd + "-------------------");
        System.out.println(TerminalColor.highlightSql(dataBaseResource.getDefinition(cmd)));
        PrintHelper.printlnNotice("-----------------" + cmd + " end-----------------\n");
    }
}
