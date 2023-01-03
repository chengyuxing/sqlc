package com.github.chengyuxing.sql.terminal.cli.cmd;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;
import com.github.chengyuxing.sql.terminal.core.DataBaseResource;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;

import java.io.File;
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
            boolean customUser = false;
            if (Files.isDirectory(output)) {
                output = output.resolve(obj.replaceAll("\\s+", "").replace(":", "_") + ".ddl.sql");
            } else {
                if (pair.getItem2().matches(".*[\\w_]\\.$")) {
                    customUser = true;
                    int dotIdx = obj.indexOf(".");
                    String newObj = obj;
                    if (dotIdx != -1) {
                        newObj = obj.substring(dotIdx + 1);
                    }
                    output = Paths.get(pair.getItem2() + newObj.replaceAll("\\s+", "").replace(":", "_") + ".ddl.sql");
                }
            }
            String def = dataBaseResource.getDefinition(obj);
            if (customUser) {
                int pathIdx = pair.getItem2().lastIndexOf(File.separator);
                String newDb = pair.getItem2();
                if (pathIdx != -1) {
                    newDb = pair.getItem2().substring(pathIdx + 1);
                    newDb = newDb.substring(0, newDb.length() - 1);
                }
                def = def.replaceAll("\\s+([\\w_]+|(([\"`])[\\w_]+([\"`])))\\.", " $3" + newDb + "$4.");
            }
            Files.write(output, def.getBytes(StandardCharsets.UTF_8));
            PrintHelper.printlnNotice("ddl script saved to: " + output);
            return;
        }
        PrintHelper.printlnNotice("\n-------------------" + cmd + "-------------------");
        System.out.println(TerminalColor.highlightSql(dataBaseResource.getDefinition(cmd)));
        PrintHelper.printlnNotice("-----------------" + cmd + " end-----------------\n");
    }
}
