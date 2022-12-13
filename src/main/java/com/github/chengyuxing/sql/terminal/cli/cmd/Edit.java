package com.github.chengyuxing.sql.terminal.cli.cmd;

import com.github.chengyuxing.sql.terminal.core.DataBaseResource;
import com.github.chengyuxing.sql.terminal.core.PrintHelper;
import com.github.chengyuxing.sql.terminal.core.UserBaki;
import com.github.chengyuxing.sql.terminal.vars.Data;
import org.jline.console.CommandRegistry;
import org.jline.console.impl.JlineCommandRegistry;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.chengyuxing.sql.terminal.vars.Constants.CURRENT_DIR;

public class Edit {
    private final DataBaseResource dataBaseResource;
    private final JlineCommandRegistry commandRegistry;
    private final CommandRegistry.CommandSession session;
    private final UserBaki baki;

    public Edit(DataBaseResource dataBaseResource, JlineCommandRegistry commandRegistry, CommandRegistry.CommandSession session, UserBaki baki) {
        this.dataBaseResource = dataBaseResource;
        this.commandRegistry = commandRegistry;
        this.session = session;
        this.baki = baki;
    }

    public void exec(String cmd) throws Exception {
        int colonIdx = cmd.indexOf(":");
        if (colonIdx == -1) {
            throw new IllegalArgumentException("invalid object name formatter, e.g: tg(trigger):test.big.my_trigger, view:test.my_view, proc:public.hello(text)");
        }
        String def = dataBaseResource.getDefinition(cmd);
        if (def.trim().equals("")) {
            throw new RuntimeException(cmd + " definition is empty.");
        }
        String procedureTemp = cmd + "_" + System.currentTimeMillis();
        Path procedurePath = Paths.get(CURRENT_DIR.toString(), procedureTemp);
        Data.tempFiles.add(procedurePath);
        try {
            Files.write(procedurePath, def.getBytes(StandardCharsets.UTF_8));
            commandRegistry.invoke(session, "nano", "-$", procedureTemp);
            String newDef = String.join("\n", Files.readAllLines(procedurePath));
            if (!def.trim().equals(newDef.trim())) {
                baki.execute(newDef);
                PrintHelper.printlnNotice(cmd + " change submitted!");
            }
        } finally {
            Files.deleteIfExists(procedurePath);
        }
    }
}
