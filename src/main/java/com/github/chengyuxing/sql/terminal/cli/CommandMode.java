package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.sql.terminal.core.executor.IExecutor;
import com.github.chengyuxing.sql.terminal.core.executor.SQLExecutor;
import com.github.chengyuxing.sql.terminal.core.BakiLoader;
import com.github.chengyuxing.sql.terminal.core.BatchInsertHelper;
import com.github.chengyuxing.sql.terminal.types.SqlType;
import com.github.chengyuxing.sql.terminal.util.SqlUtil;
import com.github.chengyuxing.sql.terminal.util.Stdout;
import com.github.chengyuxing.sql.transaction.Tx;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;

public class CommandMode extends AbstractMode implements Callable<Integer> {
    private final IExecutor executor;

    protected CommandMode(StartupShell shell, BakiLoader bakiLoader, Terminal terminal) {
        super(shell, bakiLoader, terminal);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bakiLoader.close();
            Stdout.printlnWarning("Bye bye :(");
        }));

        this.executor = new SQLExecutor(bakiLoader.getUserBaki()) {
            @Override
            public LineReader paramsReader(String sql) {
                return SqlUtil.detectSQLType(sql) == SqlType.PROCEDURE
                        ? getProcParamReader()
                        : getSqlParamReader();
            }
        };
    }

    @Override
    public Integer call() {
        if (shell.ioOptions == null) {
            return 0;
        }
        StartupShell.ExecuteOptions execute = shell.ioOptions.executeOptions;
        // read sql from -e
        if (execute != null) {
            validate(execute);
            Context.viewMode.set(execute.format);
            Context.outputPath.set(execute.output);
            if (shell.enableTransaction) {
                Tx.using(() -> doExecuteSql(execute));
            } else {
                doExecuteSql(execute);
            }
            return 0;
        }

        // --import
        StartupShell.ImportOptions _import = shell.ioOptions.importOptions;
        if (_import != null) {
            if (shell.enableTransaction) {
                Tx.using(() -> doImportData(_import));
            } else {
                doImportData(_import);
            }
        }
        return 0;
    }

    private void validate(StartupShell.ExecuteOptions executeOptions) {
        if (!executeOptions.output.isEmpty() && executeOptions.sql.length != 1) {
            throw new IllegalArgumentException("-o can only be used when exactly one -e is specified.");
        }
    }

    private void doImportData(StartupShell.ImportOptions _import) {
        try {
            BatchInsertHelper.readFile4batch(bakiLoader.getUserBaki(), _import.file, _import.sheetIndex, _import.headerIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doExecuteSql(StartupShell.ExecuteOptions execute) {
        try {
            if (execute.sql.length == 1) {
                executor.execute(execute.sql[0]);
                return;
            }
            for (String sql : execute.sql) {
                executor.execute(sql);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
