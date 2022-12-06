package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.support.IOutParam;
import com.github.chengyuxing.sql.terminal.progress.impl.WaitingPrinter;
import com.github.chengyuxing.sql.types.Param;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ProcedureExecutor {
    private final Baki baki;
    private final String procedure;

    public ProcedureExecutor(Baki baki, String procedure) {
        this.baki = baki;
        this.procedure = procedure;
    }

    public void exec(Map<String, Param> args) {
        DataRow result = WaitingPrinter.waiting(() -> baki.call(procedure, args));
        result.forEach((k, v) -> {
            PrintHelper.printlnDarkWarning("result[" + k + "]:");
            PrintHelper.printQueryResult(value2stream(k, v));
        });
    }

    @SuppressWarnings("unchecked")
    static Stream<DataRow> value2stream(String key, Object value) {
        if (value instanceof List) {
            return ((List<DataRow>) value).stream();
        }
        return Stream.of(DataRow.fromPair(key, value));
    }

    public static class OutParam implements IOutParam {
        private final int code;

        public OutParam(int code) {
            this.code = code;
        }

        @Override
        public int getTypeNumber() {
            return code;
        }

        @Override
        public String getName() {
            return "" + code;
        }
    }
}
