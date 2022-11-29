package com.github.chengyuxing.sql.terminal.types;

import com.github.chengyuxing.common.DataRow;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Cache {
    private final String sql;
    private List<DataRow> data;
    private Map<String, Object> args;
    private String status = "";
    private String error = "";

    public Cache(String sql, List<DataRow> data) {
        this.sql = sql;
        this.data = data;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }

    public void setData(List<DataRow> data) {
        this.data = data;
    }

    public List<DataRow> getData() {
        return data;
    }

    public String getSql() {
        return sql;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    public void setArgs(Map<String, Object> args) {
        this.args = args;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public int size() {
        return Optional.ofNullable(data).map(List::size).orElse(0);
    }
}
