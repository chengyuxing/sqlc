package com.github.chengyuxing.sql.terminal.vars;

import java.util.ArrayList;
import java.util.List;

public class DbObjects {
    private List<String> tables = new ArrayList<>();
    private List<String> procedures = new ArrayList<>();
    private List<String> triggers = new ArrayList<>();
    private List<String> views = new ArrayList<>();

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public List<String> getProcedures() {
        return procedures;
    }

    public void setProcedures(List<String> procedures) {
        this.procedures = procedures;
    }

    public List<String> getTriggers() {
        return triggers;
    }

    public void setTriggers(List<String> triggers) {
        this.triggers = triggers;
    }

    public List<String> getViews() {
        return views;
    }

    public void setViews(List<String> views) {
        this.views = views;
    }
}
