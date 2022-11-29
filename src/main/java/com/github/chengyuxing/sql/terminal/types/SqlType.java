package com.github.chengyuxing.sql.terminal.types;

public enum SqlType {
    /**
     * 查询
     */
    QUERY,
    /**
     * DDL,DML
     */
    OTHER,
    /**
     * 存储过程，函数
     */
    FUNCTION
}
