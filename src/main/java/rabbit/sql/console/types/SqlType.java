package rabbit.sql.console.types;

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
