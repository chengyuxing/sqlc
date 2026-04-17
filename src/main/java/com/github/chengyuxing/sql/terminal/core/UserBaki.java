package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.cli.TerminalColor;
import com.github.chengyuxing.sql.terminal.vars.StatusManager;
import com.github.chengyuxing.sql.util.JdbcUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class UserBaki extends BakiDao {
    private static final Logger log = LoggerFactory.getLogger(UserBaki.class);
    private final String username;

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     * @param username   jdbc登录用户名
     */
    public UserBaki(DataSource dataSource, String username) {
        super(dataSource);
        this.username = username;
    }

    @Override
    protected @NotNull Connection getConnection() {
        Connection connection = super.getConnection();
        initDbConfig(connection);
        return connection;
    }

    protected List<String> getTableFields(String tableName) {
        return table(tableName).fields();
    }

    public void initDbConfig(Connection connection) {
        try {
            boolean firstLoad = StatusManager.bakiFirstLoad.get();
            if (firstLoad) {
                StatusManager.bakiFirstLoad.set(false);
            }
            DatabaseMetaData metaData = connection.getMetaData();
            if (firstLoad) {
                log.info("DataBase: {}", metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion());
            }
            switch (metaData.getDatabaseProductName().toLowerCase()) {
                case "postgresql":
                    try {
                        PreparedStatement preparedStatement = connection.prepareStatement("select schema_name from information_schema.schemata where schema_owner = ?");
                        preparedStatement.setObject(1, username);
                        ResultSet resultSet = preparedStatement.executeQuery();
                        List<DataRow> paths = JdbcUtils.createDataRows(resultSet, "", -1);
                        JdbcUtils.closeResultSet(resultSet);
                        JdbcUtils.closeStatement(preparedStatement);
                        String schemas = paths.stream().map(d -> "\"" + d.getFirst() + "\"")
                                .collect(Collectors.joining(","));
                        if (!schemas.trim().isEmpty()) {
                            String searchPath = "set search_path = " + schemas;
                            PreparedStatement statement = connection.prepareStatement(searchPath);
                            statement.execute();
                            JdbcUtils.closeStatement(statement);
                            if (firstLoad) {
                                log.info(TerminalColor.highlightSql(searchPath));
                            }
                        }
                    } catch (Exception e) {
                        log.error("init postgresql", e);
                        PrintHelper.printlnError(e);
                    }
                    break;
            }
        } catch (SQLException e) {
            log.error("init postgresql", e);
            throw new RuntimeException(e);
        }
    }
}
