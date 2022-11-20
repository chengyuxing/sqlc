package rabbit.sql.console.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class SingleBaki extends BakiDao {
    private static final Logger log = LoggerFactory.getLogger(SingleBaki.class);
    private final String username;

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     * @param username   jdbc登录用户名
     */
    public SingleBaki(DataSource dataSource, String username) {
        super(dataSource);
        this.username = username;
    }

    @Override
    protected Connection getConnection() {
        Connection connection = super.getConnection();
        initDbConfig(connection);
        return connection;
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
                        List<DataRow> paths = JdbcUtil.createDataRows(resultSet, "", -1);
                        JdbcUtil.closeResultSet(resultSet);
                        JdbcUtil.closeStatement(preparedStatement);
                        String schemas = paths.stream().map(d -> "\"" + d.getFirst() + "\"")
                                .collect(Collectors.joining(","));
                        if (!schemas.trim().equals("")) {
                            String searchPath = "set search_path = " + schemas;
                            PreparedStatement statement = connection.prepareStatement(searchPath);
                            statement.execute();
                            JdbcUtil.closeStatement(statement);
                            if (firstLoad) {
                                log.info(com.github.chengyuxing.sql.utils.SqlUtil.highlightSql(searchPath));
                            }
                        }
                    } catch (Exception e) {
                        PrintHelper.printlnError(e);
                    }
                    break;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
