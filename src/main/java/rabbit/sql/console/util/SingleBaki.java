package rabbit.sql.console.util;

import com.github.chengyuxing.sql.BakiDao;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class SingleBaki extends BakiDao {
    private Connection connection;

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     */
    public SingleBaki(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {

    }

    @Override
    protected Connection getConnection() {
        try {
            if (connection == null) {
                connection = getDataSource().getConnection();
            }
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void beginTransaction() throws SQLException {
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setReadOnly(false);
    }

    public void rollbackTransaction() throws SQLException {
        connection.rollback();
        releaseTransaction();
    }

    public void commitTransaction() throws SQLException {
        connection.commit();
        releaseTransaction();
    }

    public void releaseTransaction() throws SQLException {
        connection.setAutoCommit(true);
    }
}
