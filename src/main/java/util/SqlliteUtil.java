package util;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Multiple processes can have the same database open at the same time.
 * Multiple processes can be doing a SELECT at the same time.
 * But only one process can be making changes to the database at any moment in time, however.
 */
public class SqlliteUtil implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(SqlliteUtil.class);

    private static final String dbPath = Constants.appDir.resolve("index.db").toString();

    private final Connection connection;

    private SqlliteUtil() {
        this.connection = getConnection();
        checkSchema();
        checkPoint();
    }

    private static class lazyHolder {
        static final SqlliteUtil instance = new SqlliteUtil();
    }

    public static SqlliteUtil getInstance() {
        return lazyHolder.instance;
    }

    /**
     * 创建索引的schema信息
     */
    private void checkSchema() {
        String checkSql = "select count(*) from sqlite_master where type=? and name=?";
        try {
            List<Map<String, Object>> result = query(checkSql, "table", "schema");
            if ((int) result.get(0).get("count(*)") == 0) {
                String createSql = "CREATE TABLE schema(" +
                        "name TEXT PRIMARY KEY NOT NULL, " +
                        "value TEXT NOT NULL" +
                        ")";
                update(createSql);
            }
        } catch (SQLException e) {
            logger.error("初始化schema error", e);
            System.exit(1);
        }
    }

    private void checkPoint() {
        String checkSql = "select count(*) from sqlite_master where type=? and name=?";
        try {
            List<Map<String, Object>> result = query(checkSql, "table", "point");
            if ((int) result.get(0).get("count(*)") == 0) {
                String createSql = "CREATE TABLE point(" +
                        "iname TEXT PRIMARY KEY NOT NULL, " +
                        "name TEXT DEFAULT ''," +
                        "value TEXT NOT NULL" +
                        ")";
                update(createSql);
            }
        } catch (SQLException e) {
            logger.error("初始化point error", e);
            System.exit(1);
        }
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) connection.close();
    }

    private Connection getConnection() {
        try {
            Class.forName("org.sqlite.JDBC");
            return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e);
            System.exit(1);
        }
        return null;
    }

    public List<Map<String, Object>> query(String sql, Object... params) throws SQLException {
        synchronized (connection) {
            QueryRunner runner = new QueryRunner();
            ResultSetHandler<List<Map<String, Object>>> h = new MapListHandler();
            return runner.query(connection, sql, h, params);
        }
    }

    public List<Object> queryL(String sql, Object... params) throws SQLException {
        synchronized (connection) {
            QueryRunner runner = new QueryRunner();
            ResultSetHandler<List<Object>> h = new ColumnListHandler<>();
            return runner.query(connection, sql, h, params);
        }
    }

    public int update(String sql, Object... params) throws SQLException {
        synchronized (connection) {
            QueryRunner runner = new QueryRunner();
            return runner.update(connection, sql, params);
        }
    }

    public void transaction(String s1, Object[] p1, String s2, Object[] p2) throws SQLException {
        synchronized (connection) {
            connection.setAutoCommit(false);
            try {
                QueryRunner runner = new QueryRunner();
                runner.update(connection, s1, p1);
                runner.update(connection, s2, p2);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }
}
