package util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;


public class SqlliteUtilTest {

//    @BeforeClass
//    public static void init() {
//        System.setProperty("LSDir",
//                Paths.get(SqlliteUtilTest.class.getResource("/schema_template.yaml").getPath())
//                        .getParent().toString());
//    }

    /**
     * 测试前需要创建文件${LSDir}/conf/server.properties,否则Constants解析失败直接退出
     */
    @Before
    public void setUp() throws Exception {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
    }

    @Test
    public void checkTable() throws SQLException {
        System.out.println(System.getProperty("LSDir"));
        String checkSql = "select count(*) from sqlite_master where type=? and name=?";
        List<Map<String, Object>> result = SqlliteUtil.getInstance().query(checkSql, "table", "schema");
        assertEquals(1, result.size());
        if ((int) result.get(0).get("count(*)") > 0) {
            String dropTable = "DROP TABLE schema";
            System.out.println(SqlliteUtil.getInstance().update(dropTable));
        }
        final String createSql = "CREATE TABLE schema(" +
                "name TEXT PRIMARY KEY NOT NULL, " +
                "value TEXT NOT NULL" +
                ")";

        int update = SqlliteUtil.getInstance().update(createSql);
        assertEquals(0, update);
        result = SqlliteUtil.getInstance().query(checkSql, "table", "schema");
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("count(*)"));
    }

    private static class Insert_NewC extends Thread {

        private String flag;

        Insert_NewC(String flag) {
            this.flag = flag;
        }

        @Override
        public void run() {
            try {
                Class.forName("org.sqlite.JDBC");
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" +
                        Constants.appDir.resolve("index.db").toString());
                for (int i = 0; i < 1000; i++) {
                    try {
                        String name = this.flag + i;
                        String sql = "insert into test(name,value)values('" + name + "','')";
                        QueryRunner runner = new QueryRunner();
                        runner.update(conn, sql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                conn.close();
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private static class Insert extends Thread {

        private String flag;

        Insert(String flag) {
            this.flag = flag;
        }

        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                try {
                    String name = this.flag + i;
                    String sql = "insert into test(name,value)values('" + name + "','')";
                    SqlliteUtil.getInstance().update(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * java.sql.SQLException: [SQLITE_BUSY]  The database file is locked (database is locked)
     */
    @Test
    public void SQLITE_BUSY() throws SQLException, InterruptedException {
        String checkSql = "select count(*) from sqlite_master where type=? and name=?";
        try {
            List<Map<String, Object>> result = SqlliteUtil.getInstance()
                    .query(checkSql, "table", "test");
            if ((int) result.get(0).get("count(*)") == 0) {
                String createSql = "CREATE TABLE test(" +
                        "name TEXT PRIMARY KEY NOT NULL, " +
                        "value TEXT NOT NULL" +
                        ")";
                SqlliteUtil.getInstance().update(createSql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Gson gson = new Gson();
        List<Map<String, Object>> list = SqlliteUtil.getInstance()
                .query("select name from sqlite_master where type='table'");
        System.out.println(gson.toJson(list, new TypeToken<List<Map<String, Object>>>() {
        }.getType()));
        new Insert_NewC("t1").start();
        new Insert_NewC("t2").start();
        new Insert_NewC("t3").start();
        new Insert_NewC("t4").start();
        new Insert_NewC("t5").start();
        new Insert_NewC("t6").start();
        new Insert_NewC("t7").start();
        new Insert_NewC("t8").start();
        new Insert_NewC("t9").start();
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void SyncInsert() throws SQLException, InterruptedException {
        String checkSql = "select count(*) from sqlite_master where type=? and name=?";
        try {
            List<Map<String, Object>> result = SqlliteUtil.getInstance()
                    .query(checkSql, "table", "test");
            if ((int) result.get(0).get("count(*)") == 0) {
                String createSql = "CREATE TABLE test(" +
                        "name TEXT PRIMARY KEY NOT NULL, " +
                        "value TEXT NOT NULL" +
                        ")";
                SqlliteUtil.getInstance().update(createSql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Gson gson = new Gson();
        List<Map<String, Object>> list = SqlliteUtil.getInstance()
                .query("select name from sqlite_master where type='table'");
        System.out.println(gson.toJson(list, new TypeToken<List<Map<String, Object>>>() {
        }.getType()));
        new Insert("t1").start();
        new Insert("t2").start();
        new Insert("t3").start();
        new Insert("t4").start();
        new Insert("t5").start();
        new Insert("t6").start();
        new Insert("t7").start();
        new Insert("t8").start();
        new Insert("t9").start();
        Thread.sleep(1000 * 60 * 60);
    }

    @After
    public void tearDown() throws Exception {
    }
}