package util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
        String checkSql = "select count(*) from sqlite_master where type=? and name=?";
        List<Map<String, Object>> result = SqlliteUtil.query(checkSql, "table", "schema");
        assertEquals(1, result.size());
        if ((int) result.get(0).get("count(*)") > 0) {
            String dropTable = "DROP TABLE schema";
            System.out.println(SqlliteUtil.update(dropTable));
        }
        final String createSql = "CREATE TABLE schema(" +
                "name TEXT PRIMARY KEY NOT NULL, " +
                "value TEXT NOT NULL" +
                ")";
        int update = SqlliteUtil.update(createSql);
        assertEquals(0, update);
        result = SqlliteUtil.query(checkSql, "table", "schema");
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("count(*)"));
    }

    @After
    public void tearDown() throws Exception {
    }
}