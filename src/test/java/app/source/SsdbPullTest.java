package app.source;

import bean.Pair;
import bean.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.SSDB;
import util.SqlliteUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SsdbPullTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * 测试前需要创建文件${LSDir}/conf/server.properties,否则Constants解析失败直接退出
     */
    @Test
    public void queryPoint() throws SQLException {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
        String sql = "select point from ssdb where name=?";
        List<Map<String, Object>> points = SqlliteUtil.query(sql, "test");
        assertEquals(0, points.size());
        SqlliteUtil.update("delete from ssdb where name=?", "list");
        SqlliteUtil.update("delete from ssdb where name=?", "hash");
        SqlliteUtil.insert("INSERT INTO ssdb(name,point)VALUES (?,?)", "list", "2000");
        SqlliteUtil.insert("INSERT INTO ssdb(name,point)VALUES (?,?)", "hash", "abcdeft_245");
        assertEquals(2000, Integer.parseInt((String) SqlliteUtil.query(sql, "list").get(0).get("point")));
        assertEquals("abcdeft_245", SqlliteUtil.query(sql, "hash").get(0).get("point"));
    }

    private void createData(Source.Type type) throws IOException {
        try (SSDB ssdb = SSDBs.simple()) {
            if (Source.Type.LIST == type) {
                Object[] values = new Object[100];
                for (int i = 0; i < 100; i++) values[i] = "listTest_" + i;
                ssdb.qpush_back("listTest", values);
            } else {
                for (int i = 0; i < 100; i++) {
                    ssdb.hset("hashTest", "key_" + i, "hashTest_" + i);
                }
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void listScan() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, NoSuchFieldException {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
        createData(Source.Type.LIST);
        SsdbPull ssdbPull = new SsdbPull("127.0.1", 8888, "listTest", Source.Type.LIST, "list");
        Method connect = ssdbPull.getClass().getDeclaredMethod("connect");
        connect.setAccessible(true);
        SSDB ssdb = (SSDB) connect.invoke(ssdbPull);
        Method listScan = ssdbPull.getClass().getDeclaredMethod("listScan", SSDB.class, int.class);
        listScan.setAccessible(true);
        Field point = ssdbPull.getClass().getDeclaredField("point");
        point.setAccessible(true);
        List<Pair<Object, String>> result = (List<Pair<Object, String>>) listScan.invoke(ssdbPull, ssdb, 0);
        assertEquals(Pair.of(0, "listTest_0"), result.get(0));
        assertEquals(100, point.get(ssdbPull));
        ssdb.close();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void hashScan() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
        createData(Source.Type.HASH);
        SsdbPull ssdbPull = new SsdbPull("127.0.1", 8888, "hashTest", Source.Type.LIST, "hash");
        Method connect = ssdbPull.getClass().getDeclaredMethod("connect");
        connect.setAccessible(true);
        SSDB ssdb = (SSDB) connect.invoke(ssdbPull);
        Method hashScan = ssdbPull.getClass().getDeclaredMethod("hashScan", SSDB.class, String.class);
        hashScan.setAccessible(true);
        Field point = ssdbPull.getClass().getDeclaredField("point");
        point.setAccessible(true);
        List<Pair<Object, String>> result = (List<Pair<Object, String>>) hashScan.invoke(ssdbPull, ssdb, "");
        assertEquals(Pair.of("key_0", "hashTest_0"), result.get(0));
        assertEquals("key_99", point.get(ssdbPull));
        ssdb.close();
    }
}