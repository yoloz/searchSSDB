package bean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import parser.CreateSql;
import util.SqlliteUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SchemaTest {

    private Yaml yaml;

    @Before
    public void setUp() throws Exception {
        yaml = new Yaml();
    }

    /**
     * 测试前需要创建文件${LSDir}/conf/server.properties,否则Constants解析失败直接退出
     */
    @Test
    public void createSchema() throws SQLException, LSException {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
        String sql = "CREATE TABLE test" +
                "(col1 int, col2 string, col3 date('yyyy-MM-dd HH:mm:ss.SSS'))" +
                " analyser='org.apache.lucene.analysis.standard.StandardAnalyzer' source=ssdb.test1 addr='127.0.0.1:8888' type=list";
        CreateSql createSql = new CreateSql(sql);
        String indexName = createSql.parse();
        assertEquals("test", indexName);
        String checkSql = "select name from schema";
        List<Map<String, Object>> result = SqlliteUtil.getInstance().query(checkSql);
        assertEquals("test", result.get(0).get("name"));
    }

    @Test
    public void schemaTest() throws SQLException, LSException {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
        SqlliteUtil.getInstance().update("delete from schema where name='test'");
        SqlliteUtil.getInstance().update("delete from point where iname='test'");
        String sql = "CREATE TABLE test" +
                "(index int,city string,company text,time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS'),timestamp long)" +
                " name='list*' addr='127.0.0.1:8888' type=list";
        index.parse.CreateSql createSql = new index.parse.CreateSql(sql);
        String indexName = createSql.store();
        assertEquals("test", indexName);
        String checkSql = "select value from schema where name='test'";
        List<Map<String, Object>> result = SqlliteUtil.getInstance().query(checkSql);
        System.out.println(result.get(0).get("value"));
    }

    @Test
    public void readAndWrite() throws IOException {
        try (InputStream inputStream = this.getClass()
                .getResourceAsStream("/schema_template.yaml")) {
            Schema schema = yaml.loadAs(inputStream, Schema.class);
            assertEquals("indexName", schema.getIndex());
            assertEquals(Field.Type.INT, schema.getFields().get(0).getType());
            //yaml.dump(schema)
            System.out.println(yaml.dumpAs(schema, new Tag("test"), DumperOptions.FlowStyle.BLOCK));
        }
    }

    @After
    public void tearDown() throws Exception {
    }
}