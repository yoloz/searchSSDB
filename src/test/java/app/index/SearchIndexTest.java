package app.index;

import bean.LSException;
import bean.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import util.SqlliteUtil;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SearchIndexTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * (index int,city string,company text, time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS')
     * <p>
     * {"index":2,"company":"北京三维力控科技有限公司","time":"2019-02-27T23:15:34.406000","city":"hangzhou"}
     * <p>
     * curl localhost:12580/newIndex -X POST -d "select index,city from test where time>'2019-02-28T09:43:10.224000'"
     */
    @Test
    public void search() throws SQLException, LSException {
        System.setProperty("LSDir",
                Paths.get(this.getClass().getResource("/schema_template.yaml").getPath())
                        .getParent().toString());
        List<Map<String, Object>> list = SqlliteUtil.getInstance().query("select value from schema where name=?", "test");
        assertEquals(1, list.size());
        Yaml yaml = new Yaml();
        List<String> selects = Collections.singletonList("*");
        Schema schema = yaml.loadAs((String) list.get(0).get("value"), Schema.class);
        SearchIndex searchIndex = new SearchIndex(schema);
        String right = "company:\"北京\" AND city:hangzhou";
        Map<String, Object> l1 = searchIndex.search(right, selects, 5);
        assertEquals(5, ((List) l1.get("results")).size());
        right = "company:\"北京\" AND hang";
        Map<String, Object> l2 = searchIndex.search(right, selects, 5);
        assertEquals(0, ((List) l2.get("results")).size());
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd\'T\'HH:mm:ss.SSSSSS");
        long t1 = LocalDateTime.parse("2019-02-28T09:43:10.223000", dateTimeFormatter)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        long t2 = LocalDateTime.parse("2019-02-28T09:43:10.224000", dateTimeFormatter)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        right = "{" + t1 + " TO " + t2 + "}";
        Map<String, Object> l5 = searchIndex.search(right, selects, 5);
        assertEquals(0, ((List) l5.get("results")).size());
        right = "time:[" + t1 + " TO " + t2 + "}";
        Map<String, Object> l6 = searchIndex.search(right, selects, 5);
        assertEquals(2, ((List) l6.get("results")).size());
        Map<String, Object> l3 = searchIndex.search(String.valueOf(t1), selects, 5);
        right = "company:\"北京\" AND " + t1;
        Map<String, Object> l4 = searchIndex.search(right, selects, 5);
        assertEquals(((List) l3.get("results")).size(), ((List) l4.get("results")).size());
        right = "(col3:test AND col1:a?b) OR (col2:{11 TO 26] OR col4:{1 TO 4}) AND (col4:[0 TO 5.3])";
        Map<String, Object> l7 = searchIndex.search(right, selects, 5);
        assertEquals(0, ((List) l7.get("results")).size());

    }
}