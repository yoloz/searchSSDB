package util;

import bean.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonUtilTest {

    Pair<String, Object> p1, p2;
    String s1, s2;

    @Before
    public void setUp() throws Exception {
        p1 = Pair.of("test", 1);
        p2 = Pair.of("test", "value");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void toJson() throws IOException {
        s1 = JsonUtil.toJson(p1, Pair.class);
        s2 = JsonUtil.toJson(p2, Pair.class);
        System.out.println("s1=>" + s1);
        System.out.println("s2=>" + s2);
    }

    @Test
    public void fromJson() throws IOException {
        s1 = "{\"left\":\"test\",\"right\":1}";
        s2 = "{\"left\":\"test\",\"right\":\"value\"}";
        Map<String,Object> p1m = JsonUtil.toMap(s1);
        Pair p1_1 = Pair.of(p1m.get("left"),p1m.get("right"));
        Map<String,Object> p2m = JsonUtil.toMap(s2);
        Pair p2_2 = Pair.of(p2m.get("left"),p2m.get("right"));
        assertEquals(p1_1, p1);
        assertEquals(p2_2, p2);
    }
}