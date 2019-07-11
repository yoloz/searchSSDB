package index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class IndexerTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testRemove() {
        Map<String, String> m1 = new HashMap<>(3);
        Map<String, String> m2 = new ConcurrentHashMap<>(3);
        m1.put("t1", "v1");
        m1.put("t2", "v2");
        m1.put("t3", "v3");
        m2.putAll(m1);
        for (Map.Entry<String, String> entry : m2.entrySet()) {
            if (entry.getValue().equals("v2")) {
                String key = entry.getKey();
                m2.remove(key);
                m1.remove(key);
            }
        }
        assertEquals(m1, m2);
    }
}