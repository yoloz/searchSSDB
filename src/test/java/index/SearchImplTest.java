package index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SearchImplTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCountName() {
        Map<String, Object> map = new HashMap<>();
        map.put("count", "");
        assertEquals("_count", createCountName(map, "count"));
        map.put("_count", "");
        assertEquals("__count", createCountName(map, "count"));
        map.put("__count", "");
        assertEquals("___count", createCountName(map, "count"));
        map.put("___count", "");
        assertEquals("____count", createCountName(map, "count"));
    }

    private String createCountName(Map<String, Object> map, String name) {
        if (map.containsKey(name)) {
            name = "_" + name;
            return createCountName(map, name);
        }
        return name;
    }

    @Test
    public void testArr() {
        String[] s1 = new String[]{"ab", "cd", "ef"};
        String[] s2 = new String[s1.length];
        List<String> l1 = new ArrayList<>(s1.length);
        int i = 0;
        for (String s : s1) {
            if (!"cd".equals(s)) {
                s2[i] = s;
                l1.add(s);
                i += 1;
            }
        }
        System.out.println(s2.length + "==" + Arrays.toString(s2));
        System.out.println(l1.size() + "==" + Arrays.toString(l1.toArray()));
    }

    @Test
    public void testPage() {
        int[] arr = page(1, 102);
        assertArrayEquals(new int[]{0, 102}, arr);
        arr = page(10, 102);
        assertArrayEquals(new int[]{9 * 102, 102}, arr);
        arr = page(8, 200);
        assertArrayEquals(new int[]{1400, 200}, arr);
        arr = page(1, 2000);
        assertArrayEquals(new int[]{0, 2000}, arr);
        arr = page(75, 15);
        assertArrayEquals(new int[]{74 * 15, 15}, arr);
        arr = page(101, 10);
        assertArrayEquals(new int[]{1000, 10}, arr);
    }

    private int[] page(int offset, int rowCount) {
        int prePage = (offset - 1) * rowCount;
        int curPage = offset * rowCount;
        int _count = 0;
        int[] result = new int[2];
        do {
            int n = 1000;
            if (curPage <= n || (offset - 1) * rowCount == 0) n = curPage;
            else {
                if (prePage <= 0) n = rowCount;
                else {
                    if (prePage <= n) n = prePage;
                    prePage -= 1000;
                }
            }
            result[0] = _count;
            result[1] = n;
            System.out.println("once query[" + n + "]");
            _count += n;
            if (_count == curPage) break;
        } while (_count < curPage);
        return result;
    }
}