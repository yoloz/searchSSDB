package util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.*;

public class UtilsTest {

    @Before
    public void setUp() {
    }

    @Test
    public void nanosTest() {
        String d = "2019-02-23T23:51:12.872516";
        String d1 = "2019-02-23T23:51:12.872000";
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
        LocalDateTime dt = LocalDateTime.parse(d, dateTimeFormatter);
        LocalDateTime dt1 = LocalDateTime.parse(d1, dateTimeFormatter);
        assertEquals(dt.toInstant(ZoneOffset.UTC).toEpochMilli(), dt1.toInstant(ZoneOffset.UTC).toEpochMilli());
//        long l = Long.valueOf(Utils.toNanos(dt));
//        System.out.println(l);
        assertNotEquals(Utils.toNanos(dt), Utils.toNanos(dt1));
        LocalDateTime _dt = Utils.ofNanos(Utils.toNanos(dt));
        LocalDateTime _dt1 = Utils.ofNanos(Utils.toNanos(dt1));
        assertEquals(dt, _dt);
        assertEquals(dt1, _dt1);
        assertEquals(d, _dt.format(dateTimeFormatter));
        assertEquals(d1, _dt1.format(dateTimeFormatter));
    }

    @Test
    public void nanosTest1() {
        String d = "2019-02-23T00:00:00.000000";
        String d1 = "2019-02-23T23:51:12.872000";
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
        LocalDateTime dt = LocalDateTime.parse(d, dateTimeFormatter);
        LocalDateTime dt1 = LocalDateTime.parse(d1, dateTimeFormatter);
//        long l = Long.valueOf(Utils.toNanos(dt));
//        System.out.println(l);
        System.out.println(Utils.toNanos(dt));
        System.out.println(Utils.toNanos(dt1));
        LocalDateTime _dt = Utils.ofNanos(Utils.toNanos(dt));
        LocalDateTime _dt1 = Utils.ofNanos(Utils.toNanos(dt1));
        assertEquals(dt, _dt);
        assertEquals(dt1, _dt1);
        assertEquals(d, _dt.format(dateTimeFormatter));
        assertEquals(d1, _dt1.format(dateTimeFormatter));
    }

    @Test
    public void trimPrefix() {
        String s1 = " SELECT index,city from test where time>'2019-02-28T09:43:10.224000'";
        String s2 = Utils.trimPrefix(s1);
        assertNotEquals("select", s1.substring(0, 6).toLowerCase());
        assertEquals("select", s2.substring(0, 6).toLowerCase());
    }

    @Test
    public void compareNanos() {
        String t1 = "2019-02-18 15:45:54.607415";
        String t2 = "2019-02-25 00:00:00.000000";
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
        LocalDateTime l1 = LocalDateTime.parse(t1, dateTimeFormatter);
        LocalDateTime l2 = LocalDateTime.parse(t2, dateTimeFormatter);
        String s1 = Utils.toNanos(l1);
        String s2 = Utils.toNanos(l2);
        long _l1 = Long.valueOf(s1);
        long _l2 = Long.valueOf(s2);
        System.out.println(_l1);
        System.out.println(_l2);
        assertTrue(_l2 > _l1);
        LocalDateTime _dt = Utils.ofNanos(String.valueOf(_l1));
        LocalDateTime _dt1 = Utils.ofNanos(String.valueOf(_l2));
        assertEquals(_dt, l1);
        assertEquals(_dt1, l2);
    }

    @After
    public void tearDown() {
    }
}