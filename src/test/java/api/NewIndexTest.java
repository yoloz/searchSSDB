package api;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.SSDB;
import util.Utils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class NewIndexTest {

    @Before
    public void setUp() throws Exception {
    }

    /**
     * curl localhost:12580/create -X POST -d "CREATE TABLE test(index int,city string,company text,
     * time date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS')) name=listTest addr='127.0.0.1:8888' type=list"
     * curl localhost:12580/start -X POST -d "test"
     * 测试前需要目录${LSDir}下满足conf/*
     * 启动HttpServerTest.startHttpServer监听http请求
     *
     * @throws IOException error
     */
    @Test
    public void createListData() throws IOException, InterruptedException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
        try (SSDB ssdb = SSDBs.simple()) {
            Object[] values = new Object[100];
            for (int j = 0; j < 10; j++) {
                for (int i = 0; i < 100; i++) {
                    Map<String, Object> value = new HashMap<>(6);
                    LocalDateTime lt = LocalDateTime.now();
                    value.put("city", "hz" + (j * 100 + i));
                    value.put("company", "北京三维力控科技有限公司");
                    value.put("english", "Analysis is one of the main causes of slow Indexing.");
                    value.put("time", lt.format(dateTimeFormatter));
                    value.put("index", j * 100 + i);
                    value.put("timestamp", Utils.toNanos(lt));
                    values[i] = toJson(value);
                    Thread.sleep(0, 999999);
                }
                ssdb.qpush_back("listTest", values);
            }
        }
    }

    @Test
    //timestamp yyyy-MM-dd'T'00:00:00.000000
    public void createListData1() throws IOException, InterruptedException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
        try (SSDB ssdb = SSDBs.simple()) {
            Object[] values = new Object[100];
            LocalDateTime lt = LocalDateTime.now();
            for (int i = 0; i < 100; i++) {
                Map<String, Object> value = new HashMap<>(6);
                value.put("city", "hangzhou");
                value.put("company", "北京三维力控科技有限公司");
                value.put("english", "Analysis is one of the main causes of slow indexing.");
                value.put("index", i);
                value.put("timestamp", LocalDateTime.of(lt.plusDays(i).toLocalDate(), LocalTime.MIN).format(dateTimeFormatter));
                values[i] = toJson(value);
                Thread.sleep(0, 999999);
            }
            ssdb.qpush_back("listTest", values);
        }
    }

    @Test
    public void createListData2() throws IOException, InterruptedException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
        try (SSDB ssdb = SSDBs.simple()) {
            Object[] values = new Object[100];
            int j = 0;
            while (j < Integer.MAX_VALUE) {
                for (int i = 0; i < 100; i++) {
                    Map<String, Object> value = new HashMap<>(6);
                    LocalDateTime lt = LocalDateTime.now();
                    value.put("city", "hz" + (j * 100 + i));
                    value.put("time", lt.format(dateTimeFormatter));
                    value.put("index", j * 100 + i);
                    value.put("timestamp", Utils.toNanos(lt));
                    values[i] = toJson(value);
                }
                j += 1;
                ssdb.qpush_back("listTest", values);
                Thread.sleep(10000);
            }
        }
    }

    @Test
    public void createListData3() throws IOException, InterruptedException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");
        try (SSDB ssdb = SSDBs.simple()) {
            Object[] values = new Object[100];
            int j = 0;
            while (j < 100) {
                for (int i = 0; i < 10; i++) {
                    Map<String, Object> value = new HashMap<>(6);
                    LocalDateTime lt = LocalDateTime.now();
                    value.put("city", "hz_" + j);
                    value.put("time", lt.format(dateTimeFormatter));
                    value.put("index", j * 10 + i);
                    value.put("timestamp", Utils.toNanos(lt));
                    values[i] = toJson(value);
                }
                j += 1;
                ssdb.qpush_back("listTest", values);
                Thread.sleep(1000);
            }
        }
    }

    private String toJson(Map<String, Object> map) {
        if (map == null || map.isEmpty()) return "{}";
        else {
            StringBuilder builder = new StringBuilder("{");
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                builder.append("\"").append(entry.getKey()).append("\"").append(":");
                if (entry.getValue() instanceof Integer) builder.append(entry.getValue()).append(",");
                else builder.append("\"").append(entry.getValue()).append("\",");
            }
            return builder.substring(0, builder.length() - 1) + "}";
        }
    }

    @After
    public void tearDown() throws Exception {
    }
}