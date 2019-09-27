package app.source;

import bean.Pair;
import bean.LSException;
import bean.Source;
import org.apache.log4j.Logger;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import util.SqlliteUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 非线程安全
 * <p>
 * 连接后不断开持续取数据,无数据即阻塞;
 * 定量(10000)更新point点,在异常断开后重启可以继续[异常断连会造成丢失数据]
 */
@Deprecated
public class SsdbPull extends Thread {

    private final Logger logger = Logger.getLogger(SsdbPull.class);

    private final int limit = 500;
    private Object point;

    private String ip;
    private int port;
    private String name;
    private Source.Type type;
    public final int timeout;
    private final int waitMills = 10000; //need lower than timeout

    private String indexName;

    private boolean stop = false;

    public final ArrayBlockingQueue<Pair<Object, String>> queue =
            new ArrayBlockingQueue<>(limit * 2);

    public SsdbPull(String ip, int port, String name, Source.Type type, String indexName) {
        this(ip, port, name, type, indexName, 15000);
    }

    private SsdbPull(String ip, int port, String name, Source.Type type, String indexName, int timeout) {
        this.ip = ip;
        this.port = port;
        this.name = name;
        this.type = type;
        this.indexName = indexName;
        this.timeout = timeout;
    }


    /**
     * 创建单连接
     *
     * @return SSDB {@link org.nutz.ssdb4j.spi.SSDB}
     */
    private SSDB connect() {
        return SSDBs.simple(ip, port, timeout);
    }

    private void poll() throws LSException {
        logger.debug("polling ssdb." + name + " data to index");
        initPoint();
        try (SSDB ssdb = this.connect()) {
//            int remaining;
//            do {
//                long start = System.currentTimeMillis();
//                List<ImmutablePair<Object, String>> pairs = pollOnce(ssdb);
//                if (!pairs.isEmpty()) for (ImmutablePair<Object, String> pair : pairs) {
//                    queue.put(pair);
//                }
//                remaining = pairs.size();
//                long end = System.currentTimeMillis();
//                logger.debug("pollOnce[" + pairs.size() + "] cost time[" + (end - start) + "] mills");
//            } while (remaining > 0);
            int counter = 0;
            while (!stop) {
                long start = System.currentTimeMillis();
                List<Pair<Object, String>> pairs = pollOnce(ssdb);
                if (!pairs.isEmpty()) for (Pair<Object, String> pair : pairs) {
                    queue.put(pair);
                }
                counter += pairs.size();
                long end = System.currentTimeMillis();
                logger.debug("pollOnce[" + pairs.size() + "] cost time[" + (end - start) + "] mills");
                if (pairs.size() == 0) Thread.sleep(waitMills);
                if (counter >= 10000) {
                    SqlliteUtil.getInstance().update("update ssdb set point=? where name=?", point, indexName);
                    counter = 0;
                }
            }
            SqlliteUtil.getInstance().update("update ssdb set point=? where name=?", point, indexName);
        } catch (LSException | SQLException e) {
            logger.warn(e.getMessage(), e);
        } catch (Exception e) {
            throw new LSException("poll ssdb." + name + " error", e);
        }
        logger.debug("ssdb pull has stopped...");
    }

    /*    String createSql = "CREATE TABLE ssdb(" +
            "name TEXT PRIMARY KEY NOT NULL, " +
            "point TEXT NOT NULL," +
            "pid INT DEFAULT 0" +
            ")";*/
    private void initPoint() throws LSException {
        try {
            List<Object> points = SqlliteUtil.getInstance().queryL(
                    "select point from ssdb where name=?", indexName);
            String _point = String.valueOf(points.get(0));
            switch (type) {
                case LIST:
                    point = Integer.parseInt(_point);
                    break;
                case HASH:
                    point = _point;
                    break;
                default:
                    throw new LSException("ssdb type [" + type + "] is not support...");
            }
        } catch (Exception e) {
            throw new LSException("初始化ssdb." + name + "的point信息出错", e);
        }
    }

    private List<Pair<Object, String>> pollOnce(SSDB ssdb) throws LSException {
        switch (type) {
            case LIST:
                return listScan(ssdb, (int) point);
            case HASH:
                return hashScan(ssdb, (String) point);
            default:
                throw new LSException("ssdb type [" + type + "] is not support...");
        }
    }

    private List<Pair<Object, String>> listScan(SSDB ssdb, int offset) {
        Response response = ssdb.qrange(name, offset, limit);
        if (response.datas.size() == 0) return Collections.emptyList();
        else {
//           response.datas.parallelStream()
//                    .map(v -> new String(v, SSDBs.DEFAULT_CHARSET))
//                    .collect(Collectors.toList());
            List<Pair<Object, String>> list = new ArrayList<>(response.datas.size());
            for (int i = 0; i < response.datas.size(); i++) {
                Pair<Object, String> pair = Pair.of(offset + i,
                        new String(response.datas.get(i), SSDBs.DEFAULT_CHARSET));
                list.add(pair);
            }
            point = offset + response.datas.size();
            return list;
        }
    }

    private List<Pair<Object, String>> hashScan(SSDB ssdb, String key_start) {
        Response response = ssdb.hscan(name, key_start, "", limit);
        if (response.datas.size() == 0) return Collections.emptyList();
        else {
            List<Pair<Object, String>> list = new ArrayList<>(response.datas.size());
            for (int i = 0; i < response.datas.size(); i += 2) {
                String key = new String(response.datas.get(i), SSDBs.DEFAULT_CHARSET);
                if (i == response.datas.size() - 2) point = key;
                Pair<Object, String> pair = Pair.of(key,
                        new String(response.datas.get(i + 1), SSDBs.DEFAULT_CHARSET));
                list.add(pair);
            }
            return list;
        }
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            poll();
        } catch (LSException e) {
            logger.error(e.getCause() == null ? e.getMessage() : e.getCause());
        }
    }

    public void close() {
        logger.debug("stop ssdb pull...");
        stop = true;
        try {
            Thread.sleep(waitMills + 10);
        } catch (InterruptedException ignore) {
        }
    }
}
