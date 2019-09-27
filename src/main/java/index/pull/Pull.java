package index.pull;

import bean.LSException;
import bean.Pair;
import bean.Source;
import bean.Triple;
import org.apache.log4j.Logger;
import util.SqlliteUtil;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 连接后不断开持续取数据,无数据即阻塞;
 * 定量(10000)更新point点{@link index.IndexImpl},在异常断开后重启可以继续[异常断连会造成丢失数据]
 */
public abstract class Pull implements Runnable, Closeable {

    Logger logger;
    Source source;
    String indexName;
    //<pullName,key>
    Pair<String, Object> point;
    //<pullName,key,value>,value暂时只支持string
    ArrayBlockingQueue<Triple<String, Object, Object>> queue;
    int blockSec;
    boolean stop = false;

    Pull(Source source, String name,
         ArrayBlockingQueue<Triple<String, Object, Object>> queue,
         int blockSec, Logger logger) {
        this.source = source;
        this.indexName = name;
        this.queue = queue;
        this.blockSec = blockSec;
        this.logger = logger;
    }

    public boolean isRunning() {
        return !stop;
    }

    @Override
    public void run() {
        logger.info("start pull[" + indexName + "]data");
        try {
            this.initPoint();
            this.poll();
        } catch (Exception e) {
            logger.error(indexName + " pull error", e);
            this.close();
        }
        try {
            logger.info("update [" + indexName + "] latest point message");
            SqlliteUtil.getInstance().update("update point set name=?,value=? where iname=?",
                    point.getLeft(), point.getRight(), indexName);
        } catch (SQLException e) {
            logger.error(indexName + " update point error", e);
        }
    }

    @Override
    public void close() {
        logger.info("close[" + indexName + "]pull...");
        this.stop = true;
        try {
            Thread.sleep(blockSec * 500 + 100);
        } catch (InterruptedException ignore) {
        }
    }

    abstract void poll() throws Exception;

    abstract void initPoint() throws LSException;
}
