package index;

import bean.LSException;
import bean.Schema;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import util.Constants;
import util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class Indexer {

    private static final Logger logger = Logger.getLogger(Indexer.class);

    private static Map<String, String> analyserMapper = new ConcurrentHashMap<>(1);

    static {
        try {
            updateAnalyser();
        } catch (Exception e) {
            logger.error("load file[" + Constants.confDir.resolve("analyser.properties") + "] error", e);
            System.exit(1);
        }
    }

    //<indexName,indexImpl>,运行索引实例映射
    public static final Cache<String, IndexImpl> indexes = CacheBuilder
            .newBuilder()
            .maximumSize(Constants.totalIndex)
            .removalListener(RemovalListeners.asynchronous(
                    (RemovalListener<String, IndexImpl>) notify -> {
                        if (notify.getCause() != RemovalCause.EXPLICIT) notify.getValue().close();
                    },
                    Executors.newSingleThreadExecutor()))
            .build();

    public static boolean isRunning(String indexName) {
        return indexes.getIfPresent(indexName) != null;
    }

    public static void stopIndex(String indexName) {
        IndexImpl index = indexes.getIfPresent(indexName);
        if (index != null) index.close();
    }

    public static void index(String indexName) throws LSException {
        Schema schema = Utils.getSchema(indexName);
        IndexImpl impl = new IndexImpl(schema, Utils.getLogger(indexName,
                Level.toLevel(Constants.logLevel)));
        Indexer.indexes.put(schema.getIndex(), impl);
        new Thread(impl).start();
    }

    public static String getAnalyserClass(String key) throws LSException {
        key = key.toLowerCase();
        if (!analyserMapper.containsKey(key))
            throw new LSException("analyser[" + key + "] is not exit in analyser.properties");
        return analyserMapper.get(key);
    }

    public static void updateAnalyser() throws IOException {
        analyserMapper.clear();
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(
                Constants.confDir.resolve("analyser.properties"))) {
            properties.load(inputStream);
        }
        properties.forEach((k, v) ->
                analyserMapper.put(String.valueOf(k).toLowerCase(), String.valueOf(v)));
    }
}
