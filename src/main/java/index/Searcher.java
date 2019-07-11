package index;

import bean.LSException;

import java.io.IOException;
import java.util.Map;

/**
 * lruCache<sql,page>:
 * lruCache size is Constants.searchCache*Constants.totalIndex
 * page size is Constants.pageCache
 */

public class Searcher {

    /*static final int max = Constants.searchCache * Constants.totalIndex;

    //sqlId<==>indexName
    static final Map<String, String> mapper = new ConcurrentHashMap<>(max);

    //<sqlId,<[show fields],[<pullName,key>],total>>,total<=Constants.pageCache*pageSize
    static final Cache<String, Triple<List<String>, List<Pair<String, Object>>, Integer>> searches = CacheBuilder
            .newBuilder()
            .maximumSize(max)
            .removalListener(RemovalListeners.asynchronous(
                    (RemovalListener<String, Triple<List<String>, List<Pair<String, Object>>, Integer>>)
                            notify -> mapper.remove(notify.getKey()),
                    Executors.newSingleThreadExecutor()))
            .build();*/

    public static Map<String, Object> search(String sql, long key) throws LSException, IOException {
        SearchImpl searchImpl = new SearchImpl(sql, key);
        return searchImpl.search();
    }
}
