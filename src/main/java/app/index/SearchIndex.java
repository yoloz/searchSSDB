package app.index;

import bean.Field;
import bean.LSException;
import bean.Schema;
import bean.Source;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import util.Constants;
import util.Utils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 索引创建完毕后的搜索
 */
@Deprecated
@SuppressWarnings("all")
public class SearchIndex {

    private final Logger logger = Logger.getLogger(SearchIndex.class);

    private final Path indexPath;
    private final Schema schema;

    public SearchIndex(Schema schema) {
        this.schema = schema;
        this.indexPath = Constants.indexDir.resolve(schema.getIndex());
    }

    /**
     * int,long类型数据做搜索条件搜索不到数据
     * <p>
     * 需添加where条件
     * 即select * from table不支持
     * '*' or '?' not allowed as first character in WildcardQuery
     *
     * @param qs    query string
     * @param limit query count
     * @param cols  return field
     * @return map {@link HashMap} {total:123,results:[{},{}...]}
     * @throws LSException ls error
     */
    @Deprecated
    public Map<String, Object> search(String qs, List<String> cols, Integer limit)
            throws LSException {
        Analyzer analyzer = Utils.getInstance(schema.getAnalyser(), Analyzer.class);
        QueryParser parser = new QueryParser("", analyzer);
        try {
            Query query = parser.parse(qs);
            return search(query, cols, limit);
        } catch (ParseException e) {
            throw new LSException("parse[" + qs + "] error", e);
        }
    }

    /**
     * _index:ssdb list index
     * _key:ssdb hash key
     * _name:ssdb name
     * <p>
     *
     * @throws LSException error
     */
    public Map<String, Object> search(Query query, List<String> cols, Integer limit)
            throws LSException {
        Map<String, Object> map = new HashMap<>(2);
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(query, limit);
            ScoreDoc[] hits = topDocs.scoreDocs;
            int numTotalHits = Math.toIntExact(topDocs.totalHits);
            logger.debug(numTotalHits + " total matching documents");
            int end = Math.min(numTotalHits, limit);
            if (end > hits.length) {
                logger.warn("top hits[" + hits.length + "] of total[" + numTotalHits
                        + "] matched documents collected,need collect more[" + end + "]");
                hits = searcher.search(query, numTotalHits).scoreDocs;
            }
            end = Math.min(hits.length, limit);
            map.put("total", numTotalHits);
            List<Map<String, Object>> results = new ArrayList<>(limit);
            for (int i = 0; i < end; i++) {
                Map<String, Object> m = new HashMap<>(schema.getFields().size());
                Document doc = searcher.doc(hits[i].doc);
                for (Field f : schema.getFields()) {
                    String name = f.getName();
                    boolean put = true;
                    if (!(cols.size() == 1 && "*".equals(cols.get(0)))) put = cols.contains(name);
                    if (put && doc.get(name) != null) switch (f.getType()) {
                        case INT:
                            m.put(name, Integer.valueOf(doc.get(name)));
                            break;
                        case LONG:
                            m.put(name, Long.valueOf(doc.get(name)));
                            break;
                        case TEXT:
                        case STRING:
                            m.put(name, doc.get(name));
                            break;
                        case DATE:
                            LocalDateTime time = Utils.ofNanos(doc.get(name));
                            m.put(name, time.format(DateTimeFormatter.ofPattern(f.getFormatter())));
                            break;
                    }
                }
                if (Source.Type.LIST == schema.getSource().getType()) {
                    String _index = doc.get("_index");
                    if (_index != null) m.put("_index", Integer.valueOf(_index));
                } else if (Source.Type.HASH == schema.getSource().getType()) {
                    String _key = doc.get("_key");
                    if (_key != null) m.put("_key", _key);
                }
                m.put("_name", schema.getSource().getName());
                results.add(m);
            }
            map.put("results", results);
        } catch (IOException e) {
            throw new LSException("query[" + query.toString() + "] error", e);
        }
        return map;
    }
}
