package app.index;


import org.apache.lucene.document.*;
import util.Constants;
import bean.Pair;
import bean.LSException;
import bean.Schema;
import bean.Source;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import util.JsonUtil;
import app.source.SsdbPull;
import util.Utils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 写索引
 * <p>
 * 持续取数据,无数据即阻塞
 * <p>
 * setRAMBufferSizeMB:
 * <p>
 * Optional: for better indexing performance, if you
 * are indexing many documents, increase the RAM
 * buffer.  But if you do this, increase the max heap
 * size to the JVM (eg add -Xmx512m or -Xmx1g).
 * <p>
 * forceMerge:
 * <p>
 * NOTE: if you want to maximize search performance,
 * you can optionaly call forceMerge here.  This can be
 * a terribly costly operation, so generally it's only
 * worth it when your index is relatively static (ie
 * you're done adding documents to it).
 */

@Deprecated
@SuppressWarnings("all")
public class WriteIndex {

    private Logger logger = Logger.getLogger(WriteIndex.class);

    private final Schema schema;

    private final Path indexPath;
    private IndexWriter indexWriter;

    private SsdbPull ssdbPull;

    private boolean stop = false;

    public WriteIndex(Schema schema) {
        this.schema = schema;
        this.indexPath = Constants.indexDir.resolve(schema.getIndex());
    }

    public void write() {
        try {
            Directory dir = FSDirectory.open(indexPath);
            Analyzer analyzer = Utils.getInstance(schema.getAnalyser(), Analyzer.class);
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            iwc.setRAMBufferSizeMB(256.0);
            indexWriter = new IndexWriter(dir, iwc);
            if (schema.getSource() != null) this.indexSsdb(indexWriter);
//             writer.forceMerge(1);
            logger.debug("committing index[" + schema.getIndex() + "]");
            long start = System.currentTimeMillis();
            indexWriter.commit();
            indexWriter.close();
            long end = System.currentTimeMillis();
            logger.debug("index[" + schema.getIndex() + "] commit cost time[" + (end - start) + "] mills");
        } catch (Exception e) {
            logger.error(e.getCause() == null ? e.getMessage() : e.getCause());
            this.close();
        }
    }

    private void indexSsdb(IndexWriter writer) throws LSException {
        Source source = schema.getSource();
//        String addr = ssdb.getAddr();
//        int idex = addr.indexOf(":");
//        ssdbPull = new SsdbPull(addr.substring(0, idex),
//                Integer.parseInt(addr.substring(idex + 1)),
//                ssdb.getName(), ssdb.getType(), schema.getIndex());
        ssdbPull = new SsdbPull(source.getIp(), source.getPort(), source.getName(), source.getType(),schema.getIndex());
        ssdbPull.start();
        logger.debug("indexing[" + schema.getIndex() + "]");
//        long start = System.currentTimeMillis();
//        long count = 0;
        while (!stop) {
            try {
                Pair<Object, String> pair = ssdbPull.queue.poll(ssdbPull.timeout, TimeUnit.MILLISECONDS);
//                if (pair == null) break;
                if (pair == null) continue;
                Map<String, Object> data;
                try {
                    data = JsonUtil.toMap(pair.getRight());
                } catch (IOException e) {
                    logger.warn("ssdb." + source.getName()
                            + " value[" + pair.getRight() + "] format is not support,this record will be discarded...");
                    data = null;
                }
                if (data == null) continue;
                Document doc = new Document();
                for (bean.Field field : schema.getFields()) {
                    String name = field.getName();
                    if (data.containsKey(name)) {
                        Object value = data.get(name);
                        switch (field.getType()) {
                            case INT:
                                int i;
                                if (value instanceof Integer) i = (int) value;
                                else i = Integer.valueOf(String.valueOf(value));
                                doc.add(new IntPoint(name, i));
//                                doc.add(new NumericDocValuesField(name, i));
//                                doc.add(new StoredField(name, i));
                                break;
                            case DATE:
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(field.getFormatter());
                                LocalDateTime localDateTime = LocalDateTime.parse(String.valueOf(value), formatter);
//                                doc.add(new StringField(name, Utils.toNanos(localDateTime), Field.Store.YES));
                                doc.add(new LongPoint(name, Long.valueOf(Utils.toNanos(localDateTime))));
//                                doc.add(new NumericDocValuesField(name, mills));
//                                doc.add(new StoredField(name, mills));
                                break;
                            case LONG:
                                long l;
                                if (value instanceof Long) l = (long) value;
                                else l = Long.valueOf(String.valueOf(value));
                                doc.add(new LongPoint(name, l));
//                                doc.add(new NumericDocValuesField(name, l));
//                                doc.add(new StoredField(name, l));
                                break;
                            case STRING:
                                doc.add(new StringField(name, String.valueOf(value), Field.Store.NO));
                                break;
                            case TEXT:
                                doc.add(new TextField(name, String.valueOf(value), Field.Store.NO));
                                break;
                        }
                    }
                }
                if (Source.Type.LIST == source.getType()) {
                    doc.add(new StoredField("_index", (int) pair.getLeft()));
                } else if (Source.Type.HASH == source.getType()) {
                    doc.add(new StoredField("_key", (String) pair.getLeft()));
                }
                writer.addDocument(doc);
//                count++;
            } catch (InterruptedException e) {
                logger.warn("队列中断", e);
            } catch (DateTimeParseException | IOException e) {
                throw new LSException("索引[" + schema.getIndex() + "]创建document出错", e);
            }
        }
//        long end = System.currentTimeMillis();
//        logger.debug("index [" + schema.getIndex() + "]finished,count[" + count + "],cost time[" + (end - start) + "] mills");
    }

    public void close() {
        logger.warn("强制关闭索引[" + schema.getIndex() + "]...");
        stop = true;
        if (ssdbPull != null) {
            ssdbPull.close();
            try {
                Thread.sleep(ssdbPull.timeout + 10);
            } catch (InterruptedException ignore) {
            }
        } else if (indexWriter != null) try {
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }
}
