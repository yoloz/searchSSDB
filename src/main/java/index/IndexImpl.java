package index;

import bean.Pair;
import bean.Triple;
import bean.LSException;
import bean.Schema;
import index.nrt.ControlledRealTimeThread;
import index.nrt.SearcherLifetimeManager;
import index.pull.Pull;
import index.pull.SsdbPull;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import util.Constants;
import util.JsonUtil;
import util.Utils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 索引原始数据均不存储
 */
class IndexImpl implements Runnable {

    private final Logger logger;

    private IndexWriter indexWriter;
    private SearcherManager searcherManager;
    //<pullName,key,value>
    private final ArrayBlockingQueue<Triple<String, Object, Object>> queue =
            new ArrayBlockingQueue<>(1000);

    private final int blockSec = 10;

    private final Schema schema;
    private final Path indexPath;
    private Pull pull;

    private SearcherLifetimeManager searcherLifetimeManager;
    private ControlledRealTimeThread controlledRealTimeThread;
    private long generation;
    private PerDayCommit perDayCommit;


    IndexImpl(Schema schema, Logger logger) {
        this.schema = schema;
        this.indexPath = Constants.indexDir.resolve(schema.getIndex());
        this.logger = logger;
        if (schema.getSource() != null)
            this.pull = new SsdbPull(schema.getSource(), schema.getIndex(), queue, blockSec, logger);
    }

    @Override
    public void run() {
        try {
            logger.info("start index[" + schema.getIndex() + "]");
            this.initIndex();
            new Thread(pull).start();
            perDayCommit = new PerDayCommit();
            perDayCommit.sched();
            this.impl();
            //pull stop that index writer will stop and need commit
            logger.info("committing index[" + schema.getIndex() + "]");
            //this.indexWriter.forceMerge(1);
            this.commit();
        } catch (Exception e) {
            logger.error("index[" + schema.getIndex() + "] error", e);
        }
        this.close();
    }

    private void commit() {
        try {
            if (this.indexWriter != null && this.indexWriter.isOpen()) this.indexWriter.commit();
        } catch (IOException e) {
            logger.error("index[" + schema.getIndex() + "] commit error", e);
        }
    }

    void close() {
        logger.info("close index[" + schema.getIndex() + "]...");
        if (this.pull != null) this.pull.close();
        try {
            //if (searcherManager != null) searcherManager.close();
            //all searcher release at searcherLifetimeManager close
            if (searcherLifetimeManager != null) searcherLifetimeManager.close();
            if (controlledRealTimeThread != null) controlledRealTimeThread.close();
            if (perDayCommit != null) perDayCommit.cancel();
            if (this.indexWriter != null && this.indexWriter.isOpen()) this.indexWriter.close();
        } catch (IOException e) {
            logger.error("close[" + schema.getIndex() + "] error", e);
        }
        Indexer.indexes.invalidate(schema.getIndex());
    }

    private IndexSearcher getSearcher() throws IOException {
        try {
            controlledRealTimeThread.waitForGeneration(generation);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return this.searcherManager.acquire();
    }

    /**
     * 分页搜索
     *
     * @param version searcher token
     * @return token, searcher
     * @throws IOException io error
     */
    Pair<Long, IndexSearcher> getSearcher(long version) throws IOException {
        IndexSearcher indexSearcher;
        if (version < 0) {
            indexSearcher = getSearcher();
            if (searcherLifetimeManager != null)
                version = searcherLifetimeManager.record(indexSearcher);
        } else {
            if (searcherLifetimeManager == null) throw new IOException("非分页模式,参数version[" + version + "]大于0");
            indexSearcher = searcherLifetimeManager.acquire(version);
            if (indexSearcher == null) throw new IOException("搜索[" + version + "]已经超时,请重新SQL查询");
        }
        return Pair.of(version, indexSearcher);
    }

    private void initIndex() throws IOException, LSException {
        Directory dir = FSDirectory.open(indexPath);
        Analyzer analyzer = Utils.getInstance(schema.getAnalyser(), Analyzer.class);
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        iwc.setRAMBufferSizeMB(Constants.RAMBuffer);
        this.indexWriter = new IndexWriter(dir, iwc);
        this.searcherManager = new SearcherManager(indexWriter, false,
                false, null);
        if (Constants.searchExpired > 0)
            this.searcherLifetimeManager = new SearcherLifetimeManager();
        else this.searcherLifetimeManager = null;
        controlledRealTimeThread = new ControlledRealTimeThread(indexWriter, searcherManager, searcherLifetimeManager,
                Constants.searchExpired + 1, 1.0);
        controlledRealTimeThread.setDaemon(true);
        controlledRealTimeThread.setName("update-" + schema.getIndex());
        controlledRealTimeThread.start();
    }

    private void impl() throws IOException {
        while (pull.isRunning()) {
            try {
                Triple<String, Object, Object> triple = this.queue.poll(blockSec, TimeUnit.SECONDS);
                if (triple == null) continue;
//                Object rv = triple.getRight();
//                if (!(rv instanceof String)) {
//                    logger.warn("暂时仅支持处理字符串值!");
//                }
                Map<String, Object> data;
                try {
                    data = JsonUtil.toMap((String) triple.getRight());
                } catch (IOException e) {
                    logger.warn("ssdb[" + triple.getLeft() + "," + triple.getMiddle()
                            + "]value convert to JSON fail,this record will be discarded...");
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
                                doc.add(new NumericDocValuesField(name, i));
                                break;
                            case DATE:
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(field.getFormatter());
                                LocalDateTime localDateTime = LocalDateTime.parse(String.valueOf(value), formatter);
                                long dl = Long.valueOf(Utils.toNanos(localDateTime));
                                doc.add(new LongPoint(name, dl));
                                doc.add(new NumericDocValuesField(name, dl));
                                break;
                            case LONG:
                                long l;
                                if (value instanceof Long) l = (long) value;
                                else l = Long.valueOf(String.valueOf(value));
                                doc.add(new LongPoint(name, l));
                                doc.add(new NumericDocValuesField(name, l));
                                break;
                            case STRING:
                                doc.add(new StringField(name, String.valueOf(value), Field.Store.NO));
                                doc.add(new SortedDocValuesField(name, new BytesRef(String.valueOf(value))));
                                break;
                            case TEXT:
                                doc.add(new TextField(name, String.valueOf(value), Field.Store.NO));
                                break;
                        }
                    }
                }
                doc.add(new StoredField("_key", String.valueOf(triple.getMiddle())));
                doc.add(new StoredField("_name", triple.getLeft()));
                generation = indexWriter.addDocument(doc);
            } catch (InterruptedException e) {
                logger.error(schema.getIndex() + " queue error", e);
            }
        }
    }


    private class PerDayCommit {

        private final Date firstTime;
        private final Timer timer = new Timer();

        private PerDayCommit() {
            LocalDateTime _time = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.perDayHour, 0));
            if (_time.isBefore(LocalDateTime.now())) _time = _time.plusDays(1);
            firstTime = Date.from(_time.atZone(ZoneId.systemDefault()).toInstant());
        }

        private void sched() {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    commit();
                }
            }, firstTime, 24 * 60 * 60 * 1000);
        }

        private void cancel() {
            timer.cancel();
        }
    }
}
