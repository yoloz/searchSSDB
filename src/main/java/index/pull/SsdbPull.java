package index.pull;

import bean.Pair;
import bean.Triple;
import bean.LSException;
import bean.Source;
import org.apache.log4j.Logger;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import util.SqlliteUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;


public class SsdbPull extends Pull {

    private TreeSet<String> pulls;
    private String prefix = "";

    public SsdbPull(Source source, String name,
                    ArrayBlockingQueue<Triple<String, Object, Object>> queue,
                    int blockSec, Logger logger) {
        super(source, name, queue, blockSec, logger);
    }


    @Override
    void initPoint() throws LSException {
        try {
            List<Map<String, Object>> points = SqlliteUtil.query(
                    "select name,value from point where iname=?", indexName);
            Map<String, Object> m = points.get(0);
            if (Source.Type.LIST == source.getType())
                point = Pair.of(String.valueOf(m.get("name")), Integer.parseInt(String.valueOf(m.get("value"))));
            else point = Pair.of(String.valueOf(m.get("name")), String.valueOf(m.get("value")));
            if (source.getName().contains("*")) {
                prefix = source.getName().replaceAll("\\*", "");
                pulls = new TreeSet<>();
                this.updatePullName();
                if (point.getLeft().isEmpty()) {
                    if (pulls.isEmpty()) throw new LSException("ssdb 的名称[" + source.getName() + "]匹配项为零");
                    point = Pair.of(pulls.pollFirst(), point.getRight());
                }
            } else {
                if (point.getLeft().isEmpty()) point = Pair.of(source.getName(), point.getRight());
            }
            logger.debug("[" + indexName + "] init point[" + point + "]");
        } catch (SQLException e) {
            throw new LSException("初始化" + indexName + "的point信息出错", e);
        }
    }

    @Override
    void poll() throws Exception {
        try (SSDB ssdb = this.connect()) {
            while (!stop) {
//                long start = System.currentTimeMillis();
                List<Triple<String, Object, Object>> triples = pollOnce(ssdb);
//                logger.debug("pollOnce [" + triples.size() + "]");
                if (!triples.isEmpty()) for (Triple<String, Object, Object> triple : triples) queue.put(triple);
//                long end = System.currentTimeMillis();
//                logger.debug("pollOnce[" + triples.size() + "] cost time[" + (end - start) + "] mills");
                if (triples.size() == 0) {
                    if (pulls != null && updatePoint()) continue;
                    Thread.sleep(blockSec * 500);
                }
            }
        }
    }

    private boolean updatePoint() {
        if (pulls.isEmpty()) {
            this.updatePullName();
            if (!pulls.isEmpty()) updatePoint();
            else {
                logger.debug("no new ssdb source to get,latest[" + point.getLeft() + "]");
                return false;
            }
        }
        String _pull = pulls.pollFirst();
        if (_pull == null) {
            logger.warn("pull name is null");
            return false;
        }
        logger.info("change to [" + _pull + "] and update[" + point + "]");
        try {
            SqlliteUtil.update("update point set name=?,value=? where iname=?",
                    point.getLeft(), point.getRight(), indexName);
        } catch (SQLException e) {
            logger.error(indexName + " update point error", e);
        }
        if (source.getType() == Source.Type.LIST) point = Pair.of(_pull, 0);
        else point = Pair.of(_pull, "");
        return true;
    }

    private void updatePullName() {
        if (pulls == null) return;
        try (SSDB ssdb = this.connect()) {
            Response response;
            if (Source.Type.LIST == source.getType())
                response = ssdb.qlist(point.getLeft(), "", 150);
            else response = ssdb.hlist(point.getLeft(), "", 150);
            for (int i = 0; i < response.datas.size(); i++) {
                String _name = new String(response.datas.get(i), SSDBs.DEFAULT_CHARSET);
                if (_name.startsWith(prefix)) pulls.add(_name);
            }
        } catch (IOException e) {
            logger.warn("updatePullName error,", e);
        }
    }

    /**
     * 创建单连接
     *
     * @return SSDB {@link SSDB}
     */
    private SSDB connect() {
        return SSDBs.simple(source.getIp(), source.getPort(), (blockSec - 1) * 1000);
    }

    private List<Triple<String, Object, Object>> pollOnce(SSDB ssdb) {
        if (Source.Type.LIST == source.getType()) return listScan(ssdb);
        else return hashScan(ssdb);
    }

    private List<Triple<String, Object, Object>> listScan(SSDB ssdb) {
        int offset = (int) point.getRight();
        Response response = ssdb.qrange(point.getLeft(), offset, 500);
        if (response.datas.size() == 0) return Collections.emptyList();
        else {
            List<Triple<String, Object, Object>> list = new ArrayList<>(response.datas.size());
            for (int i = 0; i < response.datas.size(); i++) {
                Triple<String, Object, Object> triple = Triple.of(point.getLeft(), offset + i,
                        new String(response.datas.get(i), SSDBs.DEFAULT_CHARSET));
                list.add(triple);
            }
            point = Pair.of(point.getLeft(), offset + response.datas.size());
            return list;
        }
    }

    private List<Triple<String, Object, Object>> hashScan(SSDB ssdb) {
        String key_start = (String) point.getRight();
        Response response = ssdb.hscan(point.getLeft(), key_start, "", 500);
        if (response.datas.size() == 0) return Collections.emptyList();
        else {
            List<Triple<String, Object, Object>> list = new ArrayList<>(response.datas.size());
            for (int i = 0; i < response.datas.size(); i += 2) {
                String key = new String(response.datas.get(i), SSDBs.DEFAULT_CHARSET);
                if (i == response.datas.size() - 2) point = Pair.of(point.getLeft(), key);
                Triple<String, Object, Object> pair = Triple.of(point.getLeft(), key,
                        new String(response.datas.get(i + 1), SSDBs.DEFAULT_CHARSET));
                list.add(pair);
            }
            return list;
        }
    }

}
