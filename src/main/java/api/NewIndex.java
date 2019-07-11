package api;

import bean.LSException;
import bean.Source;
import index.Indexer;
import index.parse.CreateSql;
import org.apache.log4j.Logger;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.SSDB;
import util.Utils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class NewIndex extends HttpServlet {

    private Logger logger = Logger.getLogger(NewIndex.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String sql = Utils.getInputStream(req.getInputStream());
        logger.debug("create sql=>[" + sql + "]");
        String error = "";
        try {
            CreateSql createSql = new CreateSql(sql);
            String indexName = createSql.store();
            Source source = Utils.getSchema(indexName).getSource();
            if (Source.Type.LIST == source.getType() || Source.Type.HASH == source.getType()) {
                try {
                    SSDB ssdb = SSDBs.simple(source.getIp(), source.getPort(), SSDBs.DEFAULT_TIMEOUT);
                    ssdb.close();
                } catch (Exception e) {
                    throw new LSException("连接[" + source.getIp() + ":" + source.getPort() + "]失败", e);
                }
            } else throw new LSException("type[" + source.getType() + "] is not support");
            Indexer.index(indexName);
        } catch (Exception e) {
            logger.error("create index[" + sql + "] error", e);
            error = Utils.responseError(e.getMessage());
        }
        resp.setContentType("application/json;charset=UTF-8");
        OutputStream outputStream = resp.getOutputStream();
        if (error.isEmpty()) outputStream.write("{\"success\":true}".getBytes(StandardCharsets.UTF_8));
        else outputStream.write(error.getBytes(StandardCharsets.UTF_8));
    }
}
