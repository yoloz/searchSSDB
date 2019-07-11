package api;

import bean.LSException;
import index.Searcher;
import org.apache.log4j.Logger;
import util.JsonUtil;
import util.Utils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * params:
 * 1,select sql {"key":""}
 * 2,nrt search and page {"key":"","offset":,"limit":}
 */
public class QueryIndex extends HttpServlet {

    private final Logger logger = Logger.getLogger(QueryIndex.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String request = Utils.getInputStream(req.getInputStream());
        logger.debug("query params=>[" + request + "]");
        Map<String, Object> map = JsonUtil.toMap(request);
        String error = "";
        Map<String, Object> results = null;
        try {
            if (map.isEmpty() || !map.containsKey("sql"))
                throw new LSException("query params=>[" + request + "] error");
            results = Searcher.search(String.valueOf(map.get("sql")),
                    Long.parseLong(String.valueOf(map.getOrDefault("key", "-1"))));
            results.put("success", true);
        } catch (Exception e) {
            logger.error("query [" + request + "] error", e);
            error = Utils.responseError(e.getMessage());
        }
        resp.setContentType("application/json;charset=UTF-8");
        OutputStream outputStream = resp.getOutputStream();
        if (error.isEmpty()) {
            outputStream.write(JsonUtil.toString(results).getBytes(StandardCharsets.UTF_8));
        } else outputStream.write(error.getBytes(StandardCharsets.UTF_8));
    }
}
