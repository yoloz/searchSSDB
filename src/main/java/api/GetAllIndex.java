package api;

import bean.Schema;
import index.Indexer;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import util.JsonUtil;
import util.SqlliteUtil;
import util.Utils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetAllIndex extends HttpServlet {

    private final Logger logger = Logger.getLogger(GetAllIndex.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        logger.debug("get all index");
        String error = "";
        Map<String, Object> results = new HashMap<>();
        Yaml yaml = new Yaml();
        try {
            results.put("success", true);
            List<Map<String, Object>> list = SqlliteUtil.query("select name,value from schema");
            List<Map<String, String>> indexes = new ArrayList<>(list.size());
            for (Map<String, Object> map : list) {
                Map<String, String> index = new HashMap<>(3);
                index.put("indexName", String.valueOf(map.get("name")));
                Schema schema = yaml.loadAs(String.valueOf(map.get("value")), Schema.class);
                index.put("ssdbAddr",schema.getSource().getIp()+":"+schema.getSource().getPort());
                index.put("ssdbName",schema.getSource().getName());
                indexes.add(index);
            }
            results.put("indexes", indexes);
            List<String> running = new ArrayList<>(Indexer.indexes.asMap().keySet());
            results.put("running", running);
        } catch (Exception e) {
            logger.error("get all index error,", e);
            error = Utils.responseError(e.getMessage());
        }
        resp.setContentType("application/json;charset=UTF-8");
        OutputStream outputStream = resp.getOutputStream();
        if (error.isEmpty())
            outputStream.write(JsonUtil.toString(results).getBytes(StandardCharsets.UTF_8));
        else outputStream.write(error.getBytes(StandardCharsets.UTF_8));
    }
}
