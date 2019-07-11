package api;

import index.Indexer;
import org.apache.log4j.Logger;
import util.Utils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 运行中动态
 */
public class UpdateAnalyser extends HttpServlet {


    private Logger logger = Logger.getLogger(UpdateAnalyser.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String error = "";
        try {
            Indexer.updateAnalyser();
        } catch (Exception e) {
            logger.error("update analyser error", e);
            error = Utils.responseError(e.getMessage());
        }
        resp.setContentType("application/json;charset=UTF-8");
        OutputStream outputStream = resp.getOutputStream();
        if (error.isEmpty()) outputStream.write("{\"success\":true}".getBytes(StandardCharsets.UTF_8));
        else outputStream.write(error.getBytes(StandardCharsets.UTF_8));
    }
}
