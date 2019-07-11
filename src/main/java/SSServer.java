import api.DelAllIndex;
import api.GetAllIndex;
import api.NewIndex;
import api.QueryIndex;
import api.StartIndex;
import api.StopIndex;
import api.UpdateAnalyser;
import bean.LSException;
import index.Indexer;
import org.apache.log4j.PropertyConfigurator;
import util.Constants;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import util.SqlliteUtil;
import util.Utils;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

/**
 * ssdb searcher server
 */
public class SSServer {

    private Server httpServer;

    private void stopHttpServer() {
        try {
            if (httpServer != null) httpServer.stop();
        } catch (Exception ignored) {
        }
    }

    private void startHttpServer() throws Exception {
        httpServer = new Server(Constants.httpPort);
        ServletHandler handler = new ServletHandler();
        httpServer.setHandler(handler);
        handler.addServletWithMapping(NewIndex.class, "/create");
        handler.addServletWithMapping(QueryIndex.class, "/query");
        handler.addServletWithMapping(StartIndex.class, "/start");
        handler.addServletWithMapping(StopIndex.class, "/stop");
        handler.addServletWithMapping(GetAllIndex.class, "/getAll");
        handler.addServletWithMapping(DelAllIndex.class, "/delAll");
        handler.addServletWithMapping(UpdateAnalyser.class, "/updateAnalyser");
        httpServer.start();
        Files.write(Constants.varDir.resolve("pid"), ManagementFactory.getRuntimeMXBean()
                .getName().split("@")[0].getBytes(StandardCharsets.UTF_8));
        httpServer.join();
    }

    public static void main(String[] args) {

        if (args == null || args.length < 1) {
            System.err.printf("command error...\n%s", "USAGE:*.sh start|stop");
            System.exit(1);
        }
        if (!"start".equals(args[0]) && !"stop".equals(args[0])) {
            System.err.printf("param %s is not defined\n%s", args[1], "USAGE:*.sh start|stop");
            System.exit(1);
        }
        PropertyConfigurator.configure(SSServer.class.getResourceAsStream("/log4j.properties"));
        Logger logger = Logger.getLogger(SSServer.class);
        try {
            if ("start".equals(args[0])) {
                Path pf = Constants.varDir.resolve("pid");
                if (!Files.notExists(pf, LinkOption.NOFOLLOW_LINKS)) throw new LSException("server pid is exit");
                SSServer SSServer = new SSServer();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        List<Object> indexList = SqlliteUtil.queryL("select name from schema");
                        indexList.forEach(k -> {
                            String index = String.valueOf(k);
                            if (Indexer.isRunning(index)) Indexer.stopIndex(index);
                        });
                        SqlliteUtil.close();
                    } catch (SQLException e) {
                        logger.warn("可能有索引数据未保存丢失", e);
                    }
                    SSServer.stopHttpServer();
                }));
                SSServer.startHttpServer();
            } else {
                int exit = Utils.stopPid(Constants.varDir.resolve("pid"));
                logger.info("luceneSearch stop exit:[" + exit + "]");
            }
        } catch (Exception e) {
            logger.error("启动失败", e);
            System.exit(1);
        }
        System.exit(0);
    }
}
