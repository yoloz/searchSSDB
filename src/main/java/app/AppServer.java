package app;

import app.index.WriteIndex;
import bean.LSException;
import bean.Schema;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import util.SqlliteUtil;
import util.Utils;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 近实时查询需要search从index writer中生成,即查询和产生绑定在一起
 * 如果新建进程作为创建,则jetty需要和这个进程通信交互
 * 简便化直接在jetty中通过线程来生成索引
 */
@Deprecated
public class AppServer {

    private static final Logger logger = Logger.getLogger(AppServer.class);

    /**
     * app.AppServer.main(indexName)
     *
     * @param args {@link String[]}
     */
    public static void main(String[] args) {
//        if (args == null || args.length != 1) {
//            System.err.println("启动失败,参数[" + Arrays.toString(args) + "]错误");
//            System.exit(1);
//        }
//        logger.info(args[0] + " starting...");
//        try {
//            List<Map<String, Object>> list = SqlliteUtil
//                    .query("select value from schema where name=?", args[0]);
//            if (list.isEmpty()) throw new LSException("index[" + args[0] + "] is not exit");
//            WriteIndex writeIndex = new WriteIndex(
//                    new Yaml().loadAs((String) list.get(0).get("value"), Schema.class));
//            Runtime.getRuntime().addShutdownHook(new Thread(writeIndex::close));
//            Utils.updateAppStatus(ManagementFactory.getRuntimeMXBean().getName().split("@")[0], args[0]);
//            writeIndex.write();
//            Utils.updateAppStatus("0", args[0]);
//        } catch (Exception e) {
//            logger.error(e.getCause() == null ? e.getMessage() : e.getCause());
//            System.err.println(e.getMessage());
//            try {
//                Utils.updateAppStatus("0", args[0]);
//            } catch (SQLException e1) {
//                logger.error(e1);
//            }
//            System.exit(1);
//        }
//        logger.info(args[0] + " finished...");
    }


}
