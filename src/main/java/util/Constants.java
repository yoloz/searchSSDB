package util;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class Constants {

    private static final Logger logger = Logger.getLogger(Constants.class);

    static final Path appDir;
    public static final Path confDir;
    public static final Path indexDir;
    public static final Path logDir;
    public static final Path varDir;
    public static final int httpPort;
    public static final String hostName;
    public static final int totalIndex;
    public static final double RAMBuffer;
    public static final int perDayHour;
    public static final int searchExpired;

    public static final String logLevel;

    static {
        logLevel = System.getProperty("LogLevel");
        String root_dir = System.getProperty("LSDir");
        appDir = Paths.get(root_dir);
        confDir = appDir.resolve("conf");
        logDir = appDir.resolve("logs");
        varDir = appDir.resolve("var");
        Properties properties = new Properties();
        try {
            try (InputStream inputStream = Files.newInputStream(confDir.resolve("server.properties"))) {
                properties.load(inputStream);
            }
        } catch (Exception e) {
            logger.error("load file[" + confDir.resolve("server.properties") + "] error", e);
            System.exit(1);
        }
        String _indexDir = properties.getProperty("indexDir");
        if (_indexDir == null || _indexDir.isEmpty() || ".var".equals(_indexDir))
            indexDir = varDir.resolve("index");
        else indexDir = Paths.get(_indexDir);
        httpPort = Integer.parseInt(properties.getProperty("httpPort"));
        hostName = properties.getProperty("hostName", "127.0.0.1");
        totalIndex = Integer.parseInt(properties.getProperty("totalIndex", "6"));
        RAMBuffer = Double.parseDouble(properties.getProperty("indexBuffer", "128"));
        perDayHour = Integer.parseInt(properties.getProperty("perDayHour", "2"));
        searchExpired = Integer.parseInt(properties.getProperty("searchExpired", "0"));
    }
}
