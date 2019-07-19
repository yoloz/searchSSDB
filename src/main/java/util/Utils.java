package util;

import bean.LSException;
import bean.Schema;
import com.google.common.io.CharStreams;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class Utils {

    /**
     * @param className to instance
     * @param t         The interface the class should implement
     * @return A instance of the class
     */
    public static <T> T getInstance(String className, Class<T> t) throws LSException {
        try {
            Class<?> c = Class.forName(className);
            if (c == null)
                return null;
            Object o = c.newInstance();
            if (!t.isInstance(o))
                throw new LSException(c.getName() + " is not an instance of " + t.getName());
            return t.cast(o);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new LSException(className + " 实例化失败", e);
        }
    }

    /**
     * inputStream内容较少
     *
     * @param inputStream {@link InputStream}
     * @return string {@link String}
     */
    public static String getInputStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[inputStream.available()];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(StandardCharsets.UTF_8.name());
    }

    /**
     * start index app
     *
     * @param indexName index name
     * @throws LSException error {@link LSException}
     * @throws IOException error {@link IOException}
     */
    public static void starApp(String indexName) throws LSException, IOException {
        Path jars = Constants.appDir.resolve("lib");
        if (Files.isDirectory(jars, LinkOption.NOFOLLOW_LINKS)) {
            Path log4j = Constants.appDir.resolve("conf").resolve("log4j.properties");
            if (Files.notExists(log4j, LinkOption.NOFOLLOW_LINKS)) throw new LSException(log4j + " is not exit");
            List<String> commands = new ArrayList<>(11);
            ProcessBuilder process = new ProcessBuilder();
            commands.add(Constants.appDir.resolve("bin").resolve("java").toString());
            commands.add("-Xmx1G");
            commands.add("-Xms512M");
            commands.add("-DLSDir=" + Constants.appDir);
            commands.add("-DINDEX=" + indexName);
            commands.add("-Dlog4j.configuration=file:" + log4j);
            commands.add("-cp");
            commands.add(jars.resolve("*").toString());
            commands.add("app.AppServer");
            commands.add(indexName);
//            commands.add("create_append");
            process.command(commands);
            process.redirectErrorStream(true);
            process.redirectOutput(Constants.logDir.resolve(indexName + ".out").toFile());
            process.start();
        } else throw new LSException(jars + " is not directory");
    }

    /**
     * 时间类型转换到纳秒
     * <p>
     * nano-of-second, from 0 to 999,999,999
     * ZoneOffset.UTC
     */
    public static String toNanos(LocalDateTime dateTime) {
        long second = dateTime.toEpochSecond(ZoneOffset.UTC);
        int nano = dateTime.getNano();
        StringBuilder result = new StringBuilder(Long.toString(second) + nano);
        int padding = 19 - result.length();
        for (int i = padding; i > 0; i--) {
            result.append("0");
        }
        return result.toString();
    }

    public static LocalDateTime ofNanos(String longValue) {
        String nano = "0";
        String second = longValue;
        if (longValue.length() > 10) {
            nano = longValue.substring(10);
            second = longValue.substring(0, 10);
        }
        return LocalDateTime.ofEpochSecond(Long.valueOf(second), Integer.valueOf(nano), ZoneOffset.UTC);
    }

    public static int stopPid(Path path) throws IOException, InterruptedException {
        String pid = Files.readAllLines(path, StandardCharsets.UTF_8).get(0);
        int exit = stopPid(pid);
        if (exit == 0) Files.deleteIfExists(path);
        return exit;
    }

    /**
     * stop by pid
     *
     * @param pid pid
     * @return exit code
     * @throws IOException          io error
     * @throws InterruptedException process interrupt
     */
    public static int stopPid(String pid) throws IOException, InterruptedException {
        if (pid == null || pid.isEmpty()) throw new IOException("pid[" + pid + "] is not exit");
        List<String> commands = new ArrayList<>(3);
        ProcessBuilder process = new ProcessBuilder();
        commands.add("kill");
        commands.add("-15");
        commands.add(pid);
        process.command(commands);
        Process p = process.start();
        try (InputStream inputStream = p.getErrorStream();
             InputStreamReader ir = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            String msg = CharStreams.toString(ir);
            if (msg.contains("No such process")) return 0;
        }
        return p.waitFor();
    }

    /**
     * get a logger for per index thread
     *
     * @param name  index name
     * @param level log level
     * @return Logger {@link Logger}
     */
    public static Logger getLogger(String name, Level level) {
        Logger logger = Logger.getLogger(name);
        logger.setLevel(level);
        String appenderName = name + "-thread";
        if (logger.getAppender(appenderName) == null) {
            RollingFileAppender appender = new RollingFileAppender();
            appender.setName(appenderName);
            appender.setFile(Constants.logDir.resolve(name + ".log").toString());
            appender.setLayout(new PatternLayout("[%d] %p %m (%c)%n"));
//            appender.setMaxFileSize("10240KB"); //default 10M
            appender.setMaxBackupIndex(5);
//            appender.setThreshold(level);
            appender.activateOptions();
            logger.addAppender(appender);
        }
        return logger;
    }

    public static Schema getSchema(String name) throws LSException {
        List<Map<String, Object>> list;
        try {
            list = SqlliteUtil.query("select value from schema where name=?", name);
        } catch (SQLException e) {
            throw new LSException("取索引[" + name + "]元数据错误", e);
        }
        if (list.isEmpty()) throw new LSException("索引[" + name + "]不存在");
        return new Yaml().loadAs((String) list.get(0).get("value"), Schema.class);
    }

    public static String md5(String str) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return new String(Base64.getEncoder().encode(
                    md5.digest(str.getBytes(StandardCharsets.UTF_8))),
                    StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException ignored) {
        }
        return str;
    }

    public static String trimPrefix(String value) {
        int len = value.length();
        int st = 0;
        char[] val = value.toCharArray();
        while ((st < len) && (val[st] <= ' ')) st++;
        return (st > 0) ? value.substring(st) : value;
    }

    public static String responseError(String error) {
        return "{\"success\":false,\"error\":\"" + error + "\"}";
    }
}
