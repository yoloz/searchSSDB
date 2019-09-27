package api;

import bean.LSException;
import bean.Schema;
import index.Indexer;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import util.Constants;
import util.SqlliteUtil;
import util.Utils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;

public class DelAllIndex extends HttpServlet {

    private final Logger logger = Logger.getLogger(DelAllIndex.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String indexName = Utils.getInputStream(req.getInputStream());
        logger.debug("delete all index=>[" + indexName + "]");
        String error = "";
        try {
            if (Indexer.isRunning(indexName)) throw new LSException("index[" + indexName + "] is running");
            logger.debug("delete all index data...");
            Schema schema = Utils.getSchema(indexName);
            Analyzer analyzer = Utils.getInstance(schema.getAnalyser(), Analyzer.class);
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            try (IndexWriter indexWriter = new IndexWriter(
                    FSDirectory.open(Constants.indexDir.resolve(indexName)),
                    iwc)) {
                indexWriter.deleteAll();
                indexWriter.commit();
            }
            Files.walkFileTree(Constants.indexDir.resolve(indexName), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Constants.logDir, indexName + ".*")) {
                for (Path p : stream) if (Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) Files.delete(p);
            }
            logger.debug("delete all db data...");
            SqlliteUtil.getInstance().transaction("delete from schema where name=?", new String[]{indexName},
                    "delete from point where iname=?", new String[]{indexName});
        } catch (Exception e) {
            logger.error("delete all index[" + indexName + "] error,", e);
            error = Utils.responseError(e.getMessage());
        }
        resp.setContentType("application/json;charset=UTF-8");
        OutputStream outputStream = resp.getOutputStream();
        if (error.isEmpty())
            outputStream.write("{\"success\":true}".getBytes(StandardCharsets.UTF_8));
        else outputStream.write(error.getBytes(StandardCharsets.UTF_8));
    }
}
