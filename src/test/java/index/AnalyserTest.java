package index;

import index.analyser.StandardAnalyserIgnoreCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class AnalyserTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void standAnalyserIgnoreCaseTest() {
        String text = "user:admin";
        text = "zhang张三";
        text = "Fea后台";
        try {
            Analyzer standardAnalyzer = new StandardAnalyserIgnoreCase();
            TokenStream ts = standardAnalyzer.tokenStream("", text);
            doToken(ts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void standAnalyserTest() {
        String text = "user:admin";
        text = "zhang张三";
        text = "Fea后台";
        try {
            Analyzer standardAnalyzer = new StandardAnalyzer();
            TokenStream ts = standardAnalyzer.tokenStream("", text);
            doToken(ts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void doToken(TokenStream ts) throws IOException {
        ts.reset();
        CharTermAttribute cta = ts.getAttribute(CharTermAttribute.class);
        while (ts.incrementToken()) {
            System.out.println(cta.toString());
        }
        ts.end();
        ts.close();
    }

}
