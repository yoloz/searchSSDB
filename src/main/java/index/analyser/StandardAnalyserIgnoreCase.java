package index.analyser;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;


/**
 * Filters {@link StandardTokenizer} with{@link StopFilter},
 * using a configurable list of stop words.
 * <p>
 * modify by {@link org.apache.lucene.analysis.standard.StandardAnalyzer}
 */
public class StandardAnalyserIgnoreCase extends StopwordAnalyzerBase {
    /**
     * An unmodifiable set containing some common English words that are not
     * usually useful for searching.
     *
     * @deprecated Use the stop words on EnglishAnalyzer in the analysis-common module
     */
    @Deprecated
    public static final CharArraySet ENGLISH_STOP_WORDS_SET;

    static {
        final List<String> stopWords = Arrays.asList(
                "a", "an", "and", "are", "as", "at", "be", "but", "by",
                "for", "if", "in", "into", "is", "it",
                "no", "not", "of", "on", "or", "such",
                "that", "the", "their", "then", "there", "these",
                "they", "this", "to", "was", "will", "with"
        );
        final CharArraySet stopSet = new CharArraySet(stopWords, false);
        ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet);
    }


    /**
     * Default maximum allowed token length
     */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

    private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

    /**
     * An unmodifiable set containing some common English words that are usually not
     * useful for searching.
     *
     * @deprecated Use the stop words on EnglishAnalyzer in the analysis-common module
     */
    @Deprecated
    public static final CharArraySet STOP_WORDS_SET = ENGLISH_STOP_WORDS_SET;

    /**
     * Builds an analyzer with the given stop words.
     *
     * @param stopWords stop words
     */
    public StandardAnalyserIgnoreCase(CharArraySet stopWords) {
        super(stopWords);
    }

    /**
     * Builds an analyzer with the default stop words ({@link #STOP_WORDS_SET}).
     */
    public StandardAnalyserIgnoreCase() {
        this(STOP_WORDS_SET);
    }

    /**
     * Builds an analyzer with the stop words from the given reader.
     *
     * @param stopwords Reader to read stop words from
     * @see WordlistLoader#getWordSet(Reader)
     */
    public StandardAnalyserIgnoreCase(Reader stopwords) throws IOException {
        this(loadStopwordSet(stopwords));
    }

    /**
     * Set the max allowed token length.  Tokens larger than this will be chopped
     * up at this token length and emitted as multiple tokens.  If you need to
     * skip such large tokens, you could increase this max length, and then
     * use {@code LengthFilter} to remove long tokens.  The default is
     * {@link StandardAnalyserIgnoreCase#DEFAULT_MAX_TOKEN_LENGTH}.
     */
    public void setMaxTokenLength(int length) {
        maxTokenLength = length;
    }

    /**
     * Returns the current maximum token length
     *
     * @see #setMaxTokenLength
     */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final StandardTokenizer src = new StandardTokenizer();
        src.setMaxTokenLength(maxTokenLength);
        TokenStream tok = new StopFilter(src, stopwords);
        return new TokenStreamComponents(src, tok) {
            @Override
            protected void setReader(final Reader reader) {
                // So that if maxTokenLength was changed, the change takes
                // effect next time tokenStream is called:
                src.setMaxTokenLength(StandardAnalyserIgnoreCase.this.maxTokenLength);
                super.setReader(reader);
            }
        };
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return in;
    }
}
