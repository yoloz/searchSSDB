package bean;

import index.Indexer;

import java.util.List;

public class Schema {

    private String index;
    private String analyser;
    private List<Field> fields;
    private Source source;

    public Schema() {
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getAnalyser() {
        return analyser;
    }

    public void setAnalyser(String analyser) {
        this.analyser = analyser;
    }

    public void setAnalyserKey(String analyser) throws LSException {
        this.analyser = Indexer.getAnalyserClass(analyser);
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }
}

