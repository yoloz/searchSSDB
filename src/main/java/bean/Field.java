package bean;

/**
 * STRING:
 * <p>
 * Use a field that is indexed (i.e. searchable), but don't tokenize
 * the field into separate words and don't index term frequency
 * or positional information.
 * <p>
 * LONG,DATE:
 * <p>
 * Use a LongPoint that is indexed (i.e. efficiently filterable with
 * PointRangeQuery).  This indexes to milli-second resolution, which
 * is often too fine.  You could instead create a number based on
 * year/month/day/hour/minutes/seconds, down the resolution you require.
 * For example the long value 2011021714 would mean
 * February 17, 2011, 2-3 PM.
 * <p>
 * TEXT:
 * The text is tokenized and indexed.
 */
public class Field {

    public enum Type {
        INT, LONG, STRING, TEXT, DATE
    }

    private String name;
    private Type type;
    private String formatter; //时间值格式

    public Field() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setType(String type) throws LSException {
        type = type.toLowerCase();
        switch (type) {
            case "int":
                this.type = Type.INT;
                break;
            case "long":
                this.type = Type.LONG;
                break;
            case "string":
                this.type = Type.STRING;
                break;
            case "text":
                this.type = Type.TEXT;
                break;
            case "date":
                this.type = Type.DATE;
                break;
            default:
                throw new LSException("字段类型[" + type + "]暂不支持");
        }
    }

    public String getFormatter() {
        return formatter;
    }

    public void setFormatter(String formatter) {
        this.formatter = formatter;
    }
}
