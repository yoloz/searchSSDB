package parser;

import bean.Field;
import bean.Pair;
import bean.Triple;
import bean.LSException;
import bean.Schema;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.log4j.Logger;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.yaml.snakeyaml.Yaml;
import util.SqlliteUtil;
import util.Utils;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 大于,大于等于,小于,小于等于:
 * 1,要求为数值或者时间值；
 * 2,处理后会有上边界或下边界(+[-]{boundary}),这样在提交lucene搜索返回的total不准确；
 * <p>
 * 查询语句中字符值需加单引号
 * <p>
 *
 * <p>
 * select col1,test.col2 from test
 * <p>
 * select * from test where (col3='test' and col1 like 'a?b') or
 * (col2>11 or col4 between 1 and 4) and (col4<=5.3)
 * <p>
 * select * from test where date between '2019-02-28T09:43:10.224000' and '2019-02-28T09:43:10.225000'
 * <p>
 * select * from test limit 10
 */
@Deprecated
public class QuerySql {

    private final Logger logger = Logger.getLogger(QuerySql.class);

    private final String sql;

    private final int boundary = 10000; //添加上下边界

    private Map<String, Pair<Field.Type, String>> columnMap = new HashMap<>(5);

    public QuerySql(String sql) {
        this.sql = sql;
    }

    /**
     * @return <selects,limit>, query, schema
     * @throws LSException error
     */
    public Triple<Pair<List<String>, Pair<Integer, Integer>>, Query, Schema> parseToQuery()
            throws LSException {
        try {
            Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
            SelectBody selectBody = select.getSelectBody();
            if (!PlainSelect.class.equals(selectBody.getClass()))
                throw new LSException(selectBody.getClass() + " is not support");
            PlainSelect ps = (PlainSelect) selectBody;
            String indexName = parseTableName(ps.getFromItem());
            List<String> selects = parseSelectItem(ps.getSelectItems());
            Pair<Integer, Integer> limit = this.parseLimit(ps.getLimit());
            List<Map<String, Object>> list = SqlliteUtil.getInstance().query("select value from schema where name=?",
                    indexName);
            if (list.isEmpty()) throw new LSException("索引[" + indexName + "]不存在");
            Schema schema = new Yaml().loadAs((String) list.get(0).get("value"), Schema.class);
            schema.getFields().forEach(f ->
                    columnMap.put(f.getName(), Pair.of(f.getType(), f.getFormatter())));
            Query query = this.parseWhere(ps.getWhere());
            if (query == null) for (Field f : schema.getFields()) {
                if (Field.Type.STRING == f.getType()) {
                    query = new WildcardQuery(new Term(f.getName(), "*"));
                    break;
                }
            }
            logger.debug("parsed query[" + (query == null ? "null" : query) + "]");
            return Triple.of(Pair.of(selects, limit), query, schema);
        } catch (JSQLParserException | SQLException e) {
            throw new LSException("parse[" + sql + "] error", e);
        }
    }

    /**
     * @return <selects,limit>, queryString, schema
     * @throws LSException error
     */
    @Deprecated
    public Triple<Pair<List<String>, Pair<Integer, Integer>>, String, Schema> parseToString()
            throws LSException {
        try {
            Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
            SelectBody selectBody = select.getSelectBody();
            if (!PlainSelect.class.equals(selectBody.getClass()))
                throw new LSException(selectBody.getClass() + " is not support");
            PlainSelect ps = (PlainSelect) selectBody;
            String indexName = parseTableName(ps.getFromItem());
            List<String> selects = parseSelectItem(ps.getSelectItems());
            Pair<Integer, Integer> limit = this.parseLimit(ps.getLimit());
            List<Map<String, Object>> list = SqlliteUtil.getInstance().query("select value from schema where name=?",
                    indexName);
            if (list.isEmpty()) throw new LSException("索引[" + indexName + "]不存在");
            Schema schema = new Yaml().loadAs((String) list.get(0).get("value"), Schema.class);
            schema.getFields().forEach(f ->
                    columnMap.put(f.getName(), Pair.of(f.getType(), f.getFormatter())));
            StringBuilder queryBuilder = new StringBuilder();
            this.parseWhere(queryBuilder, ps.getWhere());
            if (queryBuilder.toString().isEmpty()) for (Field f : schema.getFields()) {
                if (Field.Type.STRING == f.getType()) {
                    queryBuilder.append(f.getName()).append(":").append("*");
                    break;
                }
            }
            logger.debug("parsed query[" + queryBuilder.toString() + "]");
            return Triple.of(Pair.of(selects, limit), queryBuilder.toString(), schema);
        } catch (JSQLParserException | SQLException e) {
            throw new LSException("parse[" + sql + "] error", e);
        }
    }

    private String parseTableName(FromItem from) throws LSException {
        if (!Table.class.equals(from.getClass())) throw new LSException(from.getClass() + " is not support");
        Table table = (Table) from;
        return table.getName();
    }

    private List<String> parseSelectItem(List<SelectItem> selects) {
        List<String> list = new ArrayList<>(selects.size());
        if (selects.size() == 1 && selects.get(0).getClass().equals(AllColumns.class))
            list.add(selects.get(0).toString());
        else for (SelectItem item : selects) {
            SelectExpressionItem selectItem = (SelectExpressionItem) item;
            Column col = (Column) selectItem.getExpression();
            if (col.getTable() != null) logger.warn("discard [" + col.getTable().getName() +
                    "] of[" + col.getTable().getName() + "." + col.getColumnName() + "]");
            list.add(col.getColumnName());
        }
        return list;
    }

    /**
     * 添加分页支持
     * <p>
     * limit offset,count
     * offset第几页
     * <p>
     * 从offset*count条开始取数据,取count条
     * <p>
     * 默认rowCount=15
     *
     * @param limit limit
     * @return <start,end> start会转换成offset*count
     * @throws LSException error
     */
    private Pair<Integer, Integer> parseLimit(Limit limit) throws LSException {
        int _rowCount = 0, _offset = 0;
        if (limit != null) {
            Expression offset = limit.getOffset();
            Expression rowCount = limit.getRowCount();
            if (offset != null) {
                if (!LongValue.class.equals(offset.getClass()))
                    throw new LSException("limit offset type[" + rowCount.getClass() + "]not support");
                _offset = ((LongValue) offset).getBigIntegerValue().intValueExact();
            }
            if (rowCount != null) {
                if (!LongValue.class.equals(rowCount.getClass()))
                    throw new LSException("limit rowCount type[" + rowCount.getClass() + "]not support");

                _rowCount = ((LongValue) rowCount).getBigIntegerValue().intValueExact();
            }
        }
        if (_rowCount == 0) _rowCount = 15;
        return Pair.of(_offset * _rowCount, _rowCount);
    }

    /**
     * 遍历常见的expression
     *
     * @param where {@link Expression}
     * @throws LSException error or not support
     */
    private Query parseWhere(Expression where) throws LSException {
        if (where == null) return null;
        if (Parenthesis.class.equals(where.getClass())) {
            Parenthesis parenthesis = (Parenthesis) where;
            Expression pe = parenthesis.getExpression();
            return parseWhere(pe);
        } else if (AndExpression.class.equals(where.getClass())) {
            BooleanQuery.Builder ab = new BooleanQuery.Builder();
            AndExpression and = (AndExpression) where;
            Expression left = and.getLeftExpression();
            ab.add(parseWhere(left), BooleanClause.Occur.MUST);
            Expression right = and.getRightExpression();
            ab.add(parseWhere(right), BooleanClause.Occur.MUST);
            return ab.build();
        } else if (OrExpression.class.equals(where.getClass())) {
            BooleanQuery.Builder ob = new BooleanQuery.Builder();
            OrExpression or = (OrExpression) where;
            Expression left = or.getLeftExpression();
            ob.add(parseWhere(left), BooleanClause.Occur.SHOULD);
            Expression right = or.getRightExpression();
            ob.add(parseWhere(right), BooleanClause.Occur.SHOULD);
            return ob.build();
        } else if (EqualsTo.class.equals(where.getClass())) {
            EqualsTo equal = (EqualsTo) where;
            String operator = equal.getStringExpression();
            Expression left = equal.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = equal.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), false);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == columnMap.get(ln).getLeft())
                    return IntPoint.newExactQuery(ln, Integer.valueOf(String.valueOf(or)));
                else return LongPoint.newExactQuery(ln, (long) or);
            } else if (or.getClass().equals(Double.class)) return DoublePoint.newExactQuery(ln, (double) or);
            else return new TermQuery(new Term(ln, String.valueOf(or)));
        } else if (GreaterThan.class.equals(where.getClass())) {
            GreaterThan greater = (GreaterThan) where;
            String operator = greater.getStringExpression();
            Expression left = greater.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = greater.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                or = (long) or + 1;
                if (Field.Type.INT == columnMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(or)),
                            Integer.valueOf(String.valueOf(addNum(or))));
                else return LongPoint.newRangeQuery(ln, (long) or, (long) addNum(or));
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, ((double) or + 0.1), (double) addNum(or));
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (GreaterThanEquals.class.equals(where.getClass())) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) where;
            String operator = greaterThanEquals.getStringExpression();
            Expression left = greaterThanEquals.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = greaterThanEquals.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == columnMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(or)),
                            Integer.valueOf(String.valueOf(addNum(or))));
                else return LongPoint.newRangeQuery(ln, (long) or, (long) addNum(or));
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, (double) or, (double) addNum(or));
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (MinorThan.class.equals(where.getClass())) {
            MinorThan minorThan = (MinorThan) where;
            String operator = minorThan.getStringExpression();
            Expression left = minorThan.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = minorThan.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                or = (long) or - 1;
                if ((long) or < 0) or = 0;
                if (Field.Type.INT == columnMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(minusNum(or))),
                            Integer.valueOf(String.valueOf(or)));
                else return LongPoint.newRangeQuery(ln, (long) minusNum(or), ((long) or));
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, (double) minusNum(or), ((double) or - 0.1));
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (MinorThanEquals.class.equals(where.getClass())) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) where;
            String operator = minorThanEquals.getStringExpression();
            Expression left = minorThanEquals.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = minorThanEquals.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == columnMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(minusNum(or))),
                            Integer.valueOf(String.valueOf(or)));
                else return LongPoint.newRangeQuery(ln, (long) minusNum(or), (long) or);
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, (double) minusNum(or), (double) or);
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (Between.class.equals(where.getClass())) {
            Between between = (Between) where;
            if (between.isNot()) throw new LSException(" not between is not support");
            String operator = "between";
            Expression left = between.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression bstart = between.getBetweenExpressionStart();
            Object start = getItem(operator, bstart);
            if (Column.class.equals(start.getClass()))
                throw new LSException("operator[between] start is column");
            start = checkValue(operator, ln, start, false);
            Expression bend = between.getBetweenExpressionEnd();
            Object end = getItem(operator, bend);
            if (Column.class.equals(end.getClass()))
                throw new LSException("operator[between] end is column");
            end = checkValue(operator, ln, end, false);
            if (start.getClass().equals(Long.class)) {
                if (Field.Type.INT == columnMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(start)),
                            Integer.valueOf(String.valueOf(end)));
                else return LongPoint.newRangeQuery(ln, (long) start, (long) end);
            } else if (start.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, (double) start, (double) end);
            else throw new LSException(start.getClass() + " between " + end.getClass() + " is not support");
        } else if (LikeExpression.class.equals(where.getClass())) {
            LikeExpression like = (LikeExpression) where;
            String operator = "like";
            Expression left = like.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = like.getRightExpression();
            Object or = getItem(operator, right);
            if (Column.class.equals(or.getClass()))
                throw new LSException("operator[like] right is column");
            return new WildcardQuery(new Term(ln, String.valueOf(or)));
        } else throw new LSException(where.getClass() + " is not support");
    }

    /**
     * 遍历常见的expression
     *
     * @param builder result
     * @param where   {@link Expression}
     * @throws LSException error or not support
     */
    private void parseWhere(StringBuilder builder, Expression where) throws LSException {
        if (where == null) return;
        if (Parenthesis.class.equals(where.getClass())) {
            builder.append("(");
            Parenthesis parenthesis = (Parenthesis) where;
            Expression pe = parenthesis.getExpression();
            parseWhere(builder, pe);
            builder.append(")");
        } else if (AndExpression.class.equals(where.getClass())) {
            AndExpression and = (AndExpression) where;
            Expression left = and.getLeftExpression();
            parseWhere(builder, left);
            if (builder.length() != 0) builder.append(" AND ");
            Expression right = and.getRightExpression();
            parseWhere(builder, right);
        } else if (OrExpression.class.equals(where.getClass())) {
            OrExpression or = (OrExpression) where;
            Expression left = or.getLeftExpression();
            parseWhere(builder, left);
            if (builder.length() != 0) builder.append(" OR ");
            Expression right = or.getRightExpression();
            parseWhere(builder, right);
        } else if (EqualsTo.class.equals(where.getClass())) {
            EqualsTo equal = (EqualsTo) where;
            String operator = equal.getStringExpression();
            Expression left = equal.getLeftExpression();
            String ln = getLeft(operator, left, builder);
            Expression right = equal.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), false);
            builder.append(":").append(or);
        } else if (GreaterThan.class.equals(where.getClass())) {
            GreaterThan greater = (GreaterThan) where;
            String operator = greater.getStringExpression();
            Expression left = greater.getLeftExpression();
            String ln = getLeft(operator, left, builder);
            Expression right = greater.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            builder.append(":{").append(or).append(" TO ").append(addNum(or)).append("]");
        } else if (GreaterThanEquals.class.equals(where.getClass())) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) where;
            String operator = greaterThanEquals.getStringExpression();
            Expression left = greaterThanEquals.getLeftExpression();
            String ln = getLeft(operator, left, builder);
            Expression right = greaterThanEquals.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            builder.append(":[").append(or).append(" TO ").append(addNum(or)).append("]");
        } else if (MinorThan.class.equals(where.getClass())) {
            MinorThan minorThan = (MinorThan) where;
            String operator = minorThan.getStringExpression();
            Expression left = minorThan.getLeftExpression();
            String ln = getLeft(operator, left, builder);
            Expression right = minorThan.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            builder.append(":[").append(minusNum(or)).append(" TO ").append(or).append("}");
        } else if (MinorThanEquals.class.equals(where.getClass())) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) where;
            String operator = minorThanEquals.getStringExpression();
            Expression left = minorThanEquals.getLeftExpression();
            String ln = getLeft(operator, left, builder);
            Expression right = minorThanEquals.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            builder.append(":[").append(minusNum(or)).append(" TO ").append(or).append("]");
        } else if (Between.class.equals(where.getClass())) {
            Between between = (Between) where;
            if (between.isNot()) throw new LSException(" not between is not support");
            String operator = "between";
            Expression left = between.getLeftExpression();
            String ln = getLeft(operator, left, builder);
            Expression bstart = between.getBetweenExpressionStart();
            Object start = getItem(operator, bstart);
            if (Column.class.equals(start.getClass()))
                throw new LSException("operator[between] start is column");
            start = checkValue(operator, ln, start, false);
            Expression bend = between.getBetweenExpressionEnd();
            Object end = getItem(operator, bend);
            if (Column.class.equals(end.getClass()))
                throw new LSException("operator[between] end is column");
            end = checkValue(operator, ln, end, false);
            builder.append(":[").append(start).append(" TO ").append(end).append("]");
        } else if (LikeExpression.class.equals(where.getClass())) {
            LikeExpression like = (LikeExpression) where;
            String operator = "like";
            Expression left = like.getLeftExpression();
            getLeft(operator, left, builder);
            Expression right = like.getRightExpression();
            Object or = getItem(operator, right);
            if (Column.class.equals(or.getClass()))
                throw new LSException("operator[like] right is column");
            builder.append(":").append(or);
        } else throw new LSException(where.getClass() + " is not support");
    }

    private String getLeft(String operator, Expression left) throws LSException {
        Object ol = getItem(operator, left);
        if (!ol.getClass().equals(Column.class))
            throw new LSException("operator[" + operator + "] left is not column");
        Column col = (Column) ol;
        if (col.getTable() != null) System.out.println("discard [" +
                col.getTable().getName() + "] of[" + col.getTable().getName() + "." +
                col.getColumnName() + "]");
        return col.getColumnName();
    }

    private String getLeft(String operator, Expression left, StringBuilder builder)
            throws LSException {
        Object ol = getItem(operator, left);
        if (!ol.getClass().equals(Column.class))
            throw new LSException("operator[" + operator + "] left is not column");
        Column col = (Column) ol;
        if (col.getTable() != null) System.out.println("discard [" +
                col.getTable().getName() + "] of[" + col.getTable().getName() + "." +
                col.getColumnName() + "]");
        builder.append(col.getColumnName());
        return col.getColumnName();
    }

    /**
     * 时间字符转换为millis
     * 通过num判定是否为数字值
     *
     * @param operator <,>,like,between...
     * @param leftName column name
     * @param or       object right
     * @param num      if not date string that must number
     * @return Object
     * @throws LSException error
     */
    private Object checkValue(String operator, String leftName, Object or, boolean num)
            throws LSException {
        if (Column.class.equals(or.getClass()))
            throw new LSException("operator[" + operator + "] right is column");
        if (!(or instanceof Number) && columnMap.containsKey(leftName) &&
                Field.Type.DATE == columnMap.get(leftName).getLeft()) {
            logger.debug(leftName + " parse [" + or + "] by [" + columnMap.get(leftName).getRight() + "]");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(columnMap.get(leftName).getRight());
            return Long.valueOf(Utils.toNanos(LocalDateTime.parse(String.valueOf(or), formatter)));
        } else if (num && !(or instanceof Number))
            throw new LSException("operator[" + operator + "] right is not number");
        else return or;
    }

    private Object addNum(Object object) throws LSException {
        Class clazz = object.getClass();
        if (Long.class.equals(clazz)) {
            return ((long) object) + boundary;
        } else if (Double.class.equals(clazz)) {
            BigDecimal b1 = BigDecimal.valueOf((double) object);
            BigDecimal b2 = new BigDecimal(String.valueOf(boundary));
            return b1.add(b2);
        } else throw new LSException(clazz + " 加法暂未实现");
    }

    private Object minusNum(Object object) throws LSException {
        Class clazz = object.getClass();
        if (Long.class.equals(clazz)) {
            long l1 = (long) object;
            if (l1 <= boundary) return 0;
            else return l1 - boundary;
        } else if (Double.class.equals(clazz)) {
            double d1 = (double) object;
            if (d1 <= boundary) return 0.0;
            else {
                BigDecimal b1 = BigDecimal.valueOf((double) object);
                BigDecimal b2 = new BigDecimal(String.valueOf(boundary));
                return b1.subtract(b2);
            }
        } else throw new LSException(clazz + " 减法暂未实现");
    }

    private Object getItem(String operator, Expression item)
            throws LSException {
        Class clazz = item.getClass();
        Object obj;
        if (Column.class.equals(clazz)) {
            obj = item;
        } else if (StringValue.class.equals(clazz)) {
            obj = ((StringValue) item).getNotExcapedValue();
        } else if (LongValue.class.equals(clazz)) {
            obj = ((LongValue) item).getBigIntegerValue().longValueExact();
        } else if (DoubleValue.class.equals(clazz)) {
            obj = ((DoubleValue) item).getValue();
        } else throw new LSException(operator + " left/right [" + clazz + "] is not support");
        return obj;
    }
}
