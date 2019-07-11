package index.parse;

import bean.Field;
import bean.Pair;
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
import net.sf.jsqlparser.statement.select.OrderByElement;
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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import util.Utils;

import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 大于,大于等于,小于,小于等于:
 * 1,要求为数值或者时间值；
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
public class SelectSql {

    private final Logger logger = Logger.getLogger(SelectSql.class);


    //    private final int boundary = 10000; //添加上下边界
    private final PlainSelect selectBody;
    private final Schema schema;
    private final String indexName;

    private final Map<String, Pair<Field.Type, String>> colMap = new HashMap<>(5);

    public SelectSql(String sql) throws LSException {
        Select select;
        try {
            select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        } catch (JSQLParserException e) {
            throw new LSException("parse sql[" + sql + "] fail[" + e.getMessage() + "]", e);
        }
        SelectBody sb = select.getSelectBody();
        if (!PlainSelect.class.equals(sb.getClass()))
            throw new LSException(sb.getClass() + " is not support");
        this.selectBody = (PlainSelect) sb;
        FromItem from = selectBody.getFromItem();
        if (!Table.class.equals(from.getClass())) throw new LSException(from.getClass() + " is not support");
        Table table = (Table) from;
        this.indexName = table.getName();
        this.schema = Utils.getSchema(indexName);
        schema.getFields().forEach(f ->
                colMap.put(f.getName(), Pair.of(f.getType(), f.getFormatter())));
    }

    public Schema getSchema() {
        return this.schema;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public List<String> getSelects() {
        List<SelectItem> selects = selectBody.getSelectItems();
        List<String> cols = new ArrayList<>(selects.size());
        if (selects.size() == 1 && selects.get(0).getClass().equals(AllColumns.class))
            cols.addAll(colMap.keySet());
        else for (SelectItem item : selects) {
            SelectExpressionItem selectItem = (SelectExpressionItem) item;
            Column col = (Column) selectItem.getExpression();
            if (col.getTable() != null) logger.warn("discard [" + col.getTable().getName() +
                    "] of[" + col.getTable().getName() + "." + col.getColumnName() + "]");
            cols.add(col.getColumnName());
        }
        return cols;
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
     * <p>
     *
     * @return <offset,rowCount>
     * @throws LSException error
     */
    public Pair<Integer, Integer> getLimit() throws LSException {
        Limit _limit = selectBody.getLimit();
        int _rowCount = 15, _offset = 1;
        if (_limit != null) {
            Expression offset = _limit.getOffset();
            Expression rowCount = _limit.getRowCount();
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
//        if (_offset > 0) _offset -= 1;
//        return Pair.of(_offset * _rowCount, _rowCount);
        return Pair.of(_offset, _rowCount);
    }

    public Query getQuery() throws LSException {
        Query query = null;
        if (selectBody.getWhere() != null) query = this.queryImpl(selectBody.getWhere());
        if (query == null) query = new MatchAllDocsQuery();
        return query;
    }

    public Sort getOrder() throws LSException {
        List<OrderByElement> orders = selectBody.getOrderByElements();
        SortField[] sortFields = null;
        if (orders != null && !orders.isEmpty()) {
            sortFields = new SortField[orders.size()];
            for (int i = 0; i < orders.size(); i++) {
                OrderByElement order = orders.get(i);
                Expression expression = order.getExpression();
                if (!expression.getClass().equals(Column.class))
                    throw new LSException("column not support[" + expression.getClass() + "] order");
                String col = ((Column) expression).getColumnName();
                if (!colMap.containsKey(col)) throw new LSException("column name[" + col + "] is not defined");
                boolean desc = !order.isAsc();
                switch (colMap.get(col).getLeft()) {
                    case INT:
                        sortFields[i] = new SortField(col, SortField.Type.INT, desc);
                        break;
                    case DATE:
                    case LONG:
                        sortFields[i] = new SortField(col, SortField.Type.LONG, desc);
                        break;
                    case STRING:
                        sortFields[i] = new SortField(col, SortField.Type.STRING, desc);
                        break;
                    default:
                        throw new LSException("column[" + col + "] is not support to order");
                }
            }
        }
        if (sortFields != null) return new Sort(sortFields);
        return null;
    }

    public String getGroup() throws LSException {
        List<Expression> groups = selectBody.getGroupByColumnReferences();
        if (groups != null && !groups.isEmpty()) {
            if (groups.size() > 1) throw new LSException("only support one group field");
            Expression expression = groups.get(0);
            if (!expression.getClass().equals(Column.class))
                throw new LSException("column not support[" + expression.getClass() + "] group");
            String col = ((Column) expression).getColumnName();
            if (!colMap.containsKey(col)) throw new LSException("column name[" + col + "] is not defined");
            switch (colMap.get(col).getLeft()) {
                case INT:
                case DATE:
                case LONG:
                case STRING:
                    return col;
                default:
                    throw new LSException("column[" + col + "] is not support to order");
            }
        }
        return null;
    }

    /**
     * 遍历常见的expression
     *
     * @throws LSException error or not support
     */
    private Query queryImpl(Expression where) throws LSException {
        if (Parenthesis.class.equals(where.getClass())) {
            Parenthesis parenthesis = (Parenthesis) where;
            Expression pe = parenthesis.getExpression();
            return queryImpl(pe);
        } else if (AndExpression.class.equals(where.getClass())) {
            BooleanQuery.Builder ab = new BooleanQuery.Builder();
            AndExpression and = (AndExpression) where;
            Expression left = and.getLeftExpression();
            ab.add(queryImpl(left), BooleanClause.Occur.MUST);
            Expression right = and.getRightExpression();
            ab.add(queryImpl(right), BooleanClause.Occur.MUST);
            return ab.build();
        } else if (OrExpression.class.equals(where.getClass())) {
            BooleanQuery.Builder ob = new BooleanQuery.Builder();
            OrExpression or = (OrExpression) where;
            Expression left = or.getLeftExpression();
            ob.add(queryImpl(left), BooleanClause.Occur.SHOULD);
            Expression right = or.getRightExpression();
            ob.add(queryImpl(right), BooleanClause.Occur.SHOULD);
            return ob.build();
        } else if (EqualsTo.class.equals(where.getClass())) {
            EqualsTo equal = (EqualsTo) where;
            String operator = equal.getStringExpression();
            Expression left = equal.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = equal.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), false);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == colMap.get(ln).getLeft())
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
                if (Field.Type.INT == colMap.get(ln).getLeft()) {
                    int _or = Integer.parseInt(String.valueOf(or));
                    return IntPoint.newRangeQuery(ln, Math.addExact(_or, 1), Integer.MAX_VALUE);
                } else return LongPoint.newRangeQuery(ln, Math.addExact((long) or, 1), Long.MAX_VALUE);
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, DoublePoint.nextUp((double) or), Double.POSITIVE_INFINITY);
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (GreaterThanEquals.class.equals(where.getClass())) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) where;
            String operator = greaterThanEquals.getStringExpression();
            Expression left = greaterThanEquals.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = greaterThanEquals.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == colMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(or)),
                            Integer.MAX_VALUE);
                else return LongPoint.newRangeQuery(ln, (long) or, Long.MAX_VALUE);
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, (double) or, Double.POSITIVE_INFINITY);
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (MinorThan.class.equals(where.getClass())) {
            MinorThan minorThan = (MinorThan) where;
            String operator = minorThan.getStringExpression();
            Expression left = minorThan.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = minorThan.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == colMap.get(ln).getLeft()) {
                    int _or = Integer.parseInt(String.valueOf(or));
                    return IntPoint.newRangeQuery(ln,
                            Integer.MIN_VALUE,
                            Math.addExact(_or, -1));
                } else return LongPoint.newRangeQuery(ln, Long.MIN_VALUE, Math.addExact((long) or, -1));
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, Double.NEGATIVE_INFINITY, DoublePoint.nextDown((double) or));
            else throw new LSException(or.getClass() + " [" + operator + "] is not support");
        } else if (MinorThanEquals.class.equals(where.getClass())) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) where;
            String operator = minorThanEquals.getStringExpression();
            Expression left = minorThanEquals.getLeftExpression();
            String ln = getLeft(operator, left);
            Expression right = minorThanEquals.getRightExpression();
            Object or = checkValue(operator, ln, getItem(operator, right), true);
            if (or.getClass().equals(Long.class)) {
                if (Field.Type.INT == colMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.MIN_VALUE,
                            Integer.valueOf(String.valueOf(or)));
                else return LongPoint.newRangeQuery(ln, Long.MIN_VALUE, (long) or);
            } else if (or.getClass().equals(Double.class))
                return DoublePoint.newRangeQuery(ln, Double.NEGATIVE_INFINITY, (double) or);
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
                if ((long) start > (long) end) throw new LSException("起始值大于结束值");
                if (Field.Type.INT == colMap.get(ln).getLeft())
                    return IntPoint.newRangeQuery(ln,
                            Integer.valueOf(String.valueOf(start)),
                            Integer.valueOf(String.valueOf(end)));
                else return LongPoint.newRangeQuery(ln, (long) start, (long) end);
            } else if (start.getClass().equals(Double.class)) {
                if ((double) start > (double) end) throw new LSException("起始值大于结束值");
                return DoublePoint.newRangeQuery(ln, (double) start, (double) end);
            } else throw new LSException(start.getClass() + " between " + end.getClass() + " is not support");
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
        if (!(or instanceof Number) && colMap.containsKey(leftName) &&
                Field.Type.DATE == colMap.get(leftName).getLeft()) {
            logger.debug(leftName + " parse [" + or + "] by [" + colMap.get(leftName).getRight() + "]");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(colMap.get(leftName).getRight());
            return Long.valueOf(Utils.toNanos(LocalDateTime.parse(String.valueOf(or), formatter)));
        } else if (num && !(or instanceof Number))
            throw new LSException("operator[" + operator + "] right is not number");
        else return or;
    }

//    private Object addNum(Object object) throws LSException {
//        Class clazz = object.getClass();
//        if (Long.class.equals(clazz)) {
//            return ((long) object) + boundary;
//        } else if (Double.class.equals(clazz)) {
//            BigDecimal b1 = BigDecimal.valueOf((double) object);
//            BigDecimal b2 = new BigDecimal(String.valueOf(boundary));
//            return b1.add(b2);
//        } else throw new LSException(clazz + " 加法暂未实现");
//    }
//
//    private Object minusNum(Object object) throws LSException {
//        Class clazz = object.getClass();
//        if (Long.class.equals(clazz)) {
//            long l1 = (long) object;
//            if (l1 <= boundary) return 0;
//            else return l1 - boundary;
//        } else if (Double.class.equals(clazz)) {
//            double d1 = (double) object;
//            if (d1 <= boundary) return 0.0;
//            else {
//                BigDecimal b1 = BigDecimal.valueOf((double) object);
//                BigDecimal b2 = new BigDecimal(String.valueOf(boundary));
//                return b1.subtract(b2);
//            }
//        } else throw new LSException(clazz + " 减法暂未实现");
//    }

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
