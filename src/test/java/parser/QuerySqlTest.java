package parser;

import bean.Pair;
import bean.LSException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.*;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class QuerySqlTest {

    @Before
    public void setUp() {

    }

    @SuppressWarnings("unchecked")
    @Test
    public void parseSelectColumn() throws JSQLParserException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String sql = "select col1,test.col2,col3 from test";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        List<SelectItem> selects = ps.getSelectItems();
        QuerySql querySql = new QuerySql(sql);
        Method method = querySql.getClass().getDeclaredMethod("getSelects", List.class);
        method.setAccessible(true);
        List<String> list = (List<String>) method.invoke(querySql, selects);
        assertEquals(3, list.size());
        assertEquals("col2", list.get(1));
    }

    @Test
    public void parseSelectTable() throws JSQLParserException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String sql = "select col1,test.col2 from test";
//        sql = "select * from (select * from test)";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        FromItem fromItem = ps.getFromItem();
        QuerySql querySql = new QuerySql(sql);
        Method method = querySql.getClass().getDeclaredMethod("getIndexName", FromItem.class);
        method.setAccessible(true);
        String name = (String) method.invoke(querySql, fromItem);
        assertEquals("test", name);
    }

    /**
     * to_date('2007-06-12 10:00:00', 'yyyy-mm-dd hh24:mi:ss')暂未处理
     */
    @Test
    @SuppressWarnings("all")
    public void parseWhere() throws JSQLParserException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String sql = "select * from test where (col3='test' and col1 like 'a?b') or " +
                "(col2>11 or col4 between 1 and 4) and (col4<=5.3)";
        sql = "select * from test where date between '2019-02-28T09:43:10.224000' and '2019-02-28T09:43:10.225000'";
        sql = "select * from test";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        Expression where = ps.getWhere();
        StringBuilder builder = new StringBuilder();
        QuerySql querySql = new QuerySql(sql);
        Method method = querySql.getClass().getDeclaredMethod("parseWhere", StringBuilder.class, Expression.class);
        method.setAccessible(true);
        method.invoke(querySql, builder, where);
        System.out.println(builder.toString());
    }


    @SuppressWarnings("all")
    @Test
    public void parseWhereQuery() throws JSQLParserException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        String sql = "select * from test where date between '2019-02-28T09:43:10.224000' and '2019-02-28T09:43:10.225000'";
        sql = "select * from test where date > '2019-02-28T09:43:10.224000'";
        sql = "select * from test where index between 10 and 20";
        sql = "select * from test where dd between 10.2 and 20.3";
        sql = "select * from test where col1=2";
        sql = "select * from test where col2='北京'";
        sql = "select * from test where col2 like '北京'";
        sql = "select * from test where col1 like '北京' and date > '2019-02-28T09:43:10.224000'";
        sql = "select * from test where col1 like '北京' or date > '2019-02-28T09:43:10.224000'";
        sql = "select * from test where (col3='test')";
        sql = "select * from test where (col3='test' and col1 like 'a?b')";
        sql = "select * from test where (col3='test' and col1 like 'a?b') or date>'2019-02-28T09:43:10.224000'";
        sql = "select * from test where (col3='test' and col1 like 'a?b') or" +
                " (col2>3 and date>'2019-02-28T09:43:10.224000')";
        sql = "select * from test where (col3='test' and col1 like 'a?b') or " +
                "(col2>3 or date>'2019-02-28T09:43:10.224000')";
        sql = "select * from test where (col3='test' and col1 like 'a?b') or " +
                "(col2>3 or date>'2019-02-28T09:43:10.224000') and col4='北京'";
        sql = "select * from test where (col3='test' and col1 like 'a?b') or " +
                "(col2>3 or date>'2019-02-28T09:43:10.224000') and (col5<=5.3)";
        sql = "select * from test";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        Expression where = ps.getWhere();
        QuerySql querySql = new QuerySql(sql);
        Method method = querySql.getClass().getDeclaredMethod("parseWhere", Expression.class);
        method.setAccessible(true);
        Field field = querySql.getClass().getDeclaredField("columnMap");
        field.setAccessible(true);
        Map<String, Pair<bean.Field.Type, String>> columnMap =
                (Map<String, Pair<bean.Field.Type, String>>) field.get(querySql);
        columnMap.put("date", Pair.of(bean.Field.Type.DATE, "uuuu-MM-dd'T'HH:mm:ss.SSSSSS"));
        Query query = (Query) method.invoke(querySql, where);
        System.out.println(query.toString());
    }

    @Test
    public void parseLimit() throws JSQLParserException, LSException {
        String sql = "select * from test limit 10";
//        sql = "select * from test limit 10,5";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        Limit limit = ps.getLimit();
        if (limit != null) {
            Expression offset = limit.getOffset();
            if (offset != null) throw new LSException("暂不只支持limit offset,count");
            Expression rowCount = limit.getRowCount();
            if (!LongValue.class.equals(rowCount.getClass()))
                throw new LSException("limit right data type[" + rowCount.getClass() + "]not support");
            System.out.println(((LongValue) rowCount).getBigIntegerValue().longValueExact());
        }
    }


    @After
    public void tearDown() {
    }
}