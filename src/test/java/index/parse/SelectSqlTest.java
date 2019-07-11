package index.parse;

import bean.LSException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.lucene.search.SortField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;

import static org.junit.Assert.*;

public class SelectSqlTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void parseOrderBy() throws JSQLParserException, LSException {
        String sql = "select * from test order by id desc,name asc,score";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        List<OrderByElement> orders = ps.getOrderByElements();
        SortField[] sortFields = null;
        if (orders != null && !orders.isEmpty()) {
            sortFields = new SortField[orders.size()];
            for (int i = 0; i < orders.size(); i++) {
                OrderByElement order = orders.get(i);
                Expression expression = order.getExpression();
                if (!expression.getClass().equals(Column.class))
                    throw new LSException("field not support[" + expression.getClass() + "]");
                Column f = (Column) expression;
                boolean asc = order.isAsc();
                sortFields[i] = new SortField(f.getColumnName(), SortField.Type.STRING, asc);
            }
        }
    }

    @Test
    public void parseGroupBy() throws JSQLParserException, LSException {
        String sql = "select * from test group by id";
        Select select = (Select) new CCJSqlParserManager().parse(new StringReader(sql));
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        List<Expression> groups = ps.getGroupByColumnReferences();
        if (groups != null && !groups.isEmpty()) {
            for (Expression expression : groups) {
                if (!expression.getClass().equals(Column.class))
                    throw new LSException("field not support[" + expression.getClass() + "]");
                Column f = (Column) expression;
                System.out.println(f.getColumnName());
            }
        }
    }
}