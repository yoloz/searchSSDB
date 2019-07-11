package parser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;

import static org.junit.Assert.*;

public class CreateSqlTest {

    @Before
    public void setUp() {
    }

    @Test
    public void parseSQL() {
        //uuuu-MM-dd'T'HH:mm:ss.SSSSSS
        String sql = "CREATE TABLE test" +
                "(col1 int, col2 string, col3 date('yyyy-MM-dd HH:mm:ss.SSS'),col4 date('uuuu-MM-dd\'T\'HH:mm:ss.SSSSSS'))" +
                " analyser='org.apache.lucene.analysis.standard.StandardAnalyzer' source=ssdb.test1 addr='127.0.0.1:8888' type=list";
        try {
            CreateTable table = (CreateTable) new CCJSqlParserManager().parse(new StringReader(sql));
            assertEquals("test", table.getTable().getName());
            assertEquals("ssdb.test1", table.getTableOptionsStrings().get(5));
            assertEquals("'127.0.0.1:8888'", table.getTableOptionsStrings().get(8));
            assertEquals(4, table.getColumnDefinitions().size());
            assertEquals("int", table.getColumnDefinitions().get(0).getColDataType().getDataType());
            ColumnDefinition date = table.getColumnDefinitions().get(2);
            assertEquals("col3", date.getColumnName());
            assertEquals("'yyyy-MM-dd HH:mm:ss.SSS'", date.getColDataType().getArgumentsStringList().get(0));
            ColumnDefinition date1 = table.getColumnDefinitions().get(3);
            assertEquals(3, date1.getColDataType().getArgumentsStringList().size());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
    }
}