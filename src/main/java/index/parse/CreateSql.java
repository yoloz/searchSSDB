package index.parse;

import bean.Field;
import bean.LSException;
import bean.Schema;
import bean.Source;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import util.SqlliteUtil;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * date('uuuu-MM-dd'T'HH:mm:ss.SSSSSS')
 * <p>
 * CREATE TABLE test
 * (col1 int, col2 string, col3 date('yyyy-MM-dd HH:mm:ss.SSS'))
 * name=test1 addr='127.0.0.1:8888' type=list analyser=standardAnalyzer
 * <p>
 * analyser如果没定义则使用默认的分析器{@link org.apache.lucene.analysis.standard.StandardAnalyzer}
 * <p>
 * 如果字段名前缀含有'_',在解析处理时会再添加'_',内部字段使用前缀'_'
 */
public class CreateSql {

    private final Logger logger = Logger.getLogger(CreateSql.class);

    private final String sql;
    private final Yaml yaml = new Yaml();

    public CreateSql(String sql) {
        this.sql = sql;
    }

    /**
     * parse create sql
     *
     * @return indexName
     * @throws LSException error
     */
    public String store() throws LSException, SQLException {
        CreateTable table;
        try {
            table = (CreateTable) new CCJSqlParserManager().
                    parse(new StringReader(sql));
        } catch (JSQLParserException e) {
            throw new LSException("构建语句解析错误", e);
        }
        Schema schema = new Schema();
        schema.setIndex(table.getTable().getName());

        List<Map<String, Object>> list = SqlliteUtil.getInstance()
                .query("select name from schema where name=?", table.getTable().getName());
        if (!list.isEmpty()) throw new LSException("index[" + table.getTable().getName() + "] is exit...");

        Map<String, String> indexOptions = getIndexOptions(table.getTableOptionsStrings());
        this.checkIndexOptions(indexOptions);
        schema.setAnalyserKey(indexOptions.getOrDefault("analyser", "StandardAnalyzer"));

        Source source = new Source();
        String addr = indexOptions.get("addr");
        int idex = addr.indexOf(":");
        source.setIp(addr.substring(0, idex));
        source.setPort(Integer.parseInt(addr.substring(idex + 1)));
        source.setName(indexOptions.get("name"));
        source.setType(indexOptions.get("type"));
        schema.setSource(source);

        List<Field> fields = new ArrayList<>(table.getColumnDefinitions().size());
        for (ColumnDefinition column : table.getColumnDefinitions()) {
            Field field = new Field();
            String type = column.getColDataType().getDataType().toLowerCase();
            if ("date".equals(type)) {
                List<String> formatters = column.getColDataType().getArgumentsStringList();
                StringBuilder formatter = new StringBuilder();
                for (String s : formatters) {
                    if (s.charAt(0) == '\'') s = s.substring(1, s.length() - 1);
                    if (s.equalsIgnoreCase("T")) s = "'T'"; //'uuuu-MM-dd'T'HH:mm:ss.SSSSSS'
                    formatter.append(s);
                }
                field.setFormatter(formatter.toString());
            }
            String name = column.getColumnName();
            if (name.charAt(0) == '_') {
                name = String.join("_", name);
                logger.warn("'_' used by internal field, convert[" + column.getColumnName() + "]to[" + name + "]");
            }
            field.setName(name);
            field.setType(type);
            fields.add(field);
        }
        schema.setFields(fields);

        String[] p1 = new String[2];
        p1[0] = schema.getIndex();
        p1[1] = yaml.dump(schema);
        Object point = null;
        if (Source.Type.LIST == schema.getSource().getType()) point = 0;
        else if (Source.Type.HASH == schema.getSource().getType()) point = "";
        Object[] p2 = new Object[2];
        p2[0] = schema.getIndex();
        p2[1] = point;
        SqlliteUtil.getInstance().transaction("INSERT INTO schema(name,value)VALUES (?,?)", p1,
                "INSERT INTO point(iname,value)VALUES (?,?)", p2);
        return schema.getIndex();
    }


    private void checkIndexOptions(Map<String, String> options) throws LSException {
        if (!options.containsKey("addr")) throw new LSException("创建语句中未定义addr");
        if (!options.containsKey("name")) throw new LSException("创建语句中未定义name");
        if (!options.containsKey("type")) throw new LSException("创建语句中未定义type");
    }

    private Map<String, String> getIndexOptions(List<?> options) {
        Map<String, String> params = new HashMap<>(4);
        for (int i = 0; i < options.size(); i += 3) {
            String key = String.valueOf(options.get(i)).toLowerCase();
            String value = String.valueOf(options.get(i + 2));
            if (value.charAt(0) == '\'') value = value.substring(1, value.length() - 1);
            params.put(key, value);
        }
        return Collections.unmodifiableMap(params);
    }
}
