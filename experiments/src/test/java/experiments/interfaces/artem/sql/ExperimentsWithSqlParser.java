package experiments.interfaces.artem.sql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by Artem on 20.11.2016.
 */
public class ExperimentsWithSqlParser {

  @Test
  public void parseSubSelect() throws JSQLParserException {
    //Arrange
    Statement statement = CCJSqlParserUtil.parse("SELECT Name FROM Product WHERE ListPrice = " +
            "(SELECT ListPrice FROM Client WHERE Year > 2010)");

    //Act
    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    List<String> tableList = tablesNamesFinder.getTableList(statement);

    Select selectStatement = (Select) statement;
    PlainSelect selectBody = (PlainSelect) selectStatement.getSelectBody();
    List<SelectItem> selectItems = selectBody.getSelectItems();
    Table selectFrom = (Table) selectBody.getFromItem();
    EqualsTo where = (EqualsTo) selectBody.getWhere();
    Column whereColumn = (Column) where.getLeftExpression();

    SubSelect subSelect = (SubSelect) where.getRightExpression();
    PlainSelect subSelectBody = (PlainSelect) subSelect.getSelectBody();
    List<SelectItem> subSelectItems = subSelectBody.getSelectItems();
    Table subSelectFrom = (Table) subSelectBody.getFromItem();
    ComparisonOperator subSelectWhere = (ComparisonOperator) subSelectBody.getWhere();
    Column subSelectWhereColumn = (Column) subSelectWhere.getLeftExpression();
    LongValue subSelectWhereValue = (LongValue) subSelectWhere.getRightExpression();

    //Assert
    Assert.assertArrayEquals(new String[]{"Product", "Client"}, tableList.toArray());

    Assert.assertTrue(selectItems.size() > 0);
    Assert.assertEquals("Name", selectItems.get(0).toString());
    Assert.assertEquals("Product", selectFrom.getName());

    Assert.assertTrue(subSelectItems.size() > 0);
    Assert.assertEquals("ListPrice", subSelectItems.get(0).toString());
    Assert.assertEquals("Client", subSelectFrom.getName());
    Assert.assertEquals("ListPrice", whereColumn.getColumnName());
    Assert.assertEquals("Year", subSelectWhereColumn.getColumnName());
    Assert.assertEquals(">", subSelectWhere.getStringExpression());
    Assert.assertEquals(2010, subSelectWhereValue.getValue());
  }
}
