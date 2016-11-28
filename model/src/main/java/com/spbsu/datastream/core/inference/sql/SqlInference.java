package com.spbsu.datastream.core.inference.sql;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.InvalidQueryException;
import com.spbsu.datastream.core.exceptions.UnsupportedQueryException;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.IdentityJoba;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.ReplicatorJoba;
import com.spbsu.datastream.example.bl.sql.SqlWhereEqualsToFilter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Created by Artem on 15.11.2016.
 */
public class SqlInference {
  @Nullable
  public DataType type(String name) {
    return new DataType.Stub(name);
  }

  public Joba query(String query, Class dataClass) throws InvalidQueryException, UnsupportedQueryException {
    try {
      final Joba result;
      final Statement sqlStatement = CCJSqlParserUtil.parse(query);
      if (sqlStatement instanceof Select) {
        final Select select = (Select) sqlStatement;
        if (select.getSelectBody() instanceof PlainSelect) {
          final PlainSelect selectBody = (PlainSelect) select.getSelectBody();
          result = processPlainSelect(selectBody, dataClass);
          return result;
        }
      }
      throw new UnsupportedQueryException();
    } catch (JSQLParserException jpe) {
      throw new InvalidQueryException(jpe);
    }
  }

  private Joba processPlainSelect(PlainSelect select, Class dataClass) throws UnsupportedQueryException {
    final Joba result;
    if (select.getFromItem() instanceof Table) {
      final Table table = (Table) select.getFromItem();
      final DataType sourceDataType = type(table.getName());
      final Function<DataItem, DataItem> whereFilter = whereFilter(select.getWhere());
      result = selectJoba(sourceDataType, whereFilter, dataClass);
      return result;
    }
    throw new UnsupportedQueryException();
  }

  private Function<DataItem, DataItem> whereFilter(Expression where) throws UnsupportedQueryException {
    final Function<DataItem, DataItem> whereFilter;
    if (where instanceof EqualsTo) {
      final EqualsTo equalsTo = (EqualsTo) where;
      if (equalsTo.getLeftExpression() instanceof Column) {
        final Column column = (Column) equalsTo.getLeftExpression();
        if (equalsTo.getRightExpression() instanceof StringValue) {
          final StringValue stringValue = (StringValue) equalsTo.getRightExpression();
          whereFilter = new SqlWhereEqualsToFilter<>(column.getColumnName(), stringValue.getValue());
          return whereFilter;
        }
      }
    }
    throw new UnsupportedQueryException();
  }

  private Joba selectJoba(DataType sourceDataType, Function<DataItem, DataItem> whereFilter, Class dataClass) {
    final String filterJobaGeneratesTypeName = String.format("Filter(%s, %s)", sourceDataType.name(), whereFilter);
    final FilterJoba filterJoba = new FilterJoba(new IdentityJoba(sourceDataType), type(filterJobaGeneratesTypeName), whereFilter, dataClass, dataClass);
    return new ReplicatorJoba(filterJoba);
  }
}
