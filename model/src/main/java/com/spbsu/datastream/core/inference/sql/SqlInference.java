package com.spbsu.datastream.core.inference.sql;

import com.spbsu.datastream.core.*;
import com.spbsu.datastream.core.exceptions.InvalidQueryException;
import com.spbsu.datastream.core.exceptions.UnsupportedQueryException;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.IndicatorJoba;
import com.spbsu.datastream.example.bl.sql.SqlLimitCondition;
import com.spbsu.datastream.example.bl.sql.SqlWhereEqualsToFilter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Limit;
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

  public Sink query(String query, Sink sink, Class blClass) throws InvalidQueryException, UnsupportedQueryException {
    try {
      final Sink queryJoba;
      final Statement sqlStatement = CCJSqlParserUtil.parse(query);
      if (sqlStatement instanceof Select) {
        final Select select = (Select) sqlStatement;
        if (select.getSelectBody() instanceof PlainSelect) {
          final PlainSelect selectBody = (PlainSelect) select.getSelectBody();
          queryJoba = processPlainSelect(selectBody, sink, blClass);
          return queryJoba;
        }
      }
      throw new UnsupportedQueryException();
    } catch (JSQLParserException jpe) {
      throw new InvalidQueryException(jpe);
    }
  }

  private Sink processPlainSelect(PlainSelect select, Sink sink, Class blClass) throws UnsupportedQueryException {
    final Sink selectJoba;
    if (select.getFromItem() instanceof Table) {
      final Table table = (Table) select.getFromItem();
      final DataType sourceDataType = type(table.getName());
      final Function<DataItem, DataItem> whereFilter = whereFilter(select.getWhere());
      final Condition limitCondition = limitCondition(select.getLimit());
      selectJoba = selectJoba(sourceDataType, whereFilter, limitCondition, sink, blClass);
      return selectJoba;
    }
    throw new UnsupportedQueryException();
  }

  private Function<DataItem, DataItem> whereFilter(Expression where) throws UnsupportedQueryException {
    if (where == null) {
      return null;
    }

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

  private Condition limitCondition(Limit limit) {
    if (limit == null) {
      return null;
    }
    return new SqlLimitCondition(limit.getRowCount());

  }

  private Sink selectJoba(DataType sourceDataType, Function<DataItem, DataItem> whereFilter, Condition topCondition, Sink sink, Class blClass) {
    Sink selectJoba = sink;
    String generatesTypeName = sourceDataType.name();
    if (topCondition != null) {
      generatesTypeName = String.format("Indicator(%s, %s)", generatesTypeName, topCondition);
      selectJoba = new IndicatorJoba(selectJoba, type(generatesTypeName), blClass, topCondition);
    }
    if (whereFilter != null) {
      generatesTypeName = String.format("Filter(%s, %s)", generatesTypeName, whereFilter);
      selectJoba = new FilterJoba(selectJoba, type(generatesTypeName), whereFilter, blClass, blClass);
    }
    return selectJoba;
  }
}
