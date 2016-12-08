package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.core.condition.DoneCondition;

/**
 * Created by Artem on 01.12.2016.
 */
public class SqlLimitCondition<T> implements DoneCondition<T, SqlLimitConditionState> {
  private final long limitValue;

  public SqlLimitCondition(long limitValue) {
    this.limitValue = limitValue;
  }

  @Override
  public boolean taskDone(T item, SqlLimitConditionState state) {
    //noinspection unchecked
    return state.update(item) >= limitValue;
  }

  @Override
  public String toString() {
    return String.format("(Limit = %d)", limitValue);
  }

  @Override
  public Class<SqlLimitConditionState> conditionState() {
    return SqlLimitConditionState.class;
  }
}
