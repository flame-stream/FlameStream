package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.core.Condition;

/**
 * Created by Artem on 01.12.2016.
 */
public class SqlLimitCondition<T> implements Condition<T> {
  private final long limitValue;
  private long processedItems;

  public SqlLimitCondition(long limitValue) {
    this.limitValue = limitValue;
    processedItems = 0;
  }

  @Override
  public boolean update(T item) {
    return ++processedItems <= limitValue;
  }

  @Override
  public boolean isFinished() {
    return true;
  }

  @Override
  public Condition create() {
    return new SqlLimitCondition<T>(limitValue);
  }

  @Override
  public String toString() {
    return String.format("(Limit = %d)", limitValue);
  }
}
