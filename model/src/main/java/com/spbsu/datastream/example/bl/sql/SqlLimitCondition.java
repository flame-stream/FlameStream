package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.core.condition.DoneCondition;

/**
 * Created by Artem on 01.12.2016.
 */
public class SqlLimitCondition<T> implements DoneCondition<T> {
  private final long limitValue;
  private long processedItems;

  public SqlLimitCondition(long limitValue) {
    this.limitValue = limitValue;
    processedItems = 0;
  }

  @Override
  public boolean taskDone(T item) {
    processedItems++;
    return processedItems >= limitValue;
  }

  @Override
  public String toString() {
    return String.format("(Limit = %d)", limitValue);
  }
}
