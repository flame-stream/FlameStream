package com.spbsu.datastream.example.bl.sql;

import com.spbsu.datastream.core.condition.ConditionState;

/**
 * Created by Artem on 06.12.2016.
 */
public class SqlLimitConditionState<T> implements ConditionState<T, Long> {
  private long processedItems = 0;

  @Override
  public Long update(T item) {
    return ++processedItems;
  }
}