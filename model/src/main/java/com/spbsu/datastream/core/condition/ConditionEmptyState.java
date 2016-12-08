package com.spbsu.datastream.core.condition;

/**
 * Created by Artem on 06.12.2016.
 */
public class ConditionEmptyState implements ConditionState {
  @Override
  public Object update(Object item) {
    return null;
  }
}
