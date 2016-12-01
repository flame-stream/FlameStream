package com.spbsu.datastream.core.condition;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface FailCondition<T> extends Condition {
  boolean stateOk(T item);
}
