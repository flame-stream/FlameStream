package com.spbsu.datastream.core.condition;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface FailCondition<T, S extends ConditionState> extends Condition<S> {
  boolean taskFail(T item, S state);
}
