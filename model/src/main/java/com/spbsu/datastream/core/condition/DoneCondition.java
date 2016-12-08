package com.spbsu.datastream.core.condition;

/**
 * Created by Artem on 01.12.2016.
 */
public interface DoneCondition<T, S extends ConditionState> extends Condition<S> {
  boolean taskDone(T item, S state);
}
