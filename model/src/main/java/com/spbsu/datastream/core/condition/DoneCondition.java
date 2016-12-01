package com.spbsu.datastream.core.condition;

/**
 * Created by Artem on 01.12.2016.
 */
public interface DoneCondition<T> extends Condition {
  boolean taskDone(T item);
}
