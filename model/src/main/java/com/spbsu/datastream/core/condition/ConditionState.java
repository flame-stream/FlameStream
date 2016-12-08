package com.spbsu.datastream.core.condition;

/**
 * Created by Artem on 06.12.2016.
 */
public interface ConditionState<T, S> {
  S update(T item);
}
