package com.spbsu.datastream.core;

/**
 * Created by marnikitta on 2/4/17.
 */
public interface RunningCondition<T> {
  void update(T value);

  boolean isValid();
}
