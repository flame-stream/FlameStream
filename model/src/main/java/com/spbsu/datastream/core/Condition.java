package com.spbsu.datastream.core;

import com.spbsu.commons.func.Factory;

/**
 * Created by Artem on 01.12.2016.
 */
public interface Condition<T> extends Factory<Condition> {
  boolean update(T item);

  boolean isFinished();
}