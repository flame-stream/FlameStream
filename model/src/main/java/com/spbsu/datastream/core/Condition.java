package com.spbsu.datastream.core;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface Condition<T> {
  boolean update(T item);
}
