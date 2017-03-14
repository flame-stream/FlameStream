package com.spbsu.datastream.core;

import java.util.function.ToIntFunction;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface HashFunction<T> extends ToIntFunction<T> {
  @Override
  default int applyAsInt(T value) {
    return hash(value);
  }

  int hash(T o);

  boolean hashEqual(T o1, T o2);
}
