package com.spbsu.datastream.core;

public interface Hashable<T> {
  int hash();

  boolean hashEquals(T that);
}
