package com.spbsu.datastream.core;

import java.util.function.ToIntFunction;

public interface HashFunction<T> extends ToIntFunction<T> {
  HashFunction<Object> OBJECT_HASH = new HashFunction<Object>() {
    @Override
    public boolean equal(Object o1, Object o2) {
      return o1.hashCode() == o2.hashCode();
    }

    @Override
    public int hash(Object value) {
      return value.hashCode();
    }
  };

  static <T> HashFunction<T> constantHash(int hash) {
    return new HashFunction<T>() {
      @Override
      public boolean equal(T o1, T o2) {
        return true;
      }

      @Override
      public int hash(T value) {
        return hash;
      }
    };
  }

  boolean equal(T o1, T o2);

  int hash(T value);

  @Override
  default int applyAsInt(T value) {
    return this.hash(value);
  }
}
