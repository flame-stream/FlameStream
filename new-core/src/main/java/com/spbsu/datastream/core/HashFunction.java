package com.spbsu.datastream.core;

import java.util.function.ToIntFunction;

public interface HashFunction<T> extends ToIntFunction<T> {
  HashFunction<Object> OBJECT_HASH = new HashFunction<Object>() {
    @Override
    public boolean equal(final Object o1, final Object o2) {
      return o1.hashCode() == o2.hashCode();
    }

    @Override
    public int hash(final Object value) {
      return value.hashCode();
    }
  };

  static <T> HashFunction<T> constantHash(final int hash) {
    return new HashFunction<T>() {
      @Override
      public boolean equal(final T o1, final T o2) {
        return true;
      }

      @Override
      public int hash(final T value) {
        return hash;
      }
    };
  }

  boolean equal(T o1, T o2);

  int hash(T value);

  @Override
  default int applyAsInt(final T value) {
    return this.hash(value);
  }
}
