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

  HashFunction<Object> UNIFORM_OBJECT_HASH = new HashFunction<Object>() {
    @Override
    public boolean equal(Object o1, Object o2) {
      return o1.hashCode() == o2.hashCode();
    }

    @Override
    public int hash(Object value) {
      return this.jenkinsHash(value.hashCode());
    }

    private int jenkinsHash(int value) {
      int a = value;
      a = (a + 0x7ed55d16) + (a << 12);
      a = (a ^ 0xc761c23c) ^ (a >> 19);
      a = (a + 0x165667b1) + (a << 5);
      a = (a + 0xd3a2646c) ^ (a << 9);
      a = (a + 0xfd7046c5) + (a << 3);
      a = (a ^ 0xb55a4f09) ^ (a >> 16);
      return a;
    }
  };

  static <T> HashFunction<T> uniformLimitedHash(int buckets) {
    return new HashFunction<T>() {
      private final int bucketss = buckets;

      @Override
      public boolean equal(T o1, T o2) {
        return this.applyAsInt(o1) == this.applyAsInt(o2);
      }

      @Override
      public int hash(T value) {
        return HashFunction.UNIFORM_OBJECT_HASH.applyAsInt(OBJECT_HASH.applyAsInt(value) % bucketss);
      }
    };
  }

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
