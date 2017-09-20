package com.spbsu.datastream.core;

import java.util.function.ToIntFunction;

@FunctionalInterface
public interface HashFunction<T> extends ToIntFunction<T> {
  int hash(T value);

  @Override
  default int applyAsInt(T value) {
    return hash(value);
  }

  @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
  HashFunction<Object> OBJECT_HASH = new HashFunction<Object>() {
    @Override
    public int hash(Object value) {
      return value.hashCode();
    }
  };

  @SuppressWarnings("Convert2Lambda")
  HashFunction<Object> UNIFORM_OBJECT_HASH = new HashFunction<Object>() {
    @Override
    public int hash(Object value) {
      return jenkinsHash(value.hashCode());
    }

  };

  static int jenkinsHash(int value) {
    int a = value;
    a = (a + 0x7ed55d16) + (a << 12);
    a = (a ^ 0xc761c23c) ^ (a >> 19);
    a = (a + 0x165667b1) + (a << 5);
    a = (a + 0xd3a2646c) ^ (a << 9);
    a = (a + 0xfd7046c5) + (a << 3);
    a = (a ^ 0xb55a4f09) ^ (a >> 16);
    return a;
  }

  static <T> HashFunction<T> uniformLimitedHash(int buckets) {
    return new HashFunction<T>() {
      private final int n = buckets;

      @Override
      public int hash(T value) {
        return HashFunction.UNIFORM_OBJECT_HASH.applyAsInt(OBJECT_HASH.applyAsInt(value) % n);
      }
    };
  }

  @SuppressWarnings("Convert2Lambda")
  static <T> HashFunction<T> constantHash(int hash) {
    //noinspection Convert2Lambda
    return new HashFunction<T>() {
      @Override
      public int hash(T value) {
        return hash;
      }
    };
  }
}
