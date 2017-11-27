package com.spbsu.flamestream.core;

import com.google.common.hash.Hashing;

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
      return Hashing.murmur3_32().hashInt(value.hashCode()).asInt();
    }
  };

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
