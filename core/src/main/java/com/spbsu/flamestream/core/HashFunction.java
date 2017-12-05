package com.spbsu.flamestream.core;

import com.google.common.hash.Hashing;

import java.util.function.ToIntFunction;

@FunctionalInterface
public interface HashFunction extends ToIntFunction<DataItem> {
  @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
  static <T> HashFunction objectHash(Class<T> clazz) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return item.payload(clazz).hashCode();
      }
    };
  }

  @SuppressWarnings("Convert2Lambda")
  static <T> HashFunction uniformObjectHash(Class<T> clazz) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return Hashing.murmur3_32().hashInt(item.payload(clazz).hashCode()).asInt();
      }
    };
  }

  @SuppressWarnings("Convert2Lambda")
  static HashFunction constantHash(int hash) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return hash;
      }
    };
  }

  int hash(DataItem dataItem);

  @Override
  default int applyAsInt(DataItem dataItem) {
    return hash(dataItem);
  }
}
