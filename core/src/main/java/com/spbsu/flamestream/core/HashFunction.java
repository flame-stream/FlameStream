package com.spbsu.flamestream.core;

import com.google.common.hash.Hashing;

import java.util.function.ToIntFunction;

@FunctionalInterface
@SuppressWarnings("Convert2Lambda")
public interface HashFunction extends ToIntFunction<DataItem> {
  static <T> HashFunction objectHash(Class<T> clazz) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return item.payload(clazz).hashCode();
      }
    };
  }

  static HashFunction uniformHash(HashFunction hashFunction) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return Hashing.murmur3_32().hashInt(hashFunction.applyAsInt(item)).asInt();
      }
    };
  }

  static HashFunction constantHash(int hash) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return hash;
      }
    };
  }

  static HashFunction bucketedHash(HashFunction hash, int buckets) {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return hash.applyAsInt(item) % buckets;
      }
    };
  }

  int hash(DataItem dataItem);

  @Override
  default int applyAsInt(DataItem dataItem) {
    return hash(dataItem);
  }
}
