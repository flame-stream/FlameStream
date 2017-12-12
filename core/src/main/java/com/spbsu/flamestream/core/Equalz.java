package com.spbsu.flamestream.core;

import java.util.function.BiPredicate;

public interface Equalz extends BiPredicate<DataItem, DataItem> {
  static Equalz hashEqualz(HashFunction hashFunction) {
    return new Equalz() {
      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return hashFunction.applyAsInt(dataItem) == hashFunction.applyAsInt(dataItem2);
      }
    };
  }

  static <T> Equalz objectEqualz(Class<T> clazz) {
    return new Equalz() {
      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return dataItem.payload(clazz).equals(dataItem2.payload(clazz));
      }
    };
  }
}
