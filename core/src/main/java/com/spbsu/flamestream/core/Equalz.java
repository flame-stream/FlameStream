package com.spbsu.flamestream.core;

import java.io.Serializable;
import java.util.function.BiPredicate;

public interface Equalz extends BiPredicate<DataItem, DataItem>, Serializable {
  static Equalz hashEqualz(HashFunction hashFunction) {
    return new Equalz() {
      private final HashFunction h = hashFunction;

      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return h.applyAsInt(dataItem) == h.applyAsInt(dataItem2);
      }
    };
  }

  static Equalz allEqualz() {
    return (dataItem, dataItem2) -> true;
  }

  static <T> Equalz objectEqualz(Class<T> clazz) {
    return new Equalz() {
      private final Class<T> c = clazz;

      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return dataItem.payload(c).equals(dataItem2.payload(clazz));
      }
    };
  }
}
