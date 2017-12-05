package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.function.BiPredicate;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class DataItemForTest<T> implements DataItem {
  private final DataItem inner;
  private final HashFunction hash;
  private final BiPredicate<? super T, ? super T> equalz;
  private final Class<T> clazz;


  DataItemForTest(DataItem inner, HashFunction hash, BiPredicate<> equalz, Class<T> clazz) {
    this.inner = inner;
    this.hash = hash;
    this.equalz = equalz;
    this.clazz = clazz;
  }

  @Override
  public Meta meta() {
    return inner.meta();
  }

  @Override
  public <R> R payload(Class<R> expectedClass) {
    return inner.payload(expectedClass);
  }

  @Override
  public long xor() {
    return inner.xor();
  }

  @Override
  public int hashCode() {
    return hash.applyAsInt(payload(clazz));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    //noinspection unchecked
    final DataItem that = (DataItem) o;
    return equalz.test(payload(clazz), that.payload(clazz));
  }
}
