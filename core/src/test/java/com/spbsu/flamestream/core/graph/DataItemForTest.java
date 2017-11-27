package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.function.BiPredicate;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class DataItemForTest<T> implements DataItem<T> {
  private final DataItem<T> inner;
  private final HashFunction<? super T> hash;
  private final BiPredicate<? super T, ? super T> equalz;


  DataItemForTest(DataItem<T> inner, HashFunction<? super T> hash, BiPredicate<? super T, ? super T> equalz) {
    this.inner = inner;
    this.hash = hash;
    this.equalz = equalz;
  }

  @Override
  public Meta meta() {
    return inner.meta();
  }

  @Override
  public T payload() {
    return inner.payload();
  }

  @Override
  public long xor() {
    return inner.xor();
  }

  @Override
  public int hashCode() {
    return hash.applyAsInt(payload());
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
    final DataItem<T> that = (DataItem<T>) o;
    return equalz.test(payload(), that.payload());
  }
}
