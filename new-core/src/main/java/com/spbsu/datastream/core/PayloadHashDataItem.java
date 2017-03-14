package com.spbsu.datastream.core;

import java.util.Objects;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */

public final class PayloadHashDataItem<T extends Hashable> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  public PayloadHashDataItem(final Meta meta, final T payload) {
    this.meta = meta;
    this.payload = payload;
  }

  @Override
  public int hash() {
    return payload.hash();
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public T payload() {
    return payload;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final PayloadHashDataItem<?> dataItem = (PayloadHashDataItem<?>) o;
    return Objects.equals(meta, dataItem.meta) &&
            Objects.equals(payload, dataItem.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload);
  }

  @Override
  public String toString() {
    return "PayloadHashDataItem{" + "meta=" + meta +
            ", payload=" + payload +
            '}';
  }
}
