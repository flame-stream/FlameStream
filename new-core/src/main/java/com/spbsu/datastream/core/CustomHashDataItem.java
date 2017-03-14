package com.spbsu.datastream.core;

import java.util.Objects;

public class CustomHashDataItem<T> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  private final HashFunction<T> hashFunction;

  public CustomHashDataItem(final Meta meta, final T payload, final HashFunction<T> hashFunction) {
    this.meta = meta;
    this.payload = payload;
    this.hashFunction = hashFunction;
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
  public int hash() {
    return hashFunction.hash(payload);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final CustomHashDataItem<?> that = (CustomHashDataItem<?>) o;
    return Objects.equals(meta, that.meta) &&
            Objects.equals(payload, that.payload) &&
            Objects.equals(hashFunction, that.hashFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload, hashFunction);
  }

  @Override
  public String toString() {
    return "CustomHashDataItem{" + "meta=" + meta +
            ", payload=" + payload +
            ", hashFunction=" + hashFunction +
            '}';
  }
}
