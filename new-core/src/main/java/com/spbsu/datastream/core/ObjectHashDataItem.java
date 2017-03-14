package com.spbsu.datastream.core;

import java.util.Objects;

public class ObjectHashDataItem<T> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  public ObjectHashDataItem(final Meta meta, final T payload) {
    this.meta = meta;
    this.payload = payload;
  }

  @Override
  public Meta meta() {
    return this.meta;
  }

  @Override
  public T payload() {
    return this.payload();
  }

  @Override
  public int hash() {
    return payload().hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final ObjectHashDataItem<?> that = (ObjectHashDataItem<?>) o;
    return Objects.equals(meta, that.meta) &&
            Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload);
  }

  @Override
  public String toString() {
    return "ObjectHashDataItem{" + "meta=" + meta +
            ", payload=" + payload +
            '}';
  }
}
