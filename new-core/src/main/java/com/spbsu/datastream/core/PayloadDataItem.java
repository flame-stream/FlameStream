package com.spbsu.datastream.core;

public final class PayloadDataItem<T> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  public PayloadDataItem(final Meta meta, final T payload) {
    this.payload = payload;
    this.meta = meta;
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
  public String toString() {
    return "PayloadDataItem{" + "meta=" + meta +
            ", payload=" + payload +
            '}';
  }
}