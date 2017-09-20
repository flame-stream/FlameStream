package com.spbsu.datastream.core;

import com.spbsu.datastream.core.meta.Meta;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class PayloadDataItem<T> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  private final long ackHashCode;

  public PayloadDataItem(Meta meta, T payload) {
    this.payload = payload;
    this.meta = meta;
    this.ackHashCode = ThreadLocalRandom.current().nextLong();
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
  public long ack() {
    return ackHashCode;
  }

  @Override
  public String toString() {
    return '{' + "meta=" + meta +
            ", payload=" + payload +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final PayloadDataItem<?> that = (PayloadDataItem<?>) o;
    return ackHashCode == that.ackHashCode &&
            Objects.equals(meta, that.meta) &&
            Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload, ackHashCode);
  }
}