package com.spbsu.datastream.core;

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
    return this.meta;
  }

  @Override
  public T payload() {
    return this.payload;
  }

  @Override
  public long ack() {
    return this.ackHashCode;
  }

  @Override
  public String toString() {
    return "PayloadDataItem{" + "meta=" + this.meta +
            ", payload=" + this.payload +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final PayloadDataItem<?> that = (PayloadDataItem<?>) o;
    return this.ackHashCode == that.ackHashCode &&
            Objects.equals(this.meta, that.meta) &&
            Objects.equals(this.payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.meta, this.payload, this.ackHashCode);
  }
}