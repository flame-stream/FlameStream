package com.spbsu.datastream.core;

import java.util.concurrent.ThreadLocalRandom;

public final class PayloadDataItem<T> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  private final long ackHashCode;

  public PayloadDataItem(final Meta meta, final T payload) {
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
}