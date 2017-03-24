package com.spbsu.datastream.core;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */

public final class PayloadDataItem<T> implements DataItem<T> {
  private final Meta meta;

  private final T payload;

  private final long rootId;

  private final long id;

  public PayloadDataItem(final Meta meta, final T payload) {
    this.id = ThreadLocalRandom.current().nextLong();
    this.payload = payload;
    this.meta = meta;
    this.rootId = this.id;
  }

  public PayloadDataItem(final Meta meta, final T payload, final long rootId) {
    this.id = ThreadLocalRandom.current().nextLong();
    this.payload = payload;
    this.meta = meta;
    this.rootId = rootId;
  }

  @Override
  public long id() {
    return id;
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
  public long rootId() {
    return rootId;
  }

  @Override
  public String toString() {
    return "PayloadDataItem{" + "meta=" + meta +
            ", payload=" + payload +
            ", rootId=" + rootId +
            ", id=" + id +
            '}';
  }
}
