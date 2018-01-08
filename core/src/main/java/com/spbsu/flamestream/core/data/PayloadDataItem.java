package com.spbsu.flamestream.core.data;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class PayloadDataItem implements DataItem {
  private final Meta meta;

  private final Object payload;

  private final long ackHashCode;

  public PayloadDataItem(Meta meta, Object payload) {
    this.payload = payload;
    this.meta = meta;
    this.ackHashCode = ThreadLocalRandom.current().nextLong();
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public <T> T payload(Class<T> clazz) {
    return clazz.cast(payload);
  }

  @Override
  public long xor() {
    return ackHashCode;
  }

  @Override
  public String toString() {
    return '{' + "meta=" + meta + ", payload=" + payload + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PayloadDataItem that = (PayloadDataItem) o;
    return ackHashCode == that.ackHashCode && Objects.equals(meta, that.meta) && Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload, ackHashCode);
  }
}