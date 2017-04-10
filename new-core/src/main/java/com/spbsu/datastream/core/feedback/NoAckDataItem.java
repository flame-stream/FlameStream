package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Meta;

public class NoAckDataItem<T> implements DataItem<T> {
  private final Meta meta;
  private final T payload;

  public NoAckDataItem(final Meta meta, final T payload) {
    this.meta = meta;
    this.payload = payload;
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
  public long ackHashCode() {
    throw new UnsupportedOperationException();
  }
}
