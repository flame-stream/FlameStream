package com.spbsu.datastream.core.dataitem;

import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.core.DataItem;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class SerializedDataItem<T> implements DataItem<T> {
  private final Meta meta;
  private final CharSeq line;

  public SerializedDataItem(final CharSequence line) {
    this.meta = new SystemTypeMeta(System.nanoTime());
    this.line = CharSeq.create(line);
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    return line;
  }

  @Override
  public T as(final Class<T> type) {
    throw new UnsupportedOperationException();
  }
}
