package com.spbsu.datastream.core.item;

import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class SerializedDataItem implements DataItem {
  private final Meta meta;
  private final CharSeq line;

  public SerializedDataItem(CharSequence line) {
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
  public <T> T as(Class<T> type) {
    return DataStreamsContext.serializatonRepository.read(line, type);
  }
}
