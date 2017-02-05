package com.spbsu.datastream.core.dataitem;

import com.spbsu.datastream.core.DataItem;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ObjectDataItem<T> implements DataItem<T> {
  private final Object item;
  private final Class clazz;
  private final Meta meta;

  public ObjectDataItem(final Object item, final Class clazz, final Meta meta) {
    this.item = item;
    this.clazz = clazz;
    this.meta = meta;
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T as(final Class<T> type) {
    //noinspection unchecked
    return (T) item;
  }
}
