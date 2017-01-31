package com.spbsu.datastream.core.dataitem;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ObjectDataItem implements DataItem {
  private final Object item;
  private final Class clazz;
  private final Meta meta;

  public ObjectDataItem(Object item, Class clazz, Meta meta) {
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
    return DataStreamsContext.serializatonRepository.write(item);
  }

  @Override
  public <T> T as(Class<T> type) {
    //noinspection unchecked
    return (T) item;
  }
}
