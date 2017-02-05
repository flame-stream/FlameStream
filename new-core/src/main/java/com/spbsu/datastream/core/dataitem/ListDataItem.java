package com.spbsu.datastream.core.dataitem;

import com.spbsu.datastream.core.DataItem;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ListDataItem<T> extends ArrayList<DataItem> implements DataItem<T> {
  private final Meta meta;

  public ListDataItem(final List<DataItem> copy, final Meta meta) {
    super(copy);
    this.meta = meta;
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    final StringBuilder builder = new StringBuilder();
    forEach((item) -> builder.append(item.serializedData()));
    return builder.toString();
  }

  @Override
  public T as(final Class<T> type) {
    if (!type.isArray())
      throw new ClassCastException();
    final Object array = Array.newInstance(type.getComponentType(), size());
    for (int i = 0; i < size(); i++) {
      Array.set(array, i, get(i).as(type.getComponentType()));
    }
    //noinspection unchecked
    return (T) array;
  }
}
