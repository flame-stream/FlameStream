package com.spbsu.datastream.core.item;

import com.spbsu.datastream.core.DataItem;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ListDataItem extends ArrayList<DataItem> implements DataItem {
  private final Meta meta;

  public ListDataItem(List<DataItem> copy, Meta meta) {
    super(copy);
    this.meta = meta;
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    StringBuilder builder = new StringBuilder();
    forEach((item) -> builder.append(item.serializedData()));
    return builder.toString();
  }

  @Override
  public <T> T as(Class<T> type) {
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
