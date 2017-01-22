package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.item.ObjectDataItem;
import com.spbsu.datastream.core.item.SystemTypeMeta;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Author: Artem
 * Date: 14.01.2017
 */
public class IteratorInput<T> implements Input {
  private Iterator<T> iterator;
  private Class<T> clazz;

  public IteratorInput(Iterator<T> iterator, Class<T> clazz) {
    this.iterator = iterator;
    this.clazz = clazz;
  }

  @SuppressWarnings("UnusedParameters")
  public Stream<Stream<DataItem>> stream(DataType type) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Stream<DataItem>>() {
      List<DataItem> next;

      @Override
      public boolean hasNext() {
        next = nextItems();
        return !next.isEmpty();
      }

      @Override
      public Stream<DataItem> next() {
        return next.stream();
      }
    }, Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.SORTED), false);
  }

  private List<DataItem> nextItems() {
    List<DataItem> dataItems = new ArrayList<>();
    int currentTick = -1;
    while (iterator.hasNext()) {
      T next = iterator.next();
      DataItem dataItem = new ObjectDataItem(next, clazz, new SystemTypeMeta(System.nanoTime()));
      dataItems.add(dataItem);
      if (currentTick == -1) {
        currentTick = dataItem.meta().tick();
      } else if (currentTick != dataItem.meta().tick()) {
        break;
      }
    }
    return dataItems;
  }
}