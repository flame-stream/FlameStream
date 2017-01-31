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
  private final Iterator<T> iterator;
  private final Class<T> clazz;
  private boolean hasNextStream = true;

  public IteratorInput(Iterator<T> iterator, Class<T> clazz) {
    this.iterator = iterator;
    this.clazz = clazz;
  }

  @SuppressWarnings("UnusedParameters")
  public Stream<Stream<DataItem>> stream(DataType type) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Stream<DataItem>>() {
      @Override
      public boolean hasNext() {
        return hasNextStream;
      }

      @Override
      public Stream<DataItem> next() {
        return nextInnerStream();
      }
    }, Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.SORTED), false);
  }

  private Stream<DataItem> nextInnerStream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DataItem>() {
      DataItem nextDataItem;
      boolean nextTickStart;
      int currentTick = -1;

      @Override
      public boolean hasNext() {
        if (!nextTickStart) {
          if (iterator.hasNext()) {
            T next = iterator.next();
            nextDataItem = new ObjectDataItem(next, clazz, new SystemTypeMeta(System.nanoTime()));
            if (currentTick == -1) {
              currentTick = nextDataItem.meta().tick();
            } else if (currentTick != nextDataItem.meta().tick()) {
              nextTickStart = true;
            }
            return true;
          } else {
            hasNextStream = false;
            return false;
          }
        } else {
          return false;
        }
      }

      @Override
      public DataItem next() {
        return nextDataItem;
      }
    }, Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.SORTED), false);
  }
}