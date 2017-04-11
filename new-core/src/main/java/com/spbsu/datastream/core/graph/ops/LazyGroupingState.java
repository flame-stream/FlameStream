package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 22.02.2017
 * Time: 22:29
 */
public class LazyGroupingState<T> implements GroupingState<T> {
  private final HashFunction<T> hash;
  private final TLongObjectHashMap<Object> buffers = new TLongObjectHashMap<>();

  public LazyGroupingState(final HashFunction<T> hash) {
    this.hash = hash;
  }

  public Optional<List<DataItem<T>>> get(final DataItem<T> item) {
    final long hashValue = hash.hash(item.payload());
    final Object obj = buffers.get(hashValue);
    if (obj == null)
      return Optional.empty();

    final List<?> list = (List<?>) obj;
    if (list.get(0) instanceof List) {
      //noinspection unchecked
      final List<List<DataItem<T>>> container = (List<List<DataItem<T>>>) list;
      return searchBucket(item, container);
    } else {
      //noinspection unchecked
      return Optional.of((List<DataItem<T>>) list);
    }
  }

  public void put(final List<DataItem<T>> dataItems) {
    if (dataItems.isEmpty()) {
      throw new IllegalArgumentException("List of data items is empty");
    }

    final DataItem<T> dataItem = dataItems.get(0);
    final long hashValue = hash.hash(dataItem.payload());
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      buffers.put(hashValue, dataItems);
    } else {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        //noinspection unchecked
        final List<List<DataItem<T>>> buckets = (List<List<DataItem<T>>>) list;
        final List<DataItem<T>> group = searchBucket(dataItem, buckets).orElse(null);
        if (group != null) {
          group.clear();
          group.addAll(dataItems);
        } else {
          buckets.add(dataItems);
        }
      } else {
        //noinspection unchecked
        final List<DataItem<T>> group = (List<DataItem<T>>) list;
        if (hash.equal(group.get(0).payload(), dataItem.payload())) {
          group.clear();
          group.addAll(dataItems);
        } else {
          final List<List<DataItem<T>>> buckets = new ArrayList<>();
          buckets.add(group);
          buckets.add(dataItems);
          buffers.put(hashValue, buckets);
        }
      }
    }
  }

  public void forEach(final Consumer<List<DataItem<T>>> consumer) {
    buffers.forEachValue(obj -> {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        //noinspection unchecked
        final List<List<DataItem<T>>> buckets = (List<List<DataItem<T>>>) list;
        buckets.forEach(consumer);
      } else {
        //noinspection unchecked
        consumer.accept((List<DataItem<T>>) list);
      }
      return true;
    });
  }

  private Optional<List<DataItem<T>>> searchBucket(final DataItem<T> item, final List<List<DataItem<T>>> container) {
    return Stream.of(container)
            .flatMap(Collection::stream)
            .filter(bucket -> hash.equal(bucket.get(0).payload(), item.payload())).findAny();
  }
}
