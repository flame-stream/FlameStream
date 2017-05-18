package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import gnu.trove.map.TLongObjectMap;
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
@SuppressWarnings({"TypeMayBeWeakened", "OptionalContainsCollection"})
public final class LazyGroupingState<T> implements GroupingState<T> {
  private final HashFunction<? super T> hash;
  private final TLongObjectMap<Object> buffers = new TLongObjectHashMap<>();

  public LazyGroupingState(HashFunction<? super T> hash) {
    this.hash = hash;
  }

  @Override
  public Optional<List<DataItem<T>>> get(DataItem<T> item) {
    final long hashValue = this.hash.hash(item.payload());
    final Object obj = this.buffers.get(hashValue);
    if (obj == null)
      return Optional.empty();

    final List<?> list = (List<?>) obj;
    if (list.get(0) instanceof List) {
      //noinspection unchecked
      final List<List<DataItem<T>>> container = (List<List<DataItem<T>>>) list;
      return this.searchBucket(item, container);
    } else {
      //noinspection unchecked
      return Optional.of((List<DataItem<T>>) list);
    }
  }

  @Override
  public void put(List<DataItem<T>> dataItems) {
    if (dataItems.isEmpty()) {
      throw new IllegalArgumentException("List of data items is empty");
    }

    final DataItem<T> dataItem = dataItems.get(0);
    final long hashValue = this.hash.hash(dataItem.payload());
    final Object obj = this.buffers.get(hashValue);
    if (obj == null) {
      this.buffers.put(hashValue, dataItems);
    } else {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        //noinspection unchecked
        final List<List<DataItem<T>>> buckets = (List<List<DataItem<T>>>) list;
        final List<DataItem<T>> group = this.searchBucket(dataItem, buckets).orElse(null);
        if (group != null) {
          group.clear();
          group.addAll(dataItems);
        } else {
          buckets.add(dataItems);
        }
      } else {
        //noinspection unchecked
        final List<DataItem<T>> group = (List<DataItem<T>>) list;
        if (this.hash.equal(group.get(0).payload(), dataItem.payload())) {
          group.clear();
          group.addAll(dataItems);
        } else {
          final List<List<DataItem<T>>> buckets = new ArrayList<>();
          buckets.add(group);
          buckets.add(dataItems);
          this.buffers.put(hashValue, buckets);
        }
      }
    }
  }

  @Override
  public void forEach(Consumer<List<DataItem<T>>> consumer) {
    this.buffers.forEachValue(obj -> {
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

  private Optional<List<DataItem<T>>> searchBucket(DataItem<T> item, List<List<DataItem<T>>> container) {
    return Stream.of(container)
            .flatMap(Collection::stream)
            .filter(bucket -> this.hash.equal(bucket.get(0).payload(), item.payload())).findAny();
  }
}
