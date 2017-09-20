package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * User: Artem
 * Date: 22.02.2017
 * Time: 22:29
 */
@SuppressWarnings({"TypeMayBeWeakened"})
public final class LazyGroupingState<T> implements GroupingState<T> {
  private final ToIntFunction<? super T> hash;
  private final BiPredicate<? super T, ? super T> equalz;
  private final TLongObjectMap<Object> buffers = new TLongObjectHashMap<>();

  public LazyGroupingState(ToIntFunction<? super T> hash, BiPredicate<? super T, ? super T> equalz) {
    this.hash = hash;
    this.equalz = equalz;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DataItem<T>> getGroupFor(DataItem<T> item) {
    final long hashValue = hash.applyAsInt(item.payload());
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final List<DataItem<T>> newBucket = new ArrayList<>();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        final List<List<DataItem<T>>> container = (List<List<DataItem<T>>>) list;
        return getFromContainer(item, container);
      } else {
        final List<DataItem<T>> bucket = (List<DataItem<T>>) list;
        return getFromBucket(item, bucket);
      }
    }
  }

  private List<DataItem<T>> getFromContainer(DataItem<T> item, List<List<DataItem<T>>> container) {
    final List<DataItem<T>> result = searchBucket(item, container);
    if (result.isEmpty()) {
      container.add(result);
      return result;
    } else {
      return result;
    }
  }

  private List<DataItem<T>> getFromBucket(DataItem<T> item, List<DataItem<T>> bucket) {
    if (equalz.test(bucket.get(0).payload(), item.payload())) {
      return bucket;
    } else {
      final List<List<DataItem<T>>> container = new ArrayList<>();
      container.add(bucket);
      final List<DataItem<T>> newList = new ArrayList<>();
      container.add(newList);
      buffers.put(hash.applyAsInt(item.payload()), container);
      return newList;
    }
  }

  @Override
  public void forEach(Consumer<List<DataItem<T>>> consumer) {
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

  private List<DataItem<T>> searchBucket(DataItem<T> item, List<List<DataItem<T>>> container) {
    return container.stream()
            .filter(bucket -> equalz.test(bucket.get(0).payload(), item.payload()))
            .findAny()
            .orElse(new ArrayList<>());
  }
}
