package com.spbsu.datastream.core.graph.ops.grouping_storage;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.ops.Hash;
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
public class LazyGroupingStorage implements GroupingStorage {
  private final Hash grouping;
  private final TLongObjectHashMap<Object> buffers = new TLongObjectHashMap<>();

  public LazyGroupingStorage(final Hash grouping) {
    this.grouping = grouping;
  }

  public Optional<List<DataItem>> get(DataItem item) {
    final long hash = grouping.hash(item);
    final Object obj = buffers.get(hash);
    if (obj == null)
      return Optional.empty();

    final List<?> list = (List<?>) obj;
    if (list.get(0) instanceof List) {
      //noinspection unchecked
      final List<List<DataItem>> container = (List<List<DataItem>>) list;
      return searchBucket(item, container);
    } else {
      //noinspection unchecked
      return Optional.of((List<DataItem>) list);
    }
  }

  public void put(List<DataItem> dataItems) {
    if (dataItems.isEmpty()) {
      throw new IllegalArgumentException("List of data items is empty");
    }

    final DataItem dataItem = dataItems.get(0);
    final long hash = grouping.hash(dataItem);
    final Object obj = buffers.get(hash);
    if (obj == null) {
      buffers.put(hash, dataItems);
    } else {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        //noinspection unchecked
        final List<List<DataItem>> buckets = (List<List<DataItem>>) list;
        final List<DataItem> group = searchBucket(dataItem, buckets).orElse(null);
        if (group != null) {
          group.clear();
          group.addAll(dataItems);
        } else {
          buckets.add(dataItems);
        }
      } else {
        //noinspection unchecked
        final List<DataItem> group = (List<DataItem>) list;
        if (grouping.equals(group.get(0), dataItem)) {
          group.clear();
          group.addAll(dataItems);
        } else {
          final List<List<DataItem>> buckets = new ArrayList<>();
          buckets.add(group);
          buckets.add(dataItems);
          buffers.put(hash, buckets);
        }
      }
    }
  }

  public void forEach(Consumer<List<DataItem>> consumer) {
    buffers.forEachValue(obj -> {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        //noinspection unchecked
        final List<List<DataItem>> buckets = (List<List<DataItem>>) list;
        buckets.forEach(consumer);
      } else {
        //noinspection unchecked
        consumer.accept((List<DataItem>) list);
      }
      return true;
    });
  }

  private Optional<List<DataItem>> searchBucket(DataItem item, List<List<DataItem>> container) {
    return Stream.of(container)
            .flatMap(Collection::stream)
            .filter(bucket -> grouping.equals(bucket.get(0), item)).findAny();
  }
}
