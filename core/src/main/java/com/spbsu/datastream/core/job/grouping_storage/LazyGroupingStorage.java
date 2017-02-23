package com.spbsu.datastream.core.job.grouping_storage;

import com.spbsu.datastream.core.DataItem;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 22.02.2017
 * Time: 22:29
 */
public class LazyGroupingStorage implements GroupingStorage {
  private final DataItem.Grouping grouping;
  private final TLongObjectHashMap<Object> buffers = new TLongObjectHashMap<>();

  public LazyGroupingStorage(DataItem.Grouping grouping) {
    this.grouping = grouping;
  }

  public Optional<List<DataItem>> get(long hash, DataItem item) {
    if (buffers.get(hash) == null)
      return Optional.empty();

    final List<?> list = (List<?>) buffers.get(hash);
    if (list.getClass().getGenericSuperclass() instanceof List) {
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
    if (buffers.get(hash) == null) {
      buffers.put(hash, dataItems);
    } else {
      final List<?> list = (List<?>) buffers.get(hash);
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

  public void forEach(TLongObjectProcedure<List<DataItem>> procedure) {
    buffers.forEachEntry((hash, obj) -> {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        //noinspection unchecked
        final List<List<DataItem>> buckets = (List<List<DataItem>>) list;
        buckets.forEach(dataItems -> procedure.execute(hash, dataItems));
      } else {
        //noinspection unchecked
        procedure.execute(hash, (List<DataItem>) list);
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
