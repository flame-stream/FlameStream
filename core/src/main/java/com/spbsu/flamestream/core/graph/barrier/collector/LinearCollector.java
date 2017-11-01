package com.spbsu.flamestream.core.graph.barrier.collector;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.invalidation.InvalidatingList;
import com.spbsu.flamestream.core.graph.invalidation.impl.ArrayInvalidatingList;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

public final class LinearCollector implements BarrierCollector {
  private final SortedMap<GlobalTime, InvalidatingList<Object>> invalidationPool = new TreeMap<>();

  @Override
  public void releaseFrom(GlobalTime minTime, Consumer<DataItem<?>> consumer) {
    if (!invalidationPool.isEmpty()) {
      final SortedMap<GlobalTime, InvalidatingList<Object>> headMap = invalidationPool.headMap(minTime);
      headMap.values().stream().flatMap(List::stream).forEach(consumer::accept);
      headMap.clear();
    }
  }

  @Override
  public void enqueue(DataItem<?> item) {
    //noinspection unchecked
    final DataItem<Object> dataItem = (DataItem<Object>) item;
    invalidationPool.compute(item.meta().globalTime(), (globalTime, dataItems) -> {
      final InvalidatingList<Object> items = dataItems == null ? new ArrayInvalidatingList<>() : dataItems;
      items.insert(dataItem);
      return items;
    });
  }

  @Override
  public boolean isEmpty() {
    return invalidationPool.isEmpty();
  }
}
