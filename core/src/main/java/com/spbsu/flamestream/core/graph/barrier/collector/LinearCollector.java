package com.spbsu.flamestream.core.graph.barrier.collector;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.ops.Grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

public final class LinearCollector implements BarrierCollector {
  private final SortedMap<GlobalTime, List<DataItem<Object>>> invalidationPool = new TreeMap<>();

  @Override
  public void releaseFrom(GlobalTime minTime, Consumer<DataItem<?>> consumer) {
    final SortedMap<GlobalTime, List<DataItem<Object>>> headMap = invalidationPool.headMap(minTime);
    headMap.values().stream().flatMap(List::stream).forEach(consumer::accept);
    headMap.clear();
  }

  @Override
  public void enqueue(DataItem<?> item) {
    //noinspection unchecked
    final DataItem<Object> dataItem = (DataItem<Object>) item;
    invalidationPool.compute(item.meta().globalTime(), (globalTime, dataItems) -> {
      if (dataItems == null) {
        dataItems = new ArrayList<>();
      }
      Grouping.insert(dataItems, dataItem);
      return dataItems;
    });
  }

  @Override
  public boolean isEmpty() {
    return invalidationPool.isEmpty();
  }
}
