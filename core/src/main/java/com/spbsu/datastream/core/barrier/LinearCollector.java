package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.graph.ops.Grouping;

import java.util.*;
import java.util.function.Consumer;

public final class LinearCollector implements BarrierCollector {
  private final SortedMap<GlobalTime, List<DataItem<Object>>> invalidationPool = new TreeMap<>();
  private final Queue<DataItem<?>> released = new ArrayDeque<>();

  @Override
  public void update(GlobalTime minTime) {
    final SortedMap<GlobalTime, List<DataItem<Object>>> headMap = this.invalidationPool.headMap(minTime);
    headMap.values().stream().flatMap(List::stream).forEach(this.released::add);
    headMap.clear();
  }

  @Override
  public void enqueue(DataItem<?> item) {
    //noinspection unchecked
    final DataItem<Object> dataItem = (DataItem<Object>) item;
    this.invalidationPool.compute(item.meta().globalTime(), (globalTime, dataItems) -> {
      if (dataItems == null) {
        dataItems = new ArrayList<>();
      }
      Grouping.insert(dataItems, dataItem);
      return dataItems;
      /*oldList.removeIf(di -> di.meta().trace().isInvalidatedBy(item.meta().trace()));
      if (oldList.stream().noneMatch(di -> item.meta().trace().isInvalidatedBy(di.meta().trace()))) {
        oldList.add(item);
      }
      return oldList;*/
    });
  }

  @Override
  public void release(Consumer<DataItem<?>> consumer) {
    this.released.forEach(consumer);
    this.released.clear();
  }

  @Override
  public boolean isEmpty() {
    return this.invalidationPool.isEmpty() && this.released.isEmpty();
  }

  @Override
  public String toString() {
    return "LinearCollector{" + "invalidationPool=" + this.invalidationPool +
            ", released=" + this.released +
            '}';
  }
}
