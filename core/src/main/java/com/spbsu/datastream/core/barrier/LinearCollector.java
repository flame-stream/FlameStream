package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.graph.ops.Grouping;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

public final class LinearCollector implements BarrierCollector {
  private final SortedMap<GlobalTime, List<DataItem<Object>>> invalidationPool = new TreeMap<>();

  private final Queue<DataItem<?>> released = new ArrayDeque<>();

  @Override
  public void update(GlobalTime minTime) {
    this.invalidationPool.headMap(minTime)
            .values().stream().flatMap(List::stream).forEach(this.released::add);
    this.invalidationPool.headMap(minTime).clear();
  }

  @Override
  public void enqueue(DataItem<?> item) {
    //noinspection unchecked
    final DataItem<Object> dataItem = (DataItem<Object>) item;
    this.invalidationPool.putIfAbsent(item.meta().globalTime(), new ArrayList<>());

    this.invalidationPool.computeIfPresent(item.meta().globalTime(), (key, oldList) -> {
      // TODO: 15.09.2017 refactor this shit
      Grouping.insert(oldList, dataItem);
      return oldList;
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
