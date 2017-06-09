package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.Trace;
import com.spbsu.datastream.core.graph.ops.Grouping;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

public final class LinearCollector implements BarrierCollector {
  private final SortedMap<GlobalTime, List<DataItem<?>>> invalidationPool = new TreeMap<>();

  private final Queue<DataItem<?>> released = new ArrayDeque<>();

  @Override
  public void update(GlobalTime minTime) {
    this.invalidationPool.headMap(minTime)
            .values().stream().flatMap(List::stream).forEach(this.released::add);
    this.invalidationPool.headMap(minTime).clear();
  }

  @Override
  public void enqueue(DataItem<?> item) {
    this.invalidationPool.putIfAbsent(item.meta().globalTime(), new ArrayList<>());

    this.invalidationPool.computeIfPresent(item.meta().globalTime(), (key, oldList) -> {
      oldList.removeIf(di -> Trace.INVALIDATION_COMPARATOR.compare(item.meta().trace(), di.meta().trace()) > 0);
      if (oldList.stream().noneMatch(di -> Trace.INVALIDATION_COMPARATOR.compare(item.meta().trace(), di.meta().trace()) < 0)) {
        oldList.add(item);
      }
      return oldList;
    });
  }

  @Override
  public void release(Consumer<DataItem<?>> consumer) {
    this.released.forEach(consumer);
    this.released.clear();
  }
}
