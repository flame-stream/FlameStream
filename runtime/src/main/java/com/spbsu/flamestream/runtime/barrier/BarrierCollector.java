package com.spbsu.flamestream.runtime.barrier;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

class BarrierCollector {
  private final SortedMap<GlobalTime, InvalidatingBucket> invalidationPool = new TreeMap<>();

  public void releaseFrom(GlobalTime minTime, Consumer<DataItem<?>> consumer) {
    if (!invalidationPool.isEmpty()) {
      final SortedMap<GlobalTime, InvalidatingBucket> headMap = invalidationPool.headMap(minTime);
      headMap.values().stream().flatMap(InvalidatingBucket::stream).forEach(consumer::accept);
      headMap.clear();
    }
  }

  public void enqueue(DataItem<?> item) {
    //noinspection unchecked
    final DataItem<Object> dataItem = (DataItem<Object>) item;
    invalidationPool.compute(item.meta().globalTime(), (globalTime, dataItems) -> {
      final InvalidatingBucket items = dataItems == null ? new ArrayInvalidatingBucket() : dataItems;
      items.insert(dataItem);
      return items;
    });
  }

  public boolean isEmpty() {
    return invalidationPool.isEmpty();
  }
}
