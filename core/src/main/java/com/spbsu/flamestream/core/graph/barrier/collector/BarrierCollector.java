package com.spbsu.flamestream.core.graph.barrier.collector;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

public interface BarrierCollector {
  void releaseFrom(GlobalTime minTime, Consumer<DataItem<?>> consumer);

  void enqueue(DataItem<?> item);

  boolean isEmpty();
}
