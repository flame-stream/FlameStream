package com.spbsu.flamestream.core.barrier;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.meta.GlobalTime;

import java.util.function.Consumer;

interface BarrierCollector {
  void releaseFrom(GlobalTime minTime, Consumer<DataItem<?>> consumer);

  void enqueue(DataItem<?> item);

  boolean isEmpty();
}
