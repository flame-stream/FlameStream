package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.meta.GlobalTime;

import java.util.function.Consumer;

interface BarrierCollector {
  void releaseFrom(GlobalTime minTime, Consumer<DataItem<?>> consumer);

  void enqueue(DataItem<?> item);

  boolean isEmpty();
}
