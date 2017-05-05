package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;

import java.util.function.Consumer;

interface BarrierCollector {
  void update(GlobalTime minTime);

  void enqueue(DataItem<?> item);

  void release(Consumer<DataItem<?>> consumer);
}
