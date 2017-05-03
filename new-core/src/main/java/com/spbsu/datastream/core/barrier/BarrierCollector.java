package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;

import java.util.function.Consumer;

public interface BarrierCollector {
  void update(GlobalTime maxTime);

  void enqueue(DataItem<?> item);

  void release(Consumer<Object> consumer);
}
