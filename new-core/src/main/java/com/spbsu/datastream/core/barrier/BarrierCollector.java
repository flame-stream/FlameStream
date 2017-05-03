package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;

import java.util.List;

public interface BarrierCollector {
  void update(GlobalTime maxTime);

  void enqueue(DataItem<?> item);

  List<?> released();
}
