package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.common.Statistics;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

public interface AtomicHandle {
  void push(OutPort out, DataItem<?> result);

  void ack(long xor, GlobalTime globalTime);

  void submitStatistics(Statistics stat);

  void error(String format, Object... args);
}

