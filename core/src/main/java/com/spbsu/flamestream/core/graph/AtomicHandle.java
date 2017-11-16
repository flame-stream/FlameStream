package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.stat.Statistics;

public interface AtomicHandle {
  void push(OutPort out, DataItem<?> result);

  void ack(long xor, GlobalTime globalTime);

  void submitStatistics(Statistics stat);

  void error(String format, Object... args);
}

