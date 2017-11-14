package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.atomic.AtomicGraph;

public interface Source extends AtomicGraph {
  void onNext(DataItem<?> item, SourceHandle handle);

  void onHeartbeat(GlobalTime globalTime, SourceHandle handle);
}
