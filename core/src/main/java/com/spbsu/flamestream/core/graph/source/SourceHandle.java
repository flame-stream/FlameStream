package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;

public interface SourceHandle extends AtomicHandle {
  void heartbeat(GlobalTime time);

  void accept(GlobalTime globalTime);
}
