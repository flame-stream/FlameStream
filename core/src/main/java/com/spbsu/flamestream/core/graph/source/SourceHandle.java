package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public interface SourceHandle extends AtomicHandle {
  void heartbeat(GlobalTime time);

  void accept(GlobalTime globalTime);
}
