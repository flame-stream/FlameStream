package com.spbsu.flamestream.core.front;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.source.SourceHandle;

public interface Front<T> {
  void subscribe(SourceHandle<T> sourceHandle, GlobalTime from, GlobalTime to);
}