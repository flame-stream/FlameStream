package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * AtomicGraph - graph without inner down or upstreams
 */
public interface AtomicGraph extends Graph {
  default void onStart(AtomicHandle handle) {
  }

  default void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
  }

  default void onCommit(AtomicHandle handle) {
  }

  default void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
  }
}
