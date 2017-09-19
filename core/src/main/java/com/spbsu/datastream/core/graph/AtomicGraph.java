package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

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
