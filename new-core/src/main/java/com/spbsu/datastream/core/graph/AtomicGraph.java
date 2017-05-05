package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.graph.ops.GroupingState;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

/**
 * AtomicGraph - graph without inner down or upstreams
 */
public interface AtomicGraph extends Graph {
  default void onStart(final AtomicHandle handle) {
  }

  default void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
  }

  default void onCommit(final AtomicHandle handle) {
  }

  default void onRecover(final GroupingState<?> state, final AtomicHandle handle) {
  }

  default void onMinGTimeUpdate(final GlobalTime globalTime, final AtomicHandle handle) {
  }
}
