package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

/**
 * AtomicGraph - graph without inner down or upstreams
 */
public interface AtomicGraph extends Graph {
  default void onStart(final AtomicHandle handle) {
  }

  default void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
  }
}
