package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Optional;

/**
 * AtomicGraph - graph without inner down or upstreams
 */
public interface AtomicGraph extends Graph {
  default void onStart(final AtomicHandle handle) {
  }

  default void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
  }

  default Optional<?> onCommit(final AtomicHandle handle) {
    return null;
  }

  default void onRecover(final Object state, final AtomicHandle handle) {
  }

  default void onMinGTimeUpdate(final Meta meta) {

  }
}
