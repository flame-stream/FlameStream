package com.spbsu.flamestream.runtime.graph.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface VertexJoba<T> extends Consumer<DataItem<T>>, AutoCloseable {
  void onMinTime(GlobalTime globalTime);

  void onCommit();

  @Override
  default void close() throws Exception {
  }
}
