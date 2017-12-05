package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface VertexJoba<T> extends Consumer<DataItem<T>>, AutoCloseable {
  boolean isAsync();

  default void onMinTime(GlobalTime globalTime) {
  }

  default void onCommit() {
  }

  @Override
  default void close() {
  }

  abstract class SyncStub<T> implements VertexJoba<T> {
    @Override
    public boolean isAsync() {
      return false;
    }
  }
}
