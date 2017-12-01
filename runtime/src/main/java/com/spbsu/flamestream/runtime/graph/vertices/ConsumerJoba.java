package com.spbsu.flamestream.runtime.graph.vertices;

import com.spbsu.flamestream.core.DataItem;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class ConsumerJoba<T> extends VertexJoba.Stub<T> {
  private final Consumer<DataItem<T>> barrier;

  public ConsumerJoba(Consumer<DataItem<T>> barrier) {
    this.barrier = barrier;
  }

  @Override
  public void accept(DataItem<T> dataItem) {
    barrier.accept(dataItem);
  }
}
