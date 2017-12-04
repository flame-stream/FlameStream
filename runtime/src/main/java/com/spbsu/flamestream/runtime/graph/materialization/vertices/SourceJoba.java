package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class SourceJoba<T> implements VertexJoba<T> {
  private final Consumer<DataItem<T>> sink;

  public SourceJoba(Consumer<DataItem<T>> sink) {
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<T> dataItem) {
    sink.accept(dataItem);
  }
}
