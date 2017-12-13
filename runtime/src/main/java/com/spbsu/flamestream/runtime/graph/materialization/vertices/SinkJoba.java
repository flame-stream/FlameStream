package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 12.12.2017
 */
public class SinkJoba extends Joba.Stub {
  private final Consumer<DataItem> barrier;

  SinkJoba(Joba[] outJobas, Consumer<DataItem> acker, Consumer<DataItem> barrier) {
    super(outJobas, acker);
    this.barrier = barrier;
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    barrier.accept(dataItem);
    process(dataItem, Stream.of(dataItem), fromAsync);
  }
}
