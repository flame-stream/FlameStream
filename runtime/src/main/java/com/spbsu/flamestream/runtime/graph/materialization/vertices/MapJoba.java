package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba implements VertexJoba {
  private final Consumer<DataItem> sink;
  private final Function<DataItem, Stream<DataItem>> operation;

  public MapJoba(FlameMap<?, ?> map, Consumer<DataItem> sink) {
    this.sink = sink;
    this.operation = map.operation();
  }

  @Override
  public void accept(DataItem dataItem) {
    operation.apply(dataItem).forEach(sink);
  }
}
