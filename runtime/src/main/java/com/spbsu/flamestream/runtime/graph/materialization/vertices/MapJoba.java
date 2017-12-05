package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba implements VertexJoba {
  private final FlameMap<?, ?> map;
  private final Consumer<DataItem> sink;

  public MapJoba(FlameMap<?, ?> map, Consumer<DataItem> sink) {
    this.map = map;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem dataItem) {
    map.operation().apply(dataItem).forEach(sink);
  }
}
