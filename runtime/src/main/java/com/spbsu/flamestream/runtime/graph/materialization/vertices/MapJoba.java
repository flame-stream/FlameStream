package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba implements VertexJoba {
  private final Consumer<DataItem> sink;
  private final FlameMap<?, ?> flameMap;

  private int localTime = 0;

  public MapJoba(FlameMap<?, ?> flameMap, Consumer<DataItem> sink) {
    this.sink = sink;
    this.flameMap = flameMap;
  }

  @Override
  public void accept(DataItem dataItem) {
    flameMap.operation().apply(dataItem, localTime++).forEach(sink);
  }
}
