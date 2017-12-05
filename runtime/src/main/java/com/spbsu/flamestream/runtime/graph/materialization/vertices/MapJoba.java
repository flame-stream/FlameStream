package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba<T, R> extends VertexJoba.SyncStub<T> {
  private final FlameMap<T, R> map;
  private final Consumer<DataItem<R>> sink;

  public MapJoba(FlameMap<T, R> map, Consumer<DataItem<R>> sink) {
    this.map = map;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<T> dataItem) {
    map.operation().apply(dataItem).forEach(sink);
  }
}
